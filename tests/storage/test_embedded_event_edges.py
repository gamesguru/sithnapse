#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Tests for event DAG adjacency in mtxdb's CSR store."""

import atexit
import glob
import importlib.util
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any
from unittest import TestCase, mock
from uuid import uuid4

from prometheus_client import REGISTRY

from twisted.test.proto_helpers import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.storage.databases.main import (
    embedded_event_edges as embedded_event_edges_module,
)
from synapse.storage.databases.main.embedded_common import Pool
from synapse.storage.databases.main.embedded_event_edges import (
    event_edge_publish_signal_gauge,
    get_event_edges_backward_batch,
    get_event_edges_forward_batch,
    purge_event_edges_room,
    put_event_edges_batch,
    record_event_edge_read,
    register_publish_signal_gauge,
)
from synapse.synapse_rust import mtxdb_engine
from synapse.util.clock import Clock

from tests.test_utils.event_injection import create_event
from tests.unittest import HomeserverTestCase
from tests.utils import EMBEDDED_DB_ENGINE, EMBEDDED_DB_PATH

REBUILD_SCRIPT = (
    Path(__file__).parents[2] / "scripts-dev" / "mtxdb" / "mtxdb_edges_rebuild.py"
)


def _load_rebuild_edges() -> Any:
    """Load the offline rebuild script on demand.

    It lives in `scripts-dev/`, which some test trees (the Debian package
    build) do not ship, so importing this module must not depend on it.
    """
    spec = importlib.util.spec_from_file_location("mtxdb_edges_rebuild", REBUILD_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.rebuild_edges


if EMBEDDED_DB_ENGINE and EMBEDDED_DB_PATH:
    # The test harness has already selected a worker-specific store. Reuse
    # that path instead of opening a private one per test class.
    _TEST_ENGINE_TMPDIR = EMBEDDED_DB_PATH
else:
    _TEST_ENGINE_TMPDIR = tempfile.mkdtemp(prefix="test-embedded-event-edges-")
    # The client is closed between tests, so this root must outlive the
    # module and be removed at process exit.
    atexit.register(shutil.rmtree, _TEST_ENGINE_TMPDIR, ignore_errors=True)


def open_test_engine() -> None:
    """Open this module's fixed root, closing whatever root was open before.

    The client is process-global and every suite now closes it after each test
    (so each test homeserver can open its own root), so a module-level open
    would not survive to the tests that need it.
    """
    mtxdb_engine.close_client()
    mtxdb_engine.open_client(_TEST_ENGINE_TMPDIR)


class EmbeddedAdjacencyTestCase(TestCase):
    """Pure unit tests for the adjacency FFI wrappers.

    Every test uses a fresh room id and a fresh event-id prefix: adjacency is
    scoped by the room's event collection id, and an event id has exactly one
    room locator, so both must be unique to keep the process-global store
    isolated between tests without a namespace.
    """

    def setUp(self) -> None:
        open_test_engine()
        self.room_id = f"!{uuid4().hex}:example.org"
        self._event_prefix = uuid4().hex

    def _eid(self, name: str) -> str:
        return f"${self._event_prefix}-{name}"

    def tearDown(self) -> None:
        mtxdb_engine.close_client()

    def test_put_get_backward_and_forward(self) -> None:
        # No `event_json_put` first: `adjacency_put` publishes the locators for
        # every event it touches, so the repair/backfill case still resolves.
        put_event_edges_batch(
            [
                (self.room_id, self._eid("e1"), self._eid("p1")),
                (self.room_id, self._eid("e1"), self._eid("p2")),
            ]
        )

        backward = get_event_edges_backward_batch(
            [self._eid("e1"), self._eid("nonexistent")]
        )
        self.assertEqual(backward[self._eid("e1")], [self._eid("p1"), self._eid("p2")])
        self.assertNotIn(self._eid("nonexistent"), backward)

        forward = get_event_edges_forward_batch(
            [self._eid("p1"), self._eid("p2"), self._eid("nonexistent")]
        )
        self.assertEqual(forward[self._eid("p1")], [self._eid("e1")])
        self.assertEqual(forward[self._eid("p2")], [self._eid("e1")])
        self.assertIsNone(forward[self._eid("nonexistent")])

        put_event_edges_batch([(self.room_id, self._eid("e2"), self._eid("p1"))])
        forward2 = get_event_edges_forward_batch([self._eid("p1")])
        self.assertCountEqual(
            forward2[self._eid("p1")] or [], [self._eid("e1"), self._eid("e2")]
        )

    def test_event_without_prev_has_an_empty_row(self) -> None:
        put_event_edges_batch([(self.room_id, self._eid("create"), None)])

        self.assertEqual(
            get_event_edges_backward_batch([self._eid("create")])[self._eid("create")],
            [],
        )
        self.assertIsNone(
            get_event_edges_forward_batch([self._eid("create")])[self._eid("create")]
        )

    def test_rooms_do_not_leak(self) -> None:
        # Event ids are globally unique and belong to one room; the locator
        # maps an id to exactly one room collection. Two rooms therefore use
        # distinct parents here, and each read must stay inside its own scope.
        other_room = f"!{uuid4().hex}:example.org"
        put_event_edges_batch([(self.room_id, self._eid("a"), self._eid("parent_a"))])
        put_event_edges_batch([(other_room, self._eid("b"), self._eid("parent_b"))])

        self.assertEqual(
            get_event_edges_backward_batch([self._eid("a")])[self._eid("a")],
            [self._eid("parent_a")],
        )
        self.assertEqual(
            get_event_edges_backward_batch([self._eid("b")])[self._eid("b")],
            [self._eid("parent_b")],
        )
        self.assertEqual(
            get_event_edges_forward_batch([self._eid("parent_a")])[
                self._eid("parent_a")
            ],
            [self._eid("a")],
        )
        self.assertEqual(
            get_event_edges_forward_batch([self._eid("parent_b")])[
                self._eid("parent_b")
            ],
            [self._eid("b")],
        )

    def test_cross_room_parent_does_not_overwrite_its_locator(self) -> None:
        other_room = f"!{uuid4().hex}:example.org"
        put_event_edges_batch([(other_room, self._eid("shared_parent"), None)])

        # SQL kept malformed cross-room prev edges, but room-scoped traversal
        # filtered them out. The CSR mirror should preserve that behavior by
        # dropping the foreign reference while still storing the child row.
        put_event_edges_batch(
            [(self.room_id, self._eid("child"), self._eid("shared_parent"))]
        )

        self.assertEqual(
            get_event_edges_backward_batch([self._eid("child")])[self._eid("child")],
            [],
        )
        self.assertEqual(
            get_event_edges_backward_batch([self._eid("shared_parent")])[
                self._eid("shared_parent")
            ],
            [],
        )
        self.assertIsNone(
            get_event_edges_forward_batch([self._eid("shared_parent")])[
                self._eid("shared_parent")
            ]
        )

    def test_sync_barrier_covers_event_dag_and_locator_pools(self) -> None:
        with mock.patch.object(embedded_event_edges_module, "sync_now") as sync_now:
            put_event_edges_batch(
                [(self.room_id, self._eid("synced"), None)], sync=True
            )

        sync_now.assert_called_once_with(pools=[Pool.EVENT_DAG, Pool.STATE])

    def test_purge_room_drops_the_whole_scope(self) -> None:
        put_event_edges_batch(
            [
                (self.room_id, self._eid("e1"), self._eid("p1")),
                (self.room_id, self._eid("e2"), self._eid("e1")),
            ]
        )
        self.assertIn(
            self._eid("e1"), get_event_edges_backward_batch([self._eid("e1")])
        )

        purge_event_edges_room(self.room_id)

        self.assertNotIn(
            self._eid("e1"), get_event_edges_backward_batch([self._eid("e1")])
        )
        self.assertNotIn(
            self._eid("e2"), get_event_edges_backward_batch([self._eid("e2")])
        )
        self.assertIsNone(
            get_event_edges_forward_batch([self._eid("p1")])[self._eid("p1")]
        )

    def test_write_is_immediate_even_with_a_transaction(self) -> None:
        """The CSR append does not join the persist transaction.

        Adjacency is written before the SQL COMMIT, so a committed event always
        has it; a crash can only leave adjacency for an uncommitted event,
        which readers filter against `events`. The `transaction` argument is
        accepted for call-site compatibility and has no effect.
        """
        transaction = mtxdb_engine.begin_transaction()
        if transaction is None:
            self.skipTest("mtxdb shared WAL is not enabled")

        try:
            put_event_edges_batch(
                [(self.room_id, self._eid("staged"), self._eid("parent"))],
                transaction=transaction,
            )
            self.assertEqual(
                get_event_edges_backward_batch([self._eid("staged")])[
                    self._eid("staged")
                ],
                [self._eid("parent")],
            )
        finally:
            transaction.abort()

    def test_edges_lost_with_the_unsynced_tail_are_rebuilt_on_open(self) -> None:
        """A crash that keeps the WAL but loses the CSR tail loses no edges.

        Every edge write is journaled in the edge log, which commits with the
        WAL; opening the store replays whatever the tail lost before anything
        is served.
        """
        probe = mtxdb_engine.begin_transaction()
        if probe is None:
            self.skipTest("mtxdb shared WAL is not enabled")
        probe.abort()
        # A root owned by this test, so removing tails touches no other data.
        root = tempfile.mkdtemp(prefix="test-edge-log-recovery-")
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        mtxdb_engine.close_client()
        mtxdb_engine.open_client(root)

        put_event_edges_batch(
            [(self.room_id, self._eid("r1"), self._eid("rp"))], durable=False
        )
        # The WAL (and the edge log in it) is durable; the tail is not synced.
        mtxdb_engine.wait_durable(mtxdb_engine.request_durable())
        mtxdb_engine.close_client(durable=False)
        tails = glob.glob(os.path.join(root, "**", "tail.log"), recursive=True)
        self.assertTrue(tails)
        for tail in tails:
            os.remove(tail)

        mtxdb_engine.open_client(root)
        self.assertEqual(
            get_event_edges_backward_batch([self._eid("r1")])[self._eid("r1")],
            [self._eid("rp")],
        )
        self.assertEqual(
            get_event_edges_forward_batch([self._eid("rp")])[self._eid("rp")],
            [self._eid("r1")],
        )

    def test_purged_room_edges_are_not_replayed_on_open(self) -> None:
        """A purge prunes the edge log, so recovery cannot bring the room back."""
        put_event_edges_batch(
            [(self.room_id, self._eid("p1"), self._eid("pp"))], durable=False
        )
        mtxdb_engine.wait_durable(mtxdb_engine.request_durable())
        purge_event_edges_room(self.room_id)
        mtxdb_engine.wait_durable(mtxdb_engine.request_durable())
        mtxdb_engine.close_client(durable=False)

        open_test_engine()
        self.assertNotIn(
            self._eid("p1"), get_event_edges_backward_batch([self._eid("p1")])
        )
        self.assertIsNone(
            get_event_edges_forward_batch([self._eid("pp")]).get(self._eid("pp"))
        )

    def test_deferred_edges_survive_flush_and_reopen(self) -> None:
        """Interval mode appends without an fsync; the flush makes it durable."""
        put_event_edges_batch(
            [(self.room_id, self._eid("d1"), self._eid("dp"))], durable=False
        )
        mtxdb_engine.flush_adjacency_tails()

        # Reopen the same root: the edges must come back from the tail/segments.
        open_test_engine()
        self.assertEqual(
            get_event_edges_backward_batch([self._eid("d1")])[self._eid("d1")],
            [self._eid("dp")],
        )
        self.assertEqual(
            get_event_edges_forward_batch([self._eid("dp")])[self._eid("dp")],
            [self._eid("d1")],
        )


def _sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def _reads(server: str, path: str, outcome: str) -> float:
    return _sample(
        "synapse_event_edges_reads_total",
        path=path,
        outcome=outcome,
        server_name=server,
    )


class EventEdgeReaderMetricsTestCase(TestCase):
    """The always-on Prometheus metrics the SQL-read-removal gate depends on."""

    def setUp(self) -> None:
        open_test_engine()

    def tearDown(self) -> None:
        mtxdb_engine.close_client()

    def test_record_event_edge_read_counts_without_ffi_diagnostics(self) -> None:
        server = "metrics-" + uuid4().hex
        record_event_edge_read(server, "successor", "hit")
        record_event_edge_read(server, "successor", "sql_only")
        record_event_edge_read(server, "gap", "miss")
        self.assertEqual(_reads(server, "successor", "hit"), 1)
        self.assertEqual(_reads(server, "successor", "sql_only"), 1)
        self.assertEqual(_reads(server, "gap", "miss"), 1)

    def test_publish_signal_gauge_reports_each_pool(self) -> None:
        hs = mock.Mock()
        hs.hostname = "gauge-" + uuid4().hex
        hs.get_instance_id.return_value = uuid4().hex
        fake = mock.Mock()
        fake.publish_signal_active.return_value = {
            "state": True,
            "event_dag": False,
        }
        with mock.patch(
            "synapse.storage.databases.embedded_engine.get_embedded_engine",
            return_value=fake,
        ):
            register_publish_signal_gauge(hs)
            try:
                for pool, expected in (
                    ("state", 1.0),
                    ("event_dag", 0.0),
                ):
                    self.assertEqual(
                        REGISTRY.get_sample_value(
                            "synapse_event_edges_publish_signal_active",
                            {"pool": pool, "server_name": hs.hostname},
                        ),
                        expected,
                    )
            finally:
                event_edge_publish_signal_gauge.unregister_hooks_for_homeserver_instance_id(
                    hs.get_instance_id()
                )

    def test_publish_signal_gauge_handles_older_extension(self) -> None:
        hs = mock.Mock()
        hs.hostname = "old-mtxdb-" + uuid4().hex
        # The warning is once-per-process; re-arm it so test order can't matter.
        with mock.patch.object(
            embedded_event_edges_module, "_publish_signal_api_warning_logged", False
        ):
            self._check_older_extension(hs)

    def _check_older_extension(self, hs: Any) -> None:
        hs.get_instance_id.return_value = uuid4().hex
        with (
            mock.patch(
                "synapse.storage.databases.embedded_engine.get_embedded_engine",
                return_value=object(),
            ),
            mock.patch(
                "synapse.storage.databases.main.embedded_event_edges._publish_signal_api_warning_logged",
                False,
            ),
        ):
            register_publish_signal_gauge(hs)
            try:
                with self.assertLogs(
                    "synapse.storage.databases.main.embedded_event_edges",
                    level="WARNING",
                ) as logs:
                    metrics = list(event_edge_publish_signal_gauge.collect())
                self.assertTrue(
                    any(
                        "does not expose publish_signal_active" in message
                        for message in logs.output
                    )
                )
                self.assertEqual(list(metrics[0].samples), [])
            finally:
                event_edge_publish_signal_gauge.unregister_hooks_for_homeserver_instance_id(
                    hs.get_instance_id()
                )


class EventEdgesStorageIntegrationTestCase(HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        room.register_servlets,
        login.register_servlets,
    ]

    def default_config(self) -> dict[str, Any]:
        config = super().default_config()
        # `setUp` opens this module's fixed root before the homeserver is
        # built. Point the homeserver at the same root: an already-open store
        # refuses to be reopened at a different path in one process.
        if "embedded_db" in config:
            config["embedded_db"] = {
                **config["embedded_db"],
                "path": _TEST_ENGINE_TMPDIR,
            }
        return config

    def setUp(self) -> None:
        # This class forces the embedded path on after construction, so point
        # the client at this module's root before the homeserver is built (it
        # may have nothing open at all when the suite runs in SQL mode). The
        # homeserver's own open call then sees this root already open and is a
        # no-op, so prepare() writes where the tests read.
        open_test_engine()
        super().setUp()

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        # Each test gets a distinct room: adjacency is scoped by the room's
        # event collection id in the process-global store.
        nonce = uuid4().hex
        username = f"alice_{nonce}"
        password = f"test_{nonce}"

        self.user_id = self.register_user(username, password)
        self.tok = self.login(username, password)
        self.room_id = self.helper.create_room_as(
            room_creator=self.user_id, tok=self.tok
        )

        self.store._embedded_event_edges_enabled = True
        self.store._embedded_event_edges_writable = True
        self.store._embedded_db_engine = "mtxdb"

        self.persist_store = hs.get_datastores().persist_events
        if self.persist_store is not None:
            self.persist_store._embedded_event_edges_enabled = True
            self.persist_store._embedded_event_edges_writable = True
            self.persist_store._embedded_db_engine = "mtxdb"

    def test_event_edges_stored_in_mtxdb_on_persistence(self) -> None:
        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        backward = get_event_edges_backward_batch([e2_id])
        self.assertIn(e1_id, backward.get(e2_id) or [])

        successors = self.get_success(self.store.get_successor_events(e1_id))
        self.assertIn(e2_id, successors)

    def _persist_message(
        self, *, rejected: str | None = None, **internal_metadata: Any
    ) -> str:
        """Persist a message on top of the room's latest event; return its id."""
        latest = self.get_success(self.store.get_latest_event_ids_in_room(self.room_id))
        event, context = self.get_success(
            create_event(
                self.hs,
                prev_event_ids=list(latest),
                type="m.room.message",
                sender=self.user_id,
                room_id=self.room_id,
                content={"msgtype": "m.text", "body": "x"},
                internal_metadata=internal_metadata,
            )
        )
        if rejected is not None:
            context.rejected = rejected
        persistence = self.hs.get_storage_controllers().persistence
        assert persistence is not None
        self.get_success(persistence.persist_event(event, context))
        return str(event.event_id)

    def _edge_views(self, event_id: str) -> tuple[list[str], list[str]]:
        """The event's prev edges as recorded in SQL and in mtxdb."""
        sql = self.get_success(
            self.store.db_pool.simple_select_onecol(
                "event_edges", {"event_id": event_id}, "prev_event_id"
            )
        )
        backward = get_event_edges_backward_batch([event_id])
        return sorted(sql), sorted(backward.get(event_id) or [])

    def test_soft_failed_event_gets_edges(self) -> None:
        """Soft-failed is not rejected: the event keeps its edges."""
        event_id = self._persist_message(soft_failed=True)

        sql, mtxdb = self._edge_views(event_id)
        self.assertEqual(sql, [], "mtxdb stores must not dual-write event_edges")
        self.assertTrue(mtxdb)

    def test_outlier_event_gets_edges(self) -> None:
        """Outliers are not filtered from edge writes."""
        event_id = self._persist_message(outlier=True)

        sql, mtxdb = self._edge_views(event_id)
        self.assertEqual(sql, [], "mtxdb stores must not dual-write event_edges")
        self.assertTrue(mtxdb)

    def test_rejected_event_gets_no_edges(self) -> None:
        """A rejected event is stored, but the rejected-events filter runs
        before edge writes, so it has no edges in SQL or mtxdb."""
        event_id = self._persist_message(rejected="test rejection")

        sql, mtxdb = self._edge_views(event_id)
        self.assertEqual(sql, [])
        self.assertEqual(mtxdb, [])

    def test_room_purge_drops_adjacency_scope(self) -> None:
        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        self.assertIn(e1_id, get_event_edges_backward_batch([e2_id]).get(e2_id) or [])

        purge_event_edges_room(self.room_id)

        self.assertNotIn(e2_id, get_event_edges_backward_batch([e2_id]))
        forward = get_event_edges_forward_batch([e1_id])
        self.assertNotIn(e2_id, forward.get(e1_id) or [])

    def test_rebuild_restores_edges_from_event_bodies(self) -> None:
        """The offline rebuild re-derives a room's adjacency from its events."""
        if not REBUILD_SCRIPT.exists():
            self.skipTest("scripts-dev/mtxdb/mtxdb_edges_rebuild.py is absent")
        rebuild_edges = _load_rebuild_edges()

        e1_id = self.helper.send(self.room_id, "first", tok=self.tok)["event_id"]
        e2_id = self.helper.send(self.room_id, "second", tok=self.tok)["event_id"]

        # A healthy room needs nothing: a dry run finds every edge in place.
        report = self.get_success(
            rebuild_edges(self.hs, rooms=[self.room_id], dry_run=True)
        )
        self.assertGreater(report["events_scanned"], 0)
        self.assertEqual(report["events_needing_rebuild"], 0)
        self.assertEqual(report["written"], 0)

        purge_event_edges_room(self.room_id)
        self.assertNotIn(e2_id, get_event_edges_backward_batch([e2_id]))

        report = self.get_success(rebuild_edges(self.hs, rooms=[self.room_id]))
        self.assertGreater(report["written"], 0)
        self.assertEqual(report["mismatches_after_write"], 0)
        self.assertIn(e1_id, get_event_edges_backward_batch([e2_id])[e2_id])
        self.assertIn(e2_id, get_event_edges_forward_batch([e1_id])[e1_id] or [])

        # Running it again changes nothing.
        report = self.get_success(rebuild_edges(self.hs, rooms=[self.room_id]))
        self.assertEqual(report["written"], 0)

    def test_read_only_worker_reads_edges(self) -> None:
        """A read-only worker can read adjacency without failing assert_writable."""
        res1 = self.helper.send(self.room_id, "msg1", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "msg2", tok=self.tok)
        e2_id = res2["event_id"]

        self.store._embedded_event_edges_writable = False
        self.store._embedded_event_edges_enabled = True

        successors = self.get_success(self.store.get_successor_events(e1_id))
        self.assertIn(e2_id, successors)
