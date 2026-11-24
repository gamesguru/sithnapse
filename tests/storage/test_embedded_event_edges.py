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
import shutil
import tempfile
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

    Every test uses a fresh room id: adjacency is scoped by the room's event
    collection id, so a unique room keeps the process-global store isolated
    between tests without a namespace.
    """

    def setUp(self) -> None:
        open_test_engine()
        self.room_id = f"!{uuid4().hex}:example.org"

    def tearDown(self) -> None:
        mtxdb_engine.close_client()

    def test_put_get_backward_and_forward(self) -> None:
        # No `event_json_put` first: `adjacency_put` publishes the locators for
        # every event it touches, so the repair/backfill case still resolves.
        put_event_edges_batch(
            [
                (self.room_id, "$e1", "$p1"),
                (self.room_id, "$e1", "$p2"),
            ]
        )

        backward = get_event_edges_backward_batch(["$e1", "$nonexistent"])
        self.assertEqual(backward["$e1"], ["$p1", "$p2"])
        self.assertNotIn("$nonexistent", backward)

        forward = get_event_edges_forward_batch(["$p1", "$p2", "$nonexistent"])
        self.assertEqual(forward["$p1"], ["$e1"])
        self.assertEqual(forward["$p2"], ["$e1"])
        self.assertIsNone(forward["$nonexistent"])

        put_event_edges_batch([(self.room_id, "$e2", "$p1")])
        forward2 = get_event_edges_forward_batch(["$p1"])
        self.assertCountEqual(forward2["$p1"] or [], ["$e1", "$e2"])

    def test_event_without_prev_has_an_empty_row(self) -> None:
        put_event_edges_batch([(self.room_id, "$create", None)])

        self.assertEqual(get_event_edges_backward_batch(["$create"])["$create"], [])
        self.assertIsNone(get_event_edges_forward_batch(["$create"])["$create"])

    def test_rooms_do_not_leak(self) -> None:
        # Event ids are globally unique and belong to one room; the locator
        # maps an id to exactly one room collection. Two rooms therefore use
        # distinct parents here, and each read must stay inside its own scope.
        other_room = f"!{uuid4().hex}:example.org"
        put_event_edges_batch([(self.room_id, "$a", "$parent_a")])
        put_event_edges_batch([(other_room, "$b", "$parent_b")])

        self.assertEqual(get_event_edges_backward_batch(["$a"])["$a"], ["$parent_a"])
        self.assertEqual(get_event_edges_backward_batch(["$b"])["$b"], ["$parent_b"])
        self.assertEqual(
            get_event_edges_forward_batch(["$parent_a"])["$parent_a"], ["$a"]
        )
        self.assertEqual(
            get_event_edges_forward_batch(["$parent_b"])["$parent_b"], ["$b"]
        )

    def test_purge_room_drops_the_whole_scope(self) -> None:
        put_event_edges_batch(
            [
                (self.room_id, "$e1", "$p1"),
                (self.room_id, "$e2", "$e1"),
            ]
        )
        self.assertIn("$e1", get_event_edges_backward_batch(["$e1"]))

        purge_event_edges_room(self.room_id)

        self.assertNotIn("$e1", get_event_edges_backward_batch(["$e1"]))
        self.assertNotIn("$e2", get_event_edges_backward_batch(["$e2"]))
        self.assertIsNone(get_event_edges_forward_batch(["$p1"])["$p1"])

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
                [(self.room_id, "$staged", "$parent")], transaction=transaction
            )
            self.assertEqual(
                get_event_edges_backward_batch(["$staged"])["$staged"],
                ["$parent"],
            )
        finally:
            transaction.abort()


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
