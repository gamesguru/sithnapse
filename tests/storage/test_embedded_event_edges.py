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

import shutil
import tempfile

from twisted.test.proto_helpers import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.storage.databases.main.embedded_common import (
    FLUSH_DELAY_SECS,
    enable_ffi_counting,
    get_ffi_count,
)
from synapse.storage.databases.main.embedded_event_edges import (
    delete_event_edges_batch,
    get_event_edges_backward_batch,
    get_event_edges_forward_batch,
    put_event_edges_batch,
)
from synapse.synapse_rust import mtxdb_engine
from synapse.util.clock import Clock

from tests import unittest
from tests.unittest import HomeserverTestCase


class EmbeddedEventEdgesTestCase(unittest.TestCase):
    """Pure unit tests for the embedded event-edges FFI layer.

    mtxdb_engine.open_client() is backed by a Rust OnceCell: the engine is
    opened exactly once per process and subsequent calls are no-ops.  The
    engine's data directory must therefore survive for the life of the
    process; setUp/tearDown must NOT delete it.  Use setUpClass/tearDownClass
    so the directory is created once and cleaned up after all tests in the
    class finish.
    """

    _engine_tmpdir: str  # set by setUpClass

    @classmethod
    def setUpClass(cls) -> None:
        cls._engine_tmpdir = tempfile.mkdtemp(prefix="test-embedded-event-edges-")
        mtxdb_engine.open_client(cls._engine_tmpdir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls._engine_tmpdir, ignore_errors=True)

    def setUp(self) -> None:
        self.namespace = "test-edges-ns"
        self.room_id = "!room:example.org"

    def test_put_get_backward_and_forward(self) -> None:
        # e1 has prev_events p1 and p2
        rows = [
            (self.room_id, "$e1", "$p1", False),
            (self.room_id, "$e1", "$p2", True),
        ]
        put_event_edges_batch(self.namespace, rows)

        # Backward edges for $e1
        backward = get_event_edges_backward_batch(
            self.namespace, ["$e1", "$nonexistent"]
        )
        self.assertIn("$e1", backward)
        self.assertEqual(backward["$e1"], [("$p1", False), ("$p2", True)])
        self.assertIsNone(backward["$nonexistent"])

        # Forward edges for $p1 and $p2
        forward = get_event_edges_forward_batch(
            self.namespace, ["$p1", "$p2", "$nonexistent"]
        )
        self.assertEqual(forward["$p1"], ["$e1"])
        self.assertEqual(forward["$p2"], ["$e1"])
        self.assertIsNone(forward["$nonexistent"])

        # Append another event $e2 whose prev_event is $p1
        put_event_edges_batch(self.namespace, [(self.room_id, "$e2", "$p1", False)])
        forward2 = get_event_edges_forward_batch(self.namespace, ["$p1"])
        self.assertCountEqual(forward2["$p1"] or [], ["$e1", "$e2"])

        # Delete $e1 tombstones backward edge and removes $e1 from parents' forward edges
        delete_event_edges_batch(self.namespace, ["$e1"])
        backward_after = get_event_edges_backward_batch(self.namespace, ["$e1"])
        self.assertIsNone(backward_after["$e1"])

        forward_after = get_event_edges_forward_batch(self.namespace, ["$p1", "$p2"])
        self.assertEqual(forward_after["$p1"], ["$e2"])
        self.assertIsNone(forward_after["$p2"])


class EventEdgesStorageIntegrationTestCase(HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        room.register_servlets,
        login.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self.user_id = self.register_user("alice", "test")
        self.tok = self.login("alice", "test")
        self.room_id = self.helper.create_room_as(
            room_creator=self.user_id, tok=self.tok
        )

        tmpdir = tempfile.mkdtemp(prefix="test-embedded-event-edges-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        mtxdb_engine.open_client(tmpdir)

        self.store._embedded_event_edges_enabled = True
        self.store._embedded_event_edges_writable = True
        self.store._embedded_hamt_engine = "mtxdb"
        if not getattr(self.store, "_embedded_hamt_namespace", None):
            self.store._embedded_hamt_namespace = hs.hostname

        self.persist_store = hs.get_datastores().persist_events
        if self.persist_store is not None:
            self.persist_store._embedded_event_edges_enabled = True
            self.persist_store._embedded_event_edges_writable = True
            self.persist_store._embedded_hamt_engine = "mtxdb"
            if not getattr(self.persist_store, "_embedded_hamt_namespace", None):
                self.persist_store._embedded_hamt_namespace = hs.hostname

    def test_event_edges_mirrored_on_persistence(self) -> None:
        """When events are persisted, event_edges are dual-written to mtxdb."""
        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        # e2 should have e1 in its backward edges in mtxdb
        backward = get_event_edges_backward_batch(
            self.store._embedded_hamt_namespace, [e2_id]
        )
        self.assertIsNotNone(backward[e2_id])
        prev_ids = [p for p, _ in backward[e2_id] or []]
        self.assertIn(e1_id, prev_ids)

        # e1 should have e2 in its forward successors in mtxdb
        successors = self.get_success(self.store.get_successor_events(e1_id))
        self.assertIn(e2_id, successors)

    def test_event_edges_purge_cleans_forward_edges(self) -> None:
        """Purging an event removes it from its parents' forward lists in mtxdb."""
        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        # Before deletion: e1 has e2 as forward successor
        fwd_before = get_event_edges_forward_batch(
            self.store._embedded_hamt_namespace, [e1_id]
        )
        self.assertIn(e2_id, fwd_before.get(e1_id) or [])

        # Purge e2
        delete_event_edges_batch(self.store._embedded_hamt_namespace, [e2_id])

        # After deletion: e2 is tombstoned in backward edges
        back_after = get_event_edges_backward_batch(
            self.store._embedded_hamt_namespace, [e2_id]
        )
        self.assertIsNone(back_after[e2_id])

        # And e1's forward edges no longer contain e2
        fwd_after = get_event_edges_forward_batch(
            self.store._embedded_hamt_namespace, [e1_id]
        )
        self.assertNotIn(e2_id, fwd_after.get(e1_id) or [])

    def test_rollback_leaves_mtxdb_unmodified(self) -> None:
        """A rolled back SQL transaction does not write orphaned edges to mtxdb."""
        from unittest.mock import Mock

        from synapse.storage.database import LoggingTransaction

        res1 = self.helper.send(self.room_id, "base_event", tok=self.tok)
        e1_id = res1["event_id"]

        fake_id = f"$aborted_{self.clock.time()}:test"
        mock_ev = Mock(
            room_id=self.room_id, event_id=fake_id, prev_event_ids=lambda: [e1_id]
        )

        persist_store = self.persist_store
        assert persist_store is not None

        def bad_txn(txn: LoggingTransaction) -> None:
            self.store.db_pool.simple_insert_txn(
                txn,
                table="events",
                values={
                    "instance_name": "master",
                    "stream_ordering": 999999,
                    "topological_ordering": 1,
                    "depth": 1,
                    "event_id": fake_id,
                    "room_id": self.room_id,
                    "type": "m.room.message",
                    "processed": True,
                    "outlier": False,
                    "origin_server_ts": int(self.clock.time_msec()),
                    "received_ts": int(self.clock.time_msec()),
                    "sender": self.user_id,
                    "contains_url": False,
                },
            )
            persist_store._handle_mult_prev_events(txn, [mock_ev])
            raise RuntimeError("simulated transaction failure")

        failure = self.get_failure(
            self.store.db_pool.runInteraction("test_abort", bad_txn),
            RuntimeError,
        )
        self.assertEqual(str(failure.value), "simulated transaction failure")

        # Check mtxdb: fake_id is NOT in mtxdb backward edges
        back = get_event_edges_backward_batch(
            self.store._embedded_hamt_namespace, [fake_id]
        )
        self.assertIsNone(back[fake_id])

        # And e1_id's forward edges do NOT contain fake_id
        fwd = get_event_edges_forward_batch(
            self.store._embedded_hamt_namespace, [e1_id]
        )
        self.assertNotIn(fake_id, fwd.get(e1_id) or [])

    def test_read_only_worker_reads_edges(self) -> None:
        """A read-only worker can read edges from mtxdb without failing assert_writable."""
        res1 = self.helper.send(self.room_id, "msg1", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "msg2", tok=self.tok)
        e2_id = res2["event_id"]

        # Simulate read-only worker
        self.store._embedded_event_edges_writable = False
        self.store._embedded_event_edges_enabled = True

        successors = self.get_success(self.store.get_successor_events(e1_id))
        self.assertIn(e2_id, successors)

    def test_sql_fallback_and_repair_on_missing_mtxdb_edges(self) -> None:
        """SQL fallback returns edges missing from mtxdb and repairs the local store.

        This is a same-process test: it verifies that after the coalesced flush
        fires, the same in-process mtxdb handle reflects the repaired edge when
        read through the read-only (non-writable) code path.  It does NOT verify
        cross-worker or cross-process visibility; that requires a separate
        read-only engine handle or a genuine multi-process integration test.

        The metric assertions confirm that the final read-only query was served
        from the embedded store (hit counter up, fallback counter flat) rather
        than silently succeeding through the SQL fallback path.
        """
        res1 = self.helper.send(self.room_id, "parent", tok=self.tok)
        p_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "child", tok=self.tok)
        c_id = res2["event_id"]

        # Simulate post-commit write failure or missing edge in mtxdb by deleting the edge from mtxdb
        delete_event_edges_batch(self.store._embedded_hamt_namespace, [c_id])

        # Verify mtxdb has no forward edge for p_id
        fwd_miss = get_event_edges_forward_batch(
            self.store._embedded_hamt_namespace, [p_id]
        )
        self.assertIsNone(fwd_miss.get(p_id))

        # Querying successor events falls back to SQL, successfully finding c_id,
        # and repairs mtxdb.
        successors = self.get_success(self.store.get_successor_events(p_id))
        self.assertIn(c_id, successors)

        # Now mtxdb has been repaired in-process.
        fwd_repaired = get_event_edges_forward_batch(
            self.store._embedded_hamt_namespace, [p_id]
        )
        self.assertIn(c_id, fwd_repaired.get(p_id) or [])

        # Advance the reactor clock past the flush window so the coalesced flush fires.
        # Use FLUSH_DELAY_SECS + a small margin so the test is not fragile to
        # floating-point boundaries or minor changes to the delay value.
        self.reactor.advance(FLUSH_DELAY_SECS + 0.1)

        # Exercise the read-only code path after the coalesced flush.
        # writable=False routes through the embedded read path (no SQL write).
        # enable_ffi_counting() activates counters only for this block so the
        # assertion is isolated from any other test activity and does not add
        # overhead to production deployments where diagnostics are disabled.
        self.store._embedded_event_edges_writable = False
        with enable_ffi_counting():
            reader_successors = self.get_success(self.store.get_successor_events(p_id))
            self.assertIn(c_id, reader_successors)

            # The query must have been an embedded hit, not a SQL fallback.
            self.assertEqual(
                get_ffi_count("event_edges_successor_hits"),
                1,
                "expected exactly one embedded hit for the read-only query",
            )
            self.assertEqual(
                get_ffi_count("event_edges_successor_fallbacks"),
                0,
                "expected no SQL fallback for the read-only query after repair",
            )
