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

import atexit
import shutil
import tempfile
import threading
from unittest import mock

from twisted.test.proto_helpers import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.storage.databases.main.embedded_common import (
    FLUSH_DELAY_SECS,
    _clear_coalescer,
    _FlushCoalescer,
    _set_coalescer,
    configure_sync,
    enable_ffi_counting,
    get_ffi_count,
)
from synapse.storage.databases.main.embedded_event_edges import (
    delete_event_edges_batch,
    flush_edge_writes,
    get_event_edges_backward_batch,
    get_event_edges_forward_batch,
    put_event_edges_batch,
    queue_edge_write,
    queued_edge_write_count,
)
from synapse.synapse_rust import mtxdb_engine
from synapse.util.clock import Clock

from tests import unittest
from tests.server import ThreadedMemoryReactorClock
from tests.unittest import HomeserverTestCase
from tests.utils import EMBEDDED_HAMT_ENGINE, EMBEDDED_HAMT_PATH

if EMBEDDED_HAMT_ENGINE and EMBEDDED_HAMT_PATH:
    # The test harness has already selected a worker-specific store. Reuse
    # that path instead of opening a private store before the homeserver is
    # created, which would win the Rust process-global OnceCell.
    _TEST_ENGINE_TMPDIR = EMBEDDED_HAMT_PATH
else:
    _TEST_ENGINE_TMPDIR = tempfile.mkdtemp(prefix="test-embedded-event-edges-")
    # mtxdb's Python binding owns process-global pools. Keep this directory
    # alive until process exit; deleting it during a test class teardown leaves
    # the OnceCell-backed engine pointing at an unlinked store.
    atexit.register(shutil.rmtree, _TEST_ENGINE_TMPDIR, ignore_errors=True)
mtxdb_engine.open_client(_TEST_ENGINE_TMPDIR)


class EmbeddedEventEdgesTestCase(unittest.TestCase):
    """Pure unit tests for the embedded event-edges FFI layer.

    mtxdb_engine.open_client() is backed by a Rust OnceCell: the engine is
    opened exactly once per process and subsequent calls are no-ops.  The
    engine's data directory must therefore survive for the life of the
    process; test setup and teardown must not delete it.  The module-level
    fixture opens one store and removes it only at process exit.
    """

    def setUp(self) -> None:
        self.namespace = "test-edges-ns"
        self.room_id = "!room:example.org"

    def tearDown(self) -> None:
        # Drain every namespace's coalescing queue so rows queued by
        # queue_edge_write (never flushed here -- the unit test has no flush
        # coalescer driving a timer) do not leak into the next test.
        flush_edge_writes()

    def test_put_get_backward_and_forward(self) -> None:
        # No locator seeding: `event_edges_put` publishes locators for the
        # events it touches, so an edge written with no preceding
        # `event_json_put` -- the legacy/repair case -- still resolves on read.
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

    def test_queues_are_namespace_partitioned(self) -> None:
        """Rows queued under one namespace never flush under another, and a
        no-namespace flush drains every namespace separately."""
        ns_a = "test-edges-part-a"
        ns_b = "test-edges-part-b"
        try:
            queue_edge_write(ns_a, [(self.room_id, "$a1", "$ap", False)])
            queue_edge_write(ns_b, [(self.room_id, "$b1", "$bp", False)])
            self.assertEqual(queued_edge_write_count(), 2)

            # Flushing A must only write A's rows; B's stay queued.
            self.assertTrue(flush_edge_writes(ns_a))
            self.assertEqual(queued_edge_write_count(ns_b), 1)

            back_a = get_event_edges_backward_batch(ns_a, ["$a1"])
            self.assertEqual(back_a["$a1"], [("$ap", False)])
            back_b = get_event_edges_backward_batch(ns_b, ["$b1"])
            self.assertIsNone(back_b["$b1"])

            # The no-namespace drain covers B as well.
            self.assertTrue(flush_edge_writes())
            self.assertEqual(queued_edge_write_count(ns_b), 0)
            back_b2 = get_event_edges_backward_batch(ns_b, ["$b1"])
            self.assertEqual(back_b2["$b1"], [("$bp", False)])
        finally:
            flush_edge_writes()

    def test_flush_failure_retains_queued_rows(self) -> None:
        """A failed FFI write re-queues the drained rows instead of dropping
        them, and a later flush succeeds."""
        ns = "test-edges-ffi-failure"
        rows = [(self.room_id, f"$fail{i}", f"$failp{i}", False) for i in range(3)]
        queue_edge_write(ns, rows)
        self.assertEqual(queued_edge_write_count(ns), 3)

        with mock.patch(
            "synapse.storage.databases.main.embedded_event_edges.put_event_edges_batch",
            side_effect=RuntimeError("simulated FFI failure"),
        ):
            with self.assertRaises(RuntimeError):
                flush_edge_writes(ns)
            # Committed rows survive the failure.
            self.assertEqual(queued_edge_write_count(ns), 3)

        # A subsequent drain writes them.
        self.assertTrue(flush_edge_writes(ns))
        self.assertEqual(queued_edge_write_count(ns), 0)
        back = get_event_edges_backward_batch(ns, [rows[0][1]])
        self.assertEqual(back[rows[0][1]], [(rows[0][2], False)])

    def test_concurrent_flushes_do_not_lose_rows(self) -> None:
        """Concurrent queued flushes on the same namespace are serialized;
        every committed row is written exactly once after they settle."""
        ns = "test-edges-concurrent"
        num_threads = 4
        rows_per_thread = 32

        def worker(seed: int) -> None:
            rows = [
                (self.room_id, f"$c{seed}_{i}", f"$cp{seed}", False)
                for i in range(rows_per_thread)
            ]
            for chunk in range(0, len(rows), 8):
                queue_edge_write(ns, rows[chunk : chunk + 8])
                try:
                    flush_edge_writes(ns)
                except Exception:
                    pass

        threads = [
            threading.Thread(target=worker, args=(t,)) for t in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(queued_edge_write_count(ns), 0)
        # Backward edges for every row written, and $cp<N>'s forward lists
        # carry every child.
        for seed in range(num_threads):
            for i in range(rows_per_thread):
                back = get_event_edges_backward_batch(ns, [f"$c{seed}_{i}"])
                self.assertEqual(back[f"$c{seed}_{i}"], [(f"$cp{seed}", False)])
            fwd = get_event_edges_forward_batch(ns, [f"$cp{seed}"])
            children = fwd.get(f"$cp{seed}") or []
            self.assertEqual(
                set(children), {f"$c{seed}_{i}" for i in range(rows_per_thread)}
            )

    def test_purge_cancels_queued_edges_for_purged_events(self) -> None:
        """A purge races queued writes: rows for the purged event must not be
        flushed (which would then be erased) and must not resurrect the
        tombstone later."""
        ns = "test-edges-purge-race"
        # SQL committed these edges but the mirror hasn't flushed them yet.
        queue_edge_write(ns, [(self.room_id, "$victim", "$parent", False)])
        queue_edge_write(ns, [(self.room_id, "$live", "$other", False)])
        self.assertEqual(queued_edge_write_count(ns), 2)

        # Purge races the queue: $victim's row is cancelled (not written then
        # erased), $live's row is flushed harmlessly before the tombstone.
        delete_event_edges_batch(ns, ["$victim"])
        self.assertEqual(queued_edge_write_count(ns), 0)

        back = get_event_edges_backward_batch(ns, ["$victim"])
        self.assertIsNone(back["$victim"])
        fwd = get_event_edges_forward_batch(ns, ["$parent"])
        self.assertNotIn("$victim", fwd.get("$parent") or [])

        # A later drain must not resurrect $victim's edges.
        flush_edge_writes()
        back_after = get_event_edges_backward_batch(ns, ["$victim"])
        self.assertIsNone(back_after["$victim"])
        fwd_after = get_event_edges_forward_batch(ns, ["$parent"])
        self.assertNotIn("$victim", fwd_after.get("$parent") or [])

        # The unrelated edge survived the purge barrier.
        back_live = get_event_edges_backward_batch(ns, ["$live"])
        self.assertEqual(back_live["$live"], [("$other", False)])

    def test_shutdown_drain_failure_retains_rows(self) -> None:
        """The shutdown drain retries on FFI failure, never drops rows, and
        close() still completes."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        ns = "test-edges-shutdown-fail"
        try:
            queue_edge_write(ns, [(self.room_id, "$s1", "$sp", False)])
            with mock.patch(
                "synapse.storage.databases.main.embedded_event_edges.put_event_edges_batch",
                side_effect=RuntimeError("simulated shutdown FFI failure"),
            ):
                with self.assertLogs(
                    "synapse.storage.databases.main.embedded_common", level="WARNING"
                ) as logs:
                    coalescer.close()
                self.assertTrue(
                    any("edge-write drain failed" in line for line in logs.output),
                    f"expected drain retry log, got: {logs.output}",
                )
            # Rows are retained after shutdown despite the failed drain.
            self.assertEqual(queued_edge_write_count(ns), 1)
        finally:
            _clear_coalescer(coalescer)
            flush_edge_writes()

    def test_edge_write_repairs_missing_legacy_locator(self) -> None:
        """Edges for events whose event_json locator was never mirrored are
        still resolvable: `event_edges_put` re-publishes locators."""
        ns = "test-edges-legacy-locator"
        put_event_edges_batch(ns, [(self.room_id, "$old", "$older", False)])
        back = get_event_edges_backward_batch(ns, ["$old"])
        self.assertEqual(back["$old"], [("$older", False)])
        fwd = get_event_edges_forward_batch(ns, ["$older"])
        self.assertEqual(fwd["$older"], ["$old"])


class EventEdgesStorageIntegrationTestCase(HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        room.register_servlets,
        login.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        # The test harness sets embedded_hamt.no_sync (SYNAPSE_MTXDB_NO_SYNC) to
        # skip mtxdb fsyncs, which also short-circuits the commit-aware flush
        # coalescer's dirty-marking timer.  The timer tests below exercise that
        # path, so re-enable coalescing for the class and restore the harness
        # default in tearDown.
        self._coalescer_sync_disabled = hs.config.database.embedded_hamt_no_sync
        configure_sync(no_sync=False)
        self.user_id = self.register_user("alice", "test")
        self.tok = self.login("alice", "test")
        self.room_id = self.helper.create_room_as(
            room_creator=self.user_id, tok=self.tok
        )

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

    def tearDown(self) -> None:
        # Restore the harness no-sync default before the next test class runs.
        configure_sync(no_sync=getattr(self, "_coalescer_sync_disabled", True))
        super().tearDown()

    def test_event_edges_mirrored_on_persistence(self) -> None:
        """When events are persisted, event_edges are dual-written to mtxdb."""
        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        # Edge writes are coalesced in-process; drain the queue so the mirror
        # reflects them (in production the next flush / threshold / shutdown
        # would do this, but the test asserts immediately).
        flush_edge_writes(self.store._embedded_hamt_namespace)

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

    def test_below_threshold_queue_flushes_on_timer(self) -> None:
        """A sub-threshold edge write is drained by the flush coalescer's
        bounded debounce timer -- no explicit flush, threshold, or shutdown
        required (the low-volume case the no-timer implementation got wrong)."""
        ns = self.store._embedded_hamt_namespace
        # Drain anything earlier tests left queued so this test controls queue
        # content: the single edge below is then far below the threshold.
        flush_edge_writes(ns)
        res = self.helper.send(self.room_id, "ring", tok=self.tok)
        e_id = res["event_id"]

        with enable_ffi_counting():
            # Still queued (below threshold); the mirror has not been written.
            self.assertEqual(get_ffi_count("event_edges_put_rows"), 0)
            self.assertGreaterEqual(
                queued_edge_write_count(ns),
                1,
                "the persisted edge should be waiting in the coalescing queue",
            )

            # Advance the reactor past the flush window: the coalescer drains
            # the queue and syncs EVENT_DAG of its own accord.
            self.reactor.advance(FLUSH_DELAY_SECS + 0.1)
            self.assertEqual(
                queued_edge_write_count(ns),
                0,
                "the coalescer timer should have drained the queue",
            )
            self.assertGreater(get_ffi_count("event_edges_put_rows"), 0)

        backward = get_event_edges_backward_batch(ns, [e_id])
        self.assertIsNotNone(
            backward[e_id], "mirror should reflect the edge after the timer flush"
        )
        prev_ids = [p for p, _ in backward[e_id] or []]
        self.assertTrue(prev_ids)

    def test_event_edges_purge_cleans_forward_edges(self) -> None:
        """Purging an event removes it from its parents' forward lists in mtxdb."""
        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        # Drain the coalesced edge writes so the mirror reflects both sends.
        flush_edge_writes(self.store._embedded_hamt_namespace)

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

        # Commit the event row separately. The transaction under test should
        # roll back the edge insert, not fail its foreign-key check because
        # the fixture row was inserted in the same interaction.
        self.get_success(
            self.store.db_pool.simple_insert(
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
        )

        def bad_txn(txn: LoggingTransaction) -> None:
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
        # and queues a repair of the mtxdb forward edge.  The read is served
        # by SQL; the repair only lands in mtxdb on the next coalesced flush.
        successors = self.get_success(self.store.get_successor_events(p_id))
        self.assertIn(c_id, successors)

        # Advance the reactor past the flush window so the coalescer drains
        # the queued repair (and syncs EVENT_DAG).  Use FLUSH_DELAY_SECS plus
        # a small margin so the test is not fragile to floating-point
        # boundaries or minor changes to the delay value.
        self.reactor.advance(FLUSH_DELAY_SECS + 0.1)

        # Now mtxdb has been repaired in-process.
        fwd_repaired = get_event_edges_forward_batch(
            self.store._embedded_hamt_namespace, [p_id]
        )
        self.assertIn(c_id, fwd_repaired.get(p_id) or [])

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
