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
from typing import Any
from unittest import mock, skipUnless

from twisted.test.proto_helpers import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.storage.databases.main import embedded_common
from synapse.storage.databases.main.embedded_common import (
    FLUSH_DELAY_SECS,
    _clear_coalescer,
    _FlushCoalescer,
    _set_coalescer,
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

    def test_event_dag_barrier_does_not_drain_edges(self) -> None:
        """The per-event JSON barrier must leave edge batching untouched."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            with (
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._drain_edge_writes"
                ) as drain,
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._do_sync_pools"
                ) as sync,
            ):
                coalescer.sync_event_dag_now()
                drain.assert_not_called()
                sync.assert_called_once_with({embedded_common.Pool.EVENT_DAG})
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_event_dag_barrier_failure_reschedules(self) -> None:
        """A failed narrow barrier retains dirty state and schedules retry."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            coalescer.mark_dirty(embedded_common.Pool.EVENT_DAG)
            with mock.patch(
                "synapse.storage.databases.main.embedded_common._do_sync_pools",
                side_effect=RuntimeError("simulated sync failure"),
            ):
                with self.assertRaises(RuntimeError):
                    coalescer.sync_event_dag_now()
            self.assertIn(embedded_common.Pool.EVENT_DAG, coalescer._dirty)
            self.assertIsNotNone(coalescer._delayed_call)
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_event_dag_barrier_failure_backs_off_existing_timer(self) -> None:
        """A failed narrow barrier must not ride out whatever debounce delay
        happened to already be pending (armed at ``_FLUSH_DELAY`` by
        ``mark_dirty``) -- it should cancel that timer and re-arm at the
        shorter ``_RETRY_DELAY``, so a failure always backs off consistently
        regardless of when it lands relative to the shared timer."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            coalescer.mark_dirty(embedded_common.Pool.EVENT_DAG)
            assert coalescer._delayed_call is not None
            pending_at_flush_delay = coalescer._delayed_call.getTime()
            self.assertAlmostEqual(
                pending_at_flush_delay, reactor.seconds() + 0.5, places=3
            )

            with mock.patch(
                "synapse.storage.databases.main.embedded_common._do_sync_pools",
                side_effect=RuntimeError("simulated sync failure"),
            ):
                with self.assertRaises(RuntimeError):
                    coalescer.sync_event_dag_now()

            self.assertIsNotNone(coalescer._delayed_call)
            assert coalescer._delayed_call is not None
            rearmed_at_retry_delay = coalescer._delayed_call.getTime()
            self.assertAlmostEqual(
                rearmed_at_retry_delay, reactor.seconds() + 1.0, places=3
            )
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

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
        # erased), while the unrelated $live row remains queued.
        delete_event_edges_batch(ns, ["$victim"])
        self.assertEqual(queued_edge_write_count(ns), 1)

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

    def test_enqueue_after_purge_cancel_is_tombstoned(self) -> None:
        """The racy ordering the cancel step alone cannot cover: a transaction
        that committed just before the purge runs its post-commit enqueue AFTER
        the purge cancelled the queue.  The tombstone set must drop that row so
        a later flush cannot resurrect the purged event's edges."""
        ns = "test-edges-purge-late-enqueue"
        try:
            # Purge runs first: the event is gone from SQL and its mtxdb edge
            # is tombstoned.
            delete_event_edges_batch(ns, ["$late"])

            # Now the racing enqueue lands, after cancellation.
            queue_edge_write(ns, [(self.room_id, "$late", "$lateparent", False)])
            self.assertEqual(
                queued_edge_write_count(ns),
                0,
                "a tombstoned event must not be re-queued",
            )

            # Even an explicit drain must not resurrect it.
            flush_edge_writes(ns)
            back = get_event_edges_backward_batch(ns, ["$late"])
            self.assertIsNone(back["$late"])
            fwd = get_event_edges_forward_batch(ns, ["$lateparent"])
            self.assertNotIn("$late", fwd.get("$lateparent") or [])
        finally:
            flush_edge_writes()

    def test_backward_extremity_survives_purge(self) -> None:
        """A queued forward row (live_child → purged_parent) is a backward
        extremity and must survive purge.  Synapse's SQL purge only deletes
        rows whose own event_id is purged; live children referencing a purged
        prev_event_id are preserved.  The mtxdb coalescer must match."""
        ns = "test-edges-backward-extremity"
        parent = "$purged_parent"
        child = "$live_child"
        try:
            queue_edge_write(ns, [(self.room_id, child, parent, False)])
            self.assertEqual(queued_edge_write_count(ns), 1)

            delete_event_edges_batch(ns, [parent])

            # The forward row (child → parent) was NOT cancelled — child
            # (row[1]) is live — and remains queued for the coalescer.
            self.assertEqual(queued_edge_write_count(ns), 1)
            flush_edge_writes(ns)

            # The edge exists in mtxdb: backward and forward both present.
            back = get_event_edges_backward_batch(ns, [child])
            self.assertEqual(back[child], [(parent, False)])
            fwd = get_event_edges_forward_batch(ns, [parent])
            self.assertIn(child, fwd.get(parent) or [])
        finally:
            flush_edge_writes(ns)

    def test_unrelated_queued_write_does_not_resurrect_deleted_sibling(self) -> None:
        """The queue-drain-free purge (delete_event_edges_batch no longer
        flushes the whole namespace first) leaves unrelated queued rows in
        place. This only stays correct if a later coalescer flush of that row
        appends against the *post-delete* forward list rather than replaying
        a stale snapshot taken when it was enqueued. Regression guard for
        that invariant: delete a sibling that is already durable in mtxdb,
        then flush an unrelated queued write to the same parent, and confirm
        the deleted sibling is not resurrected while the new child lands."""
        ns = "test-edges-no-resurrection-on-drainless-purge"
        parent = "$shared_parent"
        victim = "$purged_sibling"
        sibling = "$new_sibling"
        try:
            # Seed victim's edge as already-durable mtxdb state (not queued).
            queue_edge_write(ns, [(self.room_id, victim, parent, False)])
            flush_edge_writes(ns)
            fwd = get_event_edges_forward_batch(ns, [parent])
            self.assertIn(victim, fwd.get(parent) or [])

            # Queue an unrelated write to the same parent's forward list.
            # It must stay queued across the purge below (drain-free purge).
            queue_edge_write(ns, [(self.room_id, sibling, parent, False)])
            self.assertEqual(queued_edge_write_count(ns), 1)

            delete_event_edges_batch(ns, [victim])

            # Unrelated row was not touched by the drain-free delete.
            self.assertEqual(queued_edge_write_count(ns), 1)

            # Now let the coalescer apply the queued row.
            flush_edge_writes(ns)

            fwd = get_event_edges_forward_batch(ns, [parent])
            children = fwd.get(parent) or []
            self.assertIn(sibling, children)
            self.assertNotIn(victim, children)
        finally:
            flush_edge_writes(ns)

    def test_purge_enqueue_race_is_serialized(self) -> None:
        """Force the dangerous ordering: an enqueue arriving while the purge is
        paused between its cancel step and its FFI delete.  The namespace flush
        lock parks the enqueue until the purge has recorded its tombstone, so
        the row can never land in the cancel->delete window and a later drain
        cannot resurrect the purged event."""
        ns = "test-edges-purge-barrier"
        victim = "$barrier_victim"
        parent = "$barrier_parent"
        real_delete = mtxdb_engine.event_edges_delete
        delete_entered = threading.Event()
        delete_release = threading.Event()
        enqueue_started = threading.Event()
        enqueue_finished = threading.Event()

        def slow_delete(*args: Any, **kwargs: Any) -> Any:
            delete_entered.set()
            if not delete_release.wait(timeout=5):
                raise TimeoutError("mocked delete was never released")
            return real_delete(*args, **kwargs)

        def purge() -> None:
            delete_event_edges_batch(ns, [victim])

        def enqueue() -> None:
            enqueue_started.set()
            queue_edge_write(ns, [(self.room_id, victim, parent, False)])
            enqueue_finished.set()

        try:
            with mock.patch(
                "synapse.synapse_rust.mtxdb_engine.event_edges_delete",
                side_effect=slow_delete,
            ):
                purge_thread = threading.Thread(target=purge)
                purge_thread.start()
                self.assertTrue(
                    delete_entered.wait(timeout=5), "purge never reached the delete"
                )

                enqueue_thread = threading.Thread(target=enqueue)
                enqueue_thread.start()
                self.assertTrue(enqueue_started.wait(timeout=5))
                # The enqueue must be parked on the namespace flush lock for
                # the whole cancel -> delete window.
                self.assertFalse(
                    enqueue_finished.wait(timeout=0.2),
                    "enqueue landed between the purge cancel and delete",
                )

                delete_release.set()
                purge_thread.join(timeout=5)
                enqueue_thread.join(timeout=5)

            self.assertFalse(purge_thread.is_alive())
            self.assertFalse(enqueue_thread.is_alive())

            # The (post-delete) enqueue was tombstoned, not applied.
            self.assertEqual(queued_edge_write_count(ns), 0)
            flush_edge_writes(ns)
            back = get_event_edges_backward_batch(ns, [victim])
            self.assertIsNone(back[victim])
            fwd = get_event_edges_forward_batch(ns, [parent])
            self.assertNotIn(victim, fwd.get(parent) or [])
        finally:
            delete_release.set()
            flush_edge_writes(ns)

    def test_purge_delete_failure_does_not_block_repair(self) -> None:
        """If the FFI delete fails the purge must not leave a permanent
        tombstone: the stale edges are still in mtxdb and a tombstone would
        suppress the repair writes that recover from the failure.  The
        cancelled rows from the purge are also restored so the original data
        remains reachable."""
        ns = "test-edges-purge-delete-fail"
        try:
            # A committed-but-unflushed row for the victim is cancelled ...
            queue_edge_write(ns, [(self.room_id, "$victim", "$parent", False)])

            with mock.patch(
                "synapse.synapse_rust.mtxdb_engine.event_edges_delete",
                side_effect=RuntimeError("simulated FFI delete failure"),
            ):
                with self.assertRaises(RuntimeError):
                    delete_event_edges_batch(ns, ["$victim"])

            # ... but no tombstone was recorded, so a repair/backfill write for
            # the same event is accepted rather than suppressed for the TTL.
            # The cancelled row was also restored, so we have 2 rows queued.
            queue_edge_write(ns, [(self.room_id, "$victim", "$repaired", False)])
            self.assertEqual(queued_edge_write_count(ns), 2)
            flush_edge_writes(ns)
            back = get_event_edges_backward_batch(ns, ["$victim"])
            # Both the restored original and the repair write are present:
            # the restored row keeps the original edge reachable, and the
            # repair write adds the new edge.
            self.assertIn(("$parent", False), back["$victim"])
            self.assertIn(("$repaired", False), back["$victim"])
        finally:
            flush_edge_writes(ns)

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

    def test_edge_drain_failure_schedules_retry(self) -> None:
        """A failed timer drain retains rows and schedules a retry."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        ns = "test-edges-drain-retry"
        try:
            queue_edge_write(ns, [(self.room_id, "$retry", "$retry_parent", False)])

            with mock.patch(
                "synapse.storage.databases.main.embedded_event_edges.put_event_edges_batch",
                side_effect=RuntimeError("simulated timer FFI failure"),
            ):
                reactor.advance(FLUSH_DELAY_SECS + 0.1)

            self.assertEqual(queued_edge_write_count(ns), 1)
            self.assertIsNotNone(coalescer._delayed_call)

            reactor.advance(1.1)
            self.assertEqual(queued_edge_write_count(ns), 0)
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()
            flush_edge_writes()

    def test_purge_cancels_row_owned_by_purged_event(self) -> None:
        """A queued row whose event_id (row[1]) is purged is cancelled."""
        ns = "test-edges-purge-row1"
        victim = "$purged_owner"
        parent = "$owner_parent"
        try:
            queue_edge_write(ns, [(self.room_id, victim, parent, False)])
            self.assertEqual(queued_edge_write_count(ns), 1)

            delete_event_edges_batch(ns, [victim])

            # victim is row[1] and purged → row cancelled.
            self.assertEqual(queued_edge_write_count(ns), 0)
        finally:
            flush_edge_writes(ns)

    def test_purge_delete_failure_restores_cancelled_rows(self) -> None:
        """When the FFI delete fails, only rows owned by purged events (row[1])
        are restored — live-child → purged-parent rows were never cancelled."""
        ns = "test-edges-purge-fail-restore"
        victim = "$fail_victim"
        parent = "$fail_parent"
        try:
            # Row owned by the purged event (row[1] = victim).
            queue_edge_write(ns, [(self.room_id, victim, parent, False)])
            self.assertEqual(queued_edge_write_count(ns), 1)

            with mock.patch(
                "synapse.synapse_rust.mtxdb_engine.event_edges_delete",
                side_effect=RuntimeError("simulated FFI delete failure"),
            ):
                with self.assertRaises(RuntimeError):
                    delete_event_edges_batch(ns, [victim])

            # The cancelled row was restored to the queue for repair.
            self.assertEqual(queued_edge_write_count(ns), 1)
            flush_edge_writes(ns)
            back = get_event_edges_backward_batch(ns, [victim])
            self.assertIn((parent, False), back.get(victim) or [])
        finally:
            flush_edge_writes(ns)

    def test_restore_regression_row1_cancellation_and_timer_retry(self) -> None:
        """Regression: rows owned by purged events (row[1]) are cancelled,
        restored on delete failure, and later flushed by the coalescer timer.
        Live-child → purged-parent rows (backward extremities) survive purge."""
        ns = "test-edges-restore-regression"
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            # --- Cycle 1: successful purge cancels row[1]-owned row, ---
            # --- backward-extremity row (row[2] match) survives.     ---
            victim1 = "$reg_v1"
            parent1 = "$reg_p1"
            extremity1 = "$reg_ext1"
            queue_edge_write(
                ns,
                [
                    (
                        self.room_id,
                        victim1,
                        parent1,
                        False,
                    ),  # row[1] = victim → cancelled
                    (
                        self.room_id,
                        extremity1,
                        victim1,
                        False,
                    ),  # row[2] = victim → kept
                ],
            )
            self.assertEqual(queued_edge_write_count(ns), 2)

            delete_event_edges_batch(ns, [victim1])
            # The row-owned-by-victim1 was cancelled; the extremity was
            # remains queued for the normal coalescer.
            self.assertEqual(queued_edge_write_count(ns), 1)
            flush_edge_writes(ns)
            back_ext = get_event_edges_backward_batch(ns, [extremity1])
            self.assertEqual(back_ext[extremity1], [(victim1, False)])

            # --- Cycle 2: failed purge restores the cancelled row. ---
            victim2 = "$reg_v2"
            parent2 = "$reg_p2"
            queue_edge_write(ns, [(self.room_id, victim2, parent2, False)])
            self.assertEqual(queued_edge_write_count(ns), 1)

            with mock.patch(
                "synapse.synapse_rust.mtxdb_engine.event_edges_delete",
                side_effect=RuntimeError("simulated delete failure"),
            ):
                with self.assertRaises(RuntimeError):
                    delete_event_edges_batch(ns, [victim2])

            # The cancelled row was restored; the timer was (re)scheduled.
            self.assertEqual(queued_edge_write_count(ns), 1)
            self.assertIsNotNone(coalescer._delayed_call)

            # Advance past the debounce — the coalescer drains the row.
            reactor.advance(FLUSH_DELAY_SECS + 0.1)
            self.assertEqual(queued_edge_write_count(ns), 0)

            back_v2 = get_event_edges_backward_batch(ns, [victim2])
            self.assertIn((parent2, False), back_v2.get(victim2) or [])
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()
            flush_edge_writes(ns)

    def test_two_namespaces_flush_independently(self) -> None:
        """Rows in namespace A are not flushed when namespace B is drained."""
        ns_a = "test-edges-ns-a"
        ns_b = "test-edges-ns-b"
        try:
            queue_edge_write(ns_a, [(self.room_id, "$a1", "$a0", False)])
            queue_edge_write(ns_b, [(self.room_id, "$b1", "$b0", False)])
            self.assertEqual(queued_edge_write_count(ns_a), 1)
            self.assertEqual(queued_edge_write_count(ns_b), 1)

            # Flush only ns_a.
            flush_edge_writes(ns_a)
            self.assertEqual(queued_edge_write_count(ns_a), 0)
            self.assertEqual(queued_edge_write_count(ns_b), 1)

            # ns_b's row is still there.
            back_a = get_event_edges_backward_batch(ns_a, ["$a1"])
            self.assertEqual(back_a["$a1"], [("$a0", False)])
            back_b = get_event_edges_backward_batch(ns_b, ["$b1"])
            self.assertIsNone(back_b["$b1"])
        finally:
            flush_edge_writes(ns_a)
            flush_edge_writes(ns_b)

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
        # The coalescer window is auto-tuned to the store's backing disk
        # (2.0s when rotational) unless the config pinned it, so advance the
        # reactor by what this homeserver actually got instead of assuming
        # the module constant matches the runtime value.
        self._flush_delay_secs = (
            hs.config.database.embedded_hamt_flush_delay_secs or FLUSH_DELAY_SECS
        )
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

    def _advance_past_flush_window(self) -> None:
        """Advance the reactor past the coalescer's actual debounce window."""
        self.reactor.advance(self._flush_delay_secs + 0.1)

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

    @skipUnless(EMBEDDED_HAMT_ENGINE, "requires embedded HAMT engine")
    def test_below_threshold_queue_flushes_on_timer(self) -> None:
        """A sub-threshold edge write is drained by the flush coalescer's
        bounded debounce timer -- no explicit flush, threshold, or shutdown
        required -- and does so even when fsync is disabled (`no_sync`), since
        the queue must land in mtxdb regardless of the sync setting."""
        ns = self.store._embedded_hamt_namespace
        # Force the no-sync path so the test proves the edge drain is not
        # coupled to whether the sync coalescer actually fsyncs.
        previous_no_sync = embedded_common._sync_disabled
        embedded_common.configure_sync(no_sync=True)
        try:
            # Drain anything earlier tests left queued so this test controls
            # queue content: the single edge below is then far below threshold.
            flush_edge_writes(ns)
            res = self.helper.send(self.room_id, "ring", tok=self.tok)
            e_id = res["event_id"]

            with enable_ffi_counting():
                # Still queued (below threshold); mirror not written yet.
                self.assertEqual(get_ffi_count("event_edges_put_rows"), 0)
                self.assertGreaterEqual(
                    queued_edge_write_count(ns),
                    1,
                    "the persisted edge should be waiting in the coalescing queue",
                )

                # Advance the reactor past the flush window: the coalescer
                # drains the queue and syncs EVENT_DAG of its own accord.
                self._advance_past_flush_window()
                self.assertEqual(
                    queued_edge_write_count(ns),
                    0,
                    "the coalescer timer should have drained the queue",
                )
                self.assertGreater(get_ffi_count("event_edges_put_rows"), 0)

            backward = get_event_edges_backward_batch(ns, [e_id])
            self.assertIsNotNone(
                backward[e_id],
                "mirror should reflect the edge after the timer flush",
            )
            prev_ids = [p for p, _ in backward[e_id] or []]
            self.assertTrue(prev_ids)
        finally:
            embedded_common.configure_sync(no_sync=previous_no_sync)

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

    @skipUnless(EMBEDDED_HAMT_ENGINE, "requires embedded HAMT engine")
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

        ns = self.store._embedded_hamt_namespace
        # Persist the coalesced edges, then simulate a post-commit mirror write
        # that never landed by deleting the edge directly at the FFI layer.
        # Deliberately NOT `delete_event_edges_batch`: that records a purge
        # tombstone and this scenario is a lost write, not a purge.  (The purge
        # tombstone + SQL-fallback repair case is `test_sql_fallback_repairs_preserved_row2_edge_despite_purge`.)
        flush_edge_writes(ns)
        mtxdb_engine.event_edges_delete(ns, [c_id])

        # Verify mtxdb has no forward edge for p_id
        fwd_miss = get_event_edges_forward_batch(ns, [p_id])
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
        self._advance_past_flush_window()

        # Now mtxdb has been repaired in-process.
        fwd_repaired = get_event_edges_forward_batch(ns, [p_id])
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

    @skipUnless(EMBEDDED_HAMT_ENGINE, "requires embedded HAMT engine")
    def test_sql_fallback_repairs_preserved_row2_edge_despite_purge(self) -> None:
        """SQL keeps the live-child → purged-parent `event_edges` row (a
        backward extremity).  Even after a genuine purge tombstoned the parent,
        the SQL fallback still returns that edge and the repair enqueue is
        accepted: the tombstone is owner-based, so it only blocks rows whose
        own event_id is purged -- the child is not.  Without the owner-based
        filter this repair would be suppressed for the tombstone's TTL."""
        res1 = self.helper.send(self.room_id, "parent", tok=self.tok)
        p_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "child", tok=self.tok)
        c_id = res2["event_id"]

        ns = self.store._embedded_hamt_namespace
        flush_edge_writes(ns)

        # True purge of the parent: records an owner-based tombstone for p_id.
        delete_event_edges_batch(ns, [p_id])

        # Simulate the child's forward edge write never landing in mtxdb.
        mtxdb_engine.event_edges_delete(ns, [c_id])
        fwd_miss = get_event_edges_forward_batch(ns, [p_id])
        self.assertIsNone(fwd_miss.get(p_id))

        # get_successor_events falls back to the preserved SQL row and queues
        # the repair (child, purged parent).  The parent's tombstone must not
        # suppress it.
        successors = self.get_success(self.store.get_successor_events(p_id))
        self.assertIn(c_id, successors)

        self._advance_past_flush_window()

        # The preserved row[2] edge is repaired in mtxdb despite the tombstone.
        fwd_repaired = get_event_edges_forward_batch(ns, [p_id])
        self.assertIn(c_id, fwd_repaired.get(p_id) or [])
