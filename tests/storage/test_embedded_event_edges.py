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
from unittest import TestCase, mock, skipUnless
from uuid import uuid4

from prometheus_client import REGISTRY

from twisted.test.proto_helpers import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.storage.databases.main import (
    embedded_common,
    embedded_event_edges as embedded_event_edges_module,
)
from synapse.storage.databases.main.embedded_common import (
    FLUSH_DELAY_SECS,
    _clear_coalescer,
    _FlushCoalescer,
    _set_coalescer,
    enable_ffi_counting,
    get_ffi_count,
    suppress_diagnostic_timings,
)
from synapse.storage.databases.main.embedded_event_edges import (
    _retry_on_contention,
    delete_event_edges_batch,
    event_edge_publish_signal_gauge,
    flush_edge_writes,
    get_event_edges_backward_batch,
    get_event_edges_forward_batch,
    migrate_event_edges,
    put_event_edges_batch,
    queue_edge_write,
    queued_edge_write_count,
    record_event_edge_read,
    register_publish_signal_gauge,
)
from synapse.synapse_rust import mtxdb_engine
from synapse.util.clock import Clock

from tests import unittest
from tests.server import ThreadedMemoryReactorClock
from tests.test_utils.event_injection import create_event
from tests.unittest import HomeserverTestCase
from tests.utils import EMBEDDED_DB_ENGINE, EMBEDDED_DB_PATH

if EMBEDDED_DB_ENGINE and EMBEDDED_DB_PATH:
    # The test harness has already selected a worker-specific store. Reuse
    # that path instead of opening a private store before the homeserver is
    # created, which would win the Rust process-global OnceCell.
    _TEST_ENGINE_TMPDIR = EMBEDDED_DB_PATH
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

    def test_event_dag_barrier_failure_does_not_reset_existing_timer(self) -> None:
        """A failed narrow barrier must not cancel-and-reschedule an
        already-pending debounce timer (armed at ``_FLUSH_DELAY`` by
        ``mark_dirty``). Doing so under sustained failures -- e.g. calls
        arriving faster than ``_RETRY_DELAY`` apart, plausible since this
        barrier runs per persisted event -- would perpetually push the timer
        out and starve ``_flush`` forever: the same livelock shape the
        success path avoids, just triggered by errors instead of successes.
        An existing timer, whatever its delay, must be left to fire on its
        own; only the absence of any timer should cause a new one to be
        armed at ``_RETRY_DELAY``."""
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
            # Unchanged: the pre-existing _FLUSH_DELAY timer was left alone,
            # not cancelled and re-armed at _RETRY_DELAY.
            self.assertEqual(coalescer._delayed_call.getTime(), pending_at_flush_delay)
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_sync_now_drain_failure_reschedules_and_raises(self) -> None:
        """A failing ``_drain_edge_writes()`` inside ``sync_now`` must not
        strand EVENT_DAG: the timer is cancelled at the top of the method, so
        a raise from the drain has to still reach the retry-scheduling
        ``finally`` (via the outer try/except) or nothing would ever retry
        it -- the same stranded-work shape as the narrow-barrier livelock
        fixed earlier, just via the drain instead of the sync call itself."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            with (
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._drain_edge_writes",
                    side_effect=RuntimeError("simulated drain failure"),
                ),
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._do_sync_pools"
                ) as sync,
            ):
                with self.assertRaises(RuntimeError):
                    coalescer.sync_now(pools=[embedded_common.Pool.EVENT_DAG])
                # The sync call must never be reached: the drain failed first.
                sync.assert_not_called()
            self.assertIn(embedded_common.Pool.EVENT_DAG, coalescer._dirty)
            self.assertIsNotNone(coalescer._delayed_call)
            assert coalescer._delayed_call is not None
            self.assertAlmostEqual(
                coalescer._delayed_call.getTime(), reactor.seconds() + 1.0, places=3
            )
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_sync_now_attributes_failure_to_failing_pool_only(self) -> None:
        """Per-pool sync failures must not cross-contaminate: if EVENT_DAG
        syncs cleanly but STATE fails, only STATE should end up re-dirtied,
        and the retry timer should use the failure backoff since the call
        did not fully succeed."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:

            def fake_sync(pools: set) -> None:
                if embedded_common.Pool.STATE in pools:
                    raise RuntimeError("simulated state sync failure")

            with (
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._drain_edge_writes",
                    return_value=False,
                ),
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._do_sync_pools",
                    side_effect=fake_sync,
                ),
            ):
                with self.assertRaises(RuntimeError):
                    coalescer.sync_now(
                        pools=[
                            embedded_common.Pool.EVENT_DAG,
                            embedded_common.Pool.STATE,
                        ]
                    )
            self.assertNotIn(embedded_common.Pool.EVENT_DAG, coalescer._dirty)
            self.assertIn(embedded_common.Pool.STATE, coalescer._dirty)
            self.assertIsNotNone(coalescer._delayed_call)
            assert coalescer._delayed_call is not None
            self.assertAlmostEqual(
                coalescer._delayed_call.getTime(), reactor.seconds() + 1.0, places=3
            )
        finally:
            _clear_coalescer(coalescer)
            coalescer.close()

    def test_sync_now_success_with_leftover_dirty_uses_flush_delay(self) -> None:
        """When ``sync_now`` fully succeeds but an unrelated pool is left
        dirty (e.g. a concurrent write raced in), the rearmed timer should
        use the normal debounce delay, not the failure backoff -- that pool
        never failed anything this call."""
        reactor = ThreadedMemoryReactorClock()
        clock = Clock(reactor, server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        _set_coalescer(coalescer)
        try:
            coalescer._dirty.add(embedded_common.Pool.AUTH_CHAIN)
            with (
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._drain_edge_writes",
                    return_value=False,
                ),
                mock.patch(
                    "synapse.storage.databases.main.embedded_common._do_sync_pools"
                ),
            ):
                coalescer.sync_now(pools=[embedded_common.Pool.EVENT_DAG])
            self.assertIn(embedded_common.Pool.AUTH_CHAIN, coalescer._dirty)
            self.assertIsNotNone(coalescer._delayed_call)
            assert coalescer._delayed_call is not None
            self.assertAlmostEqual(
                coalescer._delayed_call.getTime(), reactor.seconds() + 0.5, places=3
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
        enqueue_lock_attempted = threading.Event()
        enqueue_finished = threading.Event()
        thread_role = threading.local()

        real_namespace_flush_lock = embedded_event_edges_module._namespace_flush_lock

        class TrackingLock:
            def __init__(self, lock: threading.Lock) -> None:
                self._lock = lock

            def acquire(self, *args: Any, **kwargs: Any) -> Any:
                if getattr(thread_role, "is_enqueue", False):
                    enqueue_lock_attempted.set()
                return self._lock.acquire(*args, **kwargs)

            def release(self) -> None:
                self._lock.release()

            def __enter__(self) -> "TrackingLock":
                self.acquire()
                return self

            def __exit__(self, *args: Any) -> None:
                self.release()

        def tracking_namespace_flush_lock(namespace: str) -> TrackingLock:
            return TrackingLock(real_namespace_flush_lock(namespace))

        def slow_delete(*args: Any, **kwargs: Any) -> Any:
            delete_entered.set()
            if not delete_release.wait(timeout=5):
                raise TimeoutError("mocked delete was never released")
            return real_delete(*args, **kwargs)

        def purge() -> None:
            delete_event_edges_batch(ns, [victim])

        def enqueue() -> None:
            enqueue_started.set()
            thread_role.is_enqueue = True
            try:
                queue_edge_write(ns, [(self.room_id, victim, parent, False)])
                enqueue_finished.set()
            finally:
                thread_role.is_enqueue = False

        try:
            with (
                suppress_diagnostic_timings(),
                mock.patch(
                    "synapse.synapse_rust.mtxdb_engine.event_edges_delete",
                    side_effect=slow_delete,
                ),
                mock.patch.object(
                    embedded_event_edges_module,
                    "_namespace_flush_lock",
                    side_effect=tracking_namespace_flush_lock,
                ),
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
                self.assertTrue(
                    enqueue_lock_attempted.wait(timeout=5),
                    "enqueue never attempted the namespace flush lock",
                )
                self.assertFalse(
                    enqueue_finished.is_set(),
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


class ContentionRetryCounterTestCase(unittest.TestCase):
    """`_retry_on_contention` makes journal/checkpoint contention observable."""

    def test_counts_retries_then_succeeds(self) -> None:
        values: list[str | Exception] = [BlockingIOError(), BlockingIOError(), "ok"]
        calls = iter(values)

        def flaky() -> str:
            item = next(calls)
            if isinstance(item, Exception):
                raise item
            return item

        with enable_ffi_counting():
            self.assertEqual(_retry_on_contention(flaky), "ok")
            self.assertEqual(get_ffi_count("event_edges_contention_retries"), 2)
            self.assertEqual(get_ffi_count("event_edges_contention_exhausted"), 0)

    def test_counts_exhaustion(self) -> None:
        def always() -> None:
            raise BlockingIOError()

        with enable_ffi_counting():
            with self.assertRaises(BlockingIOError):
                _retry_on_contention(always)
            self.assertGreaterEqual(get_ffi_count("event_edges_contention_retries"), 1)
            self.assertEqual(get_ffi_count("event_edges_contention_exhausted"), 1)


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
            "auth_chain": True,
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
                    ("auth_chain", 1.0),
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

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        # The coalescer window is auto-tuned to the store's backing disk
        # (2.0s when rotational) unless the config pinned it, so advance the
        # reactor by what this homeserver actually got instead of assuming
        # the module constant matches the runtime value.
        self._flush_delay_secs = (
            hs.config.database.embedded_db_flush_delay_secs or FLUSH_DELAY_SECS
        )
        # The process-global embedded FWD index is keyed by room_id, not by
        # `_embedded_db_namespace`, and each test here gets a distinct namespace
        # but deterministic create-event content. Register a unique creator so
        # the create event -- and therefore the room id -- differs per test and
        # the shared FWD meta/collections cannot leak between tests (see the
        # room_forward_meta key layout).
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
        if not getattr(self.store, "_embedded_db_namespace", None):
            self.store._embedded_db_namespace = hs.hostname

        self.persist_store = hs.get_datastores().persist_events
        if self.persist_store is not None:
            self.persist_store._embedded_event_edges_enabled = True
            self.persist_store._embedded_event_edges_writable = True
            self.persist_store._embedded_db_engine = "mtxdb"
            if not getattr(self.persist_store, "_embedded_db_namespace", None):
                self.persist_store._embedded_db_namespace = hs.hostname

    def tearDown(self) -> None:
        # The embedded edge engine is process-global (one mtxdb store per
        # process, one active flush coalescer). Drain the coalesced edge-write
        # queues and shut down this test's coalescer so queued rows, tombstones,
        # pending delayed flush calls, and dirty EVENT_DAG/EJSON pools cannot
        # leak into the next test's process-global mtxdb state.
        try:
            flush_edge_writes()
            embedded_common.close_coalescer()
        finally:
            super().tearDown()

    def _advance_past_flush_window(self) -> None:
        """Advance the reactor past the coalescer's actual debounce window."""
        self.reactor.advance(self._flush_delay_secs + 0.1)

    def test_event_edges_stored_in_mtxdb_on_persistence(self) -> None:
        """When mtxdb is authoritative, persisted edges are stored there."""
        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        # e2 should have e1 in its backward edges in mtxdb
        backward = get_event_edges_backward_batch(
            self.store._embedded_db_namespace, [e2_id]
        )
        self.assertIsNotNone(backward[e2_id])
        prev_ids = [p for p, _ in backward[e2_id] or []]
        self.assertIn(e1_id, prev_ids)

        # e1 should have e2 in its forward successors in mtxdb
        successors = self.get_success(self.store.get_successor_events(e1_id))
        self.assertIn(e2_id, successors)

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_below_threshold_edge_committed_synchronously(self) -> None:
        """In authoritative MTXDB mode a persisted edge is committed to mtxdb
        synchronously in the persistence transaction's post-commit callback --
        it is already visible on the next read and is never left waiting in
        the coalescer, even below the batch threshold and with fsync disabled
        (`no_sync`)."""
        ns = self.store._embedded_db_namespace
        # Force the no-sync path so the test proves the edge commit is not
        # coupled to whether the sync coalescer actually fsyncs.
        previous_no_sync = embedded_common._sync_disabled
        embedded_common.configure_sync(no_sync=True)
        try:
            # Drain anything earlier tests left queued so this test controls
            # queue content.
            flush_edge_writes(ns)
            with enable_ffi_counting():
                res = self.helper.send(self.room_id, "ring", tok=self.tok)
                e_id = res["event_id"]

                # Committed synchronously; nothing is left queued.
                self.assertGreater(get_ffi_count("event_edges_put_rows"), 0)
                self.assertEqual(
                    queued_edge_write_count(ns),
                    0,
                    "authoritative writes must not leave the edge queued",
                )

            backward = get_event_edges_backward_batch(ns, [e_id])
            self.assertIsNotNone(
                backward[e_id],
                "edge should already be visible in mtxdb",
            )
            prev_ids = [p for p, _ in backward[e_id] or []]
            self.assertTrue(prev_ids)
        finally:
            embedded_common.configure_sync(no_sync=previous_no_sync)

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
        flush_edge_writes(self.store._embedded_db_namespace)
        return str(event.event_id)

    def _edge_views(self, event_id: str) -> tuple[list[str], list[str]]:
        """The event's prev edges as recorded in SQL and in mtxdb."""
        sql = self.get_success(
            self.store.db_pool.simple_select_onecol(
                "event_edges", {"event_id": event_id}, "prev_event_id"
            )
        )
        backward = get_event_edges_backward_batch(
            self.store._embedded_db_namespace, [event_id]
        )
        return sorted(sql), sorted(p for p, _ in backward.get(event_id) or [])

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
        """A rejected event is stored and recorded in `rejections`, but the
        rejected-events filter runs before edge writes, so it has no edges in
        SQL or mtxdb."""
        event_id = self._persist_message(rejected="test rejection")

        rejection = self.get_success(
            self.store.db_pool.simple_select_one_onecol(
                "rejections", {"event_id": event_id}, "reason", allow_none=True
            )
        )
        self.assertEqual(rejection, "test rejection")
        sql, mtxdb = self._edge_views(event_id)
        self.assertEqual(sql, [])
        self.assertEqual(mtxdb, [])

    def test_event_edges_purge_cleans_forward_edges(self) -> None:
        """Purging an event removes it from its parents' forward lists in mtxdb."""
        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        # Drain the coalesced edge writes so the mirror reflects both sends.
        flush_edge_writes(self.store._embedded_db_namespace)

        # Before deletion: e1 has e2 as forward successor
        fwd_before = get_event_edges_forward_batch(
            self.store._embedded_db_namespace, [e1_id]
        )
        self.assertIn(e2_id, fwd_before.get(e1_id) or [])

        # Purge e2
        delete_event_edges_batch(self.store._embedded_db_namespace, [e2_id])

        # After deletion: e2 is tombstoned in backward edges
        back_after = get_event_edges_backward_batch(
            self.store._embedded_db_namespace, [e2_id]
        )
        self.assertIsNone(back_after[e2_id])

        # And e1's forward edges no longer contain e2
        fwd_after = get_event_edges_forward_batch(
            self.store._embedded_db_namespace, [e1_id]
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
            self.store._embedded_db_namespace, [fake_id]
        )
        self.assertIsNone(back[fake_id])

        # And e1_id's forward edges do NOT contain fake_id
        fwd = get_event_edges_forward_batch(self.store._embedded_db_namespace, [e1_id])
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


class EventEdgesSqlOnlyMetricTestCase(EventEdgesStorageIntegrationTestCase):
    """With the embedded backend off for a store, lookups bypass mtxdb and must
    still be counted, so `sql_only` gives a denominator for all lookups."""

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_gap_lookup_counts_hit(self) -> None:
        """`is_event_next_to_forward_gap` records an mtxdb hit."""
        parent_id = self.helper.send(self.room_id, "gap-parent", tok=self.tok)[
            "event_id"
        ]
        self.helper.send(self.room_id, "gap-child", tok=self.tok)
        parent = self.get_success(self.store.get_event(parent_id))
        ns = self.store._embedded_db_namespace
        flush_edge_writes(ns)
        sn = self.store.server_name

        hit0 = _reads(sn, "gap", "hit")
        self.assertFalse(
            self.get_success(self.store.is_event_next_to_forward_gap(parent))
        )
        self.assertEqual(
            _reads(sn, "gap", "hit") - hit0,
            1,
            [_reads(sn, "gap", o) for o in ("hit", "miss", "sql_only")],
        )

        hit1 = _reads(sn, "gap", "hit")
        self.assertFalse(
            self.get_success(self.store.is_event_next_to_forward_gap(parent))
        )
        self.assertEqual(_reads(sn, "gap", "hit") - hit1, 1)

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_disabled_embedded_edges_count_sql_only_on_both_callers(self) -> None:
        parent_id = self.helper.send(self.room_id, "so-parent", tok=self.tok)[
            "event_id"
        ]
        self.helper.send(self.room_id, "so-child", tok=self.tok)
        parent = self.get_success(self.store.get_event(parent_id))
        sn = self.store.server_name
        self.store._embedded_event_edges_enabled = False

        succ0 = _reads(sn, "successor", "sql_only")
        gap0 = _reads(sn, "gap", "sql_only")
        self.get_success(self.store.get_successor_events(parent_id))
        self.get_success(self.store.is_event_next_to_forward_gap(parent))
        self.assertEqual(_reads(sn, "successor", "sql_only") - succ0, 1)
        self.assertEqual(_reads(sn, "gap", "sql_only") - gap0, 1)

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_unresolvable_successor_lookup_counts_miss(self) -> None:
        sn = self.store.server_name
        miss0 = _reads(sn, "successor", "miss")
        self.assertEqual(
            self.get_success(
                self.store.get_successor_events("$unknown-event:example.org")
            ),
            [],
        )
        self.assertEqual(_reads(sn, "successor", "miss") - miss0, 1)

    @skipUnless(EMBEDDED_DB_ENGINE, "requires embedded DB engine")
    def test_successor_lookup_counts_a_clean_miss(self) -> None:
        parent_id = self.helper.send(self.room_id, "miss-parent", tok=self.tok)[
            "event_id"
        ]
        ns = self.store._embedded_db_namespace
        flush_edge_writes(ns)

        sn = self.store.server_name
        miss0 = _reads(sn, "successor", "miss")
        self.assertEqual(
            self.get_success(self.store.get_successor_events(parent_id)), []
        )
        self.assertEqual(_reads(sn, "successor", "miss") - miss0, 1)


class EventEdgesMigrationTestCase(EventEdgesStorageIntegrationTestCase):
    """Manual SQL <-> mtxdb edge migration and its marker handoff."""

    def _migrate(self, **kwargs: Any) -> Any:
        # This module opens one process-global mtxdb fixture even when the
        # homeserver config is SQL-only. Tell the migration code that fixture
        # is available; the ordinary integration fixture enables edge reads
        # and writes on the stores without changing the homeserver config.
        with mock.patch.object(
            embedded_event_edges_module,
            "open_embedded_event_edges_engine",
            return_value=True,
        ):
            return self.get_success(migrate_event_edges(self.hs, **kwargs))

    def _marker(self) -> str:
        return self.get_success(
            self.store.db_pool.simple_select_one_onecol(
                "edge_backend", {"lock": "X"}, "backend"
            )
        )

    def _set_marker(self, backend: str) -> None:
        def _set(txn: Any) -> None:
            txn.execute(
                "UPDATE edge_backend SET backend = ? WHERE lock = 'X'",
                (backend,),
            )
            if txn.rowcount == 0:
                txn.execute(
                    "INSERT INTO edge_backend (lock, backend) VALUES ('X', ?)",
                    (backend,),
                )

        self.get_success(self.store.db_pool.runInteraction("set_edge_backend", _set))

    def _sql_edge_count(self) -> int:
        def _count(txn: Any) -> int:
            txn.execute("SELECT COUNT(*) FROM event_edges")
            row = txn.fetchone()
            return int(row[0]) if row else 0

        return self.get_success(
            self.store.db_pool.runInteraction("count_sql_edges", _count)
        )

    def test_stale_sql_marker_refuses_to_wipe_mtxdb(self) -> None:
        parent_id = self.helper.send(self.room_id, "parent", tok=self.tok)["event_id"]
        child_id = self.helper.send(self.room_id, "child", tok=self.tok)["event_id"]
        ns = self.store._embedded_db_namespace
        flush_edge_writes(ns)
        self.get_success(
            self.store.db_pool.runInteraction(
                "clear_sql_edges",
                lambda txn: txn.execute("DELETE FROM event_edges"),
            )
        )
        self.assertEqual(self._sql_edge_count(), 0)
        self._set_marker("sql")

        with mock.patch.object(
            embedded_event_edges_module,
            "open_embedded_event_edges_engine",
            return_value=True,
        ):
            failure = self.get_failure(
                migrate_event_edges(self.hs, target="mtxdb"), RuntimeError
            )
        self.assertIn("marker may be stale", str(failure.value))
        backward = get_event_edges_backward_batch(ns, [child_id])
        self.assertIn((parent_id, False), backward.get(child_id) or [])

        # Explicitly adopting the existing mtxdb data flips the stale marker.
        result = self._migrate(target="mtxdb", source="mtxdb")
        self.assertEqual(result["already"], 1)
        self.assertEqual(self._marker(), "mtxdb")

    def test_sql_to_mtxdb_copies_and_flips_marker(self) -> None:
        parent_id = self.helper.send(self.room_id, "parent", tok=self.tok)["event_id"]
        child_id = self.helper.send(self.room_id, "child", tok=self.tok)["event_id"]
        ns = self.store._embedded_db_namespace

        flush_edge_writes(ns)
        # Simulate the pre-migration state: the edge is in SQL, not mtxdb.
        delete_event_edges_batch(ns, [child_id])
        self.get_success(
            self.store.db_pool.runInteraction(
                "clear_sql_edges",
                lambda txn: txn.execute("DELETE FROM event_edges"),
            )
        )
        self.get_success(
            self.store.db_pool.simple_insert(
                table="event_edges",
                values={
                    "event_id": child_id,
                    "prev_event_id": parent_id,
                    "room_id": self.room_id,
                    "is_state": False,
                },
            )
        )
        self._set_marker("sql")

        result = self._migrate(target="mtxdb")

        self.assertEqual(result["source"], "sql")
        self.assertEqual(result["target"], "mtxdb")
        self.assertEqual(result["edges"], 1)
        self.assertEqual(self._marker(), "mtxdb")
        backward = get_event_edges_backward_batch(ns, [child_id])
        self.assertIn((parent_id, False), backward.get(child_id) or [])

    def test_mtxdb_to_sql_replaces_stale_rows_and_flips_marker(self) -> None:
        parent_id = self.helper.send(self.room_id, "parent", tok=self.tok)["event_id"]
        child_id = self.helper.send(self.room_id, "child", tok=self.tok)["event_id"]
        ns = self.store._embedded_db_namespace

        flush_edge_writes(ns)
        self._set_marker("mtxdb")
        # A stale SQL row from before the switch must not survive the rebuild.
        self.get_success(
            self.store.db_pool.simple_insert(
                table="event_edges",
                values={
                    "event_id": parent_id,
                    "prev_event_id": child_id,
                    "room_id": self.room_id,
                    "is_state": False,
                },
            )
        )

        result = self._migrate(target="sql")

        self.assertEqual(result["source"], "mtxdb")
        self.assertEqual(result["target"], "sql")
        self.assertEqual(self._marker(), "sql")
        rows = self.get_success(
            self.store.db_pool.simple_select_onecol(
                "event_edges", {"event_id": child_id}, "prev_event_id"
            )
        )
        self.assertIn(parent_id, rows)
        stale = self.get_success(
            self.store.db_pool.simple_select_onecol(
                "event_edges", {"event_id": parent_id}, "prev_event_id"
            )
        )
        self.assertNotIn(child_id, stale)


class EventEdgesBackendAdoptionTestCase(HomeserverTestCase):
    """A database with no events adopts whatever backend it is configured for."""

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.hs = hs
        self.store = hs.get_datastores().main

    def test_empty_database_adopts_configured_backend(self) -> None:
        def _count(txn: Any) -> tuple[int, str]:
            txn.execute("SELECT COUNT(*) FROM events")
            events = int(txn.fetchone()[0])
            txn.execute("SELECT backend FROM edge_backend WHERE lock = 'X'")
            row = txn.fetchone()
            return events, str(row[0]) if row else "sql"

        events, marker = self.get_success(
            self.store.db_pool.runInteraction("inspect_empty_db", _count)
        )
        self.assertEqual(events, 0)
        self.assertEqual(marker, "sql")

        with mock.patch.object(
            embedded_event_edges_module,
            "configured_event_edges_backend",
            return_value="mtxdb",
        ):
            self.get_success(
                embedded_event_edges_module.check_event_edges_backend(self.hs)
            )

        self.assertEqual(
            self.get_success(
                self.store.db_pool.simple_select_one_onecol(
                    "edge_backend", {"lock": "X"}, "backend"
                )
            ),
            "mtxdb",
        )


class EventEdgesBackendMarkerTestCase(EventEdgesStorageIntegrationTestCase):
    """`check_event_edges_backend`: startup refuses a backend that does not
    hold the migrated edges."""

    def _set_marker(self, backend: str) -> None:
        def _set(txn: Any) -> None:
            txn.execute(
                "UPDATE edge_backend SET backend = ? WHERE lock = 'X'",
                (backend,),
            )
            if txn.rowcount == 0:
                txn.execute(
                    "INSERT INTO edge_backend (lock, backend) VALUES ('X', ?)",
                    (backend,),
                )

        self.get_success(self.store.db_pool.runInteraction("set_marker", _set))

    def _check(self, configured: str) -> None:
        with mock.patch.object(
            embedded_event_edges_module,
            "configured_event_edges_backend",
            return_value=configured,
        ):
            self.get_success(
                embedded_event_edges_module.check_event_edges_backend(self.hs)
            )

    def test_matching_backend_starts(self) -> None:
        self._set_marker("mtxdb")
        self._check("mtxdb")

    def test_mismatch_with_events_panics(self) -> None:
        self._set_marker("sql")
        with mock.patch.object(
            embedded_event_edges_module,
            "configured_event_edges_backend",
            return_value="mtxdb",
        ):
            self.get_failure(
                embedded_event_edges_module.check_event_edges_backend(self.hs),
                embedded_event_edges_module.EventEdgesBackendMismatchError,
            )
