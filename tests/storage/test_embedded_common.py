#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

import os
import threading
import time
from typing import cast
from unittest import TestCase, mock

from synapse.storage.databases.main import embedded_common
from synapse.storage.databases.main.embedded_common import (
    Pool,
    SyncMode,
    SyncTier,
    _format_mtxdb_snapshot_segment,
    _mtxdb_snapshot_metrics,
)
from synapse.util.duration import Duration


class MtxdbSnapshotMetricsTestCase(TestCase):
    def test_extracts_cumulative_and_interval_diagnostics(self) -> None:
        metrics = _mtxdb_snapshot_metrics(
            {
                "index_bytes": 123,
                "sync_totals": {
                    "calls": 7,
                    "total_us": 800,
                    "pending_publish_age_us": 350,
                    "max_journal_lock_wait_us": 90,
                },
                "sync_diagnostics": {"peak_journal_in_flight": 4},
            }
        )

        self.assertEqual(metrics["sync_calls"], 7)
        self.assertEqual(metrics["sync_us"], 800)
        self.assertEqual(metrics["pending_publish_age_us"], 350)
        self.assertEqual(metrics["max_journal_lock_wait_us"], 90)
        self.assertEqual(metrics["peak_journal_in_flight"], 4)

    def test_missing_optional_metrics_default_to_zero(self) -> None:
        metrics = _mtxdb_snapshot_metrics({})

        self.assertEqual(metrics["pending_publish_age_us"], 0)
        self.assertEqual(metrics["peak_journal_in_flight"], 0)
        self.assertEqual(metrics["journal_coalesced"], 0)

    def test_formats_snapshot_segment(self) -> None:
        metrics = _mtxdb_snapshot_metrics(
            {
                "sync_totals": {
                    "calls": 2,
                    "total_us": 5000,
                    "pending_publish_age_us": 3000,
                    "max_journal_lock_wait_us": 7000,
                    "max_journal_fsync_us": 9000,
                },
                "sync_diagnostics": {"peak_journal_in_flight": 3},
            }
        )
        delta = dict(metrics)

        segment = _format_mtxdb_snapshot_segment("event_dag", metrics, delta)

        self.assertIn("event_dag[", segment)
        self.assertIn("peak=3", segment)
        self.assertIn("lmax=7.0/9.0ms", segment)
        self.assertIn("age_avg=1.5ms", segment)


class SyncDisabledPublicationTestCase(TestCase):
    def test_sync_disabled_suppresses_fsync_and_touches_nothing_else(self) -> None:
        """Durability off must not also turn off cross-process visibility.

        ``_sync_disabled`` is set by the test-only no-sync mode. It gates the
        durability barriers, but publication is a read-freshness operation: if
        it were gated too, a worker would still miss committed writes whenever
        the writer deliberately skipped fsync.
        """
        engine = mock.Mock()
        with (
            mock.patch.dict(os.environ, {"SYNAPSE_MTXDB_WAL": "1"}),
            mock.patch.object(embedded_common, "_engine_configured", True),
            mock.patch.object(embedded_common, "_sync_disabled", True),
            mock.patch(
                "synapse.storage.databases.embedded_engine.get_embedded_engine",
                return_value=engine,
            ),
        ):
            embedded_common.maybe_sync(SyncTier.DURABLE, pools=[Pool.EVENT_DAG])
            engine.sync.assert_not_called()
            engine.sync_state.assert_not_called()
            engine.sync_event_dag.assert_not_called()
            self.assertEqual(engine.mock_calls, [])

    def test_sync_now_with_durability_off_is_a_noop(self) -> None:
        """`sync_now` (the barrier most call sites use) must not become a no-op."""
        engine = mock.Mock()
        with (
            mock.patch.dict(os.environ, {"SYNAPSE_MTXDB_WAL": "1"}),
            mock.patch.object(embedded_common, "_engine_configured", True),
            mock.patch.object(embedded_common, "_sync_disabled", True),
            mock.patch.object(embedded_common, "_coalescer", None),
            mock.patch(
                "synapse.storage.databases.embedded_engine.get_embedded_engine",
                return_value=engine,
            ),
        ):
            embedded_common.sync_now([Pool.STATE])

        engine.sync_state.assert_not_called()
        self.assertEqual(engine.mock_calls, [])


class GroupCommitWiringTestCase(TestCase):
    def test_lifecycle_and_durability_request_are_forwarded(self) -> None:
        engine = mock.Mock()
        with (
            mock.patch.object(embedded_common, "_engine_configured", True),
            mock.patch.object(embedded_common, "_sync_disabled", False),
            mock.patch.object(
                embedded_common, "publishes_at_commit", return_value=True
            ),
            mock.patch(
                "synapse.storage.databases.embedded_engine.get_embedded_engine",
                return_value=engine,
            ),
        ):
            embedded_common.start_background_commit(0.25, max_pending=17)
            embedded_common.request_durable()
            embedded_common.stop_background_commit()

        engine.start_background_commit.assert_called_once_with(250, 17)
        engine.request_durable.assert_called_once_with()
        engine.stop_background_commit.assert_called_once_with()

    def test_interval_flush_requests_durability(self) -> None:
        coalescer = object.__new__(embedded_common._FlushCoalescer)
        coalescer._delayed_call = None
        coalescer._closed = False
        coalescer._active_sync_barriers = 0
        coalescer._dirty = {Pool.STATE, Pool.EVENT_DAG}
        calls: list[str] = []

        with (
            mock.patch.object(
                embedded_common, "publishes_at_commit", return_value=True
            ),
            mock.patch.object(
                embedded_common,
                "request_durable",
                side_effect=lambda: calls.append("request"),
            ),
            mock.patch.object(
                embedded_common, "background_commit_error", return_value=None
            ),
        ):
            coalescer._flush()

        # The edge log makes tail flushing independent of the WAL boundary, so
        # the coalescer only asks for durability.
        self.assertEqual(calls, ["request"])
        self.assertFalse(coalescer._dirty)

    def test_background_failure_keeps_dirty_pools_for_retry(self) -> None:
        coalescer = object.__new__(embedded_common._FlushCoalescer)
        coalescer._delayed_call = None
        coalescer._closed = False
        coalescer._active_sync_barriers = 0
        coalescer._dirty = {Pool.EVENT_DAG}
        coalescer._clock = mock.Mock()
        coalescer._RETRY_DELAY = Duration(seconds=1)

        with (
            mock.patch.object(
                embedded_common, "publishes_at_commit", return_value=True
            ),
            mock.patch.object(embedded_common, "request_durable"),
            mock.patch.object(
                embedded_common,
                "background_commit_error",
                return_value="disk full",
            ),
            # Strict recovery fails too, so the pools must be kept for retry.
            mock.patch.object(
                embedded_common,
                "_do_sync_pools",
                side_effect=RuntimeError("still failing"),
            ),
            mock.patch.object(embedded_common, "stop_background_commit"),
            mock.patch.object(embedded_common, "start_background_commit"),
        ):
            coalescer._flush()

        self.assertEqual(coalescer._dirty, {Pool.EVENT_DAG})
        coalescer._clock.call_later.assert_called_once_with(
            coalescer._RETRY_DELAY, coalescer._flush
        )

    def test_background_failure_recovers_with_strict_sync_and_restart(self) -> None:
        coalescer = object.__new__(embedded_common._FlushCoalescer)
        coalescer._delayed_call = None
        coalescer._closed = False
        coalescer._active_sync_barriers = 0
        coalescer._dirty = {Pool.STATE, Pool.EVENT_DAG}
        coalescer._FLUSH_DELAY = mock.Mock()
        coalescer._FLUSH_DELAY.as_secs.return_value = 0.5
        calls: list[object] = []

        def failed_stop() -> None:
            calls.append("stop")
            raise RuntimeError("old failure")

        with (
            mock.patch.object(
                embedded_common, "publishes_at_commit", return_value=True
            ),
            mock.patch.object(
                embedded_common,
                "request_durable",
                side_effect=lambda: calls.append("request"),
            ),
            mock.patch.object(
                embedded_common, "background_commit_error", return_value="disk full"
            ),
            mock.patch.object(
                embedded_common,
                "_do_sync_pools",
                side_effect=lambda pools: calls.append(("strict", pools)),
            ),
            mock.patch.object(
                embedded_common,
                "stop_background_commit",
                side_effect=failed_stop,
            ),
            mock.patch.object(
                embedded_common,
                "start_background_commit",
                side_effect=lambda interval: calls.append(("start", interval)),
            ),
        ):
            coalescer._flush()

        self.assertEqual(coalescer._dirty, set())
        self.assertEqual(calls[0], "request")
        strict_call = cast(tuple[str, object], calls[1])
        self.assertEqual(strict_call[0], "strict")
        self.assertEqual(calls[2:], ["stop", ("start", 0.5)])


class SyncModeTestCase(TestCase):
    def setUp(self) -> None:
        self.addCleanup(
            embedded_common.configure_sync,
            no_sync=embedded_common._sync_disabled,
            mode=embedded_common._sync_mode,
        )

    def _publishes(self, mode: SyncMode | None, *, no_sync: bool, wal: str) -> bool:
        embedded_common.configure_sync(no_sync=no_sync, mode=mode)
        with mock.patch.dict(os.environ, {"SYNAPSE_MTXDB_WAL": wal}):
            return embedded_common.publishes_at_commit()

    def test_default_mode_syncs_per_persist(self) -> None:
        """Unset/`always` keeps the per-persist barriers, with or without the WAL."""
        for wal in ("", "1"):
            self.assertFalse(self._publishes(None, no_sync=False, wal=wal))
            self.assertFalse(self._publishes(SyncMode.ALWAYS, no_sync=False, wal=wal))

    def test_interval_and_off_defer_durability_only_with_the_wal(self) -> None:
        for mode in (SyncMode.INTERVAL, SyncMode.OFF):
            self.assertTrue(self._publishes(mode, no_sync=False, wal="1"))
            # No WAL, no journal to defer to: fall back to the barriers.
            self.assertFalse(self._publishes(mode, no_sync=False, wal=""))

    def test_no_sync_means_off(self) -> None:
        """The older boolean switch still disables fsync, and defers like `off`."""
        self.assertTrue(self._publishes(None, no_sync=True, wal="1"))
        self.assertTrue(embedded_common._sync_disabled)
        self.assertIs(embedded_common._sync_mode, SyncMode.OFF)

    def test_only_off_disables_fsync(self) -> None:
        embedded_common.configure_sync(no_sync=False, mode=SyncMode.INTERVAL)
        self.assertFalse(embedded_common._sync_disabled)
        embedded_common.configure_sync(no_sync=False, mode=SyncMode.OFF)
        self.assertTrue(embedded_common._sync_disabled)


class FfiCountingScopeTestCase(TestCase):
    def test_counting_scope_keeps_counts_made_before_it(self) -> None:
        """A test that scopes its own counters must not wipe the run's totals."""
        with mock.patch.dict(embedded_common._FFI_COUNTERS, {"before": 3}, clear=True):
            with embedded_common.enable_ffi_counting():
                # Clean inside the block, so before/after snapshots work.
                self.assertEqual(embedded_common._FFI_COUNTERS.get("before", 0), 0)
                embedded_common.ffi_count("inside", 2)
                embedded_common.ffi_count("before", 1)

            self.assertEqual(embedded_common._FFI_COUNTERS["before"], 4)
            self.assertEqual(embedded_common._FFI_COUNTERS["inside"], 2)


class ShutdownCheckpointTestCase(TestCase):
    """A clean stop leaves a durable checkpoint beside the final sync."""

    def _patched_engine(self, engine: mock.Mock, *, sync_disabled: bool = False):  # type: ignore[no-untyped-def]
        return (
            mock.patch.object(embedded_common, "_engine_configured", True),
            mock.patch.object(embedded_common, "_sync_disabled", sync_disabled),
            mock.patch(
                "synapse.storage.databases.embedded_engine.get_embedded_engine",
                return_value=engine,
            ),
        )

    def test_checkpoint_pools_forwards_to_the_engine(self) -> None:
        engine = mock.Mock()
        a, b, c = self._patched_engine(engine)
        with a, b, c:
            embedded_common.checkpoint_pools()
        engine.checkpoint_all.assert_called_once_with()

    def test_checkpoint_pools_is_a_noop_when_unconfigured_or_sync_disabled(
        self,
    ) -> None:
        engine = mock.Mock()
        with (
            mock.patch.object(embedded_common, "_engine_configured", False),
            mock.patch(
                "synapse.storage.databases.embedded_engine.get_embedded_engine",
                return_value=engine,
            ),
        ):
            embedded_common.checkpoint_pools()
        engine.checkpoint_all.assert_not_called()

        a, b, c = self._patched_engine(engine, sync_disabled=True)
        with a, b, c:
            embedded_common.checkpoint_pools()
        engine.checkpoint_all.assert_not_called()

    def test_checkpoint_pools_tolerates_an_older_extension(self) -> None:
        """An extension built before `checkpoint_all` must not break shutdown."""
        engine = mock.Mock(spec=[])  # no attributes at all
        a, b, c = self._patched_engine(engine)
        with a, b, c:
            embedded_common.checkpoint_pools()  # must not raise

    def test_close_checkpoints_after_the_final_sync(self) -> None:
        """The checkpoint runs after the dirty pools are synced, never before."""
        from synapse.storage.databases.main.embedded_common import _FlushCoalescer
        from synapse.util.clock import Clock

        from tests.server import ThreadedMemoryReactorClock

        order: list[str] = []
        clock = Clock(ThreadedMemoryReactorClock(), server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        coalescer._dirty.add(Pool.STATE)
        with (
            mock.patch.object(
                embedded_common,
                "_do_sync_pools",
                side_effect=lambda pools: order.append("sync"),
            ),
            mock.patch.object(
                embedded_common,
                "checkpoint_pools",
                side_effect=lambda: order.append("checkpoint"),
            ),
        ):
            coalescer.close()
        self.assertEqual(order, ["sync", "checkpoint"])

    def test_close_still_checkpoints_when_nothing_is_dirty(self) -> None:
        """A stop with no dirty pool still moves the baseline forward."""
        from synapse.storage.databases.main.embedded_common import _FlushCoalescer
        from synapse.util.clock import Clock

        from tests.server import ThreadedMemoryReactorClock

        clock = Clock(ThreadedMemoryReactorClock(), server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        with (
            mock.patch.object(embedded_common, "_do_sync_pools") as sync,
            mock.patch.object(embedded_common, "checkpoint_pools") as checkpoint,
        ):
            coalescer.close()
        sync.assert_not_called()
        checkpoint.assert_called_once_with()

    def test_close_skips_checkpoint_when_final_sync_fails(self) -> None:
        """A failed final sync must not publish a baseline for dirty writes."""
        from synapse.storage.databases.main.embedded_common import _FlushCoalescer
        from synapse.util.clock import Clock

        from tests.server import ThreadedMemoryReactorClock

        clock = Clock(ThreadedMemoryReactorClock(), server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        coalescer._dirty.add(Pool.STATE)
        with (
            mock.patch.object(
                embedded_common,
                "_do_sync_pools",
                side_effect=RuntimeError("sync failed"),
            ),
            mock.patch.object(embedded_common, "checkpoint_pools") as checkpoint,
        ):
            coalescer.close()
        checkpoint.assert_not_called()

    def test_close_survives_a_checkpoint_failure(self) -> None:
        """A failed shutdown checkpoint is logged, not raised."""
        from synapse.storage.databases.main.embedded_common import _FlushCoalescer
        from synapse.util.clock import Clock

        from tests.server import ThreadedMemoryReactorClock

        clock = Clock(ThreadedMemoryReactorClock(), server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        with (
            mock.patch.object(
                embedded_common,
                "checkpoint_pools",
                side_effect=RuntimeError("checkpoint failed"),
            ),
        ):
            coalescer.close()  # must not raise

    def test_close_skips_the_final_flush_while_a_barrier_is_wedged(self) -> None:
        """A barrier that outlives the shutdown deadline must not block close().

        A wedged barrier may hold the locks the final flush needs, so close()
        gives up on it, skips the flush, and still reports the pools as pending.
        """
        from synapse.storage.databases.main.embedded_common import _FlushCoalescer
        from synapse.util.clock import Clock

        from tests.server import ThreadedMemoryReactorClock

        clock = Clock(ThreadedMemoryReactorClock(), server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        coalescer._dirty.add(Pool.STATE)
        # Stand in for a flush barrier stuck past the shutdown deadline.
        coalescer._active_sync_barriers = 1
        with (
            mock.patch.object(embedded_common, "_CLOSE_BARRIER_WAIT_SECS", 0.0),
            mock.patch.object(embedded_common, "flush_adjacency_tails") as flush_tails,
            mock.patch.object(embedded_common, "_do_sync_pools") as sync,
            mock.patch.object(embedded_common, "checkpoint_pools") as checkpoint,
        ):
            coalescer.close()

        flush_tails.assert_not_called()
        sync.assert_not_called()
        # The write is still dirty, so no durable baseline may be published.
        self.assertIn(Pool.STATE, coalescer._dirty)
        checkpoint.assert_not_called()

    def test_close_syncs_every_pool_strictly_after_a_committer_error(self) -> None:
        """A failed submitted boundary is repaired even with nothing dirty.

        The dirty set only covers writes not yet submitted, so an empty set is
        not proof of durability once the background committer has failed.
        """
        from synapse.storage.databases.main.embedded_common import _FlushCoalescer
        from synapse.util.clock import Clock

        from tests.server import ThreadedMemoryReactorClock

        clock = Clock(ThreadedMemoryReactorClock(), server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        with (
            mock.patch.object(
                embedded_common,
                "background_commit_error",
                return_value="background commit failed",
            ),
            mock.patch.object(embedded_common, "flush_adjacency_tails"),
            mock.patch.object(embedded_common, "_do_sync_pools") as sync,
            mock.patch.object(embedded_common, "checkpoint_pools") as checkpoint,
        ):
            order = mock.Mock()
            order.attach_mock(sync, "sync")
            order.attach_mock(checkpoint, "checkpoint")
            coalescer.close()

        # Both pools, not just the (empty) dirty set: the strict path proves all,
        # and only then is the baseline checkpointed.
        self.assertEqual(
            order.mock_calls, [mock.call.sync(set(Pool)), mock.call.checkpoint()]
        )

    def test_close_skips_the_checkpoint_when_strict_recovery_fails(self) -> None:
        """If the committer failed and the strict sync also fails, keep the pool pending."""
        from synapse.storage.databases.main.embedded_common import _FlushCoalescer
        from synapse.util.clock import Clock

        from tests.server import ThreadedMemoryReactorClock

        clock = Clock(ThreadedMemoryReactorClock(), server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        with (
            mock.patch.object(
                embedded_common,
                "background_commit_error",
                return_value="background commit failed",
            ),
            mock.patch.object(embedded_common, "flush_adjacency_tails"),
            mock.patch.object(
                embedded_common,
                "_do_sync_pools",
                side_effect=RuntimeError("sync failed"),
            ) as sync,
            mock.patch.object(embedded_common, "checkpoint_pools") as checkpoint,
        ):
            coalescer.close()

        self.assertEqual(sync.call_count, 3)
        checkpoint.assert_not_called()


class CoalescerConcurrentWriteTestCase(TestCase):
    def test_write_marked_during_a_barrier_stays_dirty(self) -> None:
        """A commit that lands while `sync_now` runs must not be cleared by it."""
        from synapse.storage.databases.main.embedded_common import _FlushCoalescer
        from synapse.util.clock import Clock

        from tests.server import ThreadedMemoryReactorClock

        clock = Clock(ThreadedMemoryReactorClock(), server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        coalescer._dirty.add(Pool.STATE)

        def sync_and_commit_more(pools: set[Pool]) -> None:
            # A persist on another thread commits after the snapshot was taken.
            coalescer.mark_dirty(Pool.STATE)

        with (
            mock.patch.object(embedded_common, "flush_adjacency_tails"),
            mock.patch.object(
                embedded_common, "_do_sync_pools", side_effect=sync_and_commit_more
            ),
        ):
            coalescer.sync_now([Pool.STATE])

        self.assertIn(Pool.STATE, coalescer._dirty)

    def test_close_waits_for_in_flight_barrier_before_checkpoint(self) -> None:
        """Close must wait until a barrier has finished restoring dirty state."""
        from synapse.storage.databases.main.embedded_common import _FlushCoalescer
        from synapse.util.clock import Clock

        from tests.server import ThreadedMemoryReactorClock

        clock = Clock(ThreadedMemoryReactorClock(), server_name="test_server")  # type: ignore[multiple-internal-clocks]
        coalescer = _FlushCoalescer(clock)
        barrier_entered = threading.Event()
        finish_barrier = threading.Event()
        checkpointed = threading.Event()
        synced_before_checkpoint: list[bool] = []
        synced = threading.Event()

        def block_barrier() -> None:
            barrier_entered.set()
            if not finish_barrier.wait(timeout=5):
                raise TimeoutError("test did not release the active barrier")

        close_waiting = threading.Event()
        real_wait = embedded_common._BARRIER_CONDITION.wait

        def signalling_wait(timeout: float | None = None) -> bool:
            close_waiting.set()
            return real_wait(timeout)

        def record_checkpoint() -> None:
            synced_before_checkpoint.append(synced.is_set())
            checkpointed.set()

        with (
            mock.patch.object(embedded_common, "flush_adjacency_tails", block_barrier),
            mock.patch.object(
                embedded_common._BARRIER_CONDITION, "wait", signalling_wait
            ),
            mock.patch.object(
                embedded_common,
                "_do_sync_pools",
                side_effect=lambda pools: synced.set(),
            ),
            mock.patch.object(
                embedded_common,
                "checkpoint_pools",
                side_effect=record_checkpoint,
            ),
        ):
            barrier_thread = threading.Thread(
                target=lambda: coalescer.sync_now([Pool.STATE])
            )
            barrier_thread.start()
            self.assertTrue(barrier_entered.wait(timeout=5))

            close_thread = threading.Thread(target=coalescer.close)
            close_thread.start()
            deadline = time.monotonic() + 5
            while not coalescer._closed and time.monotonic() < deadline:
                time.sleep(0.001)

            self.assertTrue(coalescer._closed)
            # Release the barrier only once close() is really blocked on it.
            reached_wait = close_waiting.wait(timeout=5)
            self.assertFalse(checkpointed.is_set())
            finish_barrier.set()
            barrier_thread.join(timeout=5)
            close_thread.join(timeout=5)

        self.assertTrue(reached_wait)
        self.assertFalse(barrier_thread.is_alive())
        self.assertFalse(close_thread.is_alive())
        self.assertTrue(checkpointed.is_set())
        # The barrier's own pool sync had finished when the checkpoint ran.
        self.assertEqual(synced_before_checkpoint, [True])
