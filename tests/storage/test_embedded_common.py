#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

import os
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
    def test_sync_disabled_suppresses_fsync_but_not_publication(self) -> None:
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
            engine.sync_auth_chain.assert_not_called()
            # A barrier with durability off still publishes: its callers need
            # the write visible to other workers, and it was their only way to
            # publish it.
            engine.publish_pending.assert_called_once_with()

            engine.publish_pending.reset_mock()
            embedded_common.maybe_publish(SyncTier.DURABLE, pools=[Pool.EVENT_DAG])
            engine.publish_pending.assert_called_once_with()

    def test_sync_now_with_durability_off_publishes(self) -> None:
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
        engine.publish_pending.assert_called_once_with()

    def test_publication_is_a_noop_without_the_wal(self) -> None:
        """Without the WAL there is no journal: publishing must not be attempted.

        The engine raises "journal is unavailable" from ``publish_pending``
        when the WAL is off, and no read-committed worker can exist to see a
        published write, so ``maybe_publish`` has to leave it alone.
        """
        engine = mock.Mock()
        with (
            mock.patch.dict(os.environ, {"SYNAPSE_MTXDB_WAL": ""}),
            mock.patch.object(embedded_common, "_engine_configured", True),
            mock.patch(
                "synapse.storage.databases.embedded_engine.get_embedded_engine",
                return_value=engine,
            ),
        ):
            embedded_common.maybe_publish(SyncTier.DURABLE, pools=[Pool.EVENT_DAG])

        engine.publish_pending.assert_not_called()


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

    def test_interval_flush_publishes_before_requesting_durability(self) -> None:
        coalescer = object.__new__(embedded_common._FlushCoalescer)
        coalescer._delayed_call = None
        coalescer._closed = False
        coalescer._dirty = {Pool.STATE, Pool.EVENT_DAG}
        calls: list[str] = []

        with (
            mock.patch.object(
                embedded_common, "publishes_at_commit", return_value=True
            ),
            mock.patch.object(
                embedded_common,
                "_drain_edge_writes",
                return_value=False,
            ),
            mock.patch.object(
                embedded_common,
                "maybe_publish",
                side_effect=lambda *args, **kwargs: calls.append("publish"),
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

        self.assertEqual(calls, ["publish", "request"])
        self.assertFalse(coalescer._dirty)

    def test_background_failure_keeps_dirty_pools_for_retry(self) -> None:
        coalescer = object.__new__(embedded_common._FlushCoalescer)
        coalescer._delayed_call = None
        coalescer._closed = False
        coalescer._dirty = {Pool.EVENT_DAG}
        coalescer._clock = mock.Mock()
        coalescer._RETRY_DELAY = Duration(seconds=1)

        with (
            mock.patch.object(
                embedded_common, "publishes_at_commit", return_value=True
            ),
            mock.patch.object(
                embedded_common, "_drain_edge_writes", return_value=False
            ),
            mock.patch.object(embedded_common, "maybe_publish"),
            mock.patch.object(embedded_common, "request_durable"),
            mock.patch.object(
                embedded_common,
                "background_commit_error",
                return_value="disk full",
            ),
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
                embedded_common, "_drain_edge_writes", return_value=False
            ),
            mock.patch.object(
                embedded_common,
                "maybe_publish",
                side_effect=lambda *args, **kwargs: calls.append("publish"),
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
        self.assertEqual(calls[0:2], ["publish", "request"])
        strict_call = cast(tuple[str, object], calls[2])
        self.assertEqual(strict_call[0], "strict")
        self.assertEqual(calls[3:], ["stop", ("start", 0.5)])


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

    def test_interval_and_off_publish_only_with_the_wal(self) -> None:
        for mode in (SyncMode.INTERVAL, SyncMode.OFF):
            self.assertTrue(self._publishes(mode, no_sync=False, wal="1"))
            # No WAL, no journal to publish to: fall back to the barriers.
            self.assertFalse(self._publishes(mode, no_sync=False, wal=""))

    def test_no_sync_means_off(self) -> None:
        """The older boolean switch still disables fsync, and now publishes too."""
        self.assertTrue(self._publishes(None, no_sync=True, wal="1"))
        self.assertTrue(embedded_common._sync_disabled)
        self.assertIs(embedded_common._sync_mode, SyncMode.OFF)

    def test_only_off_disables_fsync(self) -> None:
        embedded_common.configure_sync(no_sync=False, mode=SyncMode.INTERVAL)
        self.assertFalse(embedded_common._sync_disabled)
        embedded_common.configure_sync(no_sync=False, mode=SyncMode.OFF)
        self.assertTrue(embedded_common._sync_disabled)
