#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

from unittest import TestCase, mock

from synapse.storage.databases.main import embedded_common
from synapse.storage.databases.main.embedded_common import (
    Pool,
    SyncTier,
    _format_mtxdb_snapshot_segment,
    _mtxdb_snapshot_metrics,
)


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

            embedded_common.maybe_publish(SyncTier.DURABLE, pools=[Pool.EVENT_DAG])
            engine.publish_pending.assert_called_once_with()
