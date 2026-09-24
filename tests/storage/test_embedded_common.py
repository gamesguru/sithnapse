#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

from synapse.storage.databases.main.embedded_common import _mtxdb_snapshot_metrics

from unittest import TestCase


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
