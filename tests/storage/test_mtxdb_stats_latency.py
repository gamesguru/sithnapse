#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
"""mtxdb's per-operation latency stats reach Python through `stats()`.

mtxdb's binding owns process-global pools, so the engine is opened in a
subprocess rather than in the trial process.
"""

import json
import os
import subprocess
import sys
import tempfile

from tests import unittest

_SCRIPT = """
import json, sys
from synapse.synapse_rust import mtxdb_engine as m
m.open_client(sys.argv[1])
m.set_stats_enabled(True)
for i in range(20):
    m.batch_put([(f"vis:k{i}".encode(), b"x"), (f"event_json:e{i}".encode(), b"y")])
    m.batch_get([f"vis:k{i}".encode(), f"event_json:e{i}".encode()])
print(json.dumps(m.stats()))
"""


class MtxdbLatencyStatsTestCase(unittest.TestCase):
    def test_operation_latency_is_reported_per_pool(self) -> None:
        with tempfile.TemporaryDirectory(prefix="test-mtxdb-latency-") as store:
            result = subprocess.run(
                [sys.executable, "-c", _SCRIPT, store],
                capture_output=True,
                text=True,
                env=dict(os.environ, SYNAPSE_MTXDB_WAL="1"),
                timeout=120,
            )
        self.assertEqual(result.returncode, 0, result.stderr[-500:])
        stats = json.loads(result.stdout.strip().splitlines()[-1])

        if "put_many_latency" not in stats["state"]:
            self.skipTest("extension predates latency stats; rebuild it (maturin)")

        for pool in ("state", "event_dag"):
            with self.subTest(pool=pool):
                for op in ("put_many_latency", "get_many_latency"):
                    latency = stats[pool][op]
                    self.assertEqual(latency["calls"], 20, (pool, op, latency))
                    self.assertGreater(latency["total_us"], 0)
                    self.assertGreaterEqual(latency["max_us"], 0)
                    # Six non-cumulative buckets: <50us <100us <250us <1ms <10ms >=10ms.
                    self.assertEqual(len(latency["buckets"]), 6)
                    self.assertEqual(sum(latency["buckets"]), latency["calls"])

        # The edges pool was never touched.
        self.assertEqual(stats["auth_chain"]["put_many_latency"]["calls"], 0)
