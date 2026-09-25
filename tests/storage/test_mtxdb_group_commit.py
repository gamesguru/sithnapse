#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
"""mtxdb's durability-request / group-commit primitives, through the shim.

The engine is opened in a subprocess: mtxdb's binding owns process-global pools.
"""

import json
import os
import subprocess
import sys
import tempfile
from typing import Any

from tests import unittest

_SCRIPT = """
import json, sys
from synapse.synapse_rust import mtxdb_engine as m

m.open_client(sys.argv[1])
m.set_stats_enabled(True)
out = {}

def commits():
    return m.stats()["event_dag"]["durability"]

# 1. No committer running: wait_durable performs the blocking commit itself.
m.batch_put([("vis:a".encode(), b"1"), ("event_json:a".encode(), b"2")])
m.publish_pending()
targets = m.request_durable()
out["targets"] = targets
m.wait_durable(targets)
out["after_blocking_wait"] = commits()

# 2. With the background committer, a waiter is released by its group commit.
m.start_background_commit(5, 1000)
m.batch_put([("vis:b".encode(), b"1")])
m.publish_pending()
m.wait_durable(m.request_durable())
out["after_group_commit"] = commits()
m.stop_background_commit()
print(json.dumps(out))
"""


class MtxdbGroupCommitTestCase(unittest.TestCase):
    def _run_engine(self) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="test-mtxdb-group-commit-") as store:
            result = subprocess.run(
                [sys.executable, "-c", _SCRIPT, store],
                capture_output=True,
                text=True,
                env=dict(os.environ, SYNAPSE_MTXDB_WAL="1"),
                timeout=120,
            )
        self.assertEqual(result.returncode, 0, result.stderr[-600:])
        out: dict[str, Any] = json.loads(result.stdout.strip().splitlines()[-1])
        return out

    def test_durable_wait_commits_and_is_counted(self) -> None:
        out = self._run_engine()

        # One target per pool (state, event, edges); zero means no journal.
        self.assertEqual(len(out["targets"]), 3)
        self.assertTrue(any(out["targets"]), out["targets"])

        blocking = out["after_blocking_wait"]
        self.assertGreaterEqual(blocking["durable_requests"], 1)
        self.assertGreaterEqual(blocking["commits"], 1, blocking)
        self.assertGreater(blocking["commit_records"], 0)

        grouped = out["after_group_commit"]
        self.assertGreater(grouped["durable_requests"], blocking["durable_requests"])
        self.assertGreater(grouped["commit_records"], blocking["commit_records"])
        self.assertGreaterEqual(grouped["max_commit_records"], 1)
