#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
"""Cross-process visibility of published-but-not-fsynced mtxdb writes.

The visibility/durability split rests on one claim: a write the writer has
*published* (``publish_pending``) is readable by a read-only worker process
even though the writer has never fsynced it. Existing tests run reader and
writer in one process, so they cannot show that. Here each role is its own
process, because mtxdb's Python binding holds process-global pools.

The test drives the shim directly, not through ``events.py``: production does
not call ``publish_pending`` yet, so this pins the primitive the rewiring
will depend on.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from typing import IO

from tests import unittest

_WORKER = os.path.join(os.path.dirname(__file__), "mtxdb_visibility_worker.py")

# One key per pool: `event_json:` routes to the EventDag pool, anything else to
# the State pool.
_STATE_KEY = "vis:state"
_EVENT_DAG_KEY = "event_json:vis"


class _Process:
    def __init__(self, role: str, store_dir: str) -> None:
        env = dict(os.environ, SYNAPSE_MTXDB_WAL="1")
        self._proc = subprocess.Popen(
            [sys.executable, _WORKER, role, store_dir],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        ready = self._stdout().readline().strip()
        if ready != "READY":
            self.close()
            raise AssertionError(f"{role} process failed to start: {ready!r}")

    def _stdin(self) -> IO[str]:
        assert self._proc.stdin is not None
        return self._proc.stdin

    def _stdout(self) -> IO[str]:
        assert self._proc.stdout is not None
        return self._proc.stdout

    def call(self, line: str) -> str:
        self._stdin().write(line + "\n")
        self._stdin().flush()
        reply = self._stdout().readline().strip()
        if reply.startswith("ERR"):
            raise AssertionError(f"{line!r} failed: {reply}")
        return reply

    def get(self, key: str) -> str | None:
        value: str | None = json.loads(self.call(f"get {key}"))
        return value

    def close(self) -> None:
        try:
            if self._proc.poll() is None:
                self._stdin().write("exit\n")
                self._stdin().flush()
                self._proc.wait(timeout=10)
        except Exception:
            self._proc.kill()
        finally:
            for stream in (self._proc.stdin, self._proc.stdout):
                if stream is not None:
                    stream.close()


class MtxdbPublishVisibilityTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.store_dir = tempfile.mkdtemp(prefix="test-mtxdb-visibility-")
        self.addCleanup(shutil.rmtree, self.store_dir, ignore_errors=True)
        self.writer = _Process("writer", self.store_dir)
        self.addCleanup(self.writer.close)

    def _reader(self) -> _Process:
        reader = _Process("reader", self.store_dir)
        self.addCleanup(reader.close)
        return reader

    def test_published_write_is_visible_to_worker_without_fsync(self) -> None:
        """A worker opened *before* the write sees it after publish, with no fsync."""
        reader = self._reader()

        for key in (_STATE_KEY, _EVENT_DAG_KEY):
            with self.subTest(key=key):
                self.writer.call(f"put {key} value")
                # Control: before any barrier the write is invisible, so the
                # assertion below is about publish, not about the worker
                # happening to see everything.
                self.assertIsNone(reader.get(key), "unpublished write leaked")

        self.writer.call("publish")

        for key in (_STATE_KEY, _EVENT_DAG_KEY):
            with self.subTest(key=key):
                self.assertEqual(reader.get(key), "value")

        # Visible is not durable: nothing on the writer ever fsynced.
        self.assertEqual(self.writer.call("fsyncs"), "0")

    def test_worker_opened_after_publish_sees_write(self) -> None:
        """A worker that starts after publish (late-started or restarted) sees it.

        This pins the late-start case only. It does not show that publish is
        *required* there: a worker opened after the write can see it even when
        the writer never published (the other tests are the ones that fail
        without publish).
        """
        self.writer.call(f"put {_STATE_KEY} value")
        self.writer.call("publish")

        reader = self._reader()

        self.assertEqual(reader.get(_STATE_KEY), "value")
        self.assertEqual(self.writer.call("fsyncs"), "0")

    def test_one_publish_covers_many_writes(self) -> None:
        """One publish makes every queued write visible: the per-persist boundary."""
        reader = self._reader()
        keys = [f"vis:many:{i}" for i in range(20)] + [
            f"event_json:many:{i}" for i in range(20)
        ]
        for key in keys:
            self.writer.call(f"put {key} {key}")

        self.writer.call("publish")

        missing = [key for key in keys if reader.get(key) != key]
        self.assertEqual(missing, [], "published writes not visible to the worker")
        self.assertEqual(self.writer.call("fsyncs"), "0")

    def test_writes_after_a_publish_need_their_own_publish(self) -> None:
        """Publish is a boundary, not a mode switch: later writes stay unpublished."""
        reader = self._reader()
        self.writer.call(f"put {_STATE_KEY}:1 one")
        self.writer.call("publish")
        self.assertEqual(reader.get(f"{_STATE_KEY}:1"), "one")

        self.writer.call(f"put {_STATE_KEY}:2 two")
        self.assertIsNone(reader.get(f"{_STATE_KEY}:2"))

        self.writer.call("publish")
        self.assertEqual(reader.get(f"{_STATE_KEY}:2"), "two")
        self.assertEqual(self.writer.call("fsyncs"), "0")
