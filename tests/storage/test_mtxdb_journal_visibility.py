#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
"""Cross-process visibility of journaled-but-not-fsynced mtxdb writes.

The visibility/durability split rests on one claim: a write the writer has
*journaled* is readable by a read-only worker process
even though the writer has never fsynced it. Existing tests run reader and
writer in one process, so they cannot show that. Here each role is its own
process, because mtxdb's Python binding holds process-global pools.

Most cases drive the engine directly, pinning the primitive ``events.py``
depends on: a write is visible once journaled. The event-JSON cases use the
production ``embedded_event_json`` read/write helpers instead, so the same
journaled-but-unfsynced visibility is exercised through the real event-JSON
path a worker uses.
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
    def __init__(self, role: str, store_dir: str, namespace: str = "vis") -> None:
        env = dict(os.environ, SYNAPSE_MTXDB_WAL="1")
        self._proc = subprocess.Popen(
            [sys.executable, _WORKER, role, store_dir, namespace],
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

    def event_json_put(self, room_id: str, event_id: str, body: str) -> None:
        self.call(f"event_json_put {room_id} {event_id} {body}")

    def event_json_get(self, event_id: str) -> str | None:
        value: str | None = json.loads(self.call(f"event_json_get {event_id}"))
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


class MtxdbJournalVisibilityTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.store_dir = tempfile.mkdtemp(prefix="test-mtxdb-visibility-")
        self.addCleanup(shutil.rmtree, self.store_dir, ignore_errors=True)
        self.writer = _Process("writer", self.store_dir)
        self.addCleanup(self.writer.close)

    def _reader(self) -> _Process:
        reader = _Process("reader", self.store_dir)
        self.addCleanup(reader.close)
        return reader

    def test_a_write_is_visible_to_a_stale_worker_without_fsync(self) -> None:
        """A worker opened *before* the write sees it at once, with no fsync.

        mtxdb journals each autocommit write as its own group when it is
        written, so there is nothing further to publish.
        """
        reader = self._reader()

        for key in (_STATE_KEY, _EVENT_DAG_KEY):
            with self.subTest(key=key):
                self.writer.call(f"put {key} value")
                self.assertEqual(reader.get(key), "value")

        # Visible is not durable: nothing on the writer ever fsynced.
        self.assertEqual(self.writer.call("fsyncs"), "0")

    def test_worker_opened_after_the_write_sees_it(self) -> None:
        """A worker that starts later (late-started or restarted) sees it too."""
        self.writer.call(f"put {_STATE_KEY} value")

        reader = self._reader()

        self.assertEqual(reader.get(_STATE_KEY), "value")
        self.assertEqual(self.writer.call("fsyncs"), "0")

    def test_many_writes_are_all_visible(self) -> None:
        reader = self._reader()
        keys = [f"vis:many:{i}" for i in range(20)] + [
            f"event_json:many:{i}" for i in range(20)
        ]
        for key in keys:
            self.writer.call(f"put {key} {key}")

        missing = [key for key in keys if reader.get(key) != key]
        self.assertEqual(missing, [], "journaled writes not visible to the worker")
        self.assertEqual(self.writer.call("fsyncs"), "0")

    def test_later_writes_are_visible_too(self) -> None:
        """There is no boundary to cross: every write is visible as it lands."""
        reader = self._reader()
        self.writer.call(f"put {_STATE_KEY}:1 one")
        self.assertEqual(reader.get(f"{_STATE_KEY}:1"), "one")

        self.writer.call(f"put {_STATE_KEY}:2 two")
        self.assertEqual(reader.get(f"{_STATE_KEY}:2"), "two")
        self.assertEqual(self.writer.call("fsyncs"), "0")


_EVENT_JSON_ROOM = "!vis:test"
_EVENT_JSON_NS = "vis-event-json"


class MtxdbEventJsonVisibilityTestCase(unittest.TestCase):
    """The real event-JSON read path against a journaled-but-unfsynced write.

    In exclusive mode event JSON has no SQL copy, so a stale worker that could
    not see a committed event's JSON would report it absent. The direct write is
    visible as soon as it is journaled. A write staged in a transaction is
    invisible to the worker until the transaction commits, and never appears if
    it aborts. The reader is opened *before* every write, so it can only see a
    record through the journal, never through an open-time rescan.
    """

    def setUp(self) -> None:
        self.store_dir = tempfile.mkdtemp(prefix="test-mtxdb-event-json-vis-")
        self.addCleanup(shutil.rmtree, self.store_dir, ignore_errors=True)
        self.writer = _Process("writer", self.store_dir, _EVENT_JSON_NS)
        self.addCleanup(self.writer.close)

    def _reader(self) -> _Process:
        reader = _Process("reader", self.store_dir, _EVENT_JSON_NS)
        self.addCleanup(reader.close)
        return reader

    def test_event_json_is_visible_to_a_stale_worker_once_written(self) -> None:
        reader = self._reader()

        self.writer.event_json_put(_EVENT_JSON_ROOM, "$ej1:test", "first")
        self.assertEqual(reader.event_json_get("$ej1:test"), "first")

        self.writer.event_json_put(_EVENT_JSON_ROOM, "$ej2:test", "second")
        self.assertEqual(reader.event_json_get("$ej2:test"), "second")

        self.assertEqual(self.writer.call("fsyncs"), "0")

    def test_staged_event_json_is_invisible_until_commit(self) -> None:
        reader = self._reader()

        self.writer.call("txn_begin")
        self.writer.call(f"txn_event_json_put {_EVENT_JSON_ROOM} $ej3:test staged")
        # Control that the writer really has the write staged: it is the
        # commit, not the worker's view, that makes it appear.
        self.assertIsNone(reader.event_json_get("$ej3:test"))

        self.writer.call("txn_commit")
        self.assertEqual(reader.event_json_get("$ej3:test"), "staged")

    def test_aborted_event_json_never_becomes_visible(self) -> None:
        reader = self._reader()

        self.writer.call("txn_begin")
        self.writer.call(f"txn_event_json_put {_EVENT_JSON_ROOM} $ej4:test dropped")
        self.assertIsNone(reader.event_json_get("$ej4:test"))

        self.writer.call("txn_abort")
        self.assertIsNone(reader.event_json_get("$ej4:test"))
        self.assertEqual(self.writer.call("fsyncs"), "0")
