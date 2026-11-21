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

import atexit
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
            # Kept separate from stdout, not merged: stdout is a strict
            # line protocol (the first lines must be exactly "IMPORTED" and
            # "READY"), and
            # anything else that writes to stderr during import -- a
            # deprecation warning, a C-level/Rust warning, an unrelated
            # future log line -- would otherwise land on that same stream
            # and get misread as the readiness line (see mtxdb_visibility_
            # worker.py's logging.disable(...) around its Synapse imports,
            # which covers today's known trigger but not every possible
            # one). Capturing it separately means any such noise shows up
            # as diagnostic context below, instead of silently corrupting
            # the protocol.
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        self._role = role

    def open(self) -> "_Process":
        """Wait for the process's imports, then open the store.

        Construction only spawns the process, so callers can start several and
        let their slow Synapse imports overlap before opening each in turn.
        """
        self._expect("IMPORTED")
        self._stdin().write("open\n")
        self._stdin().flush()
        self._expect("READY")
        return self

    def _expect(self, line: str) -> None:
        got = self._stdout().readline().strip()
        if got != line:
            stderr_output = self._proc.stderr.read() if self._proc.stderr else ""
            self.close()
            raise AssertionError(
                f"{self._role} process failed to start: expected {line!r}, "
                f"got {got!r}\nstderr:\n{stderr_output}"
            )

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

    def edges_put(self, room_id: str, event_id: str, prev_event_id: str) -> None:
        self.call(f"edges_put {room_id} {event_id} {prev_event_id}")

    def edges_get(self, prev_event_id: str) -> dict[str, list[str] | None]:
        value: dict[str, list[str] | None] = json.loads(
            self.call(f"edges_get {prev_event_id}")
        )
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
            for stream in (self._proc.stdin, self._proc.stdout, self._proc.stderr):
                if stream is not None:
                    stream.close()


class _VisibilityTestCase(unittest.TestCase):
    """Keep the expensive Synapse/Rust imports to one pair of processes.

    Each worker startup imports the full Synapse package. These tests use
    unique keys/rooms, so sharing the writer and stale reader within a class
    preserves the cross-process assertions without paying that import cost
    for every test method.
    """

    namespace = "vis"
    store_prefix = "test-mtxdb-visibility-"

    def setUp(self) -> None:
        if not _visibility_fixtures:
            _start_visibility_fixtures(
                {
                    cls.namespace: cls.store_prefix
                    for cls in _VisibilityTestCase.__subclasses__()
                }
            )
        fixture = _visibility_fixtures[self.namespace]
        self.store_dir = fixture.store_dir
        self.writer = fixture.writer
        self._stale_reader = fixture.reader

    def _reader(self) -> _Process:
        return self._stale_reader

    def _new_reader(self) -> _Process:
        """A reader opened now, after whatever the test has written so far.

        Uses the pre-spawned spare, whose imports are already done, the first
        time; any later call spawns a fresh process.
        """
        fixture = _visibility_fixtures[self.namespace]
        reader = fixture.spare_reader or _Process(
            "reader", self.store_dir, self.namespace
        )
        fixture.spare_reader = None
        self.addCleanup(reader.close)
        return reader.open()


class _Fixture:
    def __init__(self, store_dir: str, namespace: str) -> None:
        self.store_dir = store_dir
        self.writer = _Process("writer", store_dir, namespace)
        self.reader = _Process("reader", store_dir, namespace)
        self.spare_reader: _Process | None = _Process("reader", store_dir, namespace)

    def close(self) -> None:
        for process in (self.spare_reader, self.reader, self.writer):
            if process is not None:
                process.close()
        shutil.rmtree(self.store_dir, ignore_errors=True)


_visibility_fixtures: dict[str, _Fixture] = {}


def _start_visibility_fixtures(store_prefixes: dict[str, str]) -> None:
    """Spawn every store's processes at once, so all their imports overlap.

    Each writer is opened before its readers: a read-only open needs the store
    the writer creates.
    """
    for namespace, prefix in store_prefixes.items():
        _visibility_fixtures[namespace] = _Fixture(
            tempfile.mkdtemp(prefix=prefix), namespace
        )
    for fixture in _visibility_fixtures.values():
        fixture.writer.open()
    for fixture in _visibility_fixtures.values():
        fixture.reader.open()


def _close_visibility_fixtures() -> None:
    for fixture in _visibility_fixtures.values():
        fixture.close()


atexit.register(_close_visibility_fixtures)


class MtxdbJournalVisibilityTestCase(_VisibilityTestCase):
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
        key = f"{_STATE_KEY}:late"
        self.writer.call(f"put {key} value")

        reader = self._new_reader()

        self.assertEqual(reader.get(key), "value")
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


class MtxdbEventJsonVisibilityTestCase(_VisibilityTestCase):
    """The real event-JSON read path against a journaled-but-unfsynced write.

    In exclusive mode event JSON has no SQL copy, so a stale worker that could
    not see a committed event's JSON would report it absent. The direct write is
    visible as soon as it is journaled. A write staged in a transaction is
    invisible to the worker until the transaction commits, and never appears if
    it aborts. The reader is opened *before* every write, so it can only see a
    record through the journal, never through an open-time rescan.
    """

    namespace = _EVENT_JSON_NS
    store_prefix = "test-mtxdb-event-json-vis-"

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


_EDGES_NS = "vis-event-edges"


class MtxdbEventEdgesVisibilityTestCase(_VisibilityTestCase):
    """The real event-edges forward read path across processes.

    A worker opened before the writer inserts an edge must see the write
    through the journal overlay without reopening. This exercises the
    production Python read helper, not the raw engine.
    """

    namespace = _EDGES_NS
    store_prefix = "test-mtxdb-edges-vis-"

    def test_missing_edge_is_an_empty_result(self) -> None:
        reader = self._reader()

        self.assertEqual(reader.edges_get("$pe1:test"), {"$pe1:test": None})

    def test_edge_visible_to_stale_worker_without_fsync(self) -> None:
        """A worker opened before the write sees the edge, without an fsync."""
        reader = self._reader()

        room_id = "!published:test"
        self.writer.edges_put(room_id, "$child1:test", "$pe2:test")

        self.assertEqual(
            reader.edges_get("$pe2:test"),
            {"$pe2:test": ["$child1:test"]},
        )
        self.assertEqual(self.writer.call("fsyncs"), "0")

    def test_repeated_put_visible_to_same_reader(self) -> None:
        """Appending to an existing parent is visible without reopening.

        A reader that refreshed only for the first write would pass the
        single-write case but fail this one.
        """
        reader = self._reader()

        room_id = "!repeat-publish:test"
        self.writer.edges_put(room_id, "$child1:test", "$pe3:test")
        self.assertEqual(
            reader.edges_get("$pe3:test"),
            {"$pe3:test": ["$child1:test"]},
        )

        self.writer.edges_put(room_id, "$child2:test", "$pe3:test")
        children = reader.edges_get("$pe3:test").get("$pe3:test") or []
        self.assertEqual(set(children), {"$child1:test", "$child2:test"})
        self.assertEqual(self.writer.call("fsyncs"), "0")
