#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
import os
import shutil
import signal
import sys
import tempfile
import threading
import time
import unittest as py_unittest
from unittest.mock import patch

from synapse.util import timings_flush

from tests import unittest


def _is_running_in(thread: threading.Thread, function_name: str) -> bool:
    """Whether `thread` currently has `function_name` on its Python stack."""
    frame = sys._current_frames().get(thread.ident or 0)
    while frame is not None:
        if frame.f_code.co_name == function_name:
            return True
        frame = frame.f_back
    return False


def _wait_for_child(pid: int, timeout: float = 10.0) -> int:
    """Wait for a forked test child, without letting a wedged child hang Trial."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        waited_pid, status = os.waitpid(pid, os.WNOHANG)
        if waited_pid == pid:
            return status
        time.sleep(0.01)

    # The child may be stuck in an at-fork callback, before its test body can
    # reach its own timeout and exit. Reap it so the worker can report failure.
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    os.waitpid(pid, 0)
    raise AssertionError(f"forked child {pid} did not exit within {timeout:g}s")


def _kill_and_reap(pid: int) -> None:
    """Cleanup: never leave a forked test child behind, even if an assert fails."""
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        os.waitpid(pid, 0)
    except ChildProcessError:
        pass  # Already reaped by _wait_for_child.


@py_unittest.skipUnless(hasattr(os, "fork"), "requires fork()")
class TimingsFlushForkTestCase(unittest.TestCase):
    def test_forked_child_runs_registered_flusher(self) -> None:
        """A forked child must keep snapshotting.

        Complement forks each worker from a starter that already imported
        Synapse, and threads do not survive ``fork()``. The child has to
        restart the flusher thread itself, or it silently stops producing
        snapshots.
        """
        # Not a `with TemporaryDirectory()`: the parent's own flusher thread is
        # still registered (and writing markers) until the test's cleanups run,
        # so removing the directory before that races it and fails with
        # "Directory not empty". Cleanups run last-in first-out, so this one,
        # registered first, runs after the flusher is unregistered.
        run_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, run_dir, ignore_errors=True)

        def flusher() -> None:
            # Marker named after the *calling* process, so the parent's own
            # flusher can't satisfy the child's check.
            open(os.path.join(run_dir, f"flushed_{os.getpid()}"), "w").close()

        with patch.dict(os.environ, {"SYNAPSE_TIMINGS_RUN_DIR": run_dir}):
            self.addCleanup(timings_flush.unregister_periodic_flush, flusher)
            self.addCleanup(setattr, timings_flush, "_INTERVAL_SECONDS", 2.0)
            timings_flush._INTERVAL_SECONDS = 0.02
            timings_flush.register_periodic_flush(flusher)

            pid = os.fork()
            if pid != 0:
                self.addCleanup(_kill_and_reap, pid)
            if pid == 0:
                # Child: only exit paths below; never return into the test runner.
                code = 1
                try:
                    marker = os.path.join(run_dir, f"flushed_{os.getpid()}")
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        if os.path.exists(marker):
                            code = 0
                            break
                        time.sleep(0.01)
                finally:
                    os._exit(code)

            status = _wait_for_child(pid)
            self.assertEqual(
                os.waitstatus_to_exitcode(status),
                0,
                "forked child never invoked the registered flusher",
            )

    def test_fork_during_flush_does_not_inherit_held_lock(self) -> None:
        """Forking while a flusher holds a diagnostics lock must not wedge the child.

        The flusher takes locks that the diagnostics' hot paths also take. If
        ``fork()`` lands mid-flush, the child inherits that lock held by a
        thread that no longer exists, so its first diagnostic call would
        deadlock. ``fork()`` therefore has to wait for the flush to finish.

        Fork from the main thread. Forking a multithreaded process from a
        worker thread can wedge CPython's at-fork handling before the child
        gets a chance to run; that is the leading explanation for the earlier
        stall, though it was not confirmed. A helper thread observes the main
        thread blocked in the fork-gate hook and releases the flush.
        """
        diagnostics_lock = threading.Lock()
        in_flush = threading.Event()
        release_flush = threading.Event()
        gate_seen = threading.Event()

        def flusher() -> None:
            with diagnostics_lock:
                in_flush.set()
                # No timeout: the helper below releases this once the fork is
                # observed blocked on the gate, so a slow runner cannot
                # self-release the flush and let ``fork()`` complete mid-check.
                release_flush.wait()

        def release_when_gate_blocks() -> None:
            """Release the flush once the main thread is stuck in the gate."""
            deadline = time.monotonic() + 10
            try:
                while time.monotonic() < deadline:
                    if _is_running_in(threading.main_thread(), "_before_fork"):
                        gate_seen.set()
                        return
                    time.sleep(0.005)
            finally:
                # Even if the gate never appears (a broken hook), never leave
                # the flusher thread blocked and the main thread in ``fork``.
                release_flush.set()

        with tempfile.TemporaryDirectory() as run_dir:
            with patch.dict(os.environ, {"SYNAPSE_TIMINGS_RUN_DIR": run_dir}):
                self.addCleanup(timings_flush._flushers.remove, flusher)
                self.addCleanup(setattr, timings_flush, "_INTERVAL_SECONDS", 2.0)
                # Registered last, so it runs first: never leave the flusher
                # thread blocked if an assertion below fails.
                self.addCleanup(release_flush.set)
                timings_flush._INTERVAL_SECONDS = 0.01
                timings_flush.register_periodic_flush(flusher)

                self.assertTrue(in_flush.wait(5), "flusher never ran")

                helper = threading.Thread(
                    target=release_when_gate_blocks,
                    name="release-flush-gate",
                    daemon=True,
                )
                helper.start()

                # The main thread is the one forking: only the forking thread
                # survives in the child, and the other at-fork hooks expect the
                # main thread to be the caller.
                pid = os.fork()
                if pid != 0:
                    self.addCleanup(_kill_and_reap, pid)
                if pid == 0:
                    # Child: never return into the test runner.
                    code = 1
                    try:
                        if diagnostics_lock.acquire(timeout=2):
                            code = 0
                    finally:
                        os._exit(code)

                status = _wait_for_child(pid)
                helper.join(5)
                self.assertTrue(
                    gate_seen.is_set(),
                    "fork() completed without blocking on the flush gate",
                )
                self.assertEqual(
                    os.waitstatus_to_exitcode(status),
                    0,
                    "child inherited a diagnostics lock held by the flusher",
                )
