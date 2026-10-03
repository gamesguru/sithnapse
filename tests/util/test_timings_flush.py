#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
import os
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


@py_unittest.skipUnless(hasattr(os, "fork"), "requires fork()")
class TimingsFlushForkTestCase(unittest.TestCase):
    def test_forked_child_runs_registered_flusher(self) -> None:
        """A forked child must keep snapshotting.

        Complement forks each worker from a starter that already imported
        Synapse, and threads do not survive ``fork()``. The child has to
        restart the flusher thread itself, or it silently stops producing
        snapshots.
        """
        with tempfile.TemporaryDirectory() as run_dir:

            def flusher() -> None:
                # Marker named after the *calling* process, so the parent's own
                # flusher can't satisfy the child's check.
                open(os.path.join(run_dir, f"flushed_{os.getpid()}"), "w").close()

            with patch.dict(os.environ, {"SYNAPSE_TIMINGS_RUN_DIR": run_dir}):
                self.addCleanup(timings_flush._flushers.remove, flusher)
                self.addCleanup(setattr, timings_flush, "_INTERVAL_SECONDS", 2.0)
                timings_flush._INTERVAL_SECONDS = 0.02
                timings_flush.register_periodic_flush(flusher)

                pid = os.fork()
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

                _, status = os.waitpid(pid, 0)
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

        The flush is held open by an event, and it is only released once the
        fork thread is *observed* blocked inside the fork-gate hook (a
        positive condition polled with a deadline, not a timed wait for
        something not to happen). If the gate is broken, ``fork()`` returns
        while the flusher still holds the lock, which is detected however the
        threads happen to be scheduled.
        """
        diagnostics_lock = threading.Lock()
        in_flush = threading.Event()
        release_flush = threading.Event()
        fork_returned = threading.Event()
        forked: list[int] = []
        child_exit_codes: list[int] = []

        def flusher() -> None:
            with diagnostics_lock:
                in_flush.set()
                # No timeout: the cleanup below sets the event, so a slow
                # runner cannot self-release the flush and let ``fork()``
                # complete mid-check (which would false-fail the gate assert).
                release_flush.wait()

        def fork_in_thread() -> None:
            pid = os.fork()
            if pid == 0:
                # Child: this thread is the only one that survived. Never
                # return into the test runner.
                code = 1
                try:
                    if diagnostics_lock.acquire(timeout=2):
                        code = 0
                finally:
                    os._exit(code)
            forked.append(pid)
            fork_returned.set()

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

                thread = threading.Thread(target=fork_in_thread, daemon=True)
                thread.start()

                # The flusher is inside its critical section and cannot finish
                # until we say so. Wait until the fork thread is observed
                # blocked in the fork-gate hook; never release the flusher on
                # a timer. This checks the hook the fork thread is stuck in,
                # not fork() in general, and it relies on CPython's
                # ``sys._current_frames`` and on the hook keeping the name
                # ``_before_fork``.
                consecutive_blocked = 0
                deadline = time.monotonic() + 10
                while consecutive_blocked < 3:
                    self.assertFalse(
                        fork_returned.is_set(),
                        "fork() completed while a flusher held the flush gate",
                    )
                    self.assertLess(
                        time.monotonic(), deadline, "fork() never blocked on the gate"
                    )
                    if _is_running_in(thread, "_before_fork"):
                        consecutive_blocked += 1
                    else:
                        consecutive_blocked = 0
                    time.sleep(0.005)

                release_flush.set()
                self.assertTrue(fork_returned.wait(5), "fork() never completed")
                thread.join(5)

                _, status = os.waitpid(forked[0], 0)
                child_exit_codes.append(os.waitstatus_to_exitcode(status))
                self.assertEqual(
                    child_exit_codes,
                    [0],
                    "child inherited a diagnostics lock held by the flusher",
                )
