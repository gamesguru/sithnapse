"""Periodic snapshots of the opt-in timing diagnostics.

The diagnostics normally flush from ``atexit``/SIGTERM handlers. Processes that
are SIGKILLed (Complement destroys its containers that way) never get to run
those, so when ``SYNAPSE_TIMINGS_RUN_DIR`` is set the registered flushers are
also called from a background thread. Each flusher writes cumulative
snapshots, so overwriting the previous one is safe.
"""

import json
import logging
import os
import threading
import time
from typing import Callable

logger = logging.getLogger(__name__)

_INTERVAL_SECONDS = 2.0

_lock = threading.Lock()
# Held by the flusher thread for the duration of each flush cycle, and by
# ``fork()`` (see below), so a fork never lands while a flusher holds one of the
# diagnostics' locks -- the child would inherit it locked forever.
_flush_gate = threading.Lock()
_flushers: list[Callable[[], None]] = []
_thread: threading.Thread | None = None
# When this process (or, after a fork, this worker) started, for the
# cpu-versus-wall snapshot below.
_process_start = time.monotonic()
_process_cpu_start = time.process_time()


def _loop() -> None:
    stop = threading.Event()
    while not stop.wait(_INTERVAL_SECONDS):
        with _flush_gate:
            # Snapshot under the gate so unregistering a flusher also waits
            # out any in-flight cycle and cannot leave a stale callback.
            with _lock:
                flushers = list(_flushers)
            for flusher in flushers:
                try:
                    flusher()
                except Exception:
                    logger.debug("periodic timings flush failed", exc_info=True)


def _start_thread_locked() -> None:
    global _thread
    if _thread is None:
        _thread = threading.Thread(target=_loop, name="timings-flush", daemon=True)
        _thread.start()


def _restart_in_forked_child() -> None:
    """Threads don't survive ``fork()``.

    Complement's ``complement_fork_starter`` imports Synapse once and forks
    each worker, so the thread started before the fork lives only in the
    parent; without this every forked worker would never snapshot.
    """
    global _lock, _flush_gate, _thread
    # The parent's locks may have been held by its (now absent) flusher thread.
    global _process_start, _process_cpu_start
    _lock = threading.Lock()
    _flush_gate = threading.Lock()
    _thread = None
    # The child's cpu clock starts at the fork, so its wall clock must too.
    _process_start = time.monotonic()
    _process_cpu_start = time.process_time()
    if _flushers:
        with _lock:
            _start_thread_locked()


def _before_fork() -> None:
    _flush_gate.acquire()


def _after_fork_in_parent() -> None:
    _flush_gate.release()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_before_fork,
        after_in_parent=_after_fork_in_parent,
        after_in_child=_restart_in_forked_child,
    )


def _write_process_cpu() -> None:
    """Snapshot this process's cpu time against its wall time.

    Replace proc_<pid>.json in SYNAPSE_TIMINGS_RUN_DIR when set; filesystem
    errors are ignored. Both values are in seconds: CPU time is cumulative
    since process start (or fork), while wall time starts at this module's
    import (or the child fork hook).

    ``process_time`` sums every thread's user and system time. A low
    cpu/wall ratio only says the process spent its time off-cpu (descheduled,
    blocked on I/O or a lock, or idle); it cannot say which.
    """
    run_dir = os.environ.get("SYNAPSE_TIMINGS_RUN_DIR")
    if not run_dir:
        return
    pid = os.getpid()
    tmp_path = os.path.join(run_dir, f"proc_{pid}.{threading.get_ident()}.tmp")
    final_path = os.path.join(run_dir, f"proc_{pid}.json")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "cpu_s": time.process_time() - _process_cpu_start,
                    "wall_s": time.monotonic() - _process_start,
                },
                f,
            )
        os.replace(tmp_path, final_path)
    except OSError:
        pass


def register_periodic_flush(flusher: Callable[[], None]) -> None:
    """Also call ``flusher`` every couple of seconds if a run dir is set.

    Include process CPU/wall snapshots in the background flush cycle.
    Exceptions from flushers are caught so later flushers can still run.
    """
    if not os.environ.get("SYNAPSE_TIMINGS_RUN_DIR"):
        return
    with _lock:
        if _write_process_cpu not in _flushers:
            _flushers.append(_write_process_cpu)
        _flushers.append(flusher)
        _start_thread_locked()


def unregister_periodic_flush(flusher: Callable[[], None]) -> None:
    """Remove a flusher and wait for any in-progress flush cycle to finish."""
    with _flush_gate:
        with _lock:
            if flusher in _flushers:
                _flushers.remove(flusher)
