"""Periodic snapshots of the opt-in timing diagnostics.

The diagnostics normally flush from ``atexit``/SIGTERM handlers. Processes that
are SIGKILLed (Complement destroys its containers that way) never get to run
those, so when ``SYNAPSE_TIMINGS_RUN_DIR`` is set the registered flushers are
also called from a background thread. Each flusher writes cumulative
snapshots, so overwriting the previous one is safe.
"""

import logging
import os
import threading
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


def _loop() -> None:
    stop = threading.Event()
    while not stop.wait(_INTERVAL_SECONDS):
        with _lock:
            flushers = list(_flushers)
        with _flush_gate:
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
    _lock = threading.Lock()
    _flush_gate = threading.Lock()
    _thread = None
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


def register_periodic_flush(flusher: Callable[[], None]) -> None:
    """Also call ``flusher`` every couple of seconds if a run dir is set."""
    if not os.environ.get("SYNAPSE_TIMINGS_RUN_DIR"):
        return
    with _lock:
        _flushers.append(flusher)
        _start_thread_locked()
