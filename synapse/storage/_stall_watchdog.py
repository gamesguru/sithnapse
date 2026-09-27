#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
"""Diagnostic: say what a process was doing while its reactor was blocked.

Enabled with `SYNAPSE_PG_TIMINGS` (the same switch as the other timing
diagnostics, so the container plumbing needs nothing new). A reactor heartbeat
records the time of each loop turn; a watchdog thread notices when the
heartbeat is late by more than `SYNAPSE_STALL_MS` (default 100) and logs every
thread's stack once for that stall. A stalled reactor delays every request in
the process, which shows up as unexplained database time.
"""

import logging
import os
import sys
import threading
import time
import traceback
from typing import Any

logger = logging.getLogger(__name__)

_THRESHOLD_S = float(os.environ.get("SYNAPSE_STALL_MS", "100")) / 1000
_BEAT_S = 0.02
# A stall storm must not become a log storm.
_MAX_REPORTS = 30

_started = False
_last_beat = 0.0


def start_stall_watchdog(reactor: Any) -> None:
    """Start the heartbeat and the watchdog thread, once per process."""
    global _started, _last_beat
    if _started:
        return
    _started = True
    _last_beat = time.monotonic()

    def beat() -> None:
        global _last_beat
        _last_beat = time.monotonic()
        reactor.callLater(_BEAT_S, beat)

    reactor.callWhenRunning(beat)
    threading.Thread(target=_watch, name="stall-watchdog", daemon=True).start()


def _stacks(skip: int) -> str:
    names = {t.ident: t.name for t in threading.enumerate()}
    out = []
    for ident, frame in sys._current_frames().items():
        if ident == skip:
            continue
        frames = traceback.format_stack(frame)[-6:]
        out.append(f"--- {names.get(ident, ident)}\n" + "".join(frames))
    return "\n".join(out)


def _watch() -> None:
    own_ident = threading.get_ident()
    reported = 0
    stalled_since = 0.0
    while reported < _MAX_REPORTS:
        time.sleep(_BEAT_S)
        late = time.monotonic() - _last_beat
        if late < _THRESHOLD_S:
            stalled_since = 0.0
            continue
        if stalled_since == _last_beat:
            continue  # this stall was already reported
        stalled_since = _last_beat
        reported += 1
        logger.warning(
            "[stall] reactor blocked for %.0f ms; thread stacks:\n%s",
            late * 1000,
            _stacks(own_ident),
        )
