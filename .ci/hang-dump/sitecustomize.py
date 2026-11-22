"""Dump every thread's stack from each trial process while a CI step runs.

Put this directory on PYTHONPATH for a step that might wedge. Each Python process
(the trial parent and every ``--jobs`` worker) writes its own stacks to
``$SYNAPSE_HANG_DUMP_DIR/hang-<pid>.txt`` after ``$SYNAPSE_HANG_DUMP_SECONDS``
and then repeats, so a worker stuck on a lock shows up with the frame it is
waiting in. Workers' stderr is not reliably surfaced, hence the files.
"""

import faulthandler
import os

_dump_dir = os.environ.get("SYNAPSE_HANG_DUMP_DIR")
_seconds = float(os.environ.get("SYNAPSE_HANG_DUMP_SECONDS", "600"))

if _dump_dir:
    try:
        os.makedirs(_dump_dir, exist_ok=True)
        # Kept open for the life of the process; faulthandler needs the fd.
        _dump_file = open(os.path.join(_dump_dir, f"hang-{os.getpid()}.txt"), "w")
        faulthandler.enable(file=_dump_file)
        faulthandler.dump_traceback_later(_seconds, repeat=True, file=_dump_file)
    except Exception:
        pass
