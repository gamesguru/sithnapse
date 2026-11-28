"""Dump every thread's stack from each trial process when it is sent SIGUSR1.

Put this directory on PYTHONPATH for a step that might wedge. Each Python process
(the trial parent and every ``--jobs`` worker) writes its own stacks to
``$SYNAPSE_HANG_DUMP_DIR/hang-<pid>.txt`` when it receives SIGUSR1, so a worker
stuck on a lock shows up with the frame it is waiting in. Workers' stderr is not
reliably surfaced, hence the files.

There is deliberately no timer. ``faulthandler.dump_traceback_later`` runs a
watchdog thread that walks other threads' frames without the GIL; on Python 3.10
that corrupted healthy workers (``SystemError: unknown opcode``, then a trial
parent that never exited). A signal dump only runs when the CI step asks for it,
after the run is already wedged.
"""

import faulthandler
import os
import signal

_dump_dir = os.environ.get("SYNAPSE_HANG_DUMP_DIR")

if _dump_dir:
    try:
        os.makedirs(_dump_dir, exist_ok=True)
        # Kept open for the life of the process; faulthandler needs the fd.
        _dump_file = open(os.path.join(_dump_dir, f"hang-{os.getpid()}.txt"), "w")
        # Fatal-signal dumps (SIGSEGV etc.) stay on; they only run on a crash.
        faulthandler.enable(file=_dump_file)
        faulthandler.register(signal.SIGUSR1, file=_dump_file, all_threads=True)
    except Exception:
        pass
