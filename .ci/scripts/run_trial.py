#!/usr/bin/env python3
"""Run Trial and stop it once it has reported its results.

Twisted's distributed Trial parent can hang in reactor shutdown after its
workers have finished (the Nov 25 CI log shows `ReactorNotRunning`,
`joining disttrial worker #0 failed` and `CancelledError` after the final
summary). The outer `timeout` then keeps the job for the full kill budget even
though every test has already reported.

This wrapper runs Trial as a child, forwards its output, and after the final
`PASSED (`/`FAILED (` line gives it a short grace period to exit before
terminating the process. A genuine hang before the summary is still handled by
the outer `timeout` and its SIGUSR1 stack dump.
"""

from __future__ import annotations

import re
import subprocess
import sys
import threading
import time

# Trial's footer is "Ran N tests in Xs", a blank line, then "PASSED (...)" or
# "FAILED (...)". Only a result line after "Ran" counts, so test output that
# happens to start with the same words cannot look like the final result.
RAN = re.compile(r"^Ran \d+ tests? in ")
SUMMARY = re.compile(r"^(PASSED|FAILED) \(")
# A healthy Trial parent exits within a second of the summary; the shutdown
# hang this guards against lasts until the outer timeout.
EXIT_GRACE_SECONDS = 20.0


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print("usage: run_trial.py [trial arguments...]", file=sys.stderr)
        return 2

    proc = subprocess.Popen(
        [sys.executable, "-m", "twisted.trial", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        bufsize=1,
    )

    summary: list[str] = []
    summary_seen = threading.Event()

    def forward_output() -> None:
        assert proc.stdout is not None
        ran_seen = False
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            if RAN.match(line):
                ran_seen = True
            if ran_seen and not summary_seen.is_set() and SUMMARY.match(line):
                summary.append(line.split(" ", 1)[0])
                summary_seen.set()

    reader = threading.Thread(target=forward_output, name="trial-output", daemon=True)
    reader.start()

    summary_deadline: float | None = None
    while True:
        try:
            returncode = proc.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            pass
        else:
            # Let the reader drain the last lines before returning.
            reader.join(5)
            # Trial exited by itself: its own status is the result.
            return returncode

        if summary_seen.is_set() and summary_deadline is None:
            summary_deadline = time.monotonic() + EXIT_GRACE_SECONDS
        if summary_deadline is not None and time.monotonic() >= summary_deadline:
            print(
                "Trial reported its results but did not exit; terminating it.",
                file=sys.stderr,
            )
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            reader.join(5)
            return 1 if summary and summary[0] == "FAILED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
