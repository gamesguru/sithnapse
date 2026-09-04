#!/usr/bin/env python
"""
Thin wrapper around `twisted.trial` that stops and prints the test summary
(counts, failures, errors) on the first Ctrl+C, instead of trial's default
behaviour.

Stock `trial` runs each test inside its own mini reactor loop, and a Ctrl+C
there is caught *per test* as a plain `KeyboardInterrupt`, which trial simply
records as an [ERROR] on that one test before carrying on to the next one.
It's only after enough repeated Ctrl+Cs that the interrupt finally escapes
uncaught, printing a raw Python traceback and skipping the summary
(`IReporter.done()`) entirely.

This wrapper installs its own SIGINT handler and runs the decorated suite: the
first Ctrl+C lets whatever test is currently running
finish, then stops before starting the next one and prints the summary for
everything that ran. A second Ctrl+C (while nothing is catching it) falls
back to the normal hard-interrupt behaviour.

Usage: same arguments as `trial`, e.g. `python scripts-dev/trial_ctrlc.py tests.foo.bar`
"""

import signal
import sys

from twisted.python import usage
from twisted.scripts.trial import Options, _getSuite, _initialDebugSetup, _makeRunner
from twisted.trial import itrial, unittest
from twisted.trial.runner import TrialRunner, _logFile, _testDirectory


def run() -> None:
    config = Options()
    try:
        config.parseOptions()
    except usage.error as ue:
        raise SystemExit(f"{sys.argv[0]}: {ue}")

    _initialDebugSetup(config)
    if config["jobs"] is not None:
        raise SystemExit(f"{sys.argv[0]}: --jobs is not supported by this wrapper")
    if config["dry-run"]:
        raise SystemExit(f"{sys.argv[0]}: --dry-run is not supported by this wrapper")
    if config["profile"]:
        raise SystemExit(f"{sys.argv[0]}: --profile is not supported by this wrapper")

    trialRunner = _makeRunner(config)
    assert isinstance(trialRunner, TrialRunner)
    suite = _getSuite(config)
    test = unittest.decorate(suite, itrial.ITestCase)

    result = trialRunner._makeResult()

    interrupted = False

    def onSigint(signum: int, frame: object) -> None:
        nonlocal interrupted
        if interrupted:
            # Second Ctrl+C: give up waiting and hard-interrupt immediately.
            signal.signal(signal.SIGINT, signal.default_int_handler)
            raise KeyboardInterrupt()
        interrupted = True
        result.shouldStop = True
        sys.stderr.write(
            "\nInterrupted -- finishing the current test, then printing "
            "results so far (Ctrl+C again to abort immediately)...\n"
        )

    previousHandler = signal.signal(signal.SIGINT, onSigint)
    try:
        with (
            _testDirectory(trialRunner.workingDirectory),
            _logFile(trialRunner.logfile),
        ):
            # Running the decorated suite preserves Trial's class/module fixtures
            # and reactor cleanup. `result.shouldStop` makes the suite stop after
            # the current test when Ctrl+C is received, and also honours --exitfirst.
            test.run(result)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        signal.signal(signal.SIGINT, previousHandler)
        # Always print the summary, whether the run finished or was
        # interrupted partway through.
        result.done()

    sys.exit(130 if interrupted else int(not result.wasSuccessful()))


if __name__ == "__main__":
    run()
