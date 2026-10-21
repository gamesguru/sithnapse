#!/usr/bin/env python3
"""Compare per-call timings between two groups of saved trial logs.

Reads the diagnostics report that ``scripts-dev/trial_ctrlc.py`` prints (the
``res/logs/*.log`` files). Nothing is run. Each metric is the median, across
the logs of a group, of that log's per-call average, so one disturbed run
cannot decide the comparison on its own::

    python scripts-dev/compare_trial_logs.py \\
        --a res/logs/2026-10-02-a.log res/logs/2026-10-08-b.log \\
        --b res/logs/2026-10-09-c.log res/logs/2026-10-12-d.log

The cpu stall of every log is printed first. A log over the review threshold
is flagged; the threshold is a prompt to look closer, not a verdict. Rows
ending in ``:cpu`` are thread cpu time for the same span, so a ratio that
holds in wall time but not in cpu time points at waiting, not at the code.
"""

from __future__ import annotations

import argparse
import re
import statistics
import sys
from pathlib import Path

STALL_REVIEW_PERCENT = 25.0
MIN_CALLS = 500

_METRIC = re.compile(
    r"^[\s│├└─]*(?P<name>\S.*?\S)\s{2,}(?P<total>[\d.]+)ms\s+(?P<calls>\d+)"
    r"\s+(?P<avg>[\d.]+)ms",
    re.MULTILINE,
)
_FIELD = {
    "commit": re.compile(r"commit:\s+(\S+)"),
    "pin": re.compile(r"mtxdb pin:\s+(\S+)"),
    "stall": re.compile(r"cpu stall ([\d.]+)%"),
    "sync": re.compile(r"sync mode:\s+(\S+)"),
    "wall": re.compile(r"^Ran \d+ tests in ([\d.]+)s", re.MULTILINE),
}


class Log:
    def __init__(self, path: Path) -> None:
        text = path.read_text(errors="replace")
        self.path = path
        self.fields = {
            key: (m.group(1) if (m := pattern.search(text)) else "?")
            for key, pattern in _FIELD.items()
        }
        self.metrics: dict[str, tuple[int, float]] = {}
        for m in _METRIC.finditer(text):
            self.metrics.setdefault(m["name"], (int(m["calls"]), float(m["avg"])))

    @property
    def stall(self) -> float | None:
        try:
            return float(self.fields["stall"])
        except ValueError:
            return None


def describe(label: str, logs: list[Log]) -> None:
    print(f"{label}:")
    for log in logs:
        stall = log.stall
        flag = (
            f"  <-- review: stall over {STALL_REVIEW_PERCENT:.0f}%"
            if stall is not None and stall > STALL_REVIEW_PERCENT
            else ""
        )
        print(
            f"  {log.path.name[:44]:44} commit={log.fields['commit'][:9]:9} "
            f"pin={log.fields['pin']:8} sync={log.fields['sync']:8} "
            f"stall={log.fields['stall']:>5}% wall={log.fields['wall']}s{flag}"
        )


def group_median(logs: list[Log], name: str) -> float | None:
    values = [
        log.metrics[name][1]
        for log in logs
        if name in log.metrics and log.metrics[name][0] >= MIN_CALLS
    ]
    return statistics.median(values) if values else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--a", nargs="+", type=Path, required=True)
    parser.add_argument("--b", nargs="+", type=Path, required=True)
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    a_logs = [Log(p) for p in args.a]
    b_logs = [Log(p) for p in args.b]
    describe("A", a_logs)
    describe("B", b_logs)

    names = {n for log in a_logs + b_logs for n in log.metrics}
    rows = []
    for name in names:
        a, b = group_median(a_logs, name), group_median(b_logs, name)
        if a and b:
            rows.append((b / a, name, a, b))
    rows.sort(reverse=True)

    print(f"\nper-call average, median across logs (metrics with >={MIN_CALLS} calls)")
    print(f"{'B/A':>6}  {'A ms':>9}  {'B ms':>9}  metric")
    for ratio, name, a, b in rows[: args.top]:
        print(f"{ratio:6.2f}  {a:9.3f}  {b:9.3f}  {name}")
    if len(rows) > args.top:
        print(f"... {len(rows) - args.top} more; lowest:")
        for ratio, name, a, b in rows[-3:]:
            print(f"{ratio:6.2f}  {a:9.3f}  {b:9.3f}  {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
