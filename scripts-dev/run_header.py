#!/usr/bin/env python
"""Run identity and machine-load lines shared by `trial_ctrlc.py` and
`complement.sh`, so a saved log says what code, pin and settings produced it and
whether the machine was busy.

    run_header.py start STATE_FILE [label=value ...]   print header, save baseline
    run_header.py end STATE_FILE                       print load/pressure over the run

Importable too: `Baseline()` snapshots the machine, `header_lines()` and
`load_lines()` format it.
"""

import json
import os
import re
import subprocess
import sys
import time
from typing import Optional

_REPO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def git(*args: str) -> str:
    try:
        return subprocess.run(
            ["git", *args],
            cwd=_REPO,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        ).stdout.strip()
    except Exception:
        return "?"


def mtxdb_pin() -> str:
    try:
        with open(os.path.join(_REPO, "Cargo.lock")) as f:
            m = re.search(r'name = "mtxdb"\n.*?#([0-9a-f]{7})', f.read(), re.S)
        if m:
            return m.group(1)
    except OSError:
        pass
    return "?"


def load_average() -> str:
    try:
        return " ".join(f"{value:.2f}" for value in os.getloadavg())
    except OSError:
        return "?"


def pressure_snapshot() -> dict[str, float]:
    """Cumulative stall seconds (PSI cpu/io/memory "some") and CPU jiffies, so
    two snapshots give what share of the run was spent stalled or in iowait."""
    snapshot: dict[str, float] = {}
    for resource in ("cpu", "io", "memory"):
        try:
            with open(f"/proc/pressure/{resource}") as f:
                for line in f:
                    if line.startswith("some"):
                        snapshot[resource] = int(line.rsplit("total=", 1)[1]) / 1e6
        except (OSError, ValueError, IndexError):
            pass
    try:
        with open("/proc/stat") as f:
            fields = [int(v) for v in f.readline().split()[1:]]
        snapshot["jiffies"] = float(sum(fields))
        snapshot["iowait"] = float(fields[4])
    except (OSError, ValueError, IndexError):
        pass
    return snapshot


class Baseline:
    """The machine's state when a run starts."""

    def __init__(
        self,
        load: Optional[str] = None,
        pressure: Optional[dict[str, float]] = None,
        wall: Optional[float] = None,
    ) -> None:
        self.load = load if load is not None else load_average()
        self.pressure = pressure if pressure is not None else pressure_snapshot()
        self.wall = wall if wall is not None else time.time()

    def save(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(
                {"load": self.load, "pressure": self.pressure, "wall": self.wall}, f
            )

    @classmethod
    def load_from(cls, path: str) -> "Baseline":
        try:
            with open(path) as f:
                data = json.load(f)
            return cls(data["load"], data["pressure"], data["wall"])
        except (OSError, ValueError, KeyError):
            return cls()

    def pressure_summary(self) -> str:
        """Share of the run stalled on cpu/io/memory (PSI) and in iowait, which
        load average cannot separate."""
        end = pressure_snapshot()
        wall = time.time() - self.wall
        if wall <= 0 or not self.pressure or not end:
            return "unavailable"
        parts = [
            f"{name} stall {100 * (end[name] - self.pressure[name]) / wall:.1f}%"
            for name in ("cpu", "io", "memory")
            if name in end and name in self.pressure
        ]
        jiffies = end.get("jiffies", 0) - self.pressure.get("jiffies", 0)
        if jiffies > 0 and "iowait" in end and "iowait" in self.pressure:
            iowait = end["iowait"] - self.pressure["iowait"]
            parts.append(f"iowait {100 * iowait / jiffies:.1f}% of cpu time")
        return ", ".join(parts)


def header_lines(
    extra: list[tuple[str, str]], baseline: Optional[Baseline] = None
) -> list[str]:
    """Code identity plus caller-specific settings (`extra`: label, value)."""
    dirty = git("status", "--porcelain", "--untracked-files=no")
    modified = f"{len(dirty.splitlines())} modified file(s)" if dirty else "clean"
    rows = [
        (
            "commit",
            f"{git('rev-parse', '--short=9', 'HEAD')} ({modified}) "
            f"{git('log', '-1', '--format=%s')}",
        ),
        ("branch", git("rev-parse", "--abbrev-ref", "HEAD")),
        ("mtxdb pin", mtxdb_pin()),
        ("cpus", str(os.cpu_count())),
        *extra,
    ]
    if baseline is not None:
        rows.append(("load avg", f"{baseline.load} at start (1/5/15 min)"))
    return [f"  {label + ':':<14}{value}" for label, value in rows]


def load_lines(baseline: Baseline) -> list[str]:
    return [
        f"  {'load avg:':<14}{baseline.load} at start, {load_average()} at end "
        "(1/5/15 min)",
        f"  {'pressure:':<14}{baseline.pressure_summary()} (over the run)",
    ]


def main() -> None:
    action, state = sys.argv[1], sys.argv[2]
    if action == "start":
        extra = [tuple(a.split("=", 1)) for a in sys.argv[3:] if "=" in a]
        baseline = Baseline()
        baseline.save(state)
        print("\n".join(header_lines(extra, baseline)))  # type: ignore[arg-type]
    elif action == "end":
        print("\n".join(load_lines(Baseline.load_from(state))))


if __name__ == "__main__":
    main()
