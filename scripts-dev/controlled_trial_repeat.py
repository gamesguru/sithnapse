"""Run the same Trial selection three times with external system sampling.

By default, runs the full ``tests/`` suite serially. Pass another selection for
the timestamp-format smoke check, for example::

    python scripts-dev/controlled_trial_repeat.py --output-dir /tmp/trial-smoke \\
        tests.http.test_additional_resource

Each run gets separate logs and per-test JSONL timings. The script does not
enable Synapse's query-level timing instrumentation.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import os
import platform
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
TRIAL = ROOT / "scripts-dev" / "trial_ctrlc.py"
PG_QUERY = """
SELECT json_build_object(
  'epoch_ns', (extract(epoch FROM clock_timestamp()) * 1000000000)::bigint,
  'active_connections', (SELECT count(*) FROM pg_stat_activity WHERE state = 'active'),
  'connections', (SELECT count(*) FROM pg_stat_activity),
  'wait_events', (SELECT coalesce(json_object_agg(wait_event_type, n), '{}'::json)
                  FROM (SELECT wait_event_type, count(*) AS n FROM pg_stat_activity
                        WHERE wait_event_type IS NOT NULL GROUP BY wait_event_type) waits),
  'database_stats', (SELECT json_build_object(
      'commits', coalesce(sum(xact_commit), 0),
      'rollbacks', coalesce(sum(xact_rollback), 0),
      'blocks_read', coalesce(sum(blks_read), 0),
      'blocks_hit', coalesce(sum(blks_hit), 0),
      'temp_bytes', coalesce(sum(temp_bytes), 0),
      'tuples_returned', coalesce(sum(tup_returned), 0),
      'tuples_fetched', coalesce(sum(tup_fetched), 0),
      'tuples_inserted', coalesce(sum(tup_inserted), 0),
      'tuples_updated', coalesce(sum(tup_updated), 0),
      'tuples_deleted', coalesce(sum(tup_deleted), 0))
    FROM pg_stat_database WHERE datname NOT IN ('template0', 'template1'))
)::text;
""".strip()


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], cwd=ROOT, stderr=subprocess.DEVNULL)


def fingerprint() -> dict[str, Any]:
    head = git("rev-parse", "HEAD").decode().strip()
    patch = git("diff", "HEAD", "--binary")
    digest = hashlib.sha256(patch)
    untracked = git("ls-files", "--others", "--exclude-standard", "-z")
    for name in untracked.split(b"\0"):
        if name:
            path = ROOT / os.fsdecode(name)
            digest.update(name)
            if path.is_file():
                digest.update(path.read_bytes())

    lock = (ROOT / "Cargo.lock").read_text(encoding="utf-8")
    match = re.search(
        r'source = "git\+https://gitlab\.com/wombat-foundation/mtxdb\.git\?branch=dev#([0-9a-f]+)"',
        lock,
    )
    extension = subprocess.check_output(
        [
            sys.executable,
            "-c",
            "from synapse.synapse_rust import mtxdb_engine; print(mtxdb_engine.__file__)",
        ],
        cwd=ROOT,
        text=True,
    ).strip()
    extension_hash = hashlib.sha256(Path(extension).read_bytes()).hexdigest()
    return {
        "synapse_commit": head,
        "synapse_worktree_patch_sha256": digest.hexdigest(),
        "mtxdb_lock_commit": match.group(1) if match else None,
        "mtxdb_extension": extension,
        "mtxdb_extension_sha256": extension_hash,
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
    }


def utc(epoch_ns: int) -> str:
    seconds, nanoseconds = divmod(epoch_ns, 1_000_000_000)
    return (
        datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        + f".{nanoseconds:09d}Z"
    )


def start_samplers(run_dir: Path, interval: int, device: str) -> list[tuple[Any, ...]]:
    samplers: list[tuple[list[str], str]] = [
        (["iostat", "-y", "-x", "-t", device, str(interval)], "iostat.log"),
        (["pidstat", "-h", "-u", "-r", "-d", str(interval)], "pidstat.log"),
        (["vmstat", "-w", "-t", str(interval)], "vmstat.log"),
    ]
    started: list[tuple[Any, ...]] = []
    for command, filename in samplers:
        output = (run_dir / filename).open("wb")
        errors = (run_dir / (filename + ".stderr")).open("wb")
        try:
            process = subprocess.Popen(command, stdout=output, stderr=errors)
        except FileNotFoundError as e:
            output.close()
            errors.write(str(e).encode())
            errors.close()
            continue
        started.append((process, output, errors))

    pg_out = (run_dir / "postgres.jsonl").open("w", encoding="utf-8")
    pg_err = (run_dir / "postgres.stderr").open("w", encoding="utf-8")
    pg_args = ["psql", "-X", "-q", "-A", "-t", "-c", PG_QUERY, "-d", "postgres"]
    pg_options = (
        ("SYNAPSE_POSTGRES_USER", "-U", "postgres"),
        (
            "SYNAPSE_POSTGRES_HOST",
            "-h",
            "/tmp/synapse-pgtest"
            if Path("/tmp/synapse-pgtest/.s.PGSQL.5433").exists()
            else "",
        ),
        ("SYNAPSE_POSTGRES_PORT", "-p", "5433"),
    )
    for env_name, flag, default in pg_options:
        value = os.environ.get(env_name, default)
        if value:
            pg_args[1:1] = [flag, value]
    stop = threading.Event()

    def sample_postgres() -> None:
        while not stop.is_set():
            try:
                result = subprocess.run(
                    pg_args, cwd=ROOT, capture_output=True, text=True, timeout=interval
                )
                if result.returncode:
                    pg_err.write(result.stderr)
                    pg_err.flush()
                else:
                    for line in result.stdout.splitlines():
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        row["sample_epoch_ns"] = time.time_ns()
                        row["sample_time_utc"] = utc(row["sample_epoch_ns"])
                        pg_out.write(json.dumps(row, separators=(",", ":")) + "\n")
                    pg_out.flush()
            except (OSError, subprocess.TimeoutExpired) as e:
                pg_err.write(f"{utc(time.time_ns())}: {e}\n")
                pg_err.flush()
            stop.wait(interval)

    thread = threading.Thread(target=sample_postgres, name="pg-sampler", daemon=True)
    thread.start()
    # Attach the stop/thread/streams to the list for simple lifecycle handling.
    started.append((stop, thread, pg_out, pg_err))
    return started


def stop_samplers(samplers: list[tuple[Any, ...]]) -> None:
    for item in samplers:
        if isinstance(item[0], threading.Event):
            stop, thread, out, err = item
            stop.set()
            thread.join(timeout=3)
            out.close()
            err.close()
            continue
        process, out, err = item
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        out.close()
        err.close()


def parse_summary(log: str) -> dict[str, Any]:
    ran = re.search(r"Ran (\d+) tests? in ([0-9.]+)s", log)
    outcome = re.search(r"^(PASSED|FAILED) \(([^\n]*)\)", log, re.MULTILINE)
    skips = re.search(r"skips=(\d+)", outcome.group(2)) if outcome else None
    return {
        "test_count": int(ran.group(1)) if ran else None,
        "trial_elapsed_seconds": float(ran.group(2)) if ran else None,
        "outcome": outcome.group(1) if outcome else "UNKNOWN",
        "skips": int(skips.group(1)) if skips else 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--interval", type=int, default=1)
    parser.add_argument("--device", default="sdc")
    parser.add_argument(
        "selection", nargs="*", help="Trial selection (default: tests/)"
    )
    args, trial_args = parser.parse_known_args()
    selection = args.selection or ["tests/"]
    if args.interval < 1:
        parser.error("--interval must be at least one second")
    if any(arg in ("-j", "--jobs") or arg.startswith("--jobs=") for arg in trial_args):
        parser.error("the repeatability runner requires serial Trial (omit -j/--jobs)")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (args.output_dir or ROOT / f"trial-repeat-{stamp}").resolve()
    output_dir.mkdir(parents=True, exist_ok=False)
    command = [sys.executable, str(TRIAL), *trial_args, *selection]
    env = os.environ.copy()
    for key in (
        "SYNAPSE_PG_TIMINGS",
        "SYNAPSE_TIMINGS_RUN_DIR",
        "SYNAPSE_MTXDB_STATS",
        "SYNAPSE_TEST_TIMINGS_FILE",
    ):
        env.pop(key, None)

    base_identity = fingerprint()
    env_identity = hashlib.sha256(
        "\0".join(f"{k}={v}" for k, v in sorted(env.items())).encode()
    ).hexdigest()
    identity = {
        **base_identity,
        "command": command,
        "environment_sha256": env_identity,
        "environment_flags": {
            k: v for k, v in sorted(env.items()) if k.startswith("SYNAPSE_TEST_")
        },
        "expected_synapse_commit": "90f3d0f17487d39d150eeba7959dfc9f241d1114",
        "synapse_commit_matches_requested_baseline": base_identity["synapse_commit"]
        == "90f3d0f17487d39d150eeba7959dfc9f241d1114",
        "postgres_query_instrumentation": "disabled",
        "samplers": ["iostat", "pidstat", "vmstat", "pg_stat_activity/database"],
    }
    (output_dir / "identity.json").write_text(
        json.dumps(identity, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    runs: list[dict[str, Any]] = []
    run_fingerprints = []
    for index in range(1, 4):
        run_dir = output_dir / f"run-{index}"
        run_dir.mkdir()
        timings_path = run_dir / "tests.jsonl"
        run_env = env.copy()
        run_env["SYNAPSE_TEST_TIMINGS_FILE"] = str(timings_path)
        current = fingerprint()
        if current != base_identity:
            raise RuntimeError("checkout/build fingerprint changed during capture")
        run_fingerprints.append(current)
        samplers = start_samplers(run_dir, args.interval, args.device)
        start_ns = time.time_ns()
        log_path = run_dir / "trial.log"
        with log_path.open("w", encoding="utf-8") as log:
            log.write(f"RUN {index}/3 START {utc(start_ns)} epoch_ns={start_ns}\n")
            log.flush()
            print(
                f"Run {index}/3 started at {utc(start_ns)}; log: {log_path}", flush=True
            )
            process = subprocess.Popen(
                command,
                cwd=ROOT,
                env=run_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert process.stdout is not None
            for line in process.stdout:
                sys.stdout.write(line)
                log.write(line)
            return_code = process.wait()
            end_ns = time.time_ns()
            log.write(f"RUN {index}/3 END {utc(end_ns)} epoch_ns={end_ns}\n")
        stop_samplers(samplers)
        text = log_path.read_text(encoding="utf-8", errors="replace")
        runs.append(
            {
                "run": index,
                "start_epoch_ns": start_ns,
                "end_epoch_ns": end_ns,
                "wall_seconds": (end_ns - start_ns) / 1e9,
                "return_code": return_code,
                **parse_summary(text),
                "test_timing_records": sum(
                    1
                    for line in timings_path.read_text(encoding="utf-8").splitlines()
                    if line
                )
                if timings_path.exists()
                else 0,
            }
        )
        (run_dir / "run.json").write_text(
            json.dumps(runs[-1], indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    all_tests: dict[str, list[dict[str, Any] | None]] = {}
    for index, _run in enumerate(runs):
        path = output_dir / f"run-{index + 1}" / "tests.jsonl"
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            record = json.loads(line)
            all_tests.setdefault(record["test"], [None, None, None])[index] = record
    comparisons: list[dict[str, Any]] = []
    for test, records in all_tests.items():
        elapsed = [r["elapsed_ms"] if r else None for r in records]
        valid = [value for value in elapsed if value is not None]
        comparisons.append(
            {
                "test": test,
                "elapsed_ms": elapsed,
                "first_minus_third_ms": elapsed[0] - elapsed[2]
                if elapsed[0] is not None and elapsed[2] is not None
                else None,
                "range_ms": max(valid) - min(valid) if valid else None,
                "records": records,
            }
        )
    comparisons.sort(key=lambda row: row["range_ms"] or 0, reverse=True)
    for row in comparisons[:20]:
        overlaps: list[dict[str, int | None] | None] = []
        for index, record in enumerate(row["records"]):
            pg_path = output_dir / f"run-{index + 1}" / "postgres.jsonl"
            samples = (
                [
                    json.loads(line)
                    for line in pg_path.read_text(encoding="utf-8").splitlines()
                    if line
                ]
                if pg_path.exists()
                else []
            )
            samples.sort(key=lambda sample: sample["sample_epoch_ns"])
            times = [sample["sample_epoch_ns"] for sample in samples]
            if record is None:
                overlaps.append(None)
                continue
            left = bisect.bisect_left(times, record["start_time_epoch_ns"])
            right = bisect.bisect_right(times, record["end_time_epoch_ns"])
            within = samples[left:right]
            overlaps.append(
                {
                    "postgres_samples": len(within),
                    "max_active_connections": max(
                        (sample["active_connections"] for sample in within),
                        default=None,
                    ),
                    "max_connections": max(
                        (sample["connections"] for sample in within), default=None
                    ),
                }
            )
        row["postgres_activity_during_test"] = overlaps
    matching_counts = (
        len({(run["test_count"], run["skips"]) for run in runs}) == 1
        and runs[0]["test_count"] is not None
    )
    matching_identity = (
        len({json.dumps(item, sort_keys=True) for item in run_fingerprints}) == 1
        and len({tuple(command) for _run in runs}) == 1
        and all(run["return_code"] == 0 for run in runs)
    )
    accepted = (
        matching_counts
        and matching_identity
        and identity["synapse_commit_matches_requested_baseline"]
    )
    acceptance: dict[str, bool] = {
        "same_test_and_skip_counts": matching_counts,
        "same_build_command_environment": matching_identity,
        "full_capture_accepted": accepted,
    }
    report = {
        "identity": identity,
        "runs": runs,
        "acceptance": acceptance,
        "tests_by_elapsed_range": comparisons,
        "system_sample_logs": {
            f"run-{i}": ["iostat.log", "pidstat.log", "vmstat.log", "postgres.jsonl"]
            for i in range(1, 4)
        },
    }
    (output_dir / "comparison.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    report_lines = [
        "# Controlled Trial repeatability",
        "",
        f"- Synapse commit: `{identity['synapse_commit']}`",
        f"- mtxdb lock commit: `{identity['mtxdb_lock_commit']}`",
        f"- mtxdb extension SHA-256: `{identity['mtxdb_extension_sha256']}`",
        f"- Command: `{' '.join(command)}`",
        f"- Requested baseline match: `{identity['synapse_commit_matches_requested_baseline']}`",
        f"- Accepted: `{accepted}`",
        "",
        "| Run | Wall (s) | Trial (s) | Tests | Skips | Outcome |",
        "|---:|---:|---:|---:|---:|:---|",
    ]
    for run in runs:
        report_lines.append(
            f"| {run['run']} | {run['wall_seconds']:.3f} | "
            f"{run['trial_elapsed_seconds']} | {run['test_count']} | "
            f"{run['skips']} | {run['outcome']} |"
        )
    report_lines.extend(
        [
            "",
            "Top per-test timing ranges; PostgreSQL activity is the maximum sampled "
            "during each test interval. Short tests may have no overlapping 1-second sample.",
            "",
            "| Test | Range (ms) | R1 (ms / active) | R2 (ms / active) | R3 (ms / active) | R1−R3 (ms) |",
            "|:---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in comparisons[:20]:
        cells = []
        for elapsed, activity in zip(
            row["elapsed_ms"], row["postgres_activity_during_test"]
        ):
            active = (
                activity["max_active_connections"]
                if activity and activity["max_active_connections"] is not None
                else "n/a"
            )
            cells.append(f"{elapsed:.2f} / {active}" if elapsed is not None else "n/a")
        delta = row["first_minus_third_ms"]
        report_lines.append(
            f"| `{row['test']}` | {row['range_ms']:.2f} | "
            f"{cells[0]} | {cells[1]} | {cells[2]} | "
            f"{delta:.2f} |"
        )
    report_lines.extend(
        [
            "",
            "System samples: `iostat.log`, `pidstat.log`, `vmstat.log`, and "
            "`postgres.jsonl` in each `run-N/` directory. Test JSONL records include "
            "UTC and epoch-nanosecond start/end times for interval alignment.",
            "",
            "Unexplained timing differences remain unattributed; this report does not "
            "infer causality from overlapping system samples.",
            "",
        ]
    )
    (output_dir / "comparison.md").write_text("\n".join(report_lines), encoding="utf-8")
    print(f"Repeatability artifacts: {output_dir}")
    print(f"Acceptance: {report['acceptance']}")
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
