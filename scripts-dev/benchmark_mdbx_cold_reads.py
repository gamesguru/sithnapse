#!/usr/bin/env python
"""Measure libmdbx point lookups after best-effort OS page-cache eviction.

This complements the steady-state benchmarks. It deliberately runs every
measured lookup in a fresh process: the Rust mdbx binding owns a process-global
environment and its mmap must be gone before asking the kernel to reclaim the
database's file-backed pages.

On Linux, ``POSIX_FADV_DONTNEED`` is advisory, so label these results
"evicted-page" rather than claiming perfectly cold storage. For a strict
device-cold result, boot into a controlled test host (or use a data set larger
than RAM) and run this script there. It never uses ``drop_caches`` and affects
only its temporary MDBX files.

Usage::

    python3 scripts-dev/benchmark_mdbx_cold_reads.py
    python3 scripts-dev/benchmark_mdbx_cold_reads.py --rows 2000000 --samples 200
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

DEFAULT_ROWS = 200_000
DEFAULT_VALUE_SIZE = 512
DEFAULT_SAMPLES = 100


def read_key(path: Path, index: int) -> bytes:
    with path.open("rb") as keys:
        keys.seek(index * 32)
        key = keys.read(32)
    if len(key) != 32:
        raise ValueError(f"key {index} is outside {path}")
    return key


def evict_database_pages(database_dir: Path) -> None:
    """Ask the kernel to reclaim clean pages from this temporary MDBX database."""
    if not hasattr(os, "posix_fadvise"):
        raise RuntimeError("this benchmark requires os.posix_fadvise (POSIX/Linux)")

    os.sync()
    for path in database_dir.iterdir():
        if not path.is_file():
            continue
        fd = os.open(path, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)


def seed(database_dir: Path, keys_path: Path, rows: int, value_size: int) -> None:
    # Import here so the coordinator process never owns the process-global MDBX
    # environment. This process exits immediately after building the corpus.
    from synapse.synapse_rust import mdbx_engine

    rng = random.Random(0)
    entries: list[tuple[bytes, bytes]] = []
    with keys_path.open("wb") as keys:
        for _ in range(rows):
            key = rng.randbytes(32)
            keys.write(key)
            entries.append((key, rng.randbytes(value_size)))

    mdbx_engine.open_client(str(database_dir))
    mdbx_engine.batch_put(entries)


def measure(database_dir: Path, key: bytes) -> None:
    # Time only the lookup. Opening the environment happens before the timer,
    # but it may itself fault metadata pages and is intentionally part of the
    # cold-cache setup, just as a restarted Synapse worker would do.
    from synapse.synapse_rust import mdbx_engine

    mdbx_engine.open_client(str(database_dir))
    started = time.perf_counter_ns()
    value = mdbx_engine.get(key)
    elapsed_us = (time.perf_counter_ns() - started) / 1_000
    if value is None:
        raise RuntimeError("seeded key was not found")
    print(json.dumps({"elapsed_us": elapsed_us}))


def child_command(role: str, *args: str) -> list[str]:
    return [sys.executable, str(Path(__file__).resolve()), f"--{role}", *args]


def run_parent(rows: int, value_size: int, samples: int, workdir: Path) -> None:
    if not workdir.is_dir():
        raise ValueError(f"--workdir is not a directory: {workdir}")
    # Do not default to /tmp: it is commonly tmpfs (as it is on CI and this
    # development host), which would turn an I/O-cold benchmark into RAM-only
    # timing. The caller may select a dedicated directory on the target NVMe.
    workspace = Path(tempfile.mkdtemp(prefix="mdbx-evicted-page-bench-", dir=workdir))
    database_dir = workspace / "database"
    keys_path = workspace / "keys.bin"
    database_dir.mkdir()
    try:
        subprocess.run(
            child_command(
                "seed",
                str(database_dir),
                str(keys_path),
                str(rows),
                str(value_size),
            ),
            check=True,
        )

        rng = random.Random(1)
        elapsed_us: list[float] = []
        for sample in range(samples):
            # A new random key and a new process per sample avoid measuring a
            # progressively warmed B-tree. The advisory eviction targets only
            # this temporary database, never the host-wide page cache.
            evict_database_pages(database_dir)
            key = read_key(keys_path, rng.randrange(rows))
            result = subprocess.run(
                child_command("measure", str(database_dir), key.hex()),
                check=True,
                capture_output=True,
                text=True,
            )
            elapsed_us.append(json.loads(result.stdout)["elapsed_us"])
            print(
                f"sample {sample + 1:>{len(str(samples))}}/{samples}: {elapsed_us[-1]:.1f} us"
            )

        ordered = sorted(elapsed_us)
        p50 = statistics.median(ordered)
        p95 = ordered[int(len(ordered) * 0.95)]
        p99 = ordered[int(len(ordered) * 0.99)]
        print("\n=== MDBX evicted-page point lookup (us) ===")
        print(f"rows={rows:,}, value_size={value_size}, samples={samples}")
        print(f"p50={p50:.1f}  p95={p95:.1f}  p99={p99:.1f}  max={ordered[-1]:.1f}")
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--value-size", type=int, default=DEFAULT_VALUE_SIZE)
    parser.add_argument("--samples", type=int, default=DEFAULT_SAMPLES)
    parser.add_argument(
        "--workdir",
        type=Path,
        default=Path.cwd(),
        help="on-disk directory for the temporary database (default: current directory)",
    )
    parser.add_argument(
        "--seed", nargs=4, metavar=("DATABASE", "KEYS", "ROWS", "VALUE_SIZE")
    )
    parser.add_argument("--measure", nargs=2, metavar=("DATABASE", "KEY_HEX"))
    args = parser.parse_args()

    if args.seed:
        database, keys, rows, value_size = args.seed
        seed(Path(database), Path(keys), int(rows), int(value_size))
    elif args.measure:
        database, key_hex = args.measure
        measure(Path(database), bytes.fromhex(key_hex))
    else:
        if args.rows < 1 or args.samples < 1 or args.value_size < 1:
            parser.error("--rows, --samples, and --value-size must be positive")
        run_parent(args.rows, args.value_size, args.samples, args.workdir)


if __name__ == "__main__":
    main()
