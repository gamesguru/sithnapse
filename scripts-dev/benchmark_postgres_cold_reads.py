"""Measure PostgreSQL point lookups after restart and page-cache eviction.

Use only with a dedicated, disposable disk-backed cluster started with::

    SYNAPSE_TEST_PG_STORAGE=disk SYNAPSE_TEST_PG_DATA=/path/to/cluster \\
      scripts-dev/start_test_postgres.sh

The benchmark stops and restarts that cluster between samples. It is deliberately
incompatible with the normal tmpfs test cluster, whose results would not measure
storage misses.
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import subprocess
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
from psycopg2.extensions import connection as PgConnection

DEFAULT_ROWS = 10_000
DEFAULT_VALUE_SIZE = 512
DEFAULT_SAMPLES = 5
DATABASE = "cold_read_bench"
TABLE = "cold_kv"


def connect(socket_dir: str, port: int) -> PgConnection:
    return psycopg2.connect(
        host=socket_dir, port=port, user="postgres", dbname=DATABASE
    )


def evict_cluster_pages(pgdata: Path) -> None:
    """Evict only the dedicated benchmark cluster's clean file-backed pages."""
    if not hasattr(os, "posix_fadvise"):
        raise RuntimeError("this benchmark requires os.posix_fadvise (POSIX/Linux)")

    os.sync()
    for path in pgdata.rglob("*"):
        if not path.is_file():
            continue
        fd = os.open(path, os.O_RDONLY)
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)


def pg_ctl(pgdata: Path, action: str, port: int, socket_dir: str) -> None:
    command = ["pg_ctl", "-D", str(pgdata), action]
    if action == "stop":
        command.extend(["-m", "fast"])
    else:
        command.extend(
            ["-o", f"-p {port} -k {socket_dir} -c listen_addresses=''", "-w"]
        )
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)


def setup(socket_dir: str, port: int, rows: int, value_size: int) -> list[bytes]:
    admin = psycopg2.connect(
        host=socket_dir, port=port, user="postgres", dbname="postgres"
    )
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE}")
        cursor.execute(f"CREATE DATABASE {DATABASE}")
    admin.close()

    keys: list[bytes] = []
    rng = random.Random(0)
    rows_to_insert: list[tuple[object, object]] = []
    for _ in range(rows):
        key = rng.randbytes(32)
        keys.append(key)
        rows_to_insert.append(
            (psycopg2.Binary(key), psycopg2.Binary(rng.randbytes(value_size)))
        )

    conn = connect(socket_dir, port)
    with conn.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {TABLE} (key BYTEA PRIMARY KEY, value BYTEA NOT NULL)"
        )
        psycopg2.extras.execute_values(
            cursor,
            f"INSERT INTO {TABLE} (key, value) VALUES %s",
            rows_to_insert,
            page_size=1000,
        )
    conn.commit()
    conn.close()

    # Ensure that the inserted relation is durable before its files are evicted.
    admin = psycopg2.connect(
        host=socket_dir, port=port, user="postgres", dbname="postgres"
    )
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute("CHECKPOINT")
    admin.close()
    return keys


def cleanup(socket_dir: str, port: int) -> None:
    admin = psycopg2.connect(
        host=socket_dir, port=port, user="postgres", dbname="postgres"
    )
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE}")
    admin.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pgdata", type=Path, required=True)
    parser.add_argument("--socket-dir", default="/tmp/synapse-pgtest")
    parser.add_argument("--port", type=int, default=5433)
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--value-size", type=int, default=DEFAULT_VALUE_SIZE)
    parser.add_argument("--samples", type=int, default=DEFAULT_SAMPLES)
    args = parser.parse_args()

    if args.rows < 1 or args.samples < 1 or args.value_size < 1:
        parser.error("--rows, --samples, and --value-size must be positive")
    if not args.pgdata.is_dir():
        parser.error(f"--pgdata is not a cluster directory: {args.pgdata}")

    keys = setup(args.socket_dir, args.port, args.rows, args.value_size)
    rng = random.Random(1)
    samples: list[float] = []
    try:
        for sample in range(args.samples):
            # Restart removes PostgreSQL's shared_buffers. Since the cluster is
            # dedicated to this run, evicting all its files is narrowly scoped.
            pg_ctl(args.pgdata, "stop", args.port, args.socket_dir)
            evict_cluster_pages(args.pgdata)
            pg_ctl(args.pgdata, "start", args.port, args.socket_dir)

            conn = connect(args.socket_dir, args.port)
            with conn.cursor() as cursor:
                started = time.perf_counter_ns()
                cursor.execute(
                    f"SELECT value FROM {TABLE} WHERE key = %s",
                    (psycopg2.Binary(rng.choice(keys)),),
                )
                value = cursor.fetchone()
                elapsed_us = (time.perf_counter_ns() - started) / 1_000
            conn.close()
            if value is None:
                raise RuntimeError("seeded key was not found")
            samples.append(elapsed_us)
            print(f"sample {sample + 1}/{args.samples}: {elapsed_us:.1f} us")

        ordered = sorted(samples)
        p50 = statistics.median(ordered)
        p95 = ordered[int(len(ordered) * 0.95)]
        p99 = ordered[int(len(ordered) * 0.99)]
        print("\n=== PostgreSQL evicted-page point lookup (us) ===")
        print(
            f"rows={args.rows:,}, value_size={args.value_size}, samples={args.samples}"
        )
        print(f"p50={p50:.1f}  p95={p95:.1f}  p99={p99:.1f}  max={ordered[-1]:.1f}")
    finally:
        # The caller still owns and can stop the dedicated cluster.
        cleanup(args.socket_dir, args.port)


if __name__ == "__main__":
    main()
