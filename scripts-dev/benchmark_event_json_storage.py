#!/usr/bin/env python
"""Benchmarks Postgres (event_json, the real table) vs. mdbx for the
event_json access pattern: an immutable blob keyed by event_id, pure point
lookups (get_event), high write volume, no aggregation/joins against it --
the next candidate identified from the same criteria that made HAMT nodes
a good embedded-engine fit (content-addressed, point-keyed, no SQL query
shape needed against the table itself). fjall was also benchmarked here
originally but was dropped after losing to mdbx on every measurement (see
scripts-dev/benchmark_hamt_mdbx.py and docs/development-gg/
persistent-typed-hamt-architecture.md for the historical comparison).

Unlike the HAMT benchmark, this needs no materialize/BFS walk -- event_json
rows don't reference each other, so the plain put/get/batch_get/batch_put
surface (already exposed by mdbx_engine) is the whole engine API surface
needed here.

Keys: 44-byte strings, matching real event_id length (`$` + 43-char
base64-ish hash, per the room version 4+ event ID format). Values: sizes
drawn from a rough small/large split (65% ~500B, 25% ~1.5KB, 10% ~5KB) to
approximate the real skew in event JSON sizes (plain state events are
small; encrypted/rich-content events run larger) -- deliberately not a
single fixed size, unlike the HAMT node benchmark's uniform 512B, since
event_json's size distribution is far less uniform than a HAMT node's.

Usage:
    eval "$(scripts-dev/start_test_postgres.sh)"
    python3 scripts-dev/benchmark_event_json_storage.py
"""

from __future__ import annotations

import base64
import os
import random
import shutil
import statistics
import tempfile
import time
from typing import Callable

import psycopg2
import psycopg2.extras

from synapse.synapse_rust import mdbx_engine

CUMULATIVE_SIZES = (200_000, 2_000_000)
READ_BATCH_SIZES = (1, 20, 100)
READ_ITERATIONS = 300
COMMIT_BATCH_SIZE = 5  # a typical events-persist txn's batch
COMMIT_ITERATIONS = 200


def rand_event_id(rng: random.Random) -> bytes:
    # "$" + 43 base64url chars, matching real room v4+ event IDs.
    raw = rng.randbytes(32)
    return b"$" + base64.urlsafe_b64encode(raw)[:43]


def rand_event_json(rng: random.Random) -> bytes:
    roll = rng.random()
    if roll < 0.65:
        size = rng.randint(300, 700)
    elif roll < 0.90:
        size = rng.randint(1000, 2000)
    else:
        size = rng.randint(3000, 6000)
    # Real event JSON is printable text with no NUL bytes; base64-encoding
    # random bytes gives a payload of the right size that's safe to store
    # as Postgres TEXT (raw randbytes can contain 0x00, which TEXT rejects).
    return base64.urlsafe_b64encode(rng.randbytes(size))[:size]


def rand_rows(rng: random.Random, n: int) -> list[tuple[bytes, bytes]]:
    return [(rand_event_id(rng), rand_event_json(rng)) for _ in range(n)]


def percentiles(samples: list[float]) -> tuple[float, float]:
    samples = sorted(samples)
    p50 = statistics.median(samples) * 1e6
    p99 = samples[int(len(samples) * 0.99)] * 1e6
    return p50, p99


def bench_reads(
    name: str,
    size: int,
    batch_size: int,
    batch_fetch: "Callable[[list[bytes]], object]",
    keys_pool: list[bytes],
) -> None:
    rng = random.Random(1)
    samples = []
    for _ in range(READ_ITERATIONS):
        batch = rng.sample(keys_pool, batch_size)
        start = time.perf_counter()
        batch_fetch(batch)
        samples.append(time.perf_counter() - start)
    p50, p99 = percentiles(samples)
    print(
        f"{name:<10} n={size:>9,}  read(batch={batch_size:<3}) "
        f"p50={p50:8.1f}us  p99={p99:8.1f}us"
    )


def bench_commits(
    name: str,
    size: int,
    commit_write: "Callable[[list[tuple[bytes, bytes]]], object]",
    rng: random.Random,
) -> None:
    samples = []
    for _ in range(COMMIT_ITERATIONS):
        rows = rand_rows(rng, COMMIT_BATCH_SIZE)
        start = time.perf_counter()
        commit_write(rows)
        samples.append(time.perf_counter() - start)
    p50, p99 = percentiles(samples)
    print(
        f"{name:<10} n={size:>9,}  commit(batch={COMMIT_BATCH_SIZE:<3}) "
        f"p50={p50:8.1f}us  p99={p99:8.1f}us"
    )


def run_postgres() -> None:
    host = os.environ.get("SYNAPSE_POSTGRES_HOST", "/tmp/synapse-pgtest")
    port = int(os.environ.get("SYNAPSE_TEST_PG_PORT", "5433"))
    user = os.environ.get("SYNAPSE_POSTGRES_USER", "postgres")
    admin = psycopg2.connect(host=host, port=port, user=user, dbname="postgres")
    admin.autocommit = True
    admin.cursor().execute("DROP DATABASE IF EXISTS event_json_bench")
    admin.cursor().execute("CREATE DATABASE event_json_bench")
    admin.close()

    conn = psycopg2.connect(host=host, port=port, user=user, dbname="event_json_bench")
    conn.autocommit = True
    cur = conn.cursor()
    # Mirrors the real table shape closely enough for I/O-cost purposes:
    # event_id TEXT (unique btree index), json TEXT payload.
    cur.execute(
        "CREATE TABLE event_json (event_id TEXT PRIMARY KEY, json TEXT NOT NULL)"
    )

    rng = random.Random(0)
    seen = 0
    for target in CUMULATIVE_SIZES:
        to_add = target - seen
        rows = rand_rows(rng, to_add)
        start = time.perf_counter()
        # execute_values pages internally (page_size=1000) via separate
        # cur.execute() calls; under autocommit=True each page is its own
        # implicit transaction/commit, while the mdbx leg below does the
        # whole chunk in a single batch_put transaction. Wrap in one
        # explicit transaction so both sides pay one commit per chunk.
        conn.autocommit = False
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO event_json (event_id, json) VALUES %s",
            [(k.decode(), v.decode("latin1")) for k, v in rows],
            page_size=1000,
        )
        conn.commit()
        conn.autocommit = True
        elapsed = time.perf_counter() - start
        seen = target
        print(
            f"postgres   bulk-load +{to_add:>9,} rows in {elapsed:6.2f}s "
            f"({to_add / elapsed:,.0f} rows/s)"
        )

        cur.execute("SELECT event_id FROM event_json TABLESAMPLE SYSTEM (1) LIMIT 5000")
        keys_pool = [row[0].encode() for row in cur.fetchall()]

        def make_batch_fetch() -> "Callable[[list[bytes]], object]":
            def batch_fetch(keys: list[bytes]) -> None:
                cur.execute(
                    "SELECT event_id, json FROM event_json WHERE event_id = ANY(%s)",
                    ([k.decode() for k in keys],),
                )
                cur.fetchall()

            return batch_fetch

        def commit_write(rows: list[tuple[bytes, bytes]]) -> None:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO event_json (event_id, json) VALUES %s",
                [(k.decode(), v.decode("latin1")) for k, v in rows],
            )

        batch_fetch = make_batch_fetch()
        for batch_size in READ_BATCH_SIZES:
            bench_reads("postgres", target, batch_size, batch_fetch, keys_pool)
        bench_commits("postgres", target, commit_write, rng)

    cur.close()
    conn.close()
    admin = psycopg2.connect(host=host, port=port, user=user, dbname="postgres")
    admin.autocommit = True
    admin.cursor().execute("DROP DATABASE IF EXISTS event_json_bench")
    admin.close()


def run_embedded(name: str, engine: object) -> None:
    tmpdir = tempfile.mkdtemp(prefix=f"event-json-{name}-bench-")
    try:
        engine.open_client(tmpdir)  # type: ignore[attr-defined]

        rng = random.Random(0)
        seen = 0
        for target in CUMULATIVE_SIZES:
            to_add = target - seen
            rows = rand_rows(rng, to_add)
            start = time.perf_counter()
            engine.batch_put(rows)  # type: ignore[attr-defined]
            elapsed = time.perf_counter() - start
            seen = target
            print(
                f"{name:<10} bulk-load +{to_add:>9,} rows in {elapsed:6.2f}s "
                f"({to_add / elapsed:,.0f} rows/s)"
            )

            keys_pool = [k for k, _ in rows[:5000]]

            def batch_fetch(keys: list[bytes]) -> None:
                engine.batch_get(keys)  # type: ignore[attr-defined]

            def commit_write(rows: list[tuple[bytes, bytes]]) -> None:
                engine.transactional_batch_put(rows)  # type: ignore[attr-defined]

            for batch_size in READ_BATCH_SIZES:
                bench_reads(name, target, batch_size, batch_fetch, keys_pool)
            bench_commits(name, target, commit_write, rng)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> None:
    print(f"cumulative sizes: {CUMULATIVE_SIZES}, event_json-shaped payloads\n")
    print("--- mdbx ---")
    run_embedded("mdbx", mdbx_engine)
    print("\n--- postgres ---")
    run_postgres()


if __name__ == "__main__":
    main()
