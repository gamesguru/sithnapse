#!/usr/bin/env python
"""Re-benchmark of mdbx vs. postgres for the embedded HAMT node store, run
to refresh docs/development-gg/persistent-typed-hamt-architecture.md's
comparison table with numbers that are actually reproducible today.

fjall is intentionally NOT included here: it was fully removed in
ab59dd8ba6 (crate, `fjall_engine` bindings, all embedded_hamt_engine=="fjall"
branches), so there is nothing left in this tree to benchmark. A "fjall + UDS
Bridge" number specifically never existed anywhere -- no bridge/RPC/socket
code for fjall was ever written, in any commit, reachable or dangling (see
session history) -- so it is dropped rather than re-estimated.

Same methodology/shape as the now-deleted benchmark_hamt_storage_engines.py
and benchmark_hamt_mdbx.py: 32-byte content-addressed keys, 512-byte node
payloads, uniformly random, 2,000,000-row corpus, batch sizes 1/5/10 for
reads, batch=5 for commit latency.

Usage:
    eval "$(scripts-dev/start_test_postgres.sh)"
    python3 scripts-dev/benchmark_hamt_mdbx_vs_postgres.py
"""

from __future__ import annotations

import os
import random
import shutil
import statistics
import tempfile
import time
import uuid
from typing import Callable

import psycopg2
import psycopg2.extras

from synapse.synapse_rust import mdbx_engine

NODE_SIZE = 512
CORPUS_SIZE = 2_000_000
BATCH_SIZES = (1, 5, 10)
READ_ITERATIONS = 300
COMMIT_BATCH_SIZE = 5
COMMIT_ITERATIONS = 200


def rand_rows(rng: random.Random, n: int) -> list[tuple[bytes, bytes]]:
    return [(rng.randbytes(32), rng.randbytes(NODE_SIZE)) for _ in range(n)]


def percentiles(samples: list[float]) -> tuple[float, float]:
    samples = sorted(samples)
    p50 = statistics.median(samples) * 1e6
    p99 = samples[int(len(samples) * 0.99)] * 1e6
    return p50, p99


def bench_reads(
    name: str, batch_fetch: "Callable[[list[bytes]], object]", keys_pool: list[bytes]
) -> dict[int, tuple[float, float]]:
    rng = random.Random(1)
    results = {}
    for batch_size in BATCH_SIZES:
        samples = []
        for _ in range(READ_ITERATIONS):
            batch = rng.sample(keys_pool, batch_size)
            start = time.perf_counter()
            batch_fetch(batch)
            samples.append(time.perf_counter() - start)
        p50, p99 = percentiles(samples)
        results[batch_size] = (p50, p99)
        print(f"{name:<10} batch={batch_size:<3} p50={p50:8.1f}us  p99={p99:8.1f}us")
    return results


def bench_commit(
    name: str,
    commit_write: "Callable[[list[tuple[bytes, bytes]]], object]",
    rng: random.Random,
) -> tuple[float, float]:
    samples = []
    for _ in range(COMMIT_ITERATIONS):
        rows = rand_rows(rng, COMMIT_BATCH_SIZE)
        start = time.perf_counter()
        commit_write(rows)
        samples.append(time.perf_counter() - start)
    p50, p99 = percentiles(samples)
    print(
        f"{name:<10} commit(batch={COMMIT_BATCH_SIZE}) p50={p50:8.1f}us  p99={p99:8.1f}us"
    )
    return p50, p99


def run_mdbx() -> tuple[dict[int, tuple[float, float]], tuple[float, float]]:
    tmpdir = tempfile.mkdtemp(prefix="hamt-mdbx-bench-")
    try:
        mdbx_engine.open_client(tmpdir)
        rng = random.Random(0)
        print(f"\n=== mdbx: corpus {CORPUS_SIZE:,} nodes x {NODE_SIZE}B ===")

        rows = rand_rows(rng, CORPUS_SIZE)
        start = time.perf_counter()
        mdbx_engine.batch_put(rows)
        elapsed = time.perf_counter() - start
        print(
            f"mdbx  bulk-load {CORPUS_SIZE:,} rows in {elapsed:6.2f}s ({CORPUS_SIZE / elapsed:,.0f} rows/s)"
        )

        keys_pool = [h for h, _ in rows[:20000]]
        reads = bench_reads("mdbx", mdbx_engine.batch_get, keys_pool)
        commit = bench_commit("mdbx", mdbx_engine.transactional_batch_put, rng)
        return reads, commit
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run_postgres() -> tuple[dict[int, tuple[float, float]], tuple[float, float]]:
    host = os.environ.get("SYNAPSE_POSTGRES_HOST", "/tmp/synapse-pgtest")
    port = int(
        os.environ.get(
            "SYNAPSE_POSTGRES_PORT", os.environ.get("SYNAPSE_TEST_PG_PORT", "5433")
        )
    )
    user = os.environ.get("SYNAPSE_POSTGRES_USER", "postgres")
    db_name = f"hamt_bench_{uuid.uuid4().hex[:8]}"
    admin = psycopg2.connect(host=host, port=port, user=user, dbname="postgres")
    admin.autocommit = True
    admin.cursor().execute(f"CREATE DATABASE {db_name}")
    admin.close()

    conn = psycopg2.connect(host=host, port=port, user=user, dbname=db_name)
    cur = conn.cursor()
    try:
        conn.autocommit = True
        cur.execute(
            "CREATE TABLE state_hamt_nodes (structural_hash BYTEA PRIMARY KEY, node_bytes BYTEA NOT NULL)"
        )

        rng = random.Random(0)
        print(f"\n=== postgres: corpus {CORPUS_SIZE:,} nodes x {NODE_SIZE}B ===")
        rows = rand_rows(rng, CORPUS_SIZE)
        start = time.perf_counter()
        # execute_values pages internally (page_size=1000) via separate
        # cur.execute() calls; under autocommit=True each of those pages is its
        # own implicit transaction/commit -- 2,000 commits for a 2M-row corpus,
        # vs. mdbx's batch_put doing the whole corpus in a single transaction.
        # Wrap the whole bulk-load in one explicit transaction so the comparison
        # is apples-to-apples (one commit each), not penalizing postgres with
        # per-page commit overhead that mdbx's side doesn't pay either.
        conn.autocommit = False
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO state_hamt_nodes (structural_hash, node_bytes) VALUES %s",
            [(psycopg2.Binary(h), psycopg2.Binary(v)) for h, v in rows],
            page_size=1000,
        )
        conn.commit()
        conn.autocommit = True
        elapsed = time.perf_counter() - start
        print(
            f"postgres bulk-load {CORPUS_SIZE:,} rows in {elapsed:6.2f}s ({CORPUS_SIZE / elapsed:,.0f} rows/s)"
        )

        keys_pool = [h for h, _ in rows[:20000]]

        def batch_fetch(keys: list[bytes]) -> None:
            cur.execute(
                "SELECT structural_hash, node_bytes FROM state_hamt_nodes WHERE structural_hash = ANY(%s)",
                ([psycopg2.Binary(k) for k in keys],),
            )
            cur.fetchall()

        def commit_write(rows: list[tuple[bytes, bytes]]) -> None:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO state_hamt_nodes (structural_hash, node_bytes) VALUES %s",
                [(psycopg2.Binary(h), psycopg2.Binary(v)) for h, v in rows],
            )

        reads = bench_reads("postgres", batch_fetch, keys_pool)
        commit = bench_commit("postgres", commit_write, rng)
        return reads, commit
    finally:
        cur.close()
        conn.close()
        admin = psycopg2.connect(host=host, port=port, user=user, dbname="postgres")
        admin.autocommit = True
        admin.cursor().execute(f"DROP DATABASE IF EXISTS {db_name}")
        admin.close()


def main() -> None:
    mdbx_reads, mdbx_commit = run_mdbx()
    pg_reads, pg_commit = run_postgres()

    print("\n=== summary (p50, us) ===")
    print(f"{'batch':<8}{'mdbx':<12}{'postgres':<12}{'speedup':<10}")
    for b in BATCH_SIZES:
        m = mdbx_reads[b][0]
        p = pg_reads[b][0]
        print(f"{b:<8}{m:<12.1f}{p:<12.1f}{p / m:<10.1f}")
    m, p = mdbx_commit[0], pg_commit[0]
    print(f"commit  {m:<12.1f}{p:<12.1f}{p / m:<10.1f}")


if __name__ == "__main__":
    main()
