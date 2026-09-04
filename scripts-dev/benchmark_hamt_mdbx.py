#!/usr/bin/env python
"""libmdbx leg of the storage-engine benchmark, using the same
methodology as benchmark_hamt_small_batches.py (batch 1/5/10 reads,
2M-row corpus) and benchmark_hamt_storage_engines.py (batch=5 commit
latency), so its numbers are directly comparable -- run in this
session rather than trusting an unverified external number.

Usage:
    python3 scripts-dev/benchmark_hamt_mdbx.py
"""

from __future__ import annotations

import random
import shutil
import statistics
import tempfile
import time

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


def main() -> None:
    tmpdir = tempfile.mkdtemp(prefix="hamt-mdbx-bench-")
    try:
        mdbx_engine.open_client(tmpdir)
        rng = random.Random(0)
        print(f"corpus: {CORPUS_SIZE:,} nodes x {NODE_SIZE}B\n")

        start = time.perf_counter()
        rows = rand_rows(rng, CORPUS_SIZE)
        mdbx_engine.batch_put(rows)
        elapsed = time.perf_counter() - start
        print(
            f"mdbx  bulk-load {CORPUS_SIZE:,} rows in {elapsed:6.2f}s ({CORPUS_SIZE / elapsed:,.0f} rows/s)"
        )

        keys_pool = [h for h, _ in rows[:20000]]
        sample_rng = random.Random(1)
        for batch_size in BATCH_SIZES:
            samples = []
            for _ in range(READ_ITERATIONS):
                batch = sample_rng.sample(keys_pool, batch_size)
                start = time.perf_counter()
                mdbx_engine.batch_get(batch)
                samples.append(time.perf_counter() - start)
            p50, p99 = percentiles(samples)
            print(
                f"mdbx       batch={batch_size:<3} p50={p50:8.1f}us  p99={p99:8.1f}us"
            )

        commit_samples = []
        for _ in range(COMMIT_ITERATIONS):
            commit_batch = rand_rows(rng, COMMIT_BATCH_SIZE)
            start = time.perf_counter()
            mdbx_engine.transactional_batch_put(commit_batch)
            commit_samples.append(time.perf_counter() - start)
        p50, p99 = percentiles(commit_samples)
        print(
            f"mdbx       commit(batch={COMMIT_BATCH_SIZE}) p50={p50:8.1f}us  p99={p99:8.1f}us"
        )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
