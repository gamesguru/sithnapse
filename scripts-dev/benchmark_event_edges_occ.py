#!/usr/bin/env python
"""Benchmarks the optimistic edge-write path under a 16-writer burst.

Every writer hammers `event_edges_put` against one room in rounds, using a
barrier so each round's writes race instead of serializing by chance. Because
a merge reads a forward node, appends, and writes it back, the expectation
granularity decides what conflicts:

- `disjoint`: each writer appends to its own parent. All parents live in the
  same room collection, so a collection-wide expectation would make every
  writer conflict on every round; per-record expectations let them proceed
  unless they touch the same parent.
- `shared`: every writer appends to one parent. Both granularities conflict
  here; this is the price a genuine fork point still pays.

`event_edges_occ_conflicts()` is the engine-side count of commits replayed
after a `StaleRead`, so it measures contention the retry loop absorbed and
Python never saw. `retries` counts the (rare) `BlockingIOError` a writer loop
observes when the engine exhausts its own budget.

Usage:
    uv run --no-sync python scripts-dev/benchmark_event_edges_occ.py
"""

from __future__ import annotations

import argparse
import statistics
import tempfile
import threading
import time

from synapse.synapse_rust import mtxdb_engine as mtxdb_engine

NAMESPACE = "edges-occ-bench"
ROOM_ID = "!occ-bench:example.org"


def run_shape(shape: str, writers: int, rounds: int) -> dict:
    if shape == "disjoint":
        parent_for = lambda writer: f"$parent-{writer:02d}"
    else:
        parent_for = lambda writer: "$parent-shared"

    conflicts_before = mtxdb_engine.event_edges_occ_conflicts()
    barrier = threading.Barrier(writers)
    latencies: list[list[float]] = [[] for _ in range(writers)]
    retries = [0] * writers
    failures: list[str] = []

    def worker(writer: int) -> None:
        parent = parent_for(writer)
        try:
            for round_index in range(rounds):
                barrier.wait()
                child = f"$child-{writer:02d}-{round_index:04d}"
                started = time.perf_counter()
                for attempt in range(8):
                    try:
                        mtxdb_engine.event_edges_put(
                            NAMESPACE, [(ROOM_ID, child, parent, False)]
                        )
                        break
                    except BlockingIOError:
                        retries[writer] += 1
                        if attempt == 7:
                            raise
                latencies[writer].append(time.perf_counter() - started)
        except BaseException as error:  # noqa: BLE001 - surfaced to the caller
            failures.append(f"writer {writer}: {error!r}")

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(writers)]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started

    if failures:
        raise SystemExit("; ".join(failures))

    flat = sorted(latency for writer in latencies for latency in writer)
    ops = writers * rounds
    return {
        "shape": shape,
        "writers": writers,
        "rounds": rounds,
        "ops": ops,
        "elapsed_s": elapsed,
        "throughput": ops / elapsed,
        "conflicts": mtxdb_engine.event_edges_occ_conflicts() - conflicts_before,
        "retries": sum(retries),
        "p50_us": statistics.median(flat) * 1e6,
        "p99_us": flat[int(len(flat) * 0.99)] * 1e6,
    }


def run_batched(writers: int, rounds: int) -> dict:
    """One coalesced commit per round carrying every writer's append.

    This is what `_flush_namespace_locked` does: all queued rows for the
    namespace go through a single `event_edges_put`, so a fork point of
    `writers` children costs one read-append-write commit.
    """
    parent = "$parent-shared"
    conflicts_before = mtxdb_engine.event_edges_occ_conflicts()
    latencies = []
    for round_index in range(rounds):
        rows = [
            (ROOM_ID, f"$child-{writer:02d}-{round_index:04d}", parent, False)
            for writer in range(writers)
        ]
        started = time.perf_counter()
        mtxdb_engine.event_edges_put(NAMESPACE, rows)
        latencies.append(time.perf_counter() - started)
    flattened = sorted(latencies)
    ops = writers * rounds
    elapsed = sum(latencies)
    return {
        "shape": "batched",
        "writers": writers,
        "rounds": rounds,
        "ops": ops,
        "elapsed_s": elapsed,
        "throughput": ops / elapsed,
        "conflicts": mtxdb_engine.event_edges_occ_conflicts() - conflicts_before,
        "retries": 0,
        "p50_us": statistics.median(flattened) * 1e6,
        "p99_us": flattened[int(len(flattened) * 0.99)] * 1e6,
    }


def run_sequential(ops: int, *, durable: bool = False) -> dict:
    """Single writer, one row per commit, a distinct parent each time.

    Isolates commit latency from any contention: each op is one append to a
    fresh parent, so no expectation can conflict. With ``durable``, each op
    also waits for the journal's fsync before timing stops, giving the
    durability floor group commit would need to beat.
    """
    latencies = []
    for index in range(ops):
        parent = f"$seq-parent-{index:06d}"
        child = f"$seq-child-{index:06d}"
        started = time.perf_counter()
        mtxdb_engine.event_edges_put(NAMESPACE, [(ROOM_ID, child, parent, False)])
        if durable:
            mtxdb_engine.wait_durable(mtxdb_engine.request_durable())
        latencies.append(time.perf_counter() - started)
    flattened = sorted(latencies)
    return {
        "ops": ops,
        "durable": durable,
        "mean_us": statistics.mean(latencies) * 1e6,
        "p50_us": statistics.median(flattened) * 1e6,
        "p99_us": flattened[int(len(flattened) * 0.99)] * 1e6,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writers", type=int, default=16)
    parser.add_argument("--rounds", type=int, default=200)
    parser.add_argument(
        "--sequential-ops",
        type=int,
        default=500,
        help="single-writer commits to time for the latency floor (0 disables)",
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="edges-occ-bench-") as store:
        mtxdb_engine.open_client(store)
        print(f"{args.writers} writers x {args.rounds} rounds\n")
        header = (
            f"{'shape':<10}{'ops':>7}{'elapsed':>10}{'ops/s':>12}"
            f"{'conflicts':>11}{'retries':>9}{'p50us':>9}{'p99us':>9}"
        )
        print(header)
        print("-" * len(header))
        results = [
            run_shape("disjoint", args.writers, args.rounds),
            run_shape("shared", args.writers, args.rounds),
            run_batched(args.writers, args.rounds),
        ]
        for result in results:
            print(
                f"{result['shape']:<10}{result['ops']:>7}"
                f"{result['elapsed_s']:>9.3f}s"
                f"{result['throughput']:>12,.0f}"
                f"{result['conflicts']:>11,}{result['retries']:>9,}"
                f"{result['p50_us']:>9.1f}{result['p99_us']:>9.1f}"
            )
        for durable in (False, True):
            seq = run_sequential(args.sequential_ops, durable=durable)
            suffix = "durable" if durable else "published"
            print(
                f"\nsingle writer, {seq['ops']} {suffix} commits (1 row, "
                f"distinct parents):"
                f"  mean={seq['mean_us']:,.0f}us"
                f"  p50={seq['p50_us']:,.0f}us"
                f"  p99={seq['p99_us']:,.0f}us"
            )


if __name__ == "__main__":
    main()
