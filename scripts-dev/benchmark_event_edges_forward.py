#!/usr/bin/env python
"""Benchmarks Postgres (`event_edges`, the real table) vs. mtxdb's forward
adjacency mirror (`prev_event_id -> [child_event_id]`) for the read pattern
both live call sites actually use: `get_successor_events`
(`event_federation.py:2699`) and `is_event_next_to_forward_gap`
(`events_worker.py:2630`) each look up the children of exactly one
`prev_event_id`, batch size 1.

This is the measurement `res/docs/2026-09-27-event-edge-forward-list-
compaction.md`'s "Status update" section calls out as the prerequisite for
dropping the forward mirror instead of compacting it: SQL already serves this
lookup on an index (`ev_edges_prev_id`), and both readers already fall back to
SQL on an mtxdb miss, so the mirror only pays for itself if it beats that
indexed query by enough to justify keeping `event_edges_put`'s only
read-modify-write (and the `RMW_LOCK` acquire that comes with it) alive.

Fan-out: real parent->children counts are almost always 1 (a linear DAG
walk), rarely 2-4 (a branch that state resolution will pick one side of), and
very rarely dozens+ (e.g. a netsplit healing merge point) -- modeled as a
95/4/1 split rather than event_json's byte-size split, since what is being
approximated here is branching, not payload size.

Usage:
    eval "$(scripts-dev/start_test_postgres.sh)"
    python3 scripts-dev/benchmark_event_edges_forward.py
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

from synapse.synapse_rust import mtxdb_engine

CUMULATIVE_PARENTS = (50_000, 500_000)
READ_BATCH_SIZES = (1, 20, 100)
READ_ITERATIONS = 300
NAMESPACE = "edges-forward-bench"
ROOM_ID = "!bench:example.org"


def rand_event_id(rng: random.Random) -> str:
    # "$" + 43 base64url chars, matching real room v4+ event IDs.
    raw = rng.randbytes(32)
    return "$" + base64.urlsafe_b64encode(raw)[:43].decode()


def rand_fanout(rng: random.Random) -> int:
    roll = rng.random()
    if roll < 0.95:
        return 1
    if roll < 0.99:
        return rng.randint(2, 4)
    return rng.randint(5, 40)


def rand_parent_children(
    rng: random.Random, n_parents: int
) -> list[tuple[str, list[str]]]:
    """`n_parents` fresh parents, each with a fan-out drawn from
    `rand_fanout`. Every child id is globally unique, matching real event ids
    (a child never has more than one `prev_event_id` edge to the same
    parent)."""
    out = []
    for _ in range(n_parents):
        parent = rand_event_id(rng)
        children = [rand_event_id(rng) for _ in range(rand_fanout(rng))]
        out.append((parent, children))
    return out


def percentiles(samples: list[float]) -> tuple[float, float]:
    samples = sorted(samples)
    p50 = statistics.median(samples) * 1e6
    p99 = samples[int(len(samples) * 0.99)] * 1e6
    return p50, p99


def bench_reads(
    name: str,
    size: int,
    batch_size: int,
    batch_fetch: "Callable[[list[str]], object]",
    keys_pool: list[str],
) -> None:
    rng = random.Random(1)
    samples = []
    for _ in range(READ_ITERATIONS):
        batch = rng.sample(keys_pool, min(batch_size, len(keys_pool)))
        start = time.perf_counter()
        batch_fetch(batch)
        samples.append(time.perf_counter() - start)
    p50, p99 = percentiles(samples)
    print(
        f"{name:<10} n={size:>9,}  read(batch={batch_size:<3}) "
        f"p50={p50:8.1f}us  p99={p99:8.1f}us"
    )


def run_postgres() -> None:
    host = os.environ.get("SYNAPSE_POSTGRES_HOST", "/tmp/synapse-pgtest")
    port = int(os.environ.get("SYNAPSE_TEST_PG_PORT", "5433"))
    user = os.environ.get("SYNAPSE_POSTGRES_USER", "postgres")
    admin = psycopg2.connect(host=host, port=port, user=user, dbname="postgres")
    admin.autocommit = True
    admin.cursor().execute("DROP DATABASE IF EXISTS event_edges_bench")
    admin.cursor().execute("CREATE DATABASE event_edges_bench")
    admin.close()

    conn = psycopg2.connect(host=host, port=port, user=user, dbname="event_edges_bench")
    conn.autocommit = True
    cur = conn.cursor()
    # Mirrors the real table + index shape (full_schemas/72/full.sql.postgres:
    # 305-310, 1183).
    cur.execute(
        "CREATE TABLE event_edges ("
        "event_id text NOT NULL, prev_event_id text NOT NULL, "
        "room_id text, is_state boolean DEFAULT false NOT NULL)"
    )
    cur.execute("CREATE INDEX ev_edges_prev_id ON event_edges (prev_event_id)")

    rng = random.Random(0)
    seen_parents = 0
    for target in CUMULATIVE_PARENTS:
        to_add = target - seen_parents
        parent_children = rand_parent_children(rng, to_add)
        rows = [
            (child, parent, ROOM_ID, False)
            for parent, children in parent_children
            for child in children
        ]
        start = time.perf_counter()
        conn.autocommit = False
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO event_edges (event_id, prev_event_id, room_id, is_state) "
            "VALUES %s",
            rows,
            page_size=1000,
        )
        conn.commit()
        conn.autocommit = True
        elapsed = time.perf_counter() - start
        seen_parents = target
        print(
            f"postgres   bulk-load +{to_add:>9,} parents "
            f"({len(rows):>9,} edge rows) in {elapsed:6.2f}s "
            f"({len(rows) / elapsed:,.0f} rows/s)"
        )

        keys_pool = [parent for parent, _ in parent_children[:5000]]

        def make_batch_fetch() -> "Callable[[list[str]], object]":
            def batch_fetch(keys: list[str]) -> None:
                cur.execute(
                    "SELECT prev_event_id, event_id FROM event_edges "
                    "WHERE prev_event_id = ANY(%s)",
                    (keys,),
                )
                cur.fetchall()

            return batch_fetch

        batch_fetch = make_batch_fetch()
        for batch_size in READ_BATCH_SIZES:
            bench_reads("postgres", target, batch_size, batch_fetch, keys_pool)

    cur.close()
    conn.close()
    admin = psycopg2.connect(host=host, port=port, user=user, dbname="postgres")
    admin.autocommit = True
    admin.cursor().execute("DROP DATABASE IF EXISTS event_edges_bench")
    admin.close()


def run_mtxdb() -> None:
    tmpdir = tempfile.mkdtemp(prefix="event-edges-forward-mtxdb-bench-")
    try:
        mtxdb_engine.open_client(tmpdir)

        rng = random.Random(0)
        seen_parents = 0
        for target in CUMULATIVE_PARENTS:
            to_add = target - seen_parents
            parent_children = rand_parent_children(rng, to_add)
            rows = [
                (ROOM_ID, child, parent, False)
                for parent, children in parent_children
                for child in children
            ]
            start = time.perf_counter()
            mtxdb_engine.event_edges_put(NAMESPACE, rows)
            elapsed = time.perf_counter() - start
            seen_parents = target
            print(
                f"mtxdb      bulk-load +{to_add:>9,} parents "
                f"({len(rows):>9,} edge rows) in {elapsed:6.2f}s "
                f"({len(rows) / elapsed:,.0f} rows/s)"
            )

            keys_pool = [parent for parent, _ in parent_children[:5000]]

            def batch_fetch(keys: list[str]) -> None:
                mtxdb_engine.event_edges_get_forward(NAMESPACE, keys)

            for batch_size in READ_BATCH_SIZES:
                bench_reads("mtxdb", target, batch_size, batch_fetch, keys_pool)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> None:
    print(
        f"cumulative parents: {CUMULATIVE_PARENTS}, "
        "fan-out 95%=1 / 4%=2-4 / 1%=5-40\n"
    )
    print("--- mtxdb ---")
    run_mtxdb()
    print("\n--- postgres ---")
    run_postgres()


if __name__ == "__main__":
    main()
