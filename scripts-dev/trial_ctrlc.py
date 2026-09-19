#!/usr/bin/env python
"""
Thin wrapper around `twisted.trial` that stops and prints the test summary
(counts, failures, errors) on the first Ctrl+C, instead of trial's default
behaviour. Also aggregates multi-worker diagnostics when SYNAPSE_PG_TIMINGS=1.

Usage: same arguments as `trial`, e.g.:
    python scripts-dev/trial_ctrlc.py -j 4 tests.storage.test_state

Set ``SYNAPSE_TEST_TIMINGS_FILE`` to write one compact JSONL record per
completed test, including its test ID, process ID, and elapsed milliseconds.
"""

import json
import os
import re
import shutil
import signal
import sys
import tempfile
import time
from collections import defaultdict
from typing import IO, Protocol, cast

from twisted.python import usage
from twisted.scripts.trial import Options, _getSuite, _initialDebugSetup, _makeRunner
from twisted.trial import itrial, unittest
from twisted.trial.runner import TrialRunner, _logFile, _testDirectory


class _DistributedRunner(Protocol):
    _workingDirectory: str


class _TimedTest(Protocol):
    def id(self) -> str: ...


class _TestResult(Protocol):
    def startTest(self, test: object) -> None: ...

    def stopTest(self, test: object) -> None: ...


def _install_test_timing_logging(
    result: _TestResult,
) -> IO[str] | None:
    """Add opt-in per-test JSONL timing around a Trial result.

    This deliberately wraps the result rather than the tests themselves, so
    setup/teardown and failures are included without changing test execution.
    It is enabled only when ``SYNAPSE_TEST_TIMINGS_FILE`` is set.
    """
    path = os.environ.get("SYNAPSE_TEST_TIMINGS_FILE")
    if not path:
        return None

    try:
        timing_file = open(path, "a", encoding="utf-8")
    except OSError as e:
        print(f"warning: unable to open test timings file {path}: {e}", file=sys.stderr)
        return None

    starts: dict[int, int] = {}
    original_start = result.startTest
    original_stop = result.stopTest

    def start_test(test: object) -> None:
        starts[id(test)] = time.perf_counter_ns()
        original_start(test)

    def stop_test(test: object) -> None:
        started = starts.pop(id(test), None)
        if started is not None:
            record = {
                "pid": os.getpid(),
                "test": cast(_TimedTest, test).id(),
                "elapsed_ms": (time.perf_counter_ns() - started) / 1_000_000,
            }
            timing_file.write(json.dumps(record, separators=(",", ":")) + "\n")
            timing_file.flush()
        original_stop(test)

    result.startTest = start_test  # type: ignore[method-assign]
    result.stopTest = stop_test  # type: ignore[method-assign]
    return timing_file


def _flush_process_timings() -> None:
    """Flush diagnostics owned by this process before aggregating them."""
    flushers = (
        ("tests.server", "flush_pg_timings"),
        ("synapse.storage.database", "flush_table_ops"),
        ("synapse.storage.databases.state.bg_updates", "flush_state_timings"),
        ("synapse.storage.databases.state.bg_updates", "flush_node_write_stats"),
        ("synapse.storage.databases.main.embedded_common", "flush_ffi_timings"),
    )
    for module_name, function_name in flushers:
        module = sys.modules.get(module_name)
        if module is not None:
            flusher = getattr(module, function_name, None)
            if flusher is not None:
                flusher()


def _aggregate_and_print_timings(timings_dir: str) -> None:
    timings_path = os.environ.get("SYNAPSE_PG_TIMINGS_FILE")
    out_file = None
    if timings_path:
        try:
            out_file = open(timings_path, "a", encoding="utf-8")
        except OSError:
            pass

    def out(*args: object) -> None:
        print(*args, file=sys.stderr)
        if out_file is not None:
            print(*args, file=out_file)
            out_file.flush()

    try:
        files = os.listdir(timings_dir)
    except OSError:
        return

    lifecycle_files = [
        f for f in files if f.startswith("lifecycle_") and f.endswith(".json")
    ]
    sql_files = [f for f in files if f.startswith("sql_") and f.endswith(".json")]
    state_files = [f for f in files if f.startswith("state_") and f.endswith(".json")]
    node_files = [
        f for f in files if f.startswith("node_writes_") and f.endswith(".json")
    ]
    ffi_files = [f for f in files if f.startswith("ffi_") and f.endswith(".json")]

    worker_pids = set()
    for fname in files:
        m = re.search(r"_(\d+)\.json$", fname)
        if m:
            worker_pids.add(m.group(1))

    if not worker_pids:
        return

    out(f"\n=== Diagnostics aggregated from {len(worker_pids)} process(es) ===")

    # 1. Lifecycle timings
    if lifecycle_files:
        lc_timings: dict[str, float] = defaultdict(float)
        lc_counts: dict[str, int] = defaultdict(int)
        strategies: set[str] = set()
        for fname in lifecycle_files:
            try:
                with open(
                    os.path.join(timings_dir, fname), encoding="utf-8"
                ) as lifecycle_fh:
                    data = json.load(lifecycle_fh)
                for k, v in data.get("timings", {}).items():
                    lc_timings[k] += v
                for k, v in data.get("counts", {}).items():
                    lc_counts[k] += v
                if "strategy" in data and data["strategy"]:
                    strategies.add(str(data["strategy"]))
            except Exception as e:
                out(f"Warning: failed to read {fname}: {e}")

        if lc_timings:
            strat_str = (
                f" (strategy: {', '.join(sorted(strategies))})" if strategies else ""
            )
            out(f"\n=== Postgres test-DB lifecycle timings{strat_str} ===")
            out("")
            out(f"  {'':44s}  {'total':>10s}  {'calls':>6s}  {'avg':>12s}")
            if "hs_setup_wall" in lc_timings:
                wall_s = lc_timings["hs_setup_wall"]
                wall_cnt = lc_counts["hs_setup_wall"]
                out(
                    f"  {'hs_setup_wall (outer)':44s}  {wall_s * 1000:8.1f}ms  {wall_cnt:6d}  {(wall_s / wall_cnt) * 1000:10.3f}ms"
                )
                if "create_database" in lc_timings:
                    cd_s = lc_timings["create_database"]
                    cd_cnt = lc_counts["create_database"]
                    out(
                        f"    ├── {'create_database':38s}  {cd_s * 1000:8.1f}ms  {cd_cnt:6d}  {(cd_s / cd_cnt) * 1000:10.3f}ms"
                    )
                if "hs_setup_total" in lc_timings:
                    st_s = lc_timings["hs_setup_total"]
                    st_cnt = lc_counts["hs_setup_total"]
                    out(
                        f"    ├── {'hs_setup_total':38s}  {st_s * 1000:8.1f}ms  {st_cnt:6d}  {(st_s / st_cnt) * 1000:10.3f}ms"
                    )
                    for inner_tag in (
                        "make_conn",
                        "prepare_database",
                        "check_database",
                    ):
                        if inner_tag in lc_timings:
                            it_s = lc_timings[inner_tag]
                            it_cnt = lc_counts[inner_tag]
                            out(
                                f"    │     ├── {inner_tag:32s}  {it_s * 1000:8.1f}ms  {it_cnt:6d}  {(it_s / it_cnt) * 1000:10.3f}ms"
                            )
                    sub_inner = sum(
                        lc_timings.get(t, 0.0)
                        for t in ("make_conn", "prepare_database", "check_database")
                    )
                    store_res = max(0.0, st_s - sub_inner)
                    out(
                        f"    │     └── {'store_init (residual)':32s}  {store_res * 1000:8.1f}ms  {st_cnt:6d}  {(store_res / st_cnt) * 1000:10.3f}ms"
                    )
                sub_wall = lc_timings.get("create_database", 0.0) + lc_timings.get(
                    "hs_setup_total", 0.0
                )
                unatt_wall = max(0.0, wall_s - sub_wall)
                out(
                    f"    └── {'hs_unattributed':38s}  {unatt_wall * 1000:8.1f}ms  {wall_cnt:6d}  {(unatt_wall / wall_cnt) * 1000:10.3f}ms"
                )
                if "hs_shutdown" in lc_timings:
                    sd_s = lc_timings["hs_shutdown"]
                    sd_cnt = lc_counts["hs_shutdown"]
                    out(
                        f"  {'hs_shutdown (teardown)':44s}  {sd_s * 1000:8.1f}ms  {sd_cnt:6d}  {(sd_s / sd_cnt) * 1000:10.3f}ms"
                    )
                known = {
                    "hs_setup_wall",
                    "create_database",
                    "hs_setup_total",
                    "make_conn",
                    "prepare_database",
                    "check_database",
                    "hs_shutdown",
                }
                for tag in sorted(lc_timings):
                    if tag not in known:
                        t_s = lc_timings[tag]
                        cnt = lc_counts[tag]
                        out(
                            f"  {tag:44s}  {t_s * 1000:8.1f}ms  {cnt:6d}  {(t_s / cnt) * 1000:10.3f}ms"
                        )
                non_overlap = wall_s + lc_timings.get("hs_shutdown", 0.0)
                out("")
                out(
                    f"  {'TOTAL NON-OVERLAPPING LIFECYCLE':44s}  {non_overlap * 1000:8.1f}ms"
                )
            else:
                for tag in sorted(lc_timings):
                    total_s = lc_timings[tag]
                    count = lc_counts[tag]
                    total_ms = total_s * 1000
                    avg_ms = (total_s / count) * 1000 if count else 0.0
                    out(f"  {tag:44s}  {total_ms:8.1f}ms  {count:6d}  {avg_ms:10.3f}ms")
                total_s = sum(lc_timings.values())
                total_ms = total_s * 1000
                out("")
                out(f"  {'TOTAL':44s}  {total_ms:8.1f}ms")
            out("==========================================")
            out("")

    # 2. SQL timings
    if sql_files:
        sql_ops: dict[str, float] = defaultdict(float)
        sql_counts: dict[str, int] = defaultdict(int)
        sql_rows: dict[str, int] = defaultdict(int)
        for fname in sql_files:
            try:
                with open(os.path.join(timings_dir, fname), encoding="utf-8") as sql_fh:
                    data = json.load(sql_fh)
                for k, v in data.get("ops", {}).items():
                    sql_ops[k] += v
                for k, v in data.get("counts", {}).items():
                    sql_counts[k] += v
                for k, v in data.get("rows", {}).items():
                    sql_rows[k] += v
            except Exception as e:
                out(f"Warning: failed to read {fname}: {e}")

        if sql_ops:
            ranked = sorted(sql_ops.items(), key=lambda kv: kv[1], reverse=True)
            rendered = []
            for table, total_s in ranked[:30]:
                count = sql_counts.get(table, 0)
                rows = sql_rows.get(table, 0)
                rendered.append(
                    (
                        table,
                        f"{total_s * 1000:.1f}ms",
                        f"{count:d}",
                        f"{rows:d}",
                        f"{(total_s / count) * 1000 if count else 0.0:.3f}ms",
                    )
                )
            total_time_s = sum(sql_ops.values())
            total_count = sum(sql_counts.values())
            total_rows = sum(sql_rows.values())
            total_row = (
                "TOTAL",
                f"{total_time_s * 1000:.1f}ms",
                f"{total_count:d}",
                f"{total_rows:d}",
                f"{(total_time_s / total_count) * 1000 if total_count else 0.0:.3f}ms",
            )
            table_width = max(40, *(len(row[0]) for row in rendered), len("TOTAL"))
            total_width = max(
                len("total"), *(len(row[1]) for row in rendered), len(total_row[1])
            )
            calls_width = max(
                len("calls"), *(len(row[2]) for row in rendered), len(total_row[2])
            )
            rows_width = max(
                len("rows"), *(len(row[3]) for row in rendered), len(total_row[3])
            )
            avg_width = max(
                len("avg"), *(len(row[4]) for row in rendered), len(total_row[4])
            )
            out("\n=== Per-table SQL timing (top 30) ===")
            out(
                f"  {'table':{table_width}s}  {'total':>{total_width}s}  {'calls':>{calls_width}s}  {'rows':>{rows_width}s}  {'avg':>{avg_width}s}"
            )
            for table, total_text, count_text, rows_text, avg_text in rendered:
                out(
                    f"  {table:{table_width}s}  {total_text:>{total_width}s}  {count_text:>{calls_width}s}  {rows_text:>{rows_width}s}  {avg_text:>{avg_width}s}"
                )
            out("")
            out(
                f"  {total_row[0]:{table_width}s}  {total_row[1]:>{total_width}s}  {total_row[2]:>{calls_width}s}  {total_row[3]:>{rows_width}s}  {total_row[4]:>{avg_width}s}"
            )
            out("=====================================")
            out("")

    # 3. State store mtxdb-vs-SQL
    if state_files:
        st_timings: dict[str, float] = defaultdict(float)
        st_counts: dict[str, int] = defaultdict(int)
        st_counters: dict[str, int] = defaultdict(int)
        for fname in state_files:
            try:
                with open(
                    os.path.join(timings_dir, fname), encoding="utf-8"
                ) as state_fh:
                    data = json.load(state_fh)
                for k, v in data.get("timings", {}).items():
                    st_timings[k] += v
                for k, v in data.get("counts", {}).items():
                    st_counts[k] += v
                for k, v in data.get("counters", {}).items():
                    st_counters[k] += v
            except Exception as e:
                out(f"Warning: failed to read {fname}: {e}")

        if st_timings:
            out("\n=== State store mtxdb-vs-SQL timings ===")
            out(f"  {'':40s}  {'total':>10s}  {'calls':>6s}  {'avg':>12s}")
            embedded_tags = sorted(t for t in st_timings if t.endswith("_embedded"))
            sql_tags = sorted(t for t in st_timings if t.endswith("_sql"))
            other_tags = sorted(
                t for t in st_timings if not t.endswith(("_embedded", "_sql"))
            )

            def _print_tag_group(label: str, tags: list[str]) -> None:
                if not tags:
                    return
                out(f"  -- {label} --")
                for tag in tags:
                    total_s = st_timings[tag]
                    count = st_counts[tag]
                    total_ms = total_s * 1000
                    avg_ms = (total_s / count) * 1000 if count else 0.0
                    out(f"  {tag:40s}  {total_ms:8.1f}ms  {count:6d}  {avg_ms:10.3f}ms")

            def _print_subtotal(label: str, tags: list[str]) -> None:
                total_s = sum(st_timings[tag] for tag in tags)
                count = sum(st_counts[tag] for tag in tags)
                total_ms = total_s * 1000
                avg_ms = (total_s / count) * 1000 if count else 0.0
                out(f"  {label:40s}  {total_ms:8.1f}ms  {count:6d}  {avg_ms:10.3f}ms")

            _print_tag_group("hits (embedded)", embedded_tags)
            out("")
            _print_subtotal("SUB-TOTAL (hits embedded)", embedded_tags)
            out("")
            _print_tag_group("misses (sql)", sql_tags)
            out("")
            _print_tag_group("other", other_tags)

            total_time_s = sum(st_timings.values())
            total_count = sum(st_counts.values())
            total_ms = total_time_s * 1000
            avg_ms = (total_time_s / total_count) * 1000 if total_count else 0.0
            out("")
            out(
                f"  {'TOTAL':40s}  {total_ms:8.1f}ms  {total_count:6d}  {avg_ms:10.3f}ms"
            )
            out("=========================================")
            out("")

        if st_counters:
            out("=== State mirror diagnostics ===")
            for tag in sorted(st_counters):
                out(f"  {tag:50s}  {st_counters[tag]:>12,d}")
            out("================================")
            out("")

    # 4. Node write diagnostics
    if node_files:
        tot_calls = 0
        tot_nodes = 0
        tot_bytes = 0
        tot_time = 0.0
        lat_dist: dict[str, int] = defaultdict(int)
        size_dist: dict[str, int] = defaultdict(int)
        for fname in node_files:
            try:
                with open(
                    os.path.join(timings_dir, fname), encoding="utf-8"
                ) as node_fh:
                    data = json.load(node_fh)
                tot_calls += data.get("calls", 0)
                tot_nodes += data.get("total_nodes", 0)
                tot_bytes += data.get("total_bytes", 0)
                tot_time += data.get("total_time", 0.0)
                for k, v in data.get("lat_buckets", {}).items():
                    lat_dist[k] += v
                for k, v in data.get("size_buckets", {}).items():
                    size_dist[k] += v
            except Exception as e:
                out(f"Warning: failed to read {fname}: {e}")

        if tot_calls > 0:
            out("\n=== put_state_hamt_nodes batch diagnostics ===")
            out(f"  calls:                    {tot_calls}")
            out(f"  total nodes:              {tot_nodes}")
            out(f"  total bytes:              {tot_bytes:,}")
            out(f"  total time:               {tot_time * 1000:.1f}ms")
            out(f"  avg nodes/call:           {tot_nodes / tot_calls:.1f}")
            out(f"  avg bytes/call:           {tot_bytes / tot_calls:.0f}")
            out(f"  avg time/call:            {(tot_time / tot_calls) * 1000:.3f}ms")
            if tot_nodes:
                out(f"  avg bytes/node:           {tot_bytes / tot_nodes:.0f}")
            out("")
            out("  Latency distribution:")
            for b in ("<0.1ms", "<0.25ms", "<0.5ms", "<1ms", ">=1ms"):
                cnt = lat_dist.get(b, 0)
                pct = (cnt / tot_calls) * 100.0 if tot_calls else 0.0
                out(f"    {b:15s}  {cnt:5d}  ({pct:5.1f}%)")
            out("")
            out("  Batch-size distribution:")
            for b in ("1", "2-5", "6-20", "21-100", ">100"):
                cnt = size_dist.get(b, 0)
                pct = (cnt / tot_calls) * 100.0 if tot_calls else 0.0
                out(f"    {b:15s}  {cnt:5d}  ({pct:5.1f}%)")
            out("=============================================")
            out("")

    # 5. FFI boundary timings
    if ffi_files:
        ffi_timings: dict[str, float] = defaultdict(float)
        ffi_counts: dict[str, int] = defaultdict(int)
        ffi_counters: dict[str, int] = defaultdict(int)
        ffi_batch_sizes: dict[str, list[int]] = defaultdict(list)
        ffi_latencies: dict[str, list[float]] = defaultdict(list)
        for fname in ffi_files:
            try:
                with open(os.path.join(timings_dir, fname), encoding="utf-8") as ffi_fh:
                    data = json.load(ffi_fh)
                for k, v in data.get("timings", {}).items():
                    ffi_timings[k] += v
                for k, v in data.get("counts", {}).items():
                    ffi_counts[k] += v
                for k, v in data.get("counters", {}).items():
                    ffi_counters[k] += v
                for k, v in data.get("batch_sizes", {}).items():
                    ffi_batch_sizes[k].extend(v)
                for k, v in data.get("latencies", {}).items():
                    ffi_latencies[k].extend(v)
            except Exception as e:
                out(f"Warning: failed to read {fname}: {e}")

        if ffi_timings:
            out("\n=== FFI boundary timings ===")
            has_hist = bool(ffi_latencies)
            if has_hist:
                out(
                    f"  {'':50s}  {'total':>10s}  {'calls':>6s}  {'avg':>12s}  {'p50':>11s}  {'p95':>11s}  {'p99':>11s}"
                )
            else:
                out(f"  {'':50s}  {'total':>10s}  {'calls':>6s}  {'avg':>12s}")

            def _pct(s: list[float], p: float) -> float:
                if not s:
                    return 0.0
                idx = min(int(len(s) * p), len(s) - 1)
                return s[idx]

            for tag in sorted(ffi_timings):
                total_s = ffi_timings[tag]
                count = ffi_counts[tag]
                total_ms = total_s * 1000
                avg_ms = (total_s / count) * 1000 if count else 0.0
                if tag in ffi_latencies:
                    s = sorted(ffi_latencies[tag])
                    p50_ms = _pct(s, 0.50) * 1000
                    p95_ms = _pct(s, 0.95) * 1000
                    p99_ms = _pct(s, 0.99) * 1000
                    out(
                        f"  {tag:50s}  {total_ms:8.1f}ms  {count:6d}  {avg_ms:10.3f}ms  {p50_ms:9.3f}ms  {p95_ms:9.3f}ms  {p99_ms:9.3f}ms"
                    )
                else:
                    out(f"  {tag:50s}  {total_ms:8.1f}ms  {count:6d}  {avg_ms:10.3f}ms")

            out("")
            for label, excluded in (
                ("TOTAL (all measured spans)", None),
                ("TOTAL (excluding engine open)", "embedded_engine_open"),
            ):
                total_s = sum(
                    elapsed for tag, elapsed in ffi_timings.items() if tag != excluded
                )
                total_count = sum(
                    count for tag, count in ffi_counts.items() if tag != excluded
                )
                total_ms = total_s * 1000
                total_avg_ms = (total_s / total_count) * 1000 if total_count else 0.0
                out(
                    f"  {label:50s}  {total_ms:8.1f}ms  {total_count:6d}  {total_avg_ms:10.3f}ms"
                )
            out("==============================")
            out("")
            batch_timings = {
                tag: (total_s, ffi_counts.get(tag, 0), ffi_latencies.get(tag, []))
                for tag, total_s in ffi_timings.items()
                if tag.endswith("_batch")
            }
            if batch_timings:
                out("=== FFI batch timings ===")
                out(
                    f"  {'operation':40s}  {'total':>10s}  {'items':>9s}  {'batches':>8s}  {'avg/batch':>12s}  {'p50':>9s}  {'p95':>9s}  {'p99':>9s}"
                )
                for tag, (total_s, batch_count, samples) in sorted(
                    batch_timings.items()
                ):
                    operation = tag[:-6]
                    request_count = next(
                        (
                            ffi_counters[key]
                            for key in (
                                f"{operation}_items_requested",
                                f"{operation}_event_ids_requested",
                                f"{operation}_node_hashes_requested",
                            )
                            if key in ffi_counters
                        ),
                        0,
                    )
                    samples = sorted(samples)
                    p50 = _pct(samples, 0.50) * 1000
                    p95 = _pct(samples, 0.95) * 1000
                    p99 = _pct(samples, 0.99) * 1000
                    avg_ms = (total_s / batch_count) * 1000 if batch_count else 0.0
                    out(
                        f"  {operation:40s}  {total_s * 1000:8.1f}ms  {request_count:9,d}  {batch_count:8,d}  {avg_ms:10.3f}ms  {p50:7.3f}ms  {p95:7.3f}ms  {p99:7.3f}ms"
                    )
                out("========================")
                out("")
            if ffi_counters:
                auth_counters = {
                    tag: value
                    for tag, value in ffi_counters.items()
                    if tag.startswith("auth_chain_")
                }
                if auth_counters:
                    tag_width = max(50, *(len(tag) for tag in auth_counters))
                    out("=== Auth chain coverage ===")
                    for tag in sorted(auth_counters):
                        out(f"  {tag:{tag_width}s}  {auth_counters[tag]:>12,d}")
                    out("===========================")
                    out("")
                out("=== FFI batch counters ===")
                tag_width = max(
                    50,
                    *(
                        len(tag)
                        for tag in ffi_counters
                        if not tag.startswith("auth_chain_")
                    ),
                )
                for tag in sorted(ffi_counters):
                    if tag.startswith("auth_chain_"):
                        continue
                    out(f"  {tag:{tag_width}s}  {ffi_counters[tag]:>12,d}")
                out("===========================")
                out("")
            if ffi_batch_sizes:
                out("=== FFI batch sizes ===")
                for tag, values in sorted(ffi_batch_sizes.items()):
                    values.sort()
                    p50 = values[len(values) // 2]
                    p95 = values[min(len(values) - 1, int(len(values) * 0.95))]
                    out(
                        f"  {tag:50s}  calls={len(values):,}  avg={sum(values) / len(values):.1f}  p50={p50}  p95={p95}"
                    )
                out("=======================")
                out("")


def run() -> None:
    config = Options()
    try:
        config.parseOptions()
    except usage.error as ue:
        raise SystemExit(f"{sys.argv[0]}: {ue}")

    _initialDebugSetup(config)
    if config["dry-run"]:
        raise SystemExit(f"{sys.argv[0]}: --dry-run is not supported by this wrapper")
    if config["profile"]:
        raise SystemExit(f"{sys.argv[0]}: --profile is not supported by this wrapper")

    timings_dir: str | None = None
    if os.environ.get("SYNAPSE_PG_TIMINGS") and config["jobs"] is not None:
        timings_dir = tempfile.mkdtemp(prefix="synapse_timings_")
        os.environ["SYNAPSE_TIMINGS_RUN_DIR"] = timings_dir

    trialRunner = _makeRunner(config)
    # Trial's default `_trial_temp` path is relative.  Distributed workers
    # chdir into their own subdirectories, so an accidentally removed or
    # inaccessible parent CWD can otherwise turn later `abspath()` calls into
    # cascading FileNotFoundErrors.  Resolve the runner-owned path once before
    # any worker is spawned.
    if config["jobs"] is not None:
        distributed_runner = cast(_DistributedRunner, trialRunner)
        distributed_runner._workingDirectory = os.path.abspath(
            distributed_runner._workingDirectory
        )
    else:
        assert isinstance(trialRunner, TrialRunner)
        trialRunner.workingDirectory = os.path.abspath(trialRunner.workingDirectory)
    suite = _getSuite(config)

    interrupted = False
    successful = False
    timing_file: IO[str] | None = None

    try:
        if config["jobs"] is not None:
            testResult = trialRunner.run(suite)
            successful = testResult.wasSuccessful()
        else:
            assert isinstance(trialRunner, TrialRunner)
            test = unittest.decorate(suite, itrial.ITestCase)
            result = trialRunner._makeResult()
            timing_file = _install_test_timing_logging(result)

            def onSigint(signum: int, frame: object) -> None:
                nonlocal interrupted
                if interrupted:
                    signal.signal(signal.SIGINT, signal.default_int_handler)
                    raise KeyboardInterrupt()
                interrupted = True
                result.shouldStop = True
                sys.stderr.write(
                    "\nInterrupted -- finishing the current test, then printing "
                    "results so far (Ctrl+C again to abort immediately)...\n"
                )

            previousHandler = signal.signal(signal.SIGINT, onSigint)
            try:
                with (
                    _testDirectory(trialRunner.workingDirectory),
                    _logFile(trialRunner.logfile),
                ):
                    test.run(result)
            except KeyboardInterrupt:
                interrupted = True
            finally:
                signal.signal(signal.SIGINT, previousHandler)
                result.done()
            successful = result.wasSuccessful()
    finally:
        if timing_file is not None:
            timing_file.close()
        if timings_dir:
            try:
                _flush_process_timings()
                _aggregate_and_print_timings(timings_dir)
            finally:
                shutil.rmtree(timings_dir, ignore_errors=True)

    sys.exit(130 if interrupted else int(not successful))


if __name__ == "__main__":
    run()
