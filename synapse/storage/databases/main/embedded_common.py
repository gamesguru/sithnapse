from __future__ import annotations

import atexit
import hashlib
import json
import logging
import os
import sys
import threading
import time
import traceback
from collections import defaultdict, deque
from contextlib import contextmanager
from enum import Enum, auto
from typing import IO, TYPE_CHECKING, Any, Callable, Iterable, Iterator

if TYPE_CHECKING:
    from synapse.util.clock import Clock, DelayedCallWrapper

logger = logging.getLogger(__name__)

# Module-level flag: when True, all DURABLE-tier sync() calls are suppressed.
# Set once during HomeServer init via configure_sync(); never mutated after.
_sync_disabled: bool = False

# Whether any store in this process configured an embedded engine. Set once
# during HomeServer init (see StateGroupDataStore.__init__); monotonic
# (only ever False -> True), so multi-homeserver processes sharing this
# module (e.g. Complement trial) can't unset it for each other. Lets
# maybe_sync/sync_now no-op when the engine was never configured instead of
# importing the native module and issuing FFI syncs against a never-opened
# engine.
_engine_configured: bool = False

# ── FFI boundary timing (opt-in via SYNAPSE_PG_TIMINGS=1) ────────────────
_FFI_TIMINGS: dict[str, float] = defaultdict(float)
_FFI_TIMING_COUNTS: dict[str, int] = defaultdict(int)
_FFI_COUNTERS: dict[str, int] = defaultdict(int)
_FFI_BATCH_SIZES: dict[str, deque[int]] = defaultdict(
    lambda: deque(maxlen=_FFI_LATENCY_LIMIT)
)
_FFI_LATENCY_LIMIT = 4096
_FFI_LATENCIES: dict[str, deque[float]] = defaultdict(
    lambda: deque(maxlen=_FFI_LATENCY_LIMIT)
)
_FFI_TIMING_LOCK: "threading.Lock | None" = (
    threading.Lock() if os.environ.get("SYNAPSE_PG_TIMINGS") else None
)

_ffi_timings_file: IO[str] | None = None
if os.environ.get("SYNAPSE_PG_TIMINGS"):
    _ffi_timings_path = os.environ.get("SYNAPSE_PG_TIMINGS_FILE")
    if _ffi_timings_path:
        try:
            _ffi_timings_file = open(_ffi_timings_path, "a")
        except OSError:
            pass


def _ffi_timings_print(*args: object) -> None:
    import sys

    print(*args, file=sys.stderr)
    if _ffi_timings_file is not None:
        print(*args, file=_ffi_timings_file)


def ffi_timing(tag: str, elapsed: float) -> None:
    if not os.environ.get("SYNAPSE_PG_TIMINGS"):
        return
    lock = _FFI_TIMING_LOCK
    if lock is not None:
        with lock:
            _FFI_TIMINGS[tag] += elapsed
            _FFI_TIMING_COUNTS[tag] += 1
            _FFI_LATENCIES[tag].append(elapsed)
    else:
        _FFI_TIMINGS[tag] += elapsed
        _FFI_TIMING_COUNTS[tag] += 1
        _FFI_LATENCIES[tag].append(elapsed)


# When True, ffi_count() writes to _FFI_COUNTERS even without SYNAPSE_PG_TIMINGS.
# Only set by enable_ffi_counting() in tests; never set in production.
_ffi_counting_enabled: bool = False


def ffi_count(tag: str, count: int) -> None:
    """Record an opt-in count alongside FFI timing diagnostics.

    In production, this is a no-op unless SYNAPSE_PG_TIMINGS is set.
    Tests may activate counting via the enable_ffi_counting() context manager.
    """
    if not _ffi_counting_enabled and not os.environ.get("SYNAPSE_PG_TIMINGS"):
        return
    lock = _FFI_TIMING_LOCK
    if lock is not None:
        with lock:
            _FFI_COUNTERS[tag] += count
    else:
        _FFI_COUNTERS[tag] += count


def get_ffi_count(tag: str) -> int:
    """Return the current accumulated value of a named ffi_count counter.

    Only meaningful when SYNAPSE_PG_TIMINGS is set or inside an
    enable_ffi_counting() block.  Returns 0 for unseen or uncounted tags.
    """
    lock = _FFI_TIMING_LOCK
    if lock is not None:
        with lock:
            return _FFI_COUNTERS[tag]
    return _FFI_COUNTERS[tag]


@contextmanager
def enable_ffi_counting() -> Iterator[None]:
    """Context manager that activates ffi_count() for the duration of the block.

    Intended for tests that need to assert on hit/fallback counters without
    requiring SYNAPSE_PG_TIMINGS.  Resets the affected counter dict on entry
    so that before/after snapshots are clean.

    Not thread-safe; use only in single-threaded test code.
    """
    global _ffi_counting_enabled
    _ffi_counting_enabled = True
    _FFI_COUNTERS.clear()
    try:
        yield
    finally:
        _ffi_counting_enabled = False


def ffi_batch_size(tag: str, size: int) -> None:
    """Record an opt-in batch-size sample for FFI diagnostics."""
    if not os.environ.get("SYNAPSE_PG_TIMINGS"):
        return
    lock = _FFI_TIMING_LOCK
    if lock is not None:
        with lock:
            _FFI_BATCH_SIZES[tag].append(size)
    else:
        _FFI_BATCH_SIZES[tag].append(size)


# ── Reactor-lag probe (opt-in via SYNAPSE_PG_TIMINGS=1) ──────────────────
#
# Every timer above brackets the duration of a call this module knows
# about. None of them can see time spent *before* a call starts -- e.g. a
# txn.call_after callback (queue_edge_write, a coalescer flush, sync_now)
# blocking the Twisted reactor thread delays every other pending reactor
# callback in the process, including unrelated tests' code that never
# touches mtxdb. That delay is invisible to ffi_timing/mirror_timing and
# shows up only as unexplained wall-clock time elsewhere. This probe makes
# it visible directly: a sampler thread posts callbacks at a fixed interval,
# and the probe separates sampler wake lateness from callback dispatch delay.
# When a prior callback is still pending, it also snapshots the main thread's
# stack to show what is occupying it. This is diagnostic evidence, not by
# itself proof that a particular subsystem caused the delay.
_REACTOR_LAG_INTERVAL_SECS: float = 0.1
_REACTOR_LAG_RECORDS: deque[tuple[float, float, float]] = deque(
    maxlen=_FFI_LATENCY_LIMIT
)
_REACTOR_LAG_STACKS: deque[tuple[float, str]] = deque(maxlen=256)
_reactor_lag_probe_started: bool = False


def start_reactor_lag_probe(
    call_from_thread: Callable[[Callable[[], None]], None],
) -> Callable[[], None] | None:
    """Start an opt-in probe for sampler and Trial-reactor callback delays.

    No-op unless SYNAPSE_PG_TIMINGS is set. Idempotent -- a second call
    is a no-op. A sampler thread posts timestamped callbacks via
    ``call_from_thread`` so the probe doesn't leave a recurring DelayedCall
    in Trial's reactor (which its per-test leak check would correctly reject).
    """
    global _reactor_lag_probe_started
    if not os.environ.get("SYNAPSE_PG_TIMINGS"):
        return None
    if _reactor_lag_probe_started:
        return None
    _reactor_lag_probe_started = True

    stop_event = threading.Event()

    pending_lock = threading.Lock()
    pending_callbacks = 0
    main_thread_id = threading.main_thread().ident

    def _record_lag(expected: float, posted_at: float) -> None:
        nonlocal pending_callbacks
        delivered_at = time.monotonic()
        with pending_lock:
            pending_callbacks -= 1
        record = (expected, posted_at, delivered_at)
        lock = _FFI_TIMING_LOCK
        if lock is not None:
            with lock:
                _REACTOR_LAG_RECORDS.append(record)
        else:
            _REACTOR_LAG_RECORDS.append(record)

    def _capture_main_stack(at: float) -> None:
        if main_thread_id is None:
            return
        frame = sys._current_frames().get(main_thread_id)
        if frame is None:
            return
        frames = traceback.extract_stack(frame, limit=12)
        signature = " <- ".join(
            f"{os.path.basename(item.filename)}:{item.lineno}:{item.name}"
            for item in frames[-8:]
        )
        _REACTOR_LAG_STACKS.append((at, signature))

    def _sample() -> None:
        nonlocal pending_callbacks
        expected = time.monotonic() + _REACTOR_LAG_INTERVAL_SECS
        while not stop_event.wait(max(0.0, expected - time.monotonic())):
            posted_at = time.monotonic()
            with pending_lock:
                reactor_callback_pending = pending_callbacks > 0
                pending_callbacks += 1
            if reactor_callback_pending:
                # Capture the main/reactor thread while an earlier posted
                # probe callback is still waiting to run. This identifies
                # the work occupying that thread during observed dispatch lag.
                _capture_main_stack(posted_at)
            try:

                def record_expected_lag(
                    deadline: float = expected, queued_at: float = posted_at
                ) -> None:
                    _record_lag(deadline, queued_at)

                call_from_thread(record_expected_lag)
            except Exception:
                with pending_lock:
                    pending_callbacks -= 1
                logger.debug(
                    "Reactor-lag probe could not enqueue a sample", exc_info=True
                )
                return
            # Keep a fixed cadence without flooding the reactor with
            # catch-up callbacks after the sampler thread itself is delayed.
            expected = max(
                expected + _REACTOR_LAG_INTERVAL_SECS,
                time.monotonic() + _REACTOR_LAG_INTERVAL_SECS,
            )

    sampler = threading.Thread(
        target=_sample, name="synapse-reactor-lag-probe", daemon=True
    )
    sampler.start()

    def stop() -> None:
        stop_event.set()
        sampler.join(timeout=1.0)

    return stop


@contextmanager
def lock_wait_timing(lock: threading.Lock, tag: str) -> Iterator[None]:
    """Measure lock acquisition wait and hold time separately.

    Recorded as `lock_wait_<tag>` and `lock_hold_<tag>` in the FFI timing
    report. No timing calls are made unless SYNAPSE_PG_TIMINGS is enabled.
    """
    if not os.environ.get("SYNAPSE_PG_TIMINGS"):
        with lock:
            yield
        return
    start = time.monotonic()
    lock.acquire()
    acquired = time.monotonic()
    try:
        yield
    finally:
        held = time.monotonic() - acquired
        lock.release()
        ffi_timing(f"lock_wait_{tag}", acquired - start)
        ffi_timing(f"lock_hold_{tag}", held)


def _print_ffi_timings() -> None:
    if not os.environ.get("SYNAPSE_PG_TIMINGS"):
        return
    lock = _FFI_TIMING_LOCK
    if lock is None:
        return
    with lock:
        if not _FFI_TIMINGS:
            return
        timings = dict(_FFI_TIMINGS)
        counts = dict(_FFI_TIMING_COUNTS)
        counters = dict(_FFI_COUNTERS)
        batch_sizes = {k: sorted(v) for k, v in _FFI_BATCH_SIZES.items() if v}
        latencies = {k: sorted(v) for k, v in _FFI_LATENCIES.items() if v}
        reactor_lag_records = list(_REACTOR_LAG_RECORDS)
        reactor_lag_stacks = list(_REACTOR_LAG_STACKS)

    run_dir = os.environ.get("SYNAPSE_TIMINGS_RUN_DIR")
    if run_dir:
        tmp_path = os.path.join(run_dir, f"ffi_{os.getpid()}.tmp")
        final_path = os.path.join(run_dir, f"ffi_{os.getpid()}.json")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "timings": timings,
                        "counts": counts,
                        "counters": counters,
                        "batch_sizes": batch_sizes,
                        "latencies": latencies,
                        "reactor_lag_records": reactor_lag_records,
                        "reactor_lag_stacks": reactor_lag_stacks,
                    },
                    f,
                )
            os.replace(tmp_path, final_path)
        except OSError:
            pass
        return

    _ffi_timings_print("\n=== FFI boundary timings ===")
    has_hist = bool(latencies)
    if has_hist:
        _ffi_timings_print(
            f"  {'':50s}  {'total':>10s}  {'calls':>6s}  {'avg':>12s}  {'p50':>11s}  {'p95':>11s}  {'p99':>11s}",
        )
    else:
        _ffi_timings_print(
            f"  {'':50s}  {'total':>10s}  {'calls':>6s}  {'avg':>12s}",
        )

    def _percentile(sorted_vals: list[float], p: float) -> float:
        if not sorted_vals:
            return 0.0
        idx = int(len(sorted_vals) * p)
        idx = min(idx, len(sorted_vals) - 1)
        return sorted_vals[idx]

    for tag in sorted(timings):
        total_s = timings[tag]
        count = counts[tag]
        total_ms = total_s * 1000
        avg_ms = (total_s / count) * 1000 if count else 0.0
        if tag in latencies:
            s = latencies[tag]
            p50_ms = _percentile(s, 0.50) * 1000
            p95_ms = _percentile(s, 0.95) * 1000
            p99_ms = _percentile(s, 0.99) * 1000
            _ffi_timings_print(
                f"  {tag:50s}  {total_ms:8.1f}ms  {count:6d}  {avg_ms:10.3f}ms  {p50_ms:9.3f}ms  {p95_ms:9.3f}ms  {p99_ms:9.3f}ms",
            )
        else:
            _ffi_timings_print(
                f"  {tag:50s}  {total_ms:8.1f}ms  {count:6d}  {avg_ms:10.3f}ms",
            )
    _ffi_timings_print("")
    for label, excluded in (
        ("TOTAL (all measured spans)", None),
        ("TOTAL (excluding engine open)", "embedded_engine_open"),
    ):
        total_s = sum(elapsed for tag, elapsed in timings.items() if tag != excluded)
        total_count = sum(count for tag, count in counts.items() if tag != excluded)
        total_ms = total_s * 1000
        total_avg_ms = (total_s / total_count) * 1000 if total_count else 0.0
        _ffi_timings_print(
            f"  {label:50s}  {total_ms:8.1f}ms  {total_count:6d}  {total_avg_ms:10.3f}ms",
        )
    _ffi_timings_print("==============================")
    _ffi_timings_print("")
    batch_timings = {
        tag: (total_s, counts.get(tag, 0), latencies.get(tag, []))
        for tag, total_s in timings.items()
        if tag.endswith("_batch")
    }
    if batch_timings:
        _ffi_timings_print("=== FFI batch timings ===")
        _ffi_timings_print(
            f"  {'operation':40s}  {'total':>10s}  {'items':>9s}  {'batches':>8s}  {'avg/batch':>12s}  {'p50':>9s}  {'p95':>9s}  {'p99':>9s}"
        )
        for tag, (total_s, batch_count, samples) in sorted(batch_timings.items()):
            operation = tag[:-6]
            request_count = next(
                (
                    counters[key]
                    for key in (
                        f"{operation}_items_requested",
                        f"{operation}_event_ids_requested",
                        f"{operation}_node_hashes_requested",
                    )
                    if key in counters
                ),
                0,
            )
            samples = sorted(samples)
            p50 = _percentile(samples, 0.50) * 1000
            p95 = _percentile(samples, 0.95) * 1000
            p99 = _percentile(samples, 0.99) * 1000
            avg_ms = (total_s / batch_count) * 1000 if batch_count else 0.0
            _ffi_timings_print(
                f"  {operation:40s}  {total_s * 1000:8.1f}ms  {request_count:9,d}  {batch_count:8,d}  {avg_ms:10.3f}ms  {p50:7.3f}ms  {p95:7.3f}ms  {p99:7.3f}ms"
            )
        _ffi_timings_print("========================")
        _ffi_timings_print("")
    if counters:
        auth_counters = {
            tag: value
            for tag, value in counters.items()
            if tag.startswith("auth_chain_")
        }
        if auth_counters:
            tag_width = max(50, *(len(tag) for tag in auth_counters))
            _ffi_timings_print("=== Auth chain coverage ===")
            for tag in sorted(auth_counters):
                _ffi_timings_print(f"  {tag:{tag_width}s}  {auth_counters[tag]:>12,d}")
            _ffi_timings_print("===========================")
            _ffi_timings_print("")
        _ffi_timings_print("=== FFI batch counters ===")
        non_auth_tags = [
            len(tag) for tag in counters if not tag.startswith("auth_chain_")
        ]
        tag_width = max(50, *non_auth_tags)
        for tag in sorted(counters):
            if tag.startswith("auth_chain_"):
                continue
            _ffi_timings_print(f"  {tag:{tag_width}s}  {counters[tag]:>12,d}")
        _ffi_timings_print("===========================")
        _ffi_timings_print("")
    if batch_sizes:
        _ffi_timings_print("=== FFI batch sizes ===")
        for tag, values in sorted(batch_sizes.items()):
            p50 = values[len(values) // 2]
            p95 = values[min(len(values) - 1, int(len(values) * 0.95))]
            _ffi_timings_print(
                f"  {tag:50s}  calls={len(values):,}  avg={sum(values) / len(values):.1f}  p50={p50}  p95={p95}"
            )
        _ffi_timings_print("=======================")
        _ffi_timings_print("")
    if reactor_lag_records:
        wake_lag = sorted(
            posted - expected for expected, posted, _ in reactor_lag_records
        )
        dispatch_lag = sorted(
            delivered - posted for _, posted, delivered in reactor_lag_records
        )
        end_to_end_lag = sorted(
            delivered - expected for expected, _, delivered in reactor_lag_records
        )

        def print_lag_summary(label: str, values: list[float]) -> None:
            p50 = _percentile(values, 0.50) * 1000
            p95 = _percentile(values, 0.95) * 1000
            p99 = _percentile(values, 0.99) * 1000
            worst = max(values) * 1000
            _ffi_timings_print(
                f"  {label}: p50={p50:.3f}ms p95={p95:.3f}ms "
                f"p99={p99:.3f}ms worst={worst:.3f}ms"
            )

        _ffi_timings_print("=== Reactor callback scheduling lag ===")
        _ffi_timings_print(
            f"  interval={_REACTOR_LAG_INTERVAL_SECS * 1000:.0f}ms  "
            f"samples={len(reactor_lag_records):,} (last {_FFI_LATENCY_LIMIT:,})"
        )
        print_lag_summary("sampler wake lateness", wake_lag)
        print_lag_summary("reactor dispatch delay", dispatch_lag)
        print_lag_summary("expected-to-dispatch", end_to_end_lag)
        if reactor_lag_stacks:
            stack_counts: dict[str, int] = defaultdict(int)
            for _, stack in reactor_lag_stacks:
                stack_counts[stack] += 1
            _ffi_timings_print(
                f"  main-thread stacks captured while callback pending: "
                f"{len(reactor_lag_stacks)}"
            )
            for stack, count in sorted(
                stack_counts.items(), key=lambda item: item[1], reverse=True
            )[:8]:
                _ffi_timings_print(f"    {count:>4}x {stack}")
        _ffi_timings_print("==========================================")
        _ffi_timings_print("")


if os.environ.get("SYNAPSE_PG_TIMINGS"):

    def flush_ffi_timings() -> None:
        _print_ffi_timings()

    atexit.register(flush_ffi_timings)


# ── mtxdb runtime stats (opt-in via SYNAPSE_MTXDB_STATS=1) ──────────────


def _print_mtxdb_stats() -> None:
    """Print an end-of-run mtxdb runtime stats report for all three pools."""
    if not os.environ.get("SYNAPSE_MTXDB_STATS"):
        return
    try:
        from synapse.storage.databases.embedded_engine import get_embedded_engine

        engine = get_embedded_engine("mtxdb")
        s = engine.stats()
    except Exception:
        return

    import sys

    out = sys.stderr

    def _fmt_us(us: int) -> str:
        if us < 1000:
            return f"{us}us"
        if us < 1_000_000:
            return f"{us / 1000:.1f}ms"
        return f"{us / 1_000_000:.2f}s"

    print("\n=== mtxdb runtime stats ===", file=out)
    for pool_name in ("state", "event_dag", "auth_chain"):
        ps = s.get(pool_name, {})
        if not ps:
            continue
        print(f"\n  [{pool_name}]", file=out)
        print(
            f"    collections: {ps.get('collection_count', 0)}  shards: {ps.get('shard_count', 0)}  index: {ps.get('index_bytes', 0):,}B",
            file=out,
        )

        # Read counters (only meaningful when stats_enabled was true).
        gc = ps.get("get_calls", 0)
        gm = ps.get("get_misses", 0)
        gmc = ps.get("get_many_calls", 0)
        gmr = ps.get("get_many_records", 0)
        gmm = ps.get("get_many_misses", 0)
        if gc or gmc:
            hit_rate = ps.get("cache_hit_rate", 0.0)
            print(
                f"    get: {gc} calls, {gm} misses | get_many: {gmc} calls, {gmr} records, {gmm} misses",
                file=out,
            )
            refcount_inits = ps.get("refcount_inits", 0)
            refcount_existing = ps.get("refcount_existing", 0)
            if refcount_inits or refcount_existing:
                print(
                    f"    refcounts: inits={refcount_inits:,}  updates={refcount_existing:,}",
                    file=out,
                )
            print(
                f"    cache: hits={ps.get('cache_hits', 0)}  misses={ps.get('cache_misses', 0)}  rate={hit_rate:.3f}",
                file=out,
            )

        # Lossy-index candidate fan-out. The 16-bit slot tag is only a
        # candidate filter: a tag match can admit a record that full-hash
        # verification then rejects. A nonzero false_rate is extra packfile
        # read work, never a wrong result. Tracked only while stats are on.
        cr = ps.get("candidate_reads", 0)
        if cr:
            cm = ps.get("candidate_hash_mismatches", 0)
            false_rate = cm / cr
            print(
                f"    candidates: index={ps.get('index_candidates', 0):,}  reads={cr:,}  "
                f"hash_mismatches={cm:,}  false_rate={false_rate:.3f}  "
                f"frame_bytes={ps.get('candidate_frame_bytes', 0):,}",
                file=out,
            )

        # Read-miss refresh attribution, captured alongside the candidate
        # counters above so one snapshot can tell them apart: recovered > 0
        # means the tail is paying full rescans, not 16-bit tag collisions.
        mr = ps.get("miss_refreshes", 0)
        mrs = ps.get("miss_refresh_skips", 0)
        mrr = ps.get("miss_refresh_recovered", 0)
        if mr or mrs or mrr:
            print(
                f"    refreshes: rescans={mr:,}  skips={mrs:,}  recovered={mrr:,}  "
                f"retry_ids={ps.get('miss_refresh_retry_ids', 0):,}",
                file=out,
            )

        # Write / batch counters.
        pc = ps.get("put_calls", 0)
        pmc = ps.get("put_many_calls", 0)
        pmr = ps.get("put_many_records", 0)
        pmb = ps.get("put_many_bytes", 0)
        if pc or pmc:
            avg_r = pmr / pmc if pmc else 0
            avg_b = pmb / pmc if pmc else 0
            fast = ps.get("put_many_fast_path_calls", 0)
            clone = ps.get("put_many_clone_path_calls", 0)
            print(
                f"    put: {pc} calls, {ps.get('put_bytes', 0):,}B | put_many: {pmc} calls, {pmr:,} records, {pmb:,}B (avg {avg_r:.1f}r/{avg_b:.0f}B)",
                file=out,
            )
            if fast or clone:
                print(
                    f"    put_many path: fast={fast}  clone={clone}  index_clone={_fmt_us(ps.get('index_clone_time_us', 0))}",
                    file=out,
                )

        # Sync / persistence.
        sc = ps.get("sync_calls", 0)
        if sc:
            print(
                f"    sync: {sc} calls  checkpoint_writes={ps.get('checkpoint_writes', 0)}  checkpoint_skips={ps.get('checkpoint_skips', 0)}  delta_appends={ps.get('delta_appends', 0)}  invalidations={ps.get('delta_invalidations', 0)}",
                file=out,
            )

        # Shard write stats (persisted across opens).
        sw = ps.get("shard_write_count", 0)
        sb = ps.get("shard_bytes_written", 0)
        ss = ps.get("shard_sync_count", 0)
        if sw:
            print(f"    shard writes: {sw:,} records, {sb:,}B, {ss} syncs", file=out)

        # Index ops.
        ig = ps.get("index_grow_count", 0)
        ir = ps.get("index_rebuild_count", 0)
        if ig or ir:
            print(f"    index: grows={ig}  rebuilds={ir}", file=out)

        # Repack.
        rc = ps.get("repack_count", 0)
        if rc:
            print(
                f"    repack: {rc} calls, kept={ps.get('repack_kept', 0):,}, dropped={ps.get('repack_dropped', 0):,}",
                file=out,
            )

        # Open timings.
        ot = ps.get("last_open_timings")
        if ot:
            is_fresh = any(
                ot.get(k, 0) > 0
                for k in (
                    "store_meta_write_us",
                    "pool_meta_persist_us",
                    "initial_pack_create_us",
                )
            )
            init_str = ""
            if is_fresh:
                init_str = (
                    f" init(store_meta={_fmt_us(ot.get('store_meta_write_us', 0))} "
                    f"pool_persist={_fmt_us(ot.get('pool_meta_persist_us', 0))} "
                    f"pack_create={_fmt_us(ot.get('initial_pack_create_us', 0))})"
                )
            meta_unatt = ot.get("metadata_unattributed_us", 0)
            meta_unatt_str = f" unatt={_fmt_us(meta_unatt)}" if meta_unatt > 0 else ""
            print(
                f"    last open: {_fmt_us(ot.get('total_us', 0))} ("
                f"shard={_fmt_us(ot.get('shard_open_us', 0))} "
                f"discovery={_fmt_us(ot.get('shard_discovery_us', 0))} "
                f"lock={_fmt_us(ot.get('writer_lock_us', 0))} "
                f"recovery={_fmt_us(ot.get('packfile_recovery_us', 0))}/{ot.get('packfile_recovery_calls', 0)} packs "
                f"open={_fmt_us(ot.get('packfile_open_us', 0))}/{ot.get('packfile_open_calls', 0)} packs "
                f"metadata={_fmt_us(ot.get('metadata_restore_us', 0))}["
                f"pool_meta={_fmt_us(ot.get('pool_meta_restore_us', 0))} "
                f"stats={_fmt_us(ot.get('persisted_stats_restore_us', 0))}"
                f"{init_str}"
                f"{meta_unatt_str}"
                f"] "
                f"unattributed={_fmt_us(ot.get('shard_open_unattributed_us', 0))})",
                file=out,
            )
            print(
                f"      index: checkpoint={_fmt_us(ot.get('checkpoint_decode_us', 0))} fingerprint={_fmt_us(ot.get('fingerprint_us', 0))} materialization={_fmt_us(ot.get('index_materialization_us', 0))} delta={_fmt_us(ot.get('delta_replay_us', 0))} scan={_fmt_us(ot.get('full_scan_us', 0))}",
                file=out,
            )

        # Sync timings.
        st_tot = ps.get("sync_totals")
        if st_tot and st_tot.get("calls", 0) > 0:
            calls = st_tot.get("calls", 0)
            print(
                f"    sync totals ({calls} calls): {_fmt_us(st_tot.get('total_us', 0))} (flush={_fmt_us(st_tot.get('pack_flush_us', 0))} fsync={_fmt_us(st_tot.get('pack_fsync_us', 0))} sidecar={_fmt_us(st_tot.get('sidecar_us', 0))} delta={_fmt_us(st_tot.get('delta_log_us', 0))} checkpoint={_fmt_us(st_tot.get('checkpoint_us', 0))})",
                file=out,
            )
        st = ps.get("last_sync_timings")
        if st:
            print(
                f"    last sync: {_fmt_us(st.get('total_us', 0))} (flush={_fmt_us(st.get('pack_flush_us', 0))} fsync={_fmt_us(st.get('pack_fsync_us', 0))} sidecar={_fmt_us(st.get('sidecar_us', 0))} delta={_fmt_us(st.get('delta_log_us', 0))} checkpoint={_fmt_us(st.get('checkpoint_us', 0))})",
                file=out,
            )

    print("=============================\n", file=out)


if os.environ.get("SYNAPSE_MTXDB_STATS"):
    atexit.register(_print_mtxdb_stats)


# ── Periodic mtxdb runtime snapshot (opt-in via SYNAPSE_MTXDB_STATS=1) ──
#
# The end-of-run report above shows totals, not their *slope*: a long
# Complement run can slow down as the store grows without any single total
# looking wrong. This emits one compact INFO line, at most once per interval,
# from the master's persist path -- so pack/shard/index growth sits next to the
# request durations it might explain, on the same thread. Gated on a monotonic
# clock, not wall time, so a clock step can neither suppress nor burst it. No
# Rust changes and nothing on the hot path beyond the gate check.
_MTXDB_SNAPSHOT_INTERVAL_SECS: float = 60.0
_mtxdb_snapshot_last: float = 0.0
_mtxdb_snapshot_prev: dict[str, dict[str, float]] = {}
_mtxdb_persist_calls = 0
_mtxdb_persist_total_secs = 0.0
_mtxdb_persist_snapshot_calls = 0
_mtxdb_persist_snapshot_total_secs = 0.0
_mtxdb_batch_calls = 0
_mtxdb_batch_total_secs = 0.0
_mtxdb_batch_snapshot_calls = 0
_mtxdb_batch_snapshot_total_secs = 0.0


def record_mtxdb_persist_timing(elapsed: float) -> None:
    """Record successful persist latency for the next diagnostic snapshot."""
    global _mtxdb_persist_calls, _mtxdb_persist_total_secs
    if os.environ.get("SYNAPSE_MTXDB_STATS"):
        _mtxdb_persist_calls += 1
        _mtxdb_persist_total_secs += elapsed


def record_mtxdb_persist_batch_timing(elapsed: float) -> None:
    """Record `_persist_event_batch` work time (queue wait excluded)."""
    global _mtxdb_batch_calls, _mtxdb_batch_total_secs
    if os.environ.get("SYNAPSE_MTXDB_STATS"):
        _mtxdb_batch_calls += 1
        _mtxdb_batch_total_secs += elapsed


def _mtxdb_snapshot_metrics(ps: dict[str, Any]) -> dict[str, float]:
    sync_totals = ps.get("sync_totals") or {}
    sync_diagnostics = ps.get("sync_diagnostics") or {}
    return {
        "index_bytes": float(ps.get("index_bytes", 0) or 0),
        "collection_count": float(ps.get("collection_count", 0) or 0),
        "shard_count": float(ps.get("shard_count", 0) or 0),
        "candidate_reads": float(ps.get("candidate_reads", 0) or 0),
        "repack_count": float(ps.get("repack_count", 0) or 0),
        "repack_kept": float(ps.get("repack_kept", 0) or 0),
        "repack_dropped": float(ps.get("repack_dropped", 0) or 0),
        "index_grow_count": float(ps.get("index_grow_count", 0) or 0),
        "index_rebuild_count": float(ps.get("index_rebuild_count", 0) or 0),
        "checkpoint_writes": float(ps.get("checkpoint_writes", 0) or 0),
        "sync_calls": float(sync_totals.get("calls", 0) or 0),
        "sync_us": float(sync_totals.get("total_us", 0) or 0),
        "fsync_us": float(sync_totals.get("pack_fsync_us", 0) or 0),
        "flush_us": float(sync_totals.get("pack_flush_us", 0) or 0),
        "sidecar_us": float(sync_totals.get("sidecar_us", 0) or 0),
        "delta_log_us": float(sync_totals.get("delta_log_us", 0) or 0),
        "sync_checkpoint_us": float(sync_totals.get("checkpoint_us", 0) or 0),
        "wal_us": float(sync_totals.get("wal_us", 0) or 0),
        "journal_lock_wait_us": float(sync_totals.get("journal_lock_wait_us", 0) or 0),
        "journal_pending_wait_us": float(
            sync_totals.get("journal_pending_wait_us", 0) or 0
        ),
        "journal_append_us": float(sync_totals.get("journal_append_us", 0) or 0),
        "journal_fsync_us": float(sync_totals.get("journal_fsync_us", 0) or 0),
        "journal_sync_calls": float(sync_totals.get("journal_sync_calls", 0) or 0),
        "journal_bytes": float(sync_totals.get("journal_bytes", 0) or 0),
        "journal_records": float(sync_totals.get("journal_records", 0) or 0),
        "journal_waiters": float(sync_totals.get("journal_waiters", 0) or 0),
        "journal_coalesced": float(sync_totals.get("journal_coalesced", 0) or 0),
        "peak_journal_in_flight": float(
            sync_diagnostics.get("peak_journal_in_flight", 0) or 0
        ),
        "max_journal_lock_wait_us": float(
            sync_totals.get("max_journal_lock_wait_us", 0) or 0
        ),
        "max_journal_fsync_us": float(sync_totals.get("max_journal_fsync_us", 0) or 0),
        "dirty_lock_wait_us": float(sync_totals.get("dirty_lock_wait_us", 0) or 0),
        "pending_publish_age_us": float(
            sync_totals.get("pending_publish_age_us", 0) or 0
        ),
    }


def _format_mtxdb_snapshot_segment(
    pool: str, m: dict[str, float], d: dict[str, float]
) -> str:
    return (
        f"{pool}[idx={m['index_bytes'] / 1e6:.1f}MB(+{d['index_bytes'] / 1e6:.1f}MB) "
        f"col={int(m['collection_count'])}(+{int(d['collection_count'])}) "
        f"shards={int(m['shard_count'])}(+{int(d['shard_count'])}) "
        f"cand={int(m['candidate_reads'])}(+{int(d['candidate_reads'])}) "
        f"index=+{int(d['index_grow_count'])}/+{int(d['index_rebuild_count'])} "
        f"checkpoint=+{int(d['checkpoint_writes'])} "
        f"repack={int(m['repack_count'])}/{int(m['repack_kept'])}/{int(m['repack_dropped'])} "
        f"grow=+{int(d['index_grow_count'])} rebuild=+{int(d['index_rebuild_count'])} "
        f"ckpt=+{int(d['checkpoint_writes'])} "
        f"sync=+{int(d['sync_calls'])}/+{d['sync_us'] / 1000:.1f}ms "
        f"fsync=+{d['fsync_us'] / 1000:.1f}ms "
        f"phases(ms)=flush+{d['flush_us'] / 1000:.1f}/side+{d['sidecar_us'] / 1000:.1f}"
        f"/delta+{d['delta_log_us'] / 1000:.1f}/ckpt+{d['sync_checkpoint_us'] / 1000:.1f}"
        f"/wal+{d['wal_us'] / 1000:.1f} "
        f"j=lock+{d['journal_lock_wait_us'] / 1000:.1f}"
        f"/pend+{d['journal_pending_wait_us'] / 1000:.1f}"
        f"/append+{d['journal_append_us'] / 1000:.1f}"
        f"/fsync+{d['journal_fsync_us'] / 1000:.1f}ms"
        f" c=+{int(d['journal_sync_calls'])}"
        f" b=+{int(d['journal_bytes'])}"
        f" r=+{int(d['journal_records'])}"
        f" wait=+{int(d['journal_waiters'])}"
        f" coal=+{int(d['journal_coalesced'])}"
        f" peak={int(m['peak_journal_in_flight'])}"
        f" lmax={m['max_journal_lock_wait_us'] / 1000:.1f}/{m['max_journal_fsync_us'] / 1000:.1f}ms"
        f" dirty+{d['dirty_lock_wait_us'] / 1000:.1f}ms"
        f" age_avg={d['pending_publish_age_us'] / max(d['sync_calls'], 1) / 1000:.1f}ms]"
    )


def _mtxdb_snapshot_once() -> None:
    """Log one per-pool line with absolute values and since-last deltas."""
    global _mtxdb_persist_snapshot_calls, _mtxdb_persist_snapshot_total_secs
    global _mtxdb_batch_snapshot_calls, _mtxdb_batch_snapshot_total_secs
    try:
        from synapse.storage.databases.embedded_engine import get_embedded_engine

        s = get_embedded_engine("mtxdb").stats_snapshot()
    except Exception:
        return

    def _delta(pool: str, key: str, value: float) -> float:
        prev = _mtxdb_snapshot_prev.setdefault(pool, {})
        last = prev.get(key)
        prev[key] = value
        return 0.0 if last is None else value - last

    segments: list[str] = []
    for pool in ("state", "event_dag", "auth_chain"):
        ps = s.get(pool, {})
        if not ps:
            continue
        m = _mtxdb_snapshot_metrics(ps)
        d = {key: _delta(pool, key, value) for key, value in m.items()}
        segments.append(_format_mtxdb_snapshot_segment(pool, m, d))

    # FFI forward-edge cost, only recorded when SYNAPSE_PG_TIMINGS is set.
    edges_us = _FFI_TIMINGS.get("ffi_event_edges_get_forward", 0.0)
    edges_calls = _FFI_TIMING_COUNTS.get("ffi_event_edges_get_forward", 0)
    if edges_calls:
        segments.append(
            f"edges_forward={edges_us / edges_calls * 1000:.2f}ms/{edges_calls}"
        )

    if segments:
        persist_calls = _mtxdb_persist_calls - _mtxdb_persist_snapshot_calls
        persist_total_secs = (
            _mtxdb_persist_total_secs - _mtxdb_persist_snapshot_total_secs
        )
        persist_avg = (
            f"{persist_total_secs / persist_calls * 1000:.1f}ms"
            if persist_calls
            else "n/a"
        )
        batch_calls = _mtxdb_batch_calls - _mtxdb_batch_snapshot_calls
        batch_total_secs = _mtxdb_batch_total_secs - _mtxdb_batch_snapshot_total_secs
        batch_avg = (
            f"{batch_total_secs / batch_calls * 1000:.1f}ms" if batch_calls else "n/a"
        )
        _mtxdb_persist_snapshot_calls = _mtxdb_persist_calls
        _mtxdb_persist_snapshot_total_secs = _mtxdb_persist_total_secs
        _mtxdb_batch_snapshot_calls = _mtxdb_batch_calls
        _mtxdb_batch_snapshot_total_secs = _mtxdb_batch_total_secs
        logger.info(
            "mtxdb snapshot: persist_total=+%.1fms/%d avg=%s "
            "persist_batch=+%.1fms/%d avg=%s %s",
            persist_total_secs * 1000,
            persist_calls,
            persist_avg,
            batch_total_secs * 1000,
            batch_calls,
            batch_avg,
            "  ".join(segments),
        )


def maybe_log_mtxdb_snapshot() -> None:
    """Emit a snapshot if opted in and an interval has elapsed.

    A cheap no-op otherwise (two env/global checks and a monotonic read), so
    it is safe to call unconditionally from the master's persist path.
    """
    global _mtxdb_snapshot_last
    if not _engine_configured or not os.environ.get("SYNAPSE_MTXDB_STATS"):
        return
    now = time.monotonic()
    if now - _mtxdb_snapshot_last < _MTXDB_SNAPSHOT_INTERVAL_SECS:
        return
    _mtxdb_snapshot_last = now
    try:
        _mtxdb_snapshot_once()
    except Exception:
        # Diagnostics must never break event persistence.
        logger.debug("mtxdb snapshot failed", exc_info=True)


@contextmanager
def mirror_timing(tag: str) -> Iterator[None]:
    """Bracket an entire mirror-helper call (Python row/metadata construction
    *and* the native call it eventually makes) under one `mirror_<tag>`
    entry in the same aggregate report `ffi_timing` feeds -- distinct from
    the narrower `ffi_<tag>` entries individual call sites record around
    just the native call itself. Diffing `mirror_<tag>` against the matching
    `ffi_<tag>` isolates the Python-side share (encoding, list/dict
    construction, `get_embedded_engine` lookup) of a given helper's cost.

    Deliberately brackets the whole containing helper rather than timing
    every sub-step (e.g. every `get_embedded_engine()` lookup) individually
    -- more, finer-grained timers add their own overhead and risk
    perturbing the very measurement they're trying to take.

    No-op (near-zero overhead: one dict lookup, no timer read) when
    `SYNAPSE_PG_TIMINGS` isn't set, same as `ffi_timing`.
    """
    if not os.environ.get("SYNAPSE_PG_TIMINGS"):
        yield
        return
    start = time.monotonic()
    try:
        yield
    finally:
        ffi_timing(f"mirror_{tag}", time.monotonic() - start)


def configure_sync(*, no_sync: bool) -> None:
    """Set the module-level sync-disable flag.  Call once during init."""
    global _sync_disabled
    _sync_disabled = no_sync


def _set_engine_configured() -> None:
    """Record that an embedded engine was configured. Called once per store
    init when `embedded_hamt.engine` + `embedded_hamt.path` are set."""
    global _engine_configured
    _engine_configured = True


def namespace_hash(namespace: str) -> bytes:
    """16-byte digest of a namespace, used to key every embedded mirror.

    Namespaced keys keep multiple homeservers sharing one mtxdb file from
    colliding on event ids / state-group ids. Kept here so every
    embedded_* module derives keys identically (see `_state_hamt_node_key`
    in `rust/src/database/core.rs` for the matching Rust-side derivation).
    """
    return hashlib.sha256(namespace.encode("utf-8")).digest()[:16]


class SyncTier(Enum):
    """Classification for embedded-sidecar write visibility and durability.

    DURABLE: No SQL fallback exists, or the write uses accumulating/delta
    semantics (counters, auth-chain links, HAMT roots). Such writes need
    immediate cross-process publication and eventual durability.

    CACHE: A SQL fallback exists on the read path. A lost unflushed write
    just means a slower read via that fallback, not data loss.  sync() is
    skipped to avoid per-event fsync cost on the hottest write path.
    """

    DURABLE = auto()
    CACHE = auto()


class Pool(Enum):
    """Which of mtxdb's three storage pools a DURABLE write touched.

    `sync()` on the Rust side used to always fsync `state`, `event_dag`,
    and `auth_chain` together, regardless of which one a given caller
    actually wrote to -- e.g. an `event_json_put` (event_dag, sharded
    across 256 locator-bucket collections) forced a flush of every
    dirty event_dag shard on the next unrelated `state.py` DURABLE sync,
    and vice versa. Passing the pool(s) a call site actually dirtied to
    `maybe_sync` lets it call the matching Rust-side `sync_state` /
    `sync_event_dag` / `sync_auth_chain` instead of the blanket `sync`,
    so unrelated modules stop forcing each other's flushes.
    """

    STATE = auto()
    EVENT_DAG = auto()
    AUTH_CHAIN = auto()


def maybe_sync(tier: SyncTier, pools: Iterable[Pool] | None = None) -> None:
    """Sync mtxdb for DURABLE writes; no-op for CACHE writes.

    A DURABLE write has no SQL fallback, or uses accumulating/delta
    semantics (counters, auth-chain links, HAMT roots). This function is the
    durability path; request-path visibility barriers should use
    ``maybe_publish`` so they do not pay an fsync per transaction.

    A CACHE write has a SQL fallback on the read path. A lost unflushed
    write just means a slower read via that fallback, not data loss.

    `pools`: which pool(s) this call site's batch actually wrote to (see
    `Pool`'s doc comment). Omit only for a generic backstop that isn't
    tied to a specific write (e.g. a periodic timer flush) -- that syncs
    all three pools, same as before this parameter existed. A call site
    that knows what it wrote should always pass `pools` explicitly, so a
    write to one pool doesn't force a flush of another pool's unrelated
    dirty shards.
    """
    if not _engine_configured:
        return

    if tier is not SyncTier.DURABLE:
        return

    if _sync_disabled:
        return

    from synapse.storage.databases.embedded_engine import get_embedded_engine

    engine = get_embedded_engine("mtxdb")
    if pools is None:
        _st = time.monotonic()
        engine.sync()
        ffi_timing("ffi_sync_all", time.monotonic() - _st)
        return
    pool_set = set(pools)
    if Pool.STATE in pool_set:
        _st = time.monotonic()
        engine.sync_state()
        ffi_timing("ffi_sync_state", time.monotonic() - _st)
    if Pool.EVENT_DAG in pool_set:
        _st = time.monotonic()
        ffi_count("event_dag_sync_requests", 1)
        try:
            engine.sync_event_dag()
        except Exception:
            ffi_count("event_dag_sync_errors", 1)
            raise
        else:
            ffi_count("event_dag_sync_completed", 1)
        finally:
            elapsed = time.monotonic() - _st
            ffi_timing("ffi_sync_event_dag", elapsed)
            ffi_timing("event_dag_sync_duration", elapsed)
    if Pool.AUTH_CHAIN in pool_set:
        _st = time.monotonic()
        engine.sync_auth_chain()
        ffi_timing("ffi_sync_auth_chain", time.monotonic() - _st)


def maybe_publish(tier: SyncTier, pools: Iterable[Pool] | None = None) -> None:
    """Publish queued mtxdb mutations for cross-process visibility.

    The operation publishes every pool journal: WAL mode shares one journal
    coordinator, while non-WAL mode has one journal per pool. ``pools`` only
    identifies whether this call site has anything requiring publication; it
    does not scope the journal operation. Publication advances the
    read-committed boundary but deliberately does not make mutations durable.
    The coalesced ``maybe_sync`` path remains responsible for durability.

    Known limitation: this publication is not transaction-scoped. A concurrent
    SQL transaction may have mtxdb mutations in the same pending journal queue,
    and this call may publish them before that SQL transaction commits. In
    non-WAL mode, the three independent journal publications are also
    sequential rather than atomic. Production-safe transaction ordering
    requires transaction-scoped publication in mtxdb or serialization of
    embedded writes across the SQL commit boundary.
    """
    if not _engine_configured or tier is not SyncTier.DURABLE or _sync_disabled:
        return

    from synapse.storage.databases.embedded_engine import get_embedded_engine

    engine = get_embedded_engine("mtxdb")
    if pools is not None and not set(pools):
        return

    _st = time.monotonic()
    engine.publish_pending()
    ffi_timing("ffi_publish_pending", time.monotonic() - _st)


# ── Commit-aware flush coalescer ──────────────────────────────────────
#
# Replaces the 1-second periodic timer with event-driven debounced
# flushing.  Dirty marking happens via txn.call_after, which only fires
# after successful SQL commit.  The coalescer debounces from the first
# committed dirty write, flushes only dirty pools, clears a pool only
# after its sync succeeds, and retries on failure with backoff.
#
# Immediate barriers (maybe_sync) are retained for destructive ops
# (purge, redaction) and shutdown.


# Exported so tests can advance the reactor by slightly more than the flush
# window without coupling to a magic literal.
FLUSH_DELAY_SECS: float = 0.5


class _FlushCoalescer:
    """Commit-aware coalescing flush for mtxdb pools.

    Owned by the writer StateGroupDataStore and initialized with its
    clock.  Dirty marking happens via txn.call_after (after SQL commit),
    so only committed writes trigger an fsync.

    Debounces from the first committed dirty write (250-500ms) without
    resetting the timer on subsequent writes.  Flushes only dirty pools.
    Clears a pool only after its sync succeeds.  Retries on failure
    with backoff.
    """

    def __init__(
        self, clock: Clock, flush_delay_secs: float = FLUSH_DELAY_SECS
    ) -> None:
        from synapse.util.duration import Duration

        self._clock = clock
        self._dirty: set[Pool] = set()
        self._delayed_call: DelayedCallWrapper | None = None
        self._closed: bool = False
        self._FLUSH_DELAY = Duration(seconds=flush_delay_secs)
        self._RETRY_DELAY = Duration(seconds=1.0)

    def mark_dirty(self, pool: Pool) -> None:
        """Mark a pool as dirty.  Called via txn.call_after after SQL commit.

        The debounce timer is scheduled even when fsync is disabled
        (`no_sync`): `_flush` also drains the coalesced edge-write queues, and
        those queued rows must land in mtxdb (and be visible to reads) whether
        or not the sync is skipped.  `_do_sync_pools` itself still no-ops under
        `_sync_disabled`.
        """
        if self._closed:
            return
        was_clean = not self._dirty
        self._dirty.add(pool)
        if was_clean and self._delayed_call is None:
            self._delayed_call = self._clock.call_later(self._FLUSH_DELAY, self._flush)

    def _flush(self) -> None:
        """Flush dirty pools.  Called by the delayed callback."""
        self._delayed_call = None
        if self._closed:
            return
        # Drain the coalesced edge-write queues first: the rows land in the
        # EVENT_DAG pool, and the fsync below must cover them.  On failure the
        # rows were restored to their queues and EVENT_DAG re-marked dirty, so
        # retry on the backoff delay.  Imported lazily (embedded_event_edges
        # imports this module).
        try:
            if _drain_edge_writes():
                self._dirty.add(Pool.EVENT_DAG)
        except Exception:
            logger.warning("Edge-write drain failed, will retry", exc_info=True)
            self._dirty.add(Pool.EVENT_DAG)
            if self._delayed_call is None:
                self._delayed_call = self._clock.call_later(
                    self._RETRY_DELAY, self._flush
                )
            return
        to_flush = set(self._dirty)  # snapshot
        if not to_flush:
            return
        try:
            _do_sync_pools(to_flush)
            if Pool.EVENT_DAG in to_flush and not _sync_disabled:
                # This is a successful delayed/coalesced flush. The native
                # sync counters above record the actual pool call; this
                # counter identifies the coalescer's contribution.
                ffi_count("event_dag_sync_coalesced", 1)
            self._dirty.difference_update(to_flush)
        except Exception:
            if Pool.EVENT_DAG in to_flush:
                # This coarse batch signal is a subset of the native
                # event_dag_sync_errors counter, not an independent error.
                ffi_count("event_dag_sync_coalesced_errors", 1)
            logger.warning(
                "Flush coalescer sync failed for %s, will retry",
                to_flush,
                exc_info=True,
            )
        # Schedule retry if still dirty
        if self._dirty and self._delayed_call is None:
            self._delayed_call = self._clock.call_later(self._RETRY_DELAY, self._flush)

    def sync_now(self, pools: Iterable[Pool] | None = None) -> None:
        """Immediate barrier for destructive ops and shutdown.

        Cancels any pending delayed flush, syncs the requested pools
        (or all dirty pools if None), draining queued EVENT_DAG edge writes
        first. Each pool is synced independently so a failure syncing one
        pool neither masks nor is blamed on another: only the pool(s) whose
        sync actually raised are re-marked dirty and cause a raise.
        """
        if self._closed:
            return
        if self._delayed_call is not None:
            self._delayed_call.cancel()
            self._delayed_call = None

        to_flush = set(pools) if pools is not None else set(self._dirty)

        first_exc: Exception | None = None
        try:
            # EVENT_DAG also owns coalesced forward-edge writes. An explicit
            # EVENT_DAG barrier must include those queued records before
            # syncing. A raise here must still hit the `finally` below, or
            # EVENT_DAG can be left dirty with the timer we just cancelled
            # never rearmed -- the same stranded-work bug fixed earlier.
            if Pool.EVENT_DAG in to_flush:
                self._dirty.add(Pool.EVENT_DAG)
                if not _drain_edge_writes():
                    self._dirty.discard(Pool.EVENT_DAG)

            for pool in to_flush:
                try:
                    _do_sync_pools({pool})
                    self._dirty.discard(pool)
                except Exception as exc:
                    self._dirty.add(pool)
                    if first_exc is None:
                        first_exc = exc
        except Exception as exc:
            if Pool.EVENT_DAG in to_flush:
                self._dirty.add(Pool.EVENT_DAG)
            first_exc = first_exc or exc
        finally:
            if self._dirty and self._delayed_call is None:
                # A pool left dirty by an unrelated write during this call
                # (not one of our own failures) should rejoin the normal
                # debounce cadence, not be punished with the failure
                # backoff delay.
                delay = (
                    self._RETRY_DELAY if first_exc is not None else self._FLUSH_DELAY
                )
                self._delayed_call = self._clock.call_later(delay, self._flush)

        if first_exc is not None:
            raise first_exc

    def sync_event_dag_now(self) -> None:
        """Sync EVENT_DAG without draining the coalesced edge queues.

        Event JSON publication uses this narrower barrier because edge writes
        are independently coalesced and should not be forced out once per
        event. Full barriers continue to use ``sync_now``.
        """
        if self._closed:
            return
        try:
            _do_sync_pools({Pool.EVENT_DAG})
            # Do not clear EVENT_DAG here: the dirty bit may represent
            # coalesced edge writes, which this narrow barrier deliberately
            # leaves queued for the shared timer.
        except Exception:
            self._dirty.add(Pool.EVENT_DAG)
            # Do not cancel an already-armed timer here either: this runs
            # per persisted event, and under sustained failures (e.g. the
            # store unreachable) cancel-then-reschedule on every call would
            # perpetually push out the retry and starve `_flush` forever --
            # the same livelock shape as the success path, just triggered by
            # errors instead. `finally` below already guarantees a retry
            # gets scheduled if none exists; an existing timer, whatever its
            # delay, is left to fire on its own.
            raise
        finally:
            # Do not cancel an already-armed timer: this barrier runs per
            # persisted event, and cancel-then-reschedule on every call would
            # perpetually push out the coalescer's flush under sustained
            # traffic, starving the actual edge drain indefinitely.
            if self._dirty and self._delayed_call is None:
                self._delayed_call = self._clock.call_later(
                    self._RETRY_DELAY, self._flush
                )

    def close(self) -> None:
        """Flush outstanding dirty pools and shut down."""
        if self._delayed_call is not None:
            self._delayed_call.cancel()
            self._delayed_call = None
        # Drain the coalesced edge-write queues before the final fsync so a
        # shutdown racing still-queued forward-edge appends does not drop
        # committed rows.  Imported lazily (embedded_event_edges imports this
        # module) and done before marking closed so flush_edge_writes's
        # mark_dirty(EVENT_DAG) lands in `_dirty` for the final sync below.
        drained = False
        for attempt in range(3):
            try:
                if _drain_edge_writes():
                    self._dirty.add(Pool.EVENT_DAG)
                drained = True
                break
            except Exception:
                logger.warning(
                    "Flush coalescer edge-write drain failed (attempt %d/3)",
                    attempt + 1,
                    exc_info=True,
                )
        if not drained:
            logger.error(
                "Unable to drain queued edge writes during shutdown; "
                "writes remain pending (SQL fallback will cover reads)"
            )
        self._closed = True
        if self._dirty:
            for attempt in range(3):
                try:
                    _do_sync_pools(self._dirty)
                except Exception:
                    logger.warning(
                        "Flush coalescer close sync failed (attempt %d/3)",
                        attempt + 1,
                        exc_info=True,
                    )
                else:
                    self._dirty.clear()
                    break
            if self._dirty:
                logger.error(
                    "Unable to durably flush embedded pools during shutdown; "
                    "writes remain pending: %s",
                    self._dirty,
                )


_coalescer: _FlushCoalescer | None = None


def _do_sync_pools(pools: set[Pool]) -> None:
    """Sync specific pools.  Internal helper for the coalescer."""
    maybe_sync(SyncTier.DURABLE, pools=pools)


def _set_coalescer(c: _FlushCoalescer) -> None:
    """Set the module-level coalescer reference.  Called by StateGroupDataStore.__init__."""
    global _coalescer
    _coalescer = c


def _clear_coalescer(c: _FlushCoalescer) -> None:
    """Clear the module-level coalescer reference only if it is still ``c``.

    Identity-safe: stopping one homeserver's coalescer will not close
    another's if they share a process (e.g. Complement trial).
    """
    global _coalescer
    if _coalescer is c:
        _coalescer = None


def _drain_edge_writes() -> bool:
    """Flush every coalesced edge-write queue into mtxdb.

    Returns ``True`` if any rows were written.  Imported lazily because
    ``embedded_event_edges`` imports this module.  Called by the flush
    coalescer's timer and by shutdown so queued edge rows land in the
    EVENT_DAG pool before it is synced.
    """
    from synapse.storage.databases.main.embedded_event_edges import flush_edge_writes

    return flush_edge_writes()


def mark_dirty(pool: Pool) -> None:
    """Mark a pool dirty.  Called via txn.call_after after SQL commit."""
    if _coalescer is not None:
        _coalescer.mark_dirty(pool)


def sync_now(pools: Iterable[Pool] | None = None) -> None:
    """Immediate barrier for destructive ops (purge, redaction, shutdown).

    Syncs the requested pools and clears their dirty flags so a pending
    delayed flush does not redundantly re-sync them.  Falls back to
    maybe_sync if the coalescer hasn't been initialized.
    """
    if not _engine_configured:
        return

    pool_set = set(pools) if pools is not None else None
    if _coalescer is not None:
        _coalescer.sync_now(pool_set)
    elif pool_set is not None:
        maybe_sync(SyncTier.DURABLE, pools=pool_set)


def sync_event_dag_now() -> None:
    """Sync event JSON's EVENT_DAG records without draining edge queues."""
    if not _engine_configured:
        return
    if _coalescer is not None:
        _coalescer.sync_event_dag_now()
    else:
        maybe_sync(SyncTier.DURABLE, pools=[Pool.EVENT_DAG])


def close_coalescer() -> None:
    """Flush outstanding dirty pools and shut down the coalescer."""
    if _coalescer is not None:
        _coalescer.close()
        # Don't clear _coalescer here — _clear_coalescer is the
        # identity-safe path called by the owning store's stop().
        # This is a fallback for process-level teardown.
