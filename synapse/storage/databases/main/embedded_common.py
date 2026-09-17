from __future__ import annotations

import atexit
import hashlib
import json
import logging
import os
import threading
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from enum import Enum, auto
from typing import IO, TYPE_CHECKING, Iterable, Iterator

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
                f"    sync: {sc} calls  checkpoint_writes={ps.get('checkpoint_writes', 0)}  delta_appends={ps.get('delta_appends', 0)}  invalidations={ps.get('delta_invalidations', 0)}",
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
        st = ps.get("last_sync_timings")
        if st:
            print(
                f"    last sync: {_fmt_us(st.get('total_us', 0))} (flush={_fmt_us(st.get('pack_flush_us', 0))} fsync={_fmt_us(st.get('pack_fsync_us', 0))} sidecar={_fmt_us(st.get('sidecar_us', 0))} delta={_fmt_us(st.get('delta_log_us', 0))} checkpoint={_fmt_us(st.get('checkpoint_us', 0))})",
                file=out,
            )

    print("=============================\n", file=out)


if os.environ.get("SYNAPSE_MTXDB_STATS"):
    atexit.register(_print_mtxdb_stats)


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
    """Classification for embedded-sidecar write durability.

    DURABLE: No SQL fallback exists, or the write uses accumulating/delta
    semantics (counters, auth-chain links, HAMT roots).  A lost unflushed
    write here means silent data loss or incorrect state, so sync() is
    called after every batch.

    CACHE: A SQL fallback exists on the read path.  A lost unflushed write
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
    semantics (counters, auth-chain links, HAMT roots). A lost unflushed
    write here means silent data loss or incorrect state, so sync() is
    called after every batch.

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
        engine.sync_event_dag()
        ffi_timing("ffi_sync_event_dag", time.monotonic() - _st)
    if Pool.AUTH_CHAIN in pool_set:
        _st = time.monotonic()
        engine.sync_auth_chain()
        ffi_timing("ffi_sync_auth_chain", time.monotonic() - _st)


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

    def __init__(self, clock: Clock) -> None:
        from synapse.util.duration import Duration

        self._clock = clock
        self._dirty: set[Pool] = set()
        self._delayed_call: DelayedCallWrapper | None = None
        self._closed: bool = False
        self._FLUSH_DELAY = Duration(seconds=FLUSH_DELAY_SECS)
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
            self._dirty.difference_update(to_flush)
        except Exception:
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
        (or all dirty pools if None), and reschedules if still dirty.
        """
        if self._closed:
            return
        if self._delayed_call is not None:
            self._delayed_call.cancel()
            self._delayed_call = None
        to_flush = set(pools) if pools is not None else set(self._dirty)
        if to_flush:
            _do_sync_pools(to_flush)
            self._dirty.difference_update(to_flush)
        if self._dirty and self._delayed_call is None:
            self._delayed_call = self._clock.call_later(self._FLUSH_DELAY, self._flush)

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

    if _coalescer is not None:
        _coalescer.sync_now(pools)
    elif pools is not None:
        maybe_sync(SyncTier.DURABLE, pools=pools)


def close_coalescer() -> None:
    """Flush outstanding dirty pools and shut down the coalescer."""
    if _coalescer is not None:
        _coalescer.close()
        # Don't clear _coalescer here — _clear_coalescer is the
        # identity-safe path called by the owning store's stop().
        # This is a fallback for process-level teardown.
