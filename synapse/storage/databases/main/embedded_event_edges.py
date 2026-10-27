#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Stores `event_edges` (backward and forward event DAG edges) in the
configured authoritative backend.

Design:
- Backward edges: `event_id -> [(prev_event_id, is_state)]`. Cryptographically
  immutable once an event is created.
- Forward edges: `prev_event_id -> [child_event_id]`. Deduplicated and appended
  as new children arrive under RMW_LOCK.
- In mtxdb mode, locators are primarily owned by `embedded_event_json`;
  `event_edges_put` re-publishes them for every event a row touches (see the
  Rust doc), so backfilled edges for legacy events still resolve.

Writes are coalesced per namespace: ``queue_edge_write`` accumulates rows
across post-commit callbacks, ``flush_edge_writes`` drains them in a single
FFI call when the queue exceeds ``_EDGE_WRITE_THRESHOLD``, and the commit
aware flush coalescer (embedded_common._FlushCoalescer) drains any remainder
on its bounded debounce timer.  Drained rows are only dropped from the queue
after a successful FFI write; a failed write is re-queued for retry.
"""

from __future__ import annotations

import collections
import logging
import threading
import time
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Callable, Iterable, TypeVar

from prometheus_client import Counter

from synapse.metrics import SERVER_NAME_LABEL, LaterGauge
from synapse.storage.database import make_in_list_sql_clause
from synapse.storage.databases.main.embedded_common import (
    Pool,
    ffi_batch_size,
    ffi_count,
    ffi_timing,
    lock_wait_timing,
    mark_dirty,
    mirror_timing,
    sync_event_dag_now,
    sync_now,
)

if TYPE_CHECKING:
    from synapse.server import HomeServer

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


# ---------------------------------------------------------------------------
# Edge write coalescer
# ---------------------------------------------------------------------------
# txn.call_after callbacks run one-by-one after each SQL commit.  Without
# coalescing every commit triggers a separate FFI call + RMW_LOCK acquire
# for as few as 1-2 rows.  The queue accumulates rows across commits and
# flushes them in bulk when the threshold is reached or the flush coalescer's
# debounce timer fires (see _FlushCoalescer._flush, which drains edge queues
# before syncing EVENT_DAG).
#
# Correctness rules for the queue:
#   * Queues are per-namespace, and a flush only ever drains the namespace it
#     was asked for.  Shutdown drains every namespace separately.
#   * Flushes for a given namespace are serialized by a per-namespace lock so
#     two concurrent FFI writes cannot interleave read/modify/write forward
#     edge updates (the Rust RMW_LOCK would serialize the native calls, but
#     retaining the rows on failure requires the ownership to be unambiguous).
#   * Drained rows are restored to the front of their queue if the FFI write
#     fails -- committed rows are never silently dropped.
#   * Destructive deletes cancel queued rows for the events being purged
#     before draining, mark those ids as in-progress "purging" for the
#     duration of the barrier, and only record a permanent TTL tombstone after
#     the FFI delete succeeds.  `queue_edge_write` checks both sets, so a late
#     enqueue racing the purge cannot resurrect a purged event's edges, while
#     a failed delete leaves repair writes unblocked.
_EDGE_WRITE_THRESHOLD: int = 64
# `(room_id, event_id, prev_event_id, is_state)`.
EdgeRow = tuple[str, str, str, bool]
# Queues are keyed by namespace rather than owned by a coalescer instance.
# Namespaces partition the embedded data (and are unique per homeserver /
# config, see `DatabaseConfig.embedded_db_namespace`), and the mtxdb engine
# itself is a process-global OnceCell, so (engine, namespace) is already the
# ownership boundary: two homeservers sharing a process cannot collide here
# without also colliding in mtxdb.  A per-coalescer queue would not change
# that, and would make the free-function write path need a coalescer handle.
_edge_write_queues: dict[str, list[EdgeRow]] = {}
_edge_write_queues_lock = threading.Lock()
# One flush lock per namespace, serializing drains (and purge barriers) so at
# most one in-flight FFI write exists per namespace at a time.  Enqueue also
# takes this lock so a row cannot be appended between a purge's cancel step
# and its tombstone write -- see `delete_event_edges_batch`.
_edge_write_flush_locks: dict[str, threading.Lock] = {}
_edge_write_flush_locks_guard = threading.Lock()
# Events whose FFI delete succeeded, per namespace, with the monotonic
# deadline after which the tombstones may be dropped.  A purge records these
# so that a transaction whose post-commit `queue_edge_write` races the purge
# (its `txn.call_after` runs after the purge's cancel step) cannot resurrect
# the purged event's edges.  The deadline only has to cover the gap between a
# transaction committing and its `call_after` callback running, which is tiny;
# the generous TTL bounds memory for repeatedly purging namespaces.  A
# tombstone is only recorded after the FFI delete returns -- and is dropped
# again if it never does -- so a failed delete cannot suppress the repair
# writes that recover from it.  Protected by `_edge_write_queues_lock`.
_EDGE_WRITE_TOMBSTONE_TTL: float = 600.0
_edge_write_tombstones: dict[str, dict[str, float]] = {}
# Events currently being purged by an in-flight `delete_event_edges_batch`, per
# namespace.  Set before the queue is cancelled and cleared once the FFI delete
# has either succeeded (promoted to a tombstone) or failed (dropped).
# `queue_edge_write` consults this as well as `_edge_write_tombstones`; the
# purge barrier holds the namespace flush lock throughout, so this marker is
# belt-and-braces rather than the primary ordering guarantee.  Protected by
# `_edge_write_queues_lock`.
_edge_write_purging: dict[str, set[str]] = {}


def _namespace_flush_lock(namespace: str) -> threading.Lock:
    """Return the per-namespace flush lock, creating it on first use."""
    with _edge_write_flush_locks_guard:
        lock = _edge_write_flush_locks.get(namespace)
        if lock is None:
            lock = threading.Lock()
            _edge_write_flush_locks[namespace] = lock
        return lock


def _prune_tombstones_locked(namespace: str, now: float) -> dict[str, float] | None:
    """Drop expired tombstones for `namespace`; caller holds the queues lock."""
    tombstones = _edge_write_tombstones.get(namespace)
    if not tombstones:
        return None
    for event_id in [eid for eid, deadline in tombstones.items() if deadline <= now]:
        del tombstones[event_id]
    if not tombstones:
        _edge_write_tombstones.pop(namespace, None)
        return None
    return tombstones


def queued_edge_write_count(namespace: str | None = None) -> int:
    """Number of edge rows still coalesced in the write queue(s).

    Exposed for tests/diagnostics; ``namespace=None`` counts every namespace.
    """
    with lock_wait_timing(_edge_write_queues_lock, "edge_queues"):
        if namespace is None:
            return sum(len(q) for q in _edge_write_queues.values())
        return len(_edge_write_queues.get(namespace, []))


def queue_edge_write(
    namespace: str,
    rows: Iterable[EdgeRow],
    *,
    sync: bool = False,
) -> None:
    """Append edge rows to the per-namespace coalescing queue.

    Rows for events already tombstoned (or currently being purged) by a purge
    are dropped, and the append participates in the same per-namespace flush
    lock the purge barrier holds, so an enqueue that races a purge is ordered
    before the cancel step (and then cancelled) or after it (and then dropped
    by the tombstone).

    Marks ``EVENT_DAG`` dirty so the flush coalescer schedules a bounded
    debounce drain, and flushes immediately when the queue grows past the
    threshold.
    """
    row_list = list(rows)
    if not row_list:
        return
    # This callback may run on the reactor thread. Measure each lock's
    # acquisition wait and hold time separately; reactor lag is measured by
    # the process-level probe, not conflated with lock wait here.
    with lock_wait_timing(_namespace_flush_lock(namespace), "edge_namespace_flush"):
        with lock_wait_timing(_edge_write_queues_lock, "edge_queues"):
            tombstones = _prune_tombstones_locked(namespace, time.monotonic()) or {}
            purging = _edge_write_purging.get(namespace) or ()
            if tombstones or purging:
                row_list = [
                    row
                    for row in row_list
                    if (row[1] not in tombstones and row[1] not in purging)
                ]
                if not row_list:
                    return
            q = _edge_write_queues.setdefault(namespace, [])
            q.extend(row_list)
            over_threshold = len(q) >= _EDGE_WRITE_THRESHOLD
        if over_threshold:
            try:
                # Take the flush lock only once: we already hold it, so drain
                # directly rather than re-entering the (non-reentrant) lock.
                _flush_namespace_locked(namespace, sync=sync)
            except Exception:
                # flush_edge_writes already restored the rows and re-marked the
                # pool dirty; log and let the coalescer retry.
                logger.warning(
                    "Edge-write threshold flush failed for namespace %s; rows retained",
                    namespace,
                    exc_info=True,
                )
        else:
            mark_dirty(Pool.EVENT_DAG)


def flush_edge_writes(namespace: str | None = None, *, sync: bool = False) -> bool:
    """Drain the coalescing queue into a single FFI call per namespace.

    ``namespace`` may be omitted to drain every namespace (used by the flush
    coalescer's timer and by shutdown).

    Returns ``True`` if any rows were flushed.  Raises on FFI failure, having
    first restored the drained rows to the front of their queue.
    """
    if namespace is not None:
        with lock_wait_timing(_namespace_flush_lock(namespace), "edge_namespace_flush"):
            return _flush_namespace_locked(namespace, sync=sync)
    with lock_wait_timing(_edge_write_queues_lock, "edge_queues"):
        namespaces = list(_edge_write_queues)
    flushed = False
    for ns in namespaces:
        with lock_wait_timing(_namespace_flush_lock(ns), "edge_namespace_flush"):
            if _flush_namespace_locked(ns, sync=sync):
                flushed = True
    return flushed


def _flush_namespace_locked(namespace: str, *, sync: bool = False) -> bool:
    """Drain one namespace's queue.  The caller must hold its flush lock.

    Removes the drained rows from the queue only after the FFI write
    succeeds; on failure the rows are restored to the front of the queue and
    the exception is re-raised.
    """
    with lock_wait_timing(_edge_write_queues_lock, "edge_queues"):
        q = _edge_write_queues.get(namespace)
        if not q:
            return False
        rows = q[:]
        q.clear()
    try:
        put_event_edges_batch(namespace, rows, sync=sync)
    except Exception:
        with lock_wait_timing(_edge_write_queues_lock, "edge_queues"):
            restored = _edge_write_queues.setdefault(namespace, [])
            restored[0:0] = rows
        mark_dirty(Pool.EVENT_DAG)
        raise
    with lock_wait_timing(_edge_write_queues_lock, "edge_queues"):
        q = _edge_write_queues.get(namespace)
        if q is not None and not q:
            _edge_write_queues.pop(namespace, None)
    mark_dirty(Pool.EVENT_DAG)
    return True


def open_embedded_event_edges_engine(hs: HomeServer) -> bool:
    """Return whether the embedded event-edges backend is available for readers and writers."""
    return bool(
        hs.config.database.embedded_db_engine == "mtxdb"
        and hs.config.database.embedded_db_path
    )


event_edge_reads_counter = Counter(
    "synapse_event_edges_reads",
    "Lookups of event edges by caller (`path`) and outcome, one increment per "
    "lookup so the outcomes sum to the total lookup rate: `hit` or `miss` "
    "when served from mtxdb, and `sql_only` when the SQL backend is "
    "authoritative. Unlike the opt-in ffi counters this is always on.",
    labelnames=["path", "outcome", SERVER_NAME_LABEL],
)

event_edge_publish_signal_gauge = LaterGauge(
    name="synapse_event_edges_publish_signal_active",
    desc="1 if the read-only mtxdb handle for this pool mapped the writer's "
    "publish signal for strict cross-process freshness, 0 if it uses the "
    "stat-based fallback.",
    labelnames=["pool", SERVER_NAME_LABEL],
)


@lru_cache(maxsize=32)
def _event_edge_read_metric(path: str, outcome: str, server_name: str) -> Any:
    return event_edge_reads_counter.labels(
        path=path, outcome=outcome, **{SERVER_NAME_LABEL: server_name}
    )


_publish_signal_api_warning_lock = threading.Lock()
_publish_signal_api_warning_logged = False


def _log_missing_publish_signal_api_once() -> None:
    global _publish_signal_api_warning_logged
    with _publish_signal_api_warning_lock:
        if _publish_signal_api_warning_logged:
            return
        _publish_signal_api_warning_logged = True
    logger.warning(
        "The installed mtxdb extension does not expose publish_signal_active; "
        "event-edge freshness capability cannot be reported"
    )


def record_event_edge_read(server_name: str, path: str, outcome: str) -> None:
    """Count one event-edge lookup (see `event_edge_reads_counter`)."""
    _event_edge_read_metric(path, outcome, server_name).inc()


def register_publish_signal_gauge(hs: HomeServer) -> None:
    """Export per-pool publish-signal state for this process's mtxdb handle.

    This is a capability snapshot, not proof of freshness. A worker that is
    not scraped has no sample at all, so rollout checks must require a sample
    of 1 for the `event_dag` pool from every live read-only worker (absence
    fails the check, same as 0).
    """
    server_name = hs.hostname

    def collect() -> dict[tuple[str, ...], int]:
        from synapse.storage.databases.embedded_engine import get_embedded_engine

        engine = get_embedded_engine("mtxdb")
        publish_signal_active = getattr(engine, "publish_signal_active", None)
        if not callable(publish_signal_active):
            _log_missing_publish_signal_api_once()
            return {}

        active = publish_signal_active()
        return {(pool, server_name): int(bool(v)) for pool, v in active.items()}

    event_edge_publish_signal_gauge.register_hook(
        homeserver_instance_id=hs.get_instance_id(), hook=collect
    )


def embedded_event_edges_is_writable(hs: HomeServer) -> bool:
    """Return whether this process is permitted to write to embedded event-edges."""
    # The embedded event DAG has a single writer: the events stream writer.
    # ``database.read_only`` is not sufficient here. Worker processes can have
    # a normal SQL connection while opening mtxdb read-only, and attempting a
    # repair write from one of those workers raises in the Rust binding.
    return (
        open_embedded_event_edges_engine(hs)
        and not getattr(hs.config.database, "read_only", False)
        and hs.get_instance_name() in hs.config.worker.writers.events
    )


class EventEdgesBackendMismatchError(Exception):
    """The configured event-edges backend does not hold the migrated data."""


def configured_event_edges_backend(hs: HomeServer) -> str:
    return "mtxdb" if open_embedded_event_edges_engine(hs) else "sql"


async def check_event_edges_backend(hs: HomeServer) -> None:
    """Refuse to start unless the configured backend is the one that holds the
    event edges.

    A database with no events yet adopts the configured backend. Otherwise the
    stored marker must match; switching backends is a manual migration.

    Raises:
        EventEdgesBackendMismatchError: on a mismatch.
    """
    store = hs.get_datastores().main
    configured = configured_event_edges_backend(hs)
    pool = store.db_pool

    def _txn(txn: Any) -> str:
        txn.execute("SELECT backend FROM edge_backend WHERE lock = 'X'")
        row = txn.fetchone()
        stored = row[0] if row else "sql"
        if stored != configured:
            txn.execute("SELECT 1 FROM events LIMIT 1")
            if txn.fetchone() is None:
                txn.execute("DELETE FROM edge_backend")
                txn.execute(
                    "INSERT INTO edge_backend (lock, backend) VALUES ('X', ?)",
                    (configured,),
                )
                return configured
        return str(stored)

    stored = await pool.runInteraction("check_event_edges_backend", _txn)
    if stored != configured:
        raise EventEdgesBackendMismatchError(
            f"Event edges are stored in {stored!r} but this server is configured "
            f"for {configured!r}. Refusing to start: stop Synapse and run "
            f"`synapse_event_edges_migrate --to {configured}` with your config, "
            "or restore the previous embedded_db configuration."
        )


async def _get_event_edges_backend(store: Any) -> str:
    def _txn(txn: Any) -> str:
        txn.execute("SELECT backend FROM edge_backend WHERE lock = 'X'")
        row = txn.fetchone()
        return str(row[0]) if row else "sql"

    return await store.db_pool.runInteraction("get_event_edges_backend", _txn)


async def _set_event_edges_backend(store: Any, backend: str) -> None:
    def _txn(txn: Any) -> None:
        txn.execute("UPDATE edge_backend SET backend = ? WHERE lock = 'X'", (backend,))
        if txn.rowcount == 0:
            txn.execute(
                "INSERT INTO edge_backend (lock, backend) VALUES ('X', ?)",
                (backend,),
            )

    await store.db_pool.runInteraction("set_event_edges_backend", _txn)


async def _migrate_sql_edges_to_mtxdb(
    store: Any, namespace: str, batch_size: int
) -> dict[str, int]:
    """Reconcile every SQL-backed event's edges into mtxdb.

    The marker is not flipped here. Each event is compared against its
    backward mtxdb record; only mismatches are rewritten (delete then put), so
    a rerun after an interrupted migration is both safe and cheap.
    """
    total_edges = 0
    changed_events = 0
    last_event_id = ""

    while True:

        def _read(txn: Any) -> tuple[list[tuple[str, str]], dict[str, list[Any]]]:
            txn.execute(
                "SELECT event_id, room_id FROM events "
                "WHERE event_id > ? ORDER BY event_id LIMIT ?",
                (last_event_id, batch_size),
            )
            events = txn.fetchall()
            if not events:
                return [], {}

            event_ids = [event_id for event_id, _room_id in events]
            clause, args = make_in_list_sql_clause(
                store.database_engine, "event_id", event_ids
            )
            txn.execute(
                "SELECT event_id, prev_event_id, room_id, is_state "
                f"FROM event_edges WHERE {clause}",
                args,
            )
            edges_by_event: dict[str, list[Any]] = collections.defaultdict(list)
            for event_id, prev_event_id, room_id, is_state in txn:
                edges_by_event[event_id].append(
                    (prev_event_id, bool(is_state), room_id)
                )
            return events, edges_by_event

        events, edges_by_event = await store.db_pool.runInteraction(
            "migrate_event_edges_read_sql", _read
        )
        if not events:
            break

        event_ids = [event_id for event_id, _room_id in events]
        current = get_event_edges_backward_batch(namespace, event_ids)
        to_delete: list[str] = []
        rows_to_put: list[EdgeRow] = []

        for event_id, room_id in events:
            desired_rows = edges_by_event.get(event_id, [])
            desired = {
                (prev_event_id, is_state) for prev_event_id, is_state, _ in desired_rows
            }
            current_edges = current.get(event_id)
            if desired != set(current_edges or ()):
                changed_events += 1
                if current_edges:
                    to_delete.append(event_id)
                rows_to_put.extend(
                    (edge_room_id or room_id, event_id, prev_event_id, is_state)
                    for prev_event_id, is_state, edge_room_id in desired_rows
                )
            total_edges += len(desired)

        if to_delete:
            delete_event_edges_batch(namespace, to_delete)
        if rows_to_put:
            put_event_edges_batch(namespace, rows_to_put, sync=True)

        last_event_id = events[-1][0]

    sync_now(pools=[Pool.EVENT_DAG])
    return {"edges": total_edges, "changed_events": changed_events}


async def _migrate_mtxdb_edges_to_sql(
    store: Any, namespace: str, batch_size: int
) -> dict[str, int]:
    """Replace the SQL edge table with the mtxdb backward records.

    mtxdb is the source while the marker says so, so stale SQL rows must be
    removed rather than merged. The SQL copy is rebuilt in batches with an
    upsert, making a rerun after an interruption idempotent.
    """

    def _clear(txn: Any) -> None:
        txn.execute("DELETE FROM event_edges")

    await store.db_pool.runInteraction("migrate_event_edges_clear_sql", _clear)

    total_edges = 0
    last_event_id = ""

    while True:

        def _read(txn: Any) -> list[tuple[str, str]]:
            txn.execute(
                "SELECT event_id, room_id FROM events "
                "WHERE event_id > ? ORDER BY event_id LIMIT ?",
                (last_event_id, batch_size),
            )
            return txn.fetchall()

        events = await store.db_pool.runInteraction(
            "migrate_event_edges_read_events", _read
        )
        if not events:
            break

        current = get_event_edges_backward_batch(
            namespace, [event_id for event_id, _room_id in events]
        )
        rows: list[tuple[str, str, str, bool]] = []
        for event_id, room_id in events:
            for prev_event_id, is_state in current.get(event_id) or ():
                rows.append((event_id, prev_event_id, room_id, bool(is_state)))

        if rows:

            def _write(txn: Any) -> None:
                store.db_pool.simple_upsert_many_txn(
                    txn,
                    table="event_edges",
                    key_names=("event_id", "prev_event_id"),
                    key_values=[
                        (event_id, prev_event_id)
                        for event_id, prev_event_id, _room_id, _is_state in rows
                    ],
                    value_names=("room_id", "is_state"),
                    value_values=[
                        (room_id, is_state)
                        for _event_id, _prev_event_id, room_id, is_state in rows
                    ],
                )

            await store.db_pool.runInteraction("migrate_event_edges_write_sql", _write)
            total_edges += len(rows)

        last_event_id = events[-1][0]

    return {"edges": total_edges}


async def _count_sql_event_edges(store: Any) -> int:
    def _txn(txn: Any) -> int:
        txn.execute("SELECT COUNT(*) FROM event_edges")
        row = txn.fetchone()
        return int(row[0]) if row else 0

    return await store.db_pool.runInteraction("count_event_edges", _txn)


async def _count_mtxdb_event_edges(
    store: Any, namespace: str, batch_size: int = 1000
) -> int:
    total = 0
    last_event_id = ""

    while True:

        def _read(txn: Any) -> list[str]:
            txn.execute(
                "SELECT event_id FROM events WHERE event_id > ? "
                "ORDER BY event_id LIMIT ?",
                (last_event_id, batch_size),
            )
            return [row[0] for row in txn]

        event_ids = await store.db_pool.runInteraction(
            "count_mtxdb_edges_read_events", _read
        )
        if not event_ids:
            return total

        backward = get_event_edges_backward_batch(namespace, event_ids)
        total += sum(len(edges or ()) for edges in backward.values())
        last_event_id = event_ids[-1]


async def _count_event_edges(
    store: Any, backend: str, namespace: str
) -> tuple[int, int]:
    """Return ``(edges, events)`` for the given backend."""

    def _count_events(txn: Any) -> int:
        txn.execute("SELECT COUNT(*) FROM events")
        row = txn.fetchone()
        return int(row[0]) if row else 0

    events = await store.db_pool.runInteraction("count_event_rows", _count_events)
    if backend == "sql":
        return await _count_sql_event_edges(store), events
    return await _count_mtxdb_event_edges(store, namespace), events


async def migrate_event_edges(
    hs: HomeServer,
    *,
    target: str,
    source: str | None = None,
    force: bool = False,
    batch_size: int = 1000,
    mtxdb_path: str | None = None,
) -> dict[str, int | str]:
    """Move event edges between the SQL table and mtxdb.

    The operator must stop Synapse first. ``target`` must be the backend the
    process is configured to use after the migration. The stored marker is the
    default source; pass ``source`` to override a stale marker. The marker is
    flipped only after the copy is verified.
    """
    if target not in ("sql", "mtxdb"):
        raise ValueError(f"unknown event-edges backend: {target!r}")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    store = hs.get_datastores().main
    namespace = store._embedded_db_namespace
    stored = await _get_event_edges_backend(store)

    if source is None:
        source = stored
    if source not in ("sql", "mtxdb"):
        raise ValueError(f"unknown source event-edges backend: {source!r}")

    # mtxdb must be open for either side of the migration. Open it read-only
    # when it is only the source; the target side must be writable.
    if target == "mtxdb" or source == "mtxdb":
        if not open_embedded_event_edges_engine(hs):
            path = mtxdb_path or hs.config.database.embedded_db_path
            if not path:
                raise RuntimeError(
                    "mtxdb is not configured; set embedded_db.engine/path or "
                    "pass --mtxdb-path"
                )
            from synapse.storage.databases.embedded_engine import get_embedded_engine
            from synapse.storage.databases.main.embedded_common import (
                _set_engine_configured,
            )

            engine = get_embedded_engine("mtxdb")
            if target == "mtxdb":
                engine.open_client(path)
            else:
                engine.open_client_read_only(path)
            _set_engine_configured()

    if source == target:
        edges, events = await _count_event_edges(store, target, namespace)
        if edges == 0 and events > 1 and not force:
            raise RuntimeError(
                f"{target} has no event edges but the database has {events} events; "
                "refusing to adopt it. Pass --force if that is genuinely intended."
            )
        await _set_event_edges_backend(store, target)
        return {
            "source": source,
            "target": target,
            "edges": edges,
            "changed_events": 0,
            "already": 1,
        }

    source_edges, _source_events = await _count_event_edges(store, source, namespace)
    target_edges, _target_events = await _count_event_edges(store, target, namespace)
    if source_edges == 0 and target_edges > 0 and not force:
        raise RuntimeError(
            f"{source} has no event edges but {target} has {target_edges}; the "
            f"marker may be stale. Pass --from {target} to adopt the existing "
            f"data, or --force to overwrite it with an empty {source}."
        )

    if target == "mtxdb":
        result = await _migrate_sql_edges_to_mtxdb(store, namespace, batch_size)
        source_edges = await _count_sql_event_edges(store)
        if source_edges != result["edges"]:
            raise RuntimeError(
                "event-edges migration verification failed: "
                f"SQL has {source_edges} rows but {result['edges']} were reconciled"
            )
    else:
        result = await _migrate_mtxdb_edges_to_sql(store, namespace, batch_size)
        target_edges = await _count_sql_event_edges(store)
        if target_edges != result["edges"]:
            raise RuntimeError(
                "event-edges migration verification failed: "
                f"mtxdb had {result['edges']} rows but SQL has {target_edges}"
            )

    await _set_event_edges_backend(store, target)
    return {
        "source": source,
        "target": target,
        "edges": result["edges"],
        "changed_events": result.get("changed_events", 0),
        "already": 0,
    }


def put_event_edges_batch(
    namespace: str,
    rows: Iterable[EdgeRow],
    *,
    sync: bool = False,
) -> None:
    """Batch put event edges into mtxdb.

    `rows`: `(room_id, event_id, prev_event_id, is_state)`.
    """
    row_list = list(rows)
    if not row_list:
        return

    with mirror_timing("event_edges_put"):
        from synapse.synapse_rust.mtxdb_engine import event_edges_put

        _et = time.monotonic()
        # Under a shared WAL, `event_edges_put` runs the optimistic loop
        # `run_edge_occ` internally: it replays collection-version conflicts up
        # to `MAX_OCC_ATTEMPTS` and only then raises a retryable
        # `BlockingIOError`. Retry here rather than surfacing transient
        # contention to event persistence.
        _retry_on_contention(lambda: event_edges_put(namespace, row_list))
        elapsed = time.monotonic() - _et
        ffi_timing("ffi_event_edges_put", elapsed)
        ffi_count("event_edges_put_rows", len(row_list))
        ffi_batch_size("event_edges_put", len(row_list))

        if sync:
            sync_now(pools=[Pool.EVENT_DAG])


def delete_event_edges_batch(
    namespace: str,
    event_ids: list[str],
) -> None:
    """Tombstones backward edges for purged events in mtxdb.

    Purge and coalesced edge writes share an ordering window: a row queued for
    an event being purged, if it flushed after the tombstone, would re-write
    the purged event's backward edge (or put it back on its parents' forward
    lists) -- resurrection.  To close that race the purge barrier holds the
    namespace's flush lock across cancel -> drain -> tombstone so no in-flight
    or threshold flush interleaves, and drops any queued row that writes an
    edge FOR a purged event (those mirror the SQL `event_edges` rows the purge
    is deleting).  Rows queued for events that are merely referencing the
    purged ids remain queued for the normal coalescer.

    The purged ids are marked as "purging" for the duration of the barrier and
    promoted to permanent tombstones only after the FFI delete succeeds:
    `queue_edge_write` takes the same flush lock and drops rows matching either
    marker, so a transaction whose post-commit enqueue lands after the cancel
    step cannot resurrect the event, while a failed delete still lets repair
    writes through. `event_edges_put` appends against the post-delete forward
    list, so leaving unrelated rows queued cannot resurrect a purged child.
    """
    if not event_ids:
        return

    purged = set(event_ids)
    with lock_wait_timing(_namespace_flush_lock(namespace), "edge_namespace_flush"):
        now = time.monotonic()
        deadline = now + _EDGE_WRITE_TOMBSTONE_TTL

        # 1. Mark the purge in progress and cancel queued rows for the events
        #    being purged, under the queues lock so a concurrent enqueue is
        #    either cancelled here or filtered by the markers.  The permanent
        #    tombstone is deferred until the FFI delete succeeds.
        cancelled_rows: list[EdgeRow] = []
        with lock_wait_timing(_edge_write_queues_lock, "edge_queues"):
            _prune_tombstones_locked(namespace, now)
            _edge_write_purging.setdefault(namespace, set()).update(purged)
            q = _edge_write_queues.get(namespace)
            if q is not None:
                kept: list[EdgeRow] = []
                for row in q:
                    if row[1] in purged:
                        cancelled_rows.append(row)
                    else:
                        kept.append(row)
                if kept:
                    q[:] = kept
                else:
                    _edge_write_queues.pop(namespace, None)

        # 2. Delete the purged events' edges. Only promote the purging marker
        #    to a permanent tombstone once the delete has actually returned:
        #    if it fails, the stale edges are still there and a tombstone would
        #    suppress the repair writes that recover from the failure.
        try:
            with mirror_timing("event_edges_delete"):
                from synapse.synapse_rust.mtxdb_engine import event_edges_delete

                _et = time.monotonic()
                phase_timings = event_edges_delete(namespace, event_ids)
                elapsed = time.monotonic() - _et
                ffi_timing("ffi_event_edges_delete", elapsed)
                ffi_timing(
                    "ffi_event_edges_delete_lock_wait", phase_timings["lock_wait"]
                )
                ffi_timing(
                    "ffi_event_edges_delete_locator_read",
                    phase_timings["locator_read"],
                )
                # Delete no longer reads or rewrites any parent's forward
                # list (see embedded_edges.rs's `event_edges_delete` doc
                # comment) -- there is no forward_read/mutate_write/
                # forward_nodes phase left to report. backward_read was
                # renamed to backward_tombstone_write: it always was a
                # write, never a read.
                ffi_timing(
                    "ffi_event_edges_delete_backward_tombstone_write",
                    phase_timings["backward_tombstone_write"],
                )
                # closure_duration times all work inside py.detach (the three
                # phases above plus untimed glue between them); detached_duration
                # times py.detach itself from outside. Their difference isolates
                # GIL-reacquisition/return overhead from mtxdb/Rust work, and
                # comparing detached_duration to `elapsed` above isolates
                # anything left in the FFI boundary itself -- see
                # embedded_edges.rs's `event_edges_delete` doc comment.
                ffi_timing(
                    "ffi_event_edges_delete_closure_duration",
                    phase_timings["closure_duration"],
                )
                ffi_timing(
                    "ffi_event_edges_delete_detached_duration",
                    phase_timings["detached_duration"],
                )
                # The Rust dict is typed float | int; the count is an integer.
                room_count = int(phase_timings["rooms"])
                ffi_count("event_edges_delete_rooms", room_count)
                # Per-call batch shape, so a slow delete can be matched to the
                # event/room counts that produced it.
                ffi_batch_size("event_edges_delete_events", len(event_ids))
                ffi_batch_size("event_edges_delete_rooms", room_count)
                ffi_count("event_edges_deleted", len(event_ids))
        except Exception:
            with lock_wait_timing(_edge_write_queues_lock, "edge_queues"):
                purging = _edge_write_purging.get(namespace)
                if purging is not None:
                    purging.difference_update(purged)
                    if not purging:
                        _edge_write_purging.pop(namespace, None)
                # Restore cancelled rows so the stale edges remain reachable
                # for repair/backfill writes -- a failed delete leaves the
                # original data in mtxdb and a tombstone would suppress the
                # repair writes that recover from it.
                if cancelled_rows:
                    restored = _edge_write_queues.setdefault(namespace, [])
                    restored[0:0] = cancelled_rows
            # Outside the queues lock to avoid lock-order surprises.
            mark_dirty(Pool.EVENT_DAG)
            raise

        with lock_wait_timing(_edge_write_queues_lock, "edge_queues"):
            purging = _edge_write_purging.get(namespace)
            if purging is not None:
                purging.difference_update(purged)
                if not purging:
                    _edge_write_purging.pop(namespace, None)
            tombstones = _edge_write_tombstones.setdefault(namespace, {})
            for event_id in purged:
                tombstones[event_id] = deadline

        # Sync the deletion itself without forcing unrelated queued edge rows
        # through the FFI. The coalescer will drain those rows later.
        sync_event_dag_now()


# Both forward/backward reads now go through mtxdb's `get_read_committed`
# (embedded_edges.rs), which -- unlike the plain `get_many` path they used
# before -- surfaces transient journal/checkpoint contention as a retryable
# `BlockingIOError` (see `map_read_storage_error`, mtxdb_syn.rs), the same
# error class `database.py`'s own `runInteraction` retry loop already catches
# for a whole SQL transaction attempt (`database.py:1291`). A caller inside
# `runInteraction` (e.g. `is_event_next_to_forward_gap`) is retried for free
# by that loop; a caller outside one (e.g. `get_successor_events`, a bare
# mtxdb read with no enclosing transaction) is not, so it must retry the FFI
# call itself or the exception is simply unhandled. Retrying here, once, in
# the module that owns the FFI boundary, covers every caller instead of
# duplicating a retry loop at each call site.
#
# Same attempt count as `database.py`'s own retry loop
# (`MAX_NUMBER_OF_ATTEMPTS = 5`), no backoff between attempts, matching that
# convention for the same underlying contention class.
_MAX_READ_CONTENTION_ATTEMPTS = 5


def _retry_on_contention(call: "Callable[[], _T]") -> "_T":
    """Retry `call` on transient mtxdb contention, re-raising the last error.

    Reads may raise this for journal/checkpoint contention. Edge writes may
    also raise it after their native collection-OCC retry budget is exhausted.
    """
    for attempt in range(1, _MAX_READ_CONTENTION_ATTEMPTS + 1):
        try:
            return call()
        except BlockingIOError:
            ffi_count("event_edges_contention_retries", 1)
            if attempt == _MAX_READ_CONTENTION_ATTEMPTS:
                ffi_count("event_edges_contention_exhausted", 1)
                raise
    raise AssertionError("unreachable: loop always returns or raises")


def get_event_edges_backward_batch(
    namespace: str,
    event_ids: list[str],
) -> dict[str, list[tuple[str, bool]] | None]:
    """Reads backward edges for `event_ids`.

    Returns `event_id -> [(prev_event_id, is_state)]` or `None` if missing.
    """
    if not event_ids:
        return {}

    with mirror_timing("event_edges_get_backward"):
        from synapse.synapse_rust.mtxdb_engine import event_edges_get_backward

        _et = time.monotonic()
        results = _retry_on_contention(
            lambda: event_edges_get_backward(namespace, event_ids)
        )
        elapsed = time.monotonic() - _et
        ffi_timing("ffi_event_edges_get_backward", elapsed)
        ffi_batch_size("event_edges_get_backward", len(event_ids))

        return dict(results)


def get_event_edges_forward_batch(
    namespace: str,
    prev_event_ids: list[str],
) -> dict[str, list[str] | None]:
    """Read forward child edges for `prev_event_ids` from the authoritative store."""
    if not prev_event_ids:
        return {}
    with mirror_timing("event_edges_get_forward"):
        from synapse.synapse_rust.mtxdb_engine import event_edges_get_forward

        results = _retry_on_contention(
            lambda: event_edges_get_forward(namespace, prev_event_ids)
        )
        ffi_batch_size("event_edges_get_forward", len(prev_event_ids))
        return dict(results)
