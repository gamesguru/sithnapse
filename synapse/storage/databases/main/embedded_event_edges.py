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

"""Mirrors `event_edges` (backward and forward event DAG edges) into the
embedded mtxdb event_dag keyspace.

Design:
- Backward edges: `event_id -> [(prev_event_id, is_state)]`. Cryptographically
  immutable once an event is created.
- Forward edges: `prev_event_id -> [child_event_id]`. Deduplicated and appended
  as new children arrive under RMW_LOCK.
- Locators are primarily owned by `embedded_event_json`; `event_edges_put`
  re-publishes them for every event a row touches (see the Rust doc), so
  repaired/backfilled edges for legacy events still resolve.

Reads are mtxdb-first with SQL fallback:
  mtxdb lookup
    ├─ complete -> return mtxdb result
    └─ missing/incomplete -> query SQL, repair mtxdb, return SQL result

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
from typing import TYPE_CHECKING, Any, Callable, Iterable, TypeVar, cast

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


def record_edge_index_deletes_txn(db_pool: Any, txn: Any, event_ids: list[str]) -> None:
    """Record edge deletions before SQL purge removes the source rows.

    The mutation and its source-version advance run in the caller's SQL
    transaction, so an outbox consumer can never observe a deletion that was
    rolled back or miss one that committed.
    """
    if not event_ids:
        return

    placeholders = ", ".join("?" for _ in event_ids)
    txn.execute(
        f"""
        SELECT ev.room_id, ee.event_id, ee.prev_event_id
        FROM event_edges AS ee
        JOIN events AS ev ON ev.event_id = ee.event_id
        WHERE ee.event_id IN ({placeholders})
        """,
        event_ids,
    )
    rows_by_room: dict[str, list[tuple[str, str]]] = collections.defaultdict(list)
    for room_id, event_id, prev_event_id in txn:
        rows_by_room[room_id].append((event_id, prev_event_id))

    for room_id, rows in rows_by_room.items():
        rows = list(dict.fromkeys(rows))
        txn.execute(
            "SELECT source_version FROM room_edge_source_version WHERE room_id = ?",
            (room_id,),
        )
        row = txn.fetchone()
        source_version = (int(row[0]) if row is not None else 0) + 1
        db_pool.simple_upsert_txn(
            txn,
            table="room_edge_source_version",
            keyvalues={"room_id": room_id},
            values={"source_version": source_version},
        )
        db_pool.simple_insert_many_txn(
            txn,
            table="edge_index_outbox",
            keys=(
                "room_id",
                "source_version",
                "event_id",
                "prev_event_id",
                "operation",
            ),
            values=[
                (room_id, source_version, event_id, prev_event_id, "delete")
                for event_id, prev_event_id in rows
            ],
        )


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


# Flip to True only in the same release that actually removes the SQL
# ``event_edges`` INSERT (``_handle_mult_prev_events``,
# ``synapse/storage/databases/main/events.py``) and the SQL fallback reads in
# ``get_successor_events``/``is_event_next_to_forward_gap``. Until then this
# stays False and `check_event_edges_migration_complete` is a deliberate
# no-op -- there is nothing to protect yet, since the SQL fallback still
# covers every server regardless of migration progress. See
# `res/docs/2026-09-28-event-edges-sql-removal-plan.md`'s "Blocker 1":
# a server that upgrades to the insert-dropping release while still
# mid-migration would have rows that exist only in mtxdb, with nothing left
# to fall back to -- this is the hard precondition that catches that,
# modeled on `prepare_database.py`'s worker schema-version check
# (`UpgradeDatabaseException`/`OUTDATED_SCHEMA_ON_WORKER_ERROR`), but for the
# events-stream writer specifically rather than every worker, and checked
# after the async datastore layer is up (`has_completed_background_update`
# needs it) rather than during synchronous schema prep.
EVENT_EDGES_SQL_INSERT_REMOVED = False


class EventEdgesMigrationIncompleteError(Exception):
    """Raised by `check_event_edges_migration_complete` when this process is
    the events-stream writer, the SQL `event_edges` insert has been removed
    (`EVENT_EDGES_SQL_INSERT_REMOVED = True`), and
    `event_edges_migrate_mtxdb` has not finished on this server.

    Deliberately a plain exception, not a process exit: the caller (expected
    to be a fatal startup check, e.g. `synapse.app._base.start`) decides how
    to fail the process. Keeping the decision here pure and the exit
    mechanism at the call site is what makes this testable without a real
    `sys.exit` in the test run.
    """


async def check_event_edges_migration_complete(hs: HomeServer) -> None:
    """Fatal precondition for the release that removes the SQL `event_edges`
    insert: refuse to let this process act as the events-stream writer
    unless `event_edges_migrate_mtxdb` has already finished on this server.

    A no-op today (`EVENT_EDGES_SQL_INSERT_REMOVED` is False) and a no-op on
    every process that isn't the events-stream writer -- workers don't write
    `event_edges` themselves, and a schema-version mismatch on an outdated
    worker is already caught elsewhere
    (`prepare_database.py`'s `OUTDATED_SCHEMA_ON_WORKER_ERROR`).

    Raises:
        EventEdgesMigrationIncompleteError: if this process is the writer,
            the SQL insert has been removed, and the migration is incomplete.
    """
    if not EVENT_EDGES_SQL_INSERT_REMOVED:
        return
    if not embedded_event_edges_is_writable(hs):
        return

    store = hs.get_datastores().main
    if await store.db_pool.updates.has_completed_background_update(
        "event_edges_migrate_mtxdb"
    ):
        return

    raise EventEdgesMigrationIncompleteError(
        "This server has not finished migrating event_edges rows into the "
        "embedded mtxdb engine (background update 'event_edges_migrate_mtxdb'), "
        "but this release no longer writes event_edges to SQL. Starting "
        "would leave rows written from this point on with no SQL fallback "
        "and no proof mtxdb already has everything written before it.\n\n"
        "Run the migration to completion before starting this version:\n"
        "    update_synapse_database --run-background-updates"
    )


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
        event_edges_put(namespace, row_list)
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
    """Retry `call` up to `_MAX_READ_CONTENTION_ATTEMPTS` times on
    `BlockingIOError` (transient mtxdb journal/checkpoint contention from a
    `get_read_committed` read), re-raising the last attempt's error."""
    for attempt in range(1, _MAX_READ_CONTENTION_ATTEMPTS + 1):
        try:
            return call()
        except BlockingIOError:
            if attempt == _MAX_READ_CONTENTION_ATTEMPTS:
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
    room_id_or_prev_event_ids: str | list[str],
    expected_source_version: int | None = None,
    prev_event_ids: list[str] | None = None,
) -> dict[str, list[str] | None]:
    """Reads forward child edges for `prev_event_ids`.

    Returns `prev_event_id -> [child_event_id]` or `None` if missing.
    """
    if prev_event_ids is None:
        # Keep the established helper contract for benchmarks and tests. The
        # gated path below is used by production callers that have room and
        # source-version context.
        legacy_prev_event_ids = cast(list[str], room_id_or_prev_event_ids)
        if not legacy_prev_event_ids:
            return {}
        with mirror_timing("event_edges_get_forward"):
            from synapse.synapse_rust.mtxdb_engine import event_edges_get_forward

            results = _retry_on_contention(
                lambda: event_edges_get_forward(namespace, legacy_prev_event_ids)
            )
            return dict(results)

    if (
        not isinstance(room_id_or_prev_event_ids, str)
        or expected_source_version is None
    ):
        raise TypeError("room_id and expected_source_version are required together")
    room_id = room_id_or_prev_event_ids
    if not prev_event_ids:
        return {}

    with mirror_timing("event_edges_get_forward"):
        from synapse.synapse_rust.mtxdb_engine import event_edges_get_forward_gated

        _et = time.monotonic()
        gated_results: Any = _retry_on_contention(
            lambda: event_edges_get_forward_gated(
                namespace, room_id, expected_source_version, prev_event_ids
            )
        )
        elapsed = time.monotonic() - _et
        ffi_timing("ffi_event_edges_get_forward", elapsed)
        ffi_batch_size("event_edges_get_forward", len(prev_event_ids))

        status, *payload = gated_results
        if status == "version_mismatch":
            ffi_count("event_edges_version_mismatches", 1)
            return {}
        if status != "hit":
            raise RuntimeError(f"unexpected forward edge result: {status!r}")
        return dict(cast(list[tuple[str, list[str] | None]], payload[0]))


async def drain_edge_index_outbox(
    store: Any, limit: int = 256, *, namespace: str = "synapse"
) -> bool:
    """Publish one room's durable edge mutations into the active FWD generation.

    SQL rows are acknowledged only after Rust commits both the FWD updates and
    publication watermark in one mtxdb transaction. The worker lock is held
    before the batch is re-read, preventing stale snapshots from racing a
    second drainer or a generation swap.
    """
    if limit <= 0:
        return False

    def _room(txn: Any) -> str | None:
        txn.execute(
            "SELECT room_id FROM edge_index_outbox "
            "ORDER BY room_id, source_version LIMIT 1"
        )
        row = txn.fetchone()
        return None if row is None else str(row[0])

    room_id = await store.db_pool.runInteraction("edge_outbox_room", _room)
    if room_id is None:
        return False

    locks = store.hs.get_worker_locks_handler()
    async with locks.acquire_lock("embedded_edge_index_outbox", room_id):
        from synapse.synapse_rust.mtxdb_engine import (
            event_edges_apply_forward_outbox,
            room_forward_meta_get,
        )

        meta = _retry_on_contention(lambda: room_forward_meta_get(room_id))
        generation, published = meta if meta is not None else (0, 0)

        def _rows(txn: Any) -> list[tuple[str, int, str, str]]:
            txn.execute(
                "SELECT event_id, source_version, prev_event_id, operation "
                "FROM edge_index_outbox "
                "WHERE room_id = ? AND source_version > ? "
                "ORDER BY source_version, event_id, prev_event_id, operation "
                "LIMIT ?",
                (room_id, published, limit),
            )
            rows = [(str(a), int(b), str(c), str(d)) for a, b, c, d in txn]
            if not rows:
                return []

            # Never publish a watermark while only part of a source-version
            # group has been applied: several edge rows intentionally share
            # one room version. Complete the final group even if it takes the
            # batch over `limit`.
            max_version = max(row[1] for row in rows)
            txn.execute(
                "SELECT event_id, source_version, prev_event_id, operation "
                "FROM edge_index_outbox "
                "WHERE room_id = ? AND source_version > ? "
                "AND source_version <= ? "
                "ORDER BY source_version, event_id, prev_event_id, operation",
                (room_id, published, max_version),
            )
            return [(str(a), int(b), str(c), str(d)) for a, b, c, d in txn]

        rows = await store.db_pool.runInteraction("edge_outbox_rows", _rows)

        def _ack_through(txn: Any, published_version: int) -> int:
            now_ms = int(time.time() * 1000)
            txn.execute(
                "SELECT MIN(last_replayed_source_version) "
                "FROM room_edge_rebuild_checkpoints "
                "WHERE room_id = ? AND lease_expires_at_ms >= ?",
                (room_id, now_ms),
            )
            row = txn.fetchone()
            safe_version = (
                published_version
                if row is None or row[0] is None
                else min(published_version, int(row[0]))
            )
            txn.execute(
                "DELETE FROM edge_index_outbox "
                "WHERE room_id = ? AND source_version <= ?",
                (room_id, safe_version),
            )
            return txn.rowcount

        if not rows:
            # A process can crash after the mtxdb commit and before the SQL
            # acknowledgement. The watermark makes those rows already
            # published; remove them so they do not permanently block the
            # room's queue.
            deleted = await store.db_pool.runInteraction(
                "edge_outbox_ack_stale",
                _ack_through,
                published,
            )
            return bool(deleted)

        max_version = max(row[1] for row in rows)
        ffi_rows = [
            (event_id, prev_id, operation) for event_id, _, prev_id, operation in rows
        ]
        _retry_on_contention(
            lambda: event_edges_apply_forward_outbox(
                namespace, room_id, generation, max_version, ffi_rows
            )
        )

        await store.db_pool.runInteraction("edge_outbox_ack", _ack_through, max_version)
        ffi_count("event_edges_outbox_rows", len(rows))
        return True
