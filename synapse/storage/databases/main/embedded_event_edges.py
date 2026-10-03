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
import uuid
from typing import TYPE_CHECKING, Any, Callable, Iterable, TypeVar, cast

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


class ForwardEdgeMap(dict[str, list[str] | None]):
    """Forward edges plus whether the embedded index was stale."""

    def __init__(self, *args: Any, stale: bool = False, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.stale = stale


def bump_room_edge_source_version(txn: Any, room_id: str) -> int:
    """Atomically increment and return a room's edge source version.

    Both the insert and delete paths advance the version inside the caller's
    SQL transaction, so the outbox rows and the version they are tagged with
    commit or roll back together. The increment must be atomic: a read followed
    by a write lets two concurrent transactions observe the same version and
    both publish it, duplicating a version in the stream and breaking the
    strictly-increasing replay cursor. `UPDATE ... RETURNING` (via an upsert on
    first use) serializes writers on the row lock instead.
    """
    txn.execute(
        """
        INSERT INTO room_edge_source_version (room_id, source_version)
        VALUES (?, 1)
        ON CONFLICT (room_id) DO UPDATE
        SET source_version = room_edge_source_version.source_version + 1
        RETURNING source_version
        """,
        (room_id,),
    )
    row = txn.fetchone()
    if row is None:
        raise RuntimeError("room_edge_source_version upsert returned no row")
    return int(row[0])


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
        source_version = bump_room_edge_source_version(txn, room_id)
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


def record_edge_index_repairs_txn(
    db_pool: Any,
    txn: Any,
    room_id: str,
    prev_event_id: str,
    event_ids: list[str],
) -> int | None:
    """Add a SQL-fallback repair to the room's ordered forward-index outbox.

    The repair gets a fresh room source version, just like a normal edge
    mutation. The outbox drainer can then apply it together with every earlier
    pending room mutation before advancing the FWD completeness watermark.
    """
    event_ids = list(dict.fromkeys(event_ids))
    if not event_ids:
        return None

    source_versions = record_edge_index_inserts_txn(
        db_pool, txn, {room_id: [(event_id, prev_event_id) for event_id in event_ids]}
    )
    return source_versions.get(room_id)


def record_edge_index_inserts_txn(
    db_pool: Any,
    txn: Any,
    rows_by_room: dict[str, list[tuple[str, str]]],
) -> dict[str, int]:
    """Record forward-index inserts in the ordered outbox."""
    source_versions = {}
    for room_id, rows in rows_by_room.items():
        rows = list(dict.fromkeys(rows))
        if not rows:
            continue
        source_version = bump_room_edge_source_version(txn, room_id)
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
                (room_id, source_version, event_id, prev_event_id, "insert")
                for event_id, prev_event_id in rows
            ],
        )
        source_versions[room_id] = source_version
    return source_versions


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


async def event_edges_fwd_is_authoritative(store: Any) -> bool:
    """Whether embedded MTXDB event edges are enabled for this store.

    Operators are responsible for importing and validating existing edges
    before enabling MTXDB. Once enabled, MTXDB is authoritative.
    """
    return bool(getattr(store, "_embedded_event_edges_enabled", False))


class EventEdgesMigrationIncompleteError(Exception):
    """Raised by `check_event_edges_migration_complete` when this process is
    the events-stream writer, the SQL `event_edges` insert has been removed
    (`EVENT_EDGES_SQL_INSERT_REMOVED = True`), and embedded event edges are
    not enabled for this store.

    Deliberately a plain exception, not a process exit: the caller (expected
    to be a fatal startup check, e.g. `synapse.app._base.start`) decides how
    to fail the process. Keeping the decision here pure and the exit
    mechanism at the call site is what makes this testable without a real
    `sys.exit` in the test run.
    """


async def check_event_edges_migration_complete(hs: HomeServer) -> None:
    """Fatal precondition for the release that removes the SQL `event_edges`
    insert: refuse to let this process act as the events-stream writer
    unless embedded event edges are enabled (and therefore authoritative) for
    this store.

    A no-op today (`EVENT_EDGES_SQL_INSERT_REMOVED` is False) and a no-op on
    every process that isn't the events-stream writer -- workers don't write
    `event_edges` themselves, and a schema-version mismatch on an outdated
    worker is already caught elsewhere
    (`prepare_database.py`'s `OUTDATED_SCHEMA_ON_WORKER_ERROR`).

    Raises:
        EventEdgesMigrationIncompleteError: if this process is the writer,
        the SQL insert has been removed, and embedded event edges are not
        enabled.
    """
    if not EVENT_EDGES_SQL_INSERT_REMOVED:
        return
    if not embedded_event_edges_is_writable(hs):
        return

    if await event_edges_fwd_is_authoritative(hs.get_datastores().main):
        return

    raise EventEdgesMigrationIncompleteError(
        "This server has not enabled authoritative embedded event edges, but "
        "this release no longer writes event_edges to SQL. Starting would "
        "leave rows written from this point on with no SQL fallback and no "
        "proof that mtxdb contains a complete forward index.\n\n"
        "Import and validate existing edges, then enable the embedded event "
        "edges backend before starting this version."
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
        # Under a shared WAL, `event_edges_put` runs the optimistic loop
        # `run_edge_occ` internally: it replays collection-version conflicts up
        # to `MAX_OCC_ATTEMPTS` and only then raises a retryable
        # `BlockingIOError`, which nothing else here would catch -- unlike the
        # forward-outbox and generation-delta writes, which already retry. Do
        # the same for event persistence rather than surfacing contention.
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
    room_id_or_prev_event_ids: str | list[str],
    expected_source_version: int | None = None,
    prev_event_ids: list[str] | None = None,
) -> ForwardEdgeMap:
    """Reads forward child edges for `prev_event_ids`.

    ``stale`` is set when the embedded index cannot satisfy the requested
    source version. A missing key in a non-stale result means that the queried
    event is not present in the index.
    """
    if prev_event_ids is None:
        # Keep the established helper contract for benchmarks and tests. The
        # gated path below is used by production callers that have room and
        # source-version context.
        legacy_prev_event_ids = cast(list[str], room_id_or_prev_event_ids)
        if not legacy_prev_event_ids:
            return ForwardEdgeMap()
        with mirror_timing("event_edges_get_forward"):
            from synapse.synapse_rust.mtxdb_engine import event_edges_get_forward

            results = _retry_on_contention(
                lambda: event_edges_get_forward(namespace, legacy_prev_event_ids)
            )
            return ForwardEdgeMap(results)

    if (
        not isinstance(room_id_or_prev_event_ids, str)
        or expected_source_version is None
    ):
        raise TypeError("room_id and expected_source_version are required together")
    room_id = room_id_or_prev_event_ids
    if not prev_event_ids:
        return ForwardEdgeMap()

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
        logger.debug(
            "get_event_edges_forward_batch: room_id=%s expected_source_version=%s count=%s status=%s",
            room_id,
            expected_source_version,
            len(prev_event_ids),
            status,
        )
        if status == "version_mismatch":
            ffi_count("event_edges_version_mismatches", 1)
            return ForwardEdgeMap(stale=True)
        if status != "hit":
            raise RuntimeError(f"unexpected forward edge result: {status!r}")
        return ForwardEdgeMap(cast(list[tuple[str, list[str] | None]], payload[0]))


async def drain_edge_index_outbox(
    store: Any,
    limit: int = 256,
    *,
    namespace: str = "synapse",
    room_id: str | None = None,
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
        if room_id is None:
            txn.execute(
                "SELECT room_id FROM edge_index_outbox "
                "ORDER BY room_id, source_version LIMIT 1"
            )
        else:
            txn.execute(
                "SELECT room_id FROM edge_index_outbox "
                "WHERE room_id = ? ORDER BY source_version LIMIT 1",
                (room_id,),
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


async def repair_edge_index_from_sql(
    store: Any,
    namespace: str,
    room_id: str,
    prev_event_id: str,
    event_ids: list[str],
) -> None:
    """Repair a SQL-fallback miss through the ordered FWD outbox.

    The PREV mirror is still queued for compatibility, while the repair is
    recorded as a fresh SQL outbox version. Draining that room applies all
    earlier pending changes before it advances the FWD completeness watermark;
    a parent-only repair must never claim the room-wide index is complete.
    """

    def _enqueue_repair(txn: Any) -> int | None:
        clause, clause_args = make_in_list_sql_clause(
            txn.database_engine, "event_id", event_ids
        )
        txn.execute(
            f"SELECT event_id FROM edge_index_outbox "
            f"WHERE room_id = ? AND prev_event_id = ? "
            f"AND operation = 'insert' AND {clause}",
            (room_id, prev_event_id, *clause_args),
        )
        pending = {str(row[0]) for row in txn}
        event_ids_to_repair = [
            event_id for event_id in event_ids if event_id not in pending
        ]
        if not event_ids_to_repair:
            return None
        return record_edge_index_repairs_txn(
            store.db_pool, txn, room_id, prev_event_id, event_ids_to_repair
        )

    await store.db_pool.runInteraction("record_edge_index_repair", _enqueue_repair)
    rows = await store.db_pool.simple_select_list(
        table="event_edges",
        keyvalues={"prev_event_id": prev_event_id},
        retcols=("event_id", "is_state"),
        desc="repair_edge_index_from_sql",
    )
    # SQLite stores BOOLEAN as 0/1, and `event_edges_put` wants a real bool.
    queue_edge_write(
        namespace,
        [
            (room_id, event_id, prev_event_id, bool(is_state))
            for event_id, is_state in rows
        ],
    )
    # The ordinary worker continues draining other rooms. Target this room so
    # a pending or newly-created repair can make the next gated read an
    # embedded hit now.
    # Any incomplete backlog remains durable in SQL and is picked up by the
    # regular worker; the current request already has its correct SQL result.
    await drain_edge_index_outbox(store, namespace=namespace, room_id=room_id)


async def acquire_rebuild_checkpoint(
    store: Any,
    room_id: str,
    start_source_version: int,
    *,
    rebuild_id: str | None = None,
    lease_duration_ms: int = 60_000,
) -> str:
    """Register a durable outbox consumer lease for a room rebuild."""
    rebuild_id = rebuild_id or str(uuid.uuid4())
    expires_at = int(time.time() * 1000) + lease_duration_ms

    def _insert(txn: Any) -> None:
        txn.execute(
            "INSERT INTO room_edge_rebuild_checkpoints "
            "(room_id, rebuild_id, start_source_version, "
            "last_replayed_source_version, lease_expires_at_ms) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                room_id,
                rebuild_id,
                start_source_version,
                start_source_version,
                expires_at,
            ),
        )

    await store.db_pool.runInteraction("acquire_rebuild_checkpoint", _insert)
    return rebuild_id


async def heartbeat_rebuild_checkpoint(
    store: Any,
    room_id: str,
    rebuild_id: str,
    replayed_source_version: int,
    *,
    lease_duration_ms: int = 60_000,
) -> None:
    """Advance a rebuild lease monotonically and extend its expiry."""
    expires_at = int(time.time() * 1000) + lease_duration_ms

    def _update(txn: Any) -> None:
        txn.execute(
            "UPDATE room_edge_rebuild_checkpoints "
            "SET last_replayed_source_version = CASE "
            "WHEN last_replayed_source_version < ? THEN ? "
            "ELSE last_replayed_source_version END, "
            "lease_expires_at_ms = ? "
            "WHERE room_id = ? AND rebuild_id = ?",
            (
                replayed_source_version,
                replayed_source_version,
                expires_at,
                room_id,
                rebuild_id,
            ),
        )
        if txn.rowcount != 1:
            raise RuntimeError("edge rebuild checkpoint disappeared")

    await store.db_pool.runInteraction("heartbeat_rebuild_checkpoint", _update)


async def release_rebuild_checkpoint(store: Any, room_id: str, rebuild_id: str) -> None:
    """Release a rebuild lease; the next drain can reclaim retained rows."""

    def _delete(txn: Any) -> None:
        txn.execute(
            "DELETE FROM room_edge_rebuild_checkpoints "
            "WHERE room_id = ? AND rebuild_id = ?",
            (room_id, rebuild_id),
        )

    await store.db_pool.runInteraction("release_rebuild_checkpoint", _delete)


async def enqueue_retired_generation(store: Any, room_id: str, generation: int) -> None:
    """Record a generation for deferred GC; do not delete it yet."""
    retired_at_ms = int(time.time() * 1000)

    def _insert(txn: Any) -> None:
        txn.execute(
            "INSERT INTO room_edge_retired_generations "
            "(room_id, generation, retired_at_ms) VALUES (?, ?, ?) "
            "ON CONFLICT (room_id, generation) DO NOTHING",
            (room_id, generation, retired_at_ms),
        )

    await store.db_pool.runInteraction("enqueue_retired_edge_generation", _insert)


async def gc_retired_forward_generations(
    store: Any,
    *,
    grace_period_ms: int = 600_000,
    limit: int = 32,
) -> bool:
    """Drop retired generations after the configured reader grace period."""
    cutoff_ms = int(time.time() * 1000) - grace_period_ms

    def _candidates(txn: Any) -> list[tuple[str, int]]:
        txn.execute(
            "SELECT room_id, generation FROM room_edge_retired_generations "
            "WHERE retired_at_ms <= ? ORDER BY retired_at_ms LIMIT ?",
            (cutoff_ms, limit),
        )
        return [(str(room_id), int(generation)) for room_id, generation in txn]

    candidates = await store.db_pool.runInteraction(
        "fetch_retired_edge_generations", _candidates
    )
    if not candidates:
        return False

    from synapse.synapse_rust.mtxdb_engine import (
        event_edges_drop_generation,
        room_forward_meta_get,
    )

    did_work = False
    locks = store.hs.get_worker_locks_handler()
    for room_id, retired_generation in candidates:
        async with locks.acquire_lock("embedded_edge_index_outbox", room_id):
            meta = _retry_on_contention(lambda: room_forward_meta_get(room_id))
            if meta is None or meta[0] <= retired_generation:
                continue

            _retry_on_contention(
                lambda: event_edges_drop_generation(room_id, retired_generation)
            )

            def _ack(txn: Any) -> None:
                txn.execute(
                    "DELETE FROM room_edge_retired_generations "
                    "WHERE room_id = ? AND generation = ?",
                    (room_id, retired_generation),
                )

            await store.db_pool.runInteraction("ack_retired_edge_generation", _ack)
            did_work = True

    return did_work


async def rebuild_room_forward_index(
    store: Any,
    room_id: str,
    *,
    batch_size: int = 1000,
    namespace: str = "synapse",
) -> bool:
    """Build and publish a complete forward index generation for one room."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    from synapse.synapse_rust.mtxdb_engine import (
        event_edges_apply_forward_generation_delta,
        event_edges_build_generation_batch,
        event_edges_reset_generation,
        room_forward_meta_get,
        room_forward_meta_swap,
    )

    locks = store.hs.get_worker_locks_handler()
    # Register at zero while taking the initial snapshot version. This closes
    # the window in which a drainer could truncate the stream before the lease
    # exists; the checkpoint is advanced to the snapshot version afterwards.
    async with locks.acquire_lock("embedded_edge_index_outbox", room_id):
        meta = _retry_on_contention(lambda: room_forward_meta_get(room_id))
        active_generation = meta[0] if meta is not None else 0
        rebuild_id = await acquire_rebuild_checkpoint(store, room_id, 0)
        try:

            def _source_version(txn: Any) -> int:
                txn.execute(
                    "SELECT source_version FROM room_edge_source_version "
                    "WHERE room_id = ?",
                    (room_id,),
                )
                row = txn.fetchone()
                return int(row[0]) if row else 0

            start_version = await store.db_pool.runInteraction(
                "get_rebuild_source_version", _source_version
            )
            await heartbeat_rebuild_checkpoint(
                store, room_id, rebuild_id, start_version
            )
        except BaseException:
            await release_rebuild_checkpoint(store, room_id, rebuild_id)
            raise

    target_generation = active_generation + 1
    last_prev_id = ""

    try:
        _retry_on_contention(
            lambda: event_edges_reset_generation(room_id, target_generation)
        )
        # Select parent keys first, then fetch every child for those parents.
        # This prevents a single parent's adjacency list being split across
        # build batches and overwritten by the next batch.
        while True:

            def _parent_batch(txn: Any) -> list[str]:
                txn.execute(
                    "SELECT DISTINCT ee.prev_event_id "
                    "FROM event_edges AS ee JOIN events AS ev "
                    "ON ev.event_id = ee.event_id "
                    "WHERE ev.room_id = ? AND ee.prev_event_id > ? "
                    "ORDER BY ee.prev_event_id LIMIT ?",
                    (room_id, last_prev_id, batch_size),
                )
                return [str(row[0]) for row in txn]

            parents = await store.db_pool.runInteraction(
                "fetch_rebuild_parents", _parent_batch
            )
            if not parents:
                break

            first_parent, last_parent = parents[0], parents[-1]

            def _edge_batch(txn: Any) -> list[tuple[str, str]]:
                txn.execute(
                    "SELECT ee.prev_event_id, ee.event_id "
                    "FROM event_edges AS ee JOIN events AS ev "
                    "ON ev.event_id = ee.event_id "
                    "WHERE ev.room_id = ? AND ee.prev_event_id >= ? "
                    "AND ee.prev_event_id <= ? "
                    "ORDER BY ee.prev_event_id, ee.event_id",
                    (room_id, first_parent, last_parent),
                )
                return [(str(prev), str(child)) for prev, child in txn]

            raw_edges = await store.db_pool.runInteraction(
                "fetch_rebuild_edges", _edge_batch
            )
            grouped: dict[str, list[str]] = collections.defaultdict(list)
            for prev_id, child_id in raw_edges:
                grouped[prev_id].append(child_id)
            _retry_on_contention(
                lambda: event_edges_build_generation_batch(
                    namespace, room_id, target_generation, list(grouped.items())
                )
            )
            last_prev_id = last_parent

        last_replayed = start_version
        while True:

            def _outbox_batch(txn: Any) -> list[tuple[str, int, str, str]]:
                txn.execute(
                    "SELECT event_id, source_version, prev_event_id, operation "
                    "FROM edge_index_outbox WHERE room_id = ? "
                    "AND source_version > ? ORDER BY source_version, event_id "
                    "LIMIT ?",
                    (room_id, last_replayed, batch_size),
                )
                rows = [(str(a), int(b), str(c), str(d)) for a, b, c, d in txn]
                if not rows:
                    return []
                max_version = max(row[1] for row in rows)
                txn.execute(
                    "SELECT event_id, source_version, prev_event_id, operation "
                    "FROM edge_index_outbox WHERE room_id = ? "
                    "AND source_version > ? AND source_version <= ? "
                    "ORDER BY source_version, event_id, prev_event_id, operation",
                    (room_id, last_replayed, max_version),
                )
                return [(str(a), int(b), str(c), str(d)) for a, b, c, d in txn]

            rows = await store.db_pool.runInteraction(
                "fetch_rebuild_outbox", _outbox_batch
            )
            if not rows:
                break
            max_version = max(row[1] for row in rows)
            _retry_on_contention(
                lambda: event_edges_apply_forward_generation_delta(
                    namespace,
                    room_id,
                    target_generation,
                    [(row[0], row[2], row[3]) for row in rows],
                )
            )
            last_replayed = max_version
            await heartbeat_rebuild_checkpoint(
                store, room_id, rebuild_id, last_replayed
            )

        async with locks.acquire_lock("embedded_edge_index_outbox", room_id):

            def _final(txn: Any) -> tuple[int, list[tuple[str, str, str]]]:
                txn.execute(
                    "SELECT source_version FROM room_edge_source_version "
                    "WHERE room_id = ?",
                    (room_id,),
                )
                row = txn.fetchone()
                current_version = int(row[0]) if row else 0
                txn.execute(
                    "SELECT event_id, prev_event_id, operation "
                    "FROM edge_index_outbox WHERE room_id = ? "
                    "AND source_version > ? ORDER BY source_version, event_id",
                    (room_id, last_replayed),
                )
                return current_version, [(str(a), str(b), str(c)) for a, b, c in txn]

            final_version, final_rows = await store.db_pool.runInteraction(
                "fetch_final_rebuild_outbox", _final
            )
            if final_rows:
                _retry_on_contention(
                    lambda: event_edges_apply_forward_generation_delta(
                        namespace, room_id, target_generation, final_rows
                    )
                )
            _retry_on_contention(
                lambda: room_forward_meta_swap(
                    room_id, target_generation, final_version
                )
            )
            await enqueue_retired_generation(store, room_id, active_generation)
        return True
    finally:
        await release_rebuild_checkpoint(store, room_id, rebuild_id)
