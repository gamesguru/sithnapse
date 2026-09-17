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

import logging
import threading
import time
from typing import TYPE_CHECKING, Iterable

from synapse.storage.databases.main.embedded_common import (
    Pool,
    ffi_batch_size,
    ffi_count,
    ffi_timing,
    mark_dirty,
    mirror_timing,
    sync_now,
)

if TYPE_CHECKING:
    from synapse.server import HomeServer

logger = logging.getLogger(__name__)

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
#     before draining+writing tombstones, so a late flush cannot resurrect a
#     purged event's edges.
_EDGE_WRITE_THRESHOLD: int = 64
# `(room_id, event_id, prev_event_id, is_state)`.
EdgeRow = tuple[str, str, str, bool]
_edge_write_queues: dict[str, list[EdgeRow]] = {}
_edge_write_queues_lock = threading.Lock()
# One flush lock per namespace, serializing drains (and purge barriers) so at
# most one in-flight FFI write exists per namespace at a time.
_edge_write_flush_locks: dict[str, threading.Lock] = {}
_edge_write_flush_locks_guard = threading.Lock()


def _namespace_flush_lock(namespace: str) -> threading.Lock:
    """Return the per-namespace flush lock, creating it on first use."""
    with _edge_write_flush_locks_guard:
        lock = _edge_write_flush_locks.get(namespace)
        if lock is None:
            lock = threading.Lock()
            _edge_write_flush_locks[namespace] = lock
        return lock


def queued_edge_write_count(namespace: str | None = None) -> int:
    """Number of edge rows still coalesced in the write queue(s).

    Exposed for tests/diagnostics; ``namespace=None`` counts every namespace.
    """
    with _edge_write_queues_lock:
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

    Marks ``EVENT_DAG`` dirty so the flush coalescer schedules a bounded
    debounce drain, and flushes immediately when the queue grows past the
    threshold.
    """
    row_list = list(rows)
    if not row_list:
        return
    with _edge_write_queues_lock:
        q = _edge_write_queues.setdefault(namespace, [])
        q.extend(row_list)
        over_threshold = len(q) >= _EDGE_WRITE_THRESHOLD
    if over_threshold:
        try:
            flush_edge_writes(namespace, sync=sync)
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
        with _namespace_flush_lock(namespace):
            return _flush_namespace_locked(namespace, sync=sync)
    with _edge_write_queues_lock:
        namespaces = list(_edge_write_queues)
    flushed = False
    for ns in namespaces:
        with _namespace_flush_lock(ns):
            if _flush_namespace_locked(ns, sync=sync):
                flushed = True
    return flushed


def _flush_namespace_locked(namespace: str, *, sync: bool = False) -> bool:
    """Drain one namespace's queue.  The caller must hold its flush lock.

    Removes the drained rows from the queue only after the FFI write
    succeeds; on failure the rows are restored to the front of the queue and
    the exception is re-raised.
    """
    with _edge_write_queues_lock:
        q = _edge_write_queues.get(namespace)
        if not q:
            return False
        rows = q[:]
        q.clear()
    try:
        put_event_edges_batch(namespace, rows, sync=sync)
    except Exception:
        with _edge_write_queues_lock:
            restored = _edge_write_queues.setdefault(namespace, [])
            restored[0:0] = rows
        mark_dirty(Pool.EVENT_DAG)
        raise
    with _edge_write_queues_lock:
        q = _edge_write_queues.get(namespace)
        if q is not None and not q:
            _edge_write_queues.pop(namespace, None)
    mark_dirty(Pool.EVENT_DAG)
    return True


def open_embedded_event_edges_engine(hs: HomeServer) -> bool:
    """Return whether the embedded event-edges backend is available for readers and writers."""
    return bool(
        hs.config.database.embedded_hamt_engine == "mtxdb"
        and hs.config.database.embedded_hamt_path
    )


def embedded_event_edges_is_writable(hs: HomeServer) -> bool:
    """Return whether this process is permitted to write to embedded event-edges."""
    return open_embedded_event_edges_engine(hs) and not getattr(
        hs.config.database, "read_only", False
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
    purged ids are kept and flushed before the tombstone.
    """
    if not event_ids:
        return

    purged = set(event_ids)
    with _namespace_flush_lock(namespace):
        # 1. Cancel queued rows for the events being purged.
        with _edge_write_queues_lock:
            q = _edge_write_queues.get(namespace)
            if q is not None:
                kept = [r for r in q if r[1] not in purged]
                if kept:
                    q[:] = kept
                else:
                    _edge_write_queues.pop(namespace, None)

        # 2. Drain the remaining queue under the same lock so the tombstone
        #    below observes a settled forward-edge state.
        try:
            _flush_namespace_locked(namespace)
        except Exception:
            # Non-purged rows stay queued for the coalescer retry; the
            # destructive tombstone below must still run.
            logger.warning(
                "Failed to flush queued edge writes before purge of %d events",
                len(event_ids),
                exc_info=True,
            )

        # 3. Tombstone the purged events' edges.
        with mirror_timing("event_edges_delete"):
            from synapse.synapse_rust.mtxdb_engine import event_edges_delete

            _et = time.monotonic()
            event_edges_delete(namespace, event_ids)
            elapsed = time.monotonic() - _et
            ffi_timing("ffi_event_edges_delete", elapsed)
            ffi_count("event_edges_deleted", len(event_ids))
        sync_now(pools=[Pool.EVENT_DAG])


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
        results = event_edges_get_backward(namespace, event_ids)
        elapsed = time.monotonic() - _et
        ffi_timing("ffi_event_edges_get_backward", elapsed)
        ffi_batch_size("event_edges_get_backward", len(event_ids))

        return dict(results)


def get_event_edges_forward_batch(
    namespace: str,
    prev_event_ids: list[str],
) -> dict[str, list[str] | None]:
    """Reads forward child edges for `prev_event_ids`.

    Returns `prev_event_id -> [child_event_id]` or `None` if missing.
    """
    if not prev_event_ids:
        return {}

    with mirror_timing("event_edges_get_forward"):
        from synapse.synapse_rust.mtxdb_engine import event_edges_get_forward

        _et = time.monotonic()
        results = event_edges_get_forward(namespace, prev_event_ids)
        elapsed = time.monotonic() - _et
        ffi_timing("ffi_event_edges_get_forward", elapsed)
        ffi_batch_size("event_edges_get_forward", len(prev_event_ids))

        return dict(results)
