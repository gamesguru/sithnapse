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
- Locators are owned by `embedded_event_json`; edge writes rely on those.

Reads are mtxdb-first with SQL fallback:
  mtxdb lookup
    ├─ complete -> return mtxdb result
    └─ missing/incomplete -> query SQL, repair mtxdb, return SQL result

Writes are coalesced: ``queue_edge_write`` accumulates rows across
post-commit callbacks and ``flush_edge_writes`` drains them in a single
FFI call when the queue exceeds ``_EDGE_WRITE_THRESHOLD``.
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
# flushes them in bulk when the threshold is reached or flush_edge_writes()
# is called explicitly (e.g. at shutdown or before a sync_now).
_EDGE_WRITE_THRESHOLD: int = 64
_edge_write_queue: list[tuple[str, str, str, bool]] = []
# The last namespace queued rows were staged under.  Tracked so the shutdown
# hook (`flush_edge_writes` with no namespace) can drain committed rows without
# a caller-supplied namespace -- production runs one namespace per process.
_edge_write_namespace: str | None = None
_edge_write_lock = threading.Lock()


def queue_edge_write(
    namespace: str,
    rows: Iterable[tuple[str, str, str, bool]],
    *,
    sync: bool = False,
) -> None:
    """Append edge rows to the coalescing queue; flush if threshold reached."""
    row_list = list(rows)
    if not row_list:
        return
    global _edge_write_namespace
    with _edge_write_lock:
        _edge_write_namespace = namespace
        _edge_write_queue.extend(row_list)
        should_flush = len(_edge_write_queue) >= _EDGE_WRITE_THRESHOLD
    if should_flush:
        flush_edge_writes(namespace, sync=sync)


def flush_edge_writes(namespace: str | None = None, *, sync: bool = False) -> bool:
    """Drain the coalescing queue into a single FFI call.

    ``namespace`` may be omitted (e.g. from the shutdown hook) and defaults
    to the last namespace rows were queued under.

    Returns ``True`` if any rows were flushed.
    """
    with _edge_write_lock:
        if not _edge_write_queue:
            return False
        if namespace is None:
            if _edge_write_namespace is None:
                return False
            namespace = _edge_write_namespace
        rows = _edge_write_queue[:]
        _edge_write_queue.clear()
    put_event_edges_batch(namespace, rows, sync=sync)
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
    rows: Iterable[tuple[str, str, str, bool]],
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
    """Tombstones backward edges for purged events in mtxdb."""
    if not event_ids:
        return

    # Destructive, immediate barrier: drain any coalesced edge writes so a
    # purge cannot race a still-queued forward-edge append.
    flush_edge_writes(namespace)

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
