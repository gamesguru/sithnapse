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

"""Event DAG adjacency (`prev_events`) in mtxdb's CSR store.

mtxdb keeps one `CsrAdjacency` per room, addressed by the room's event
collection id, with two maintained families:

- backward: `event_id -> prev_event_ids`, written with the event;
- forward: `prev_event_id -> child_event_ids`, a reverse row maintained by the
  same write.

There are no per-edge records and no namespace salt: an event's id is its
address (`sha256("event:" + event_id)`), and the room comes from the event
locator published by `event_json_put` and re-published by `adjacency_put` for
every event an edge touches.

CSR rows are immutable once written. An individually deleted (retention)
event's rows stay; every reader resolves the ids it gets back against the SQL
`events` table, so a missing row cannot resurrect a purged event. A room purge
drops the room's whole scope with `adjacency_purge_room`.

Write ordering: `put_event_edges_batch` runs inside event persistence, before
the SQL COMMIT (and before the persist's mtxdb transaction commits). The CSR
append has its own durability, so a crash between it and the SQL COMMIT can
only leave adjacency for an event that never became committed -- an orphan the
readers filter out -- never a committed event without adjacency.
"""

from __future__ import annotations

import logging
import threading
import time
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Iterable

from prometheus_client import Counter

from synapse.metrics import SERVER_NAME_LABEL, LaterGauge
from synapse.storage.databases.main.embedded_common import (
    Pool,
    ffi_batch_size,
    ffi_count,
    ffi_timing,
    mirror_timing,
    retry_on_contention,
    sync_now,
)

if TYPE_CHECKING:
    from synapse.server import HomeServer
    from synapse.synapse_rust.mtxdb_engine import MtxdbTransaction

logger = logging.getLogger(__name__)

# `(room_id, event_id, prev_event_id)`; `prev_event_id` is `None` for an event
# with no predecessor, which still needs its adjacency row recorded.
EdgeRow = tuple[str, str, str | None]


def put_event_edges_batch(
    rows: Iterable[EdgeRow],
    *,
    sync: bool = False,
    transaction: "MtxdbTransaction | None" = None,
) -> None:
    """Record `prev_events` adjacency for a batch of events.

    `rows`: `(room_id, event_id, prev_event_id)`.

    Called from event persistence before the SQL COMMIT, so a committed event
    always has its adjacency. `transaction` is accepted for call-site
    compatibility and ignored: the CSR append is durable in its own right, and
    the only failure mode it introduces is orphan rows for an event whose SQL
    row never commits, which every reader filters out.

    `sync` issues an immediate EVENT_DAG barrier after the write.
    """
    by_event: dict[tuple[str, str], list[str]] = {}
    for room_id, event_id, prev_event_id in rows:
        prevs = by_event.setdefault((room_id, event_id), [])
        if prev_event_id is not None:
            prevs.append(prev_event_id)
    if not by_event:
        return

    # `adjacency_put` takes one record per event: (room, event, prev, auth).
    # Auth edges stay in the AUTH collection for now, so only `prev` is fed.
    put_rows: list[tuple[str, str, list[str], list[str]]] = [
        (room_id, event_id, prevs, [])
        for (room_id, event_id), prevs in by_event.items()
    ]

    with mirror_timing("event_edges_put"):
        from synapse.synapse_rust.mtxdb_engine import adjacency_put

        _et = time.monotonic()
        retry_on_contention(lambda: adjacency_put(put_rows), label="event_edges")
        elapsed = time.monotonic() - _et
        ffi_timing("ffi_event_edges_put", elapsed)
        ffi_count("event_edges_put_rows", len(put_rows))
        ffi_batch_size("event_edges_put", len(put_rows))

    if sync:
        sync_now(pools=[Pool.EVENT_DAG])


def purge_event_edges_room(room_id: str) -> None:
    """Drop a purged room's whole adjacency scope."""
    from synapse.synapse_rust.mtxdb_engine import adjacency_purge_room

    with mirror_timing("event_edges_purge_room"):
        _et = time.monotonic()
        retry_on_contention(lambda: adjacency_purge_room(room_id), label="event_edges")
        ffi_timing("ffi_event_edges_purge_room", time.monotonic() - _et)
    sync_now(pools=[Pool.EVENT_DAG])


def get_event_edges_backward_batch(event_ids: list[str]) -> dict[str, list[str]]:
    """Reads `prev_events` for `event_ids`.

    Returns `event_id -> [prev_event_id]`; an event without a recorded
    adjacency row is simply absent.
    """
    if not event_ids:
        return {}

    with mirror_timing("event_edges_get_backward"):
        from synapse.synapse_rust.mtxdb_engine import adjacency_get_prev

        _et = time.monotonic()
        results = retry_on_contention(
            lambda: adjacency_get_prev(event_ids), label="event_edges"
        )
        elapsed = time.monotonic() - _et
        ffi_timing("ffi_event_edges_get_backward", elapsed)
        ffi_batch_size("event_edges_get_backward", len(event_ids))

    return {event_id: prev for event_id, prev in results if prev is not None}


def get_event_edges_forward_batch(
    prev_event_ids: list[str],
) -> dict[str, list[str] | None]:
    """Reads `prev_event_id -> [child_event_id]` from the CSR reverse row."""
    if not prev_event_ids:
        return {}
    with mirror_timing("event_edges_get_forward"):
        from synapse.synapse_rust.mtxdb_engine import adjacency_get_children

        _et = time.monotonic()
        results = retry_on_contention(
            lambda: adjacency_get_children(prev_event_ids), label="event_edges"
        )
        ffi_timing("ffi_event_edges_get_forward", time.monotonic() - _et)
        ffi_batch_size("event_edges_get_forward", len(prev_event_ids))
        return dict(results)


def open_embedded_event_edges_engine(hs: HomeServer) -> bool:
    """Return whether the embedded event-edges backend is available for readers and writers."""
    return bool(
        hs.config.database.embedded_db_engine == "mtxdb"
        and hs.config.database.embedded_db_path
    )


event_edge_reads_counter = Counter(
    "synapse_event_edges_reads",
    "Lookups of event edges by caller (`path`) and outcome, one increment per "
    "lookup so the outcomes sum to the total lookup rate: `hit`, `miss`, or "
    "`sql_only` when the SQL fallback served the lookup. Unlike the opt-in ffi "
    "counters this is always on.",
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
