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

"""Mirrors `event_json` (the raw event blob: internal_metadata, json,
format_version, keyed by event_id) into the same embedded mtxdb keyspace the
state HAMT uses -- same rationale: write-once, immutable-ish, content-
addressed by event_id, pure point lookups (`get_event`), no
aggregation/joins needed against the blob itself (see
scripts-dev/benchmark_event_json_storage.py for the measurements: mtxdb
beat Postgres 23x at batch=1, 3.8x at batch=100).

Layout is room-aware (see `rust/src/database/mtxdb.rs`'s `event_json_*`
functions for the exact derivations):

- A sharded **locator** maps event_id -> its room's EventDag collection:
  256 deterministic bucket collections derived from the event's 128-bit
  identity hash, so no single index accumulates the server's whole event
  history.
- The room's `EventDag` collection holds two physically separate records per
  event: the **body** (`format_version + json`, `event_node_id`) and
  **`internal_metadata`** (`event_meta_node_id`) -- matching SQL's own
  `event_json` table, which has always carried `internal_metadata` and
  `json` as separate columns rather than one combined blob. Both coexist
  safely because their node ids are derived from differently tagged keys,
  and both resolve via the same locator, so no second lookup is needed to
  find the metadata record.

`event_json` (Postgres) stays authoritative and is always written; the
embedded engine is consulted first on reads, and a normal SQL `event_json`
fetch is the fallback on any miss (including a partial mirror write, which
`get_event_json_batch` treats as a miss rather than serving a mismatched
metadata/body pair).

This dual-write is deliberate, not a stopgap to be "fixed" by making mtxdb
sole source of truth -- that cutover was evaluated and scoped out. Blockers:
(1) at least a dozen call sites query the SQL `event_json` table directly
with raw `JOIN`s (events.py, events_bg_updates.py's several background
updates, event_federation.py, roommember.py, sticky_events.py, purge_events.py)
and don't go through this module's read path at all -- dropping the SQL
write would silently break every one of them, not just this mirror; (2)
`put_event_json_batch` deliberately does NOT sync() by default (see its
docstring) specifically because SQL is authoritative and a lost unflushed
mtxdb write only costs a slower fallback read -- making mtxdb authoritative
would require a synchronous fsync on every persisted event, reintroducing
the exact per-event fsync cost this branch exists to avoid; (3) there's no
CAS/versioning scheme in mtxdb here, so there's no migration/recovery story
for promoting it to authoritative without one. Revisit only as its own
scoped migration project, not an incremental change to this module.

Unlike the HAMT nodes/roots this mirrors, `event_json` rows are NOT
write-once/immutable in practice: censoring, expiry, and re-signing all
replace a row's `json` in place. Both of those paths explicitly re-mirror
the new value into mtxdb as part of the same transaction that updates SQL
(passing an empty prev list, so the write-once edge record is preserved).
The read-path SQL fallback in `events_worker.py`, however, deliberately
does NOT write back into mtxdb on a miss -- doing so racing a concurrent
censor/expiry could land a stale pre-censor value in mtxdb after the pruned
one, quietly undoing it, and there's no version/CAS scheme here to prevent
that. So a mirror gap (e.g. an id that predates this feature) stays a
permanent SQL fallback rather than self-healing; closing that gap needs an
explicit, serialized backfill job, not a read-path write.

Reuses the same `embedded_hamt_engine`/`embedded_hamt_path` config and mtxdb
keyspace the state store already opens (one flat keyspace, prefixed keys --
`hamt:node:...`, `hamt:root:...`, `event_json:...` -- rather than a second
mtxdb directory/config knob), and is on whenever that is -- see
`open_embedded_event_json_engine`. Namespacing via `embedded_hamt_namespace`
keeps multiple homeservers sharing one mtxdb file from colliding on event_id.
"""

from __future__ import annotations

import logging
import struct
import time
from typing import TYPE_CHECKING

from synapse.storage.databases.main.embedded_common import (
    Pool,
    ffi_timing,
    mirror_timing,
    sync_now,
)

if TYPE_CHECKING:
    from synapse.server import HomeServer

logger = logging.getLogger(__name__)


def open_embedded_event_json_engine(hs: "HomeServer") -> bool:
    """Return whether the optional embedded event-JSON backend is enabled --
    on whenever the embedded engine itself is (`embedded_hamt_engine` +
    `embedded_hamt_path` configured), same as every other embedded mirror.

    Keys are namespaced by `embedded_hamt_namespace` (see `_event_json_key`),
    same scheme `embedded_event_to_state_group.py`/
    `embedded_event_auth_chain_links.py` already use, so multiple
    homeservers sharing one mtxdb file don't collide on event_id.
    """
    return bool(
        hs.config.database.embedded_hamt_engine
        and hs.config.database.embedded_hamt_path
    )


def _encode_event_json_body(json: str, format_version: int | None) -> bytes:
    """Encode the event_json body record: `format_version + json`. Stored as
    its own physically separate mtxdb record from `internal_metadata` (see
    `_encode_event_json_metadata`) -- matching SQL's own `event_json` table,
    which has always carried `internal_metadata` and `json` as separate
    columns (see `full_schemas/72/full.sql.sqlite`)."""
    # format_version is nullable in the schema (older rows); encode as a
    # signed int with -1 standing in for NULL rather than adding a presence
    # flag byte.
    return struct.pack(
        ">i", -1 if format_version is None else format_version
    ) + json.encode("utf-8")


def _decode_event_json_body(value: bytes) -> tuple[str, int | None]:
    """Decode an event_json body payload returned by `event_json_get`."""
    if len(value) < 4:
        raise RuntimeError("truncated event_json body record")
    (format_version_raw,) = struct.unpack(">i", value[0:4])
    json_str = value[4:].decode("utf-8")
    format_version = None if format_version_raw == -1 else format_version_raw
    return json_str, format_version


def _encode_event_json_metadata(internal_metadata: str) -> bytes:
    """Encode the event_json metadata record: `internal_metadata` verbatim,
    utf-8, with no framing -- it's its own record now, not packed alongside
    the body."""
    return internal_metadata.encode("utf-8")


def _decode_event_json_metadata(value: bytes) -> str:
    return value.decode("utf-8")


def put_event_json_batch(
    engine_name: str | None,
    namespace: str,
    rows: list[tuple[str, str, str, str, int | None]],
    *,
    sync: bool = False,
) -> None:
    """`rows`: `(event_id, room_id, internal_metadata, json, format_version)`.
    Called from the event persister only (the sole writer of `event_json`),
    synchronously in the persisting transaction -- same reasoning as
    `_store_state_hamt_root_embedded_txn`: an mtxdb call is local, no
    network round-trip to justify deferring past commit.

    By default, does not call sync() after the write: this is the hottest
    embedded write path (one call per persisted event) and a synchronous
    fsync per event is a whole-device cache flush each time. The write
    becomes durable when the flush coalescer next syncs the EVENT_DAG pool
    (see `embedded_common._FlushCoalescer`).

    Note: the earlier rationale here -- "get_event_json_batch's caller falls
    back to SQL on a miss" -- does not hold in embedded-exclusive mode.
    `_persist_events_txn` skips the SQL `event_json` insert when
    `_embedded_event_json_enabled`, so a miss has no SQL copy to fall back
    to: it stays a miss until the writer's coalescer flush lands. Don't
    treat that fallback as covering the coalescer window.

    Pass `sync=True` only at standalone barrier call sites (e.g. a purge
    that must be durable before returning). Censoring/expiry loops that
    replace one event's JSON per iteration should pass `sync=False` and
    let the flush coalescer fsync the EVENT_DAG pool once they finish;
    fsyncing per event is a whole-device cache flush each time.
    """
    with mirror_timing("event_json_put"):
        from synapse.synapse_rust.mtxdb_engine import event_json_put

        tuples = [
            (
                room_id,
                event_id,
                _encode_event_json_metadata(internal_metadata),
                _encode_event_json_body(json, format_version),
            )
            for event_id, room_id, internal_metadata, json, format_version in rows
        ]
        _et = time.monotonic()
        event_json_put(namespace, tuples)
        ffi_timing("ffi_event_json_put", time.monotonic() - _et)

        if sync:
            sync_now(pools=[Pool.EVENT_DAG])


def get_event_json_batch(
    engine_name: str | None, namespace: str, event_ids: list[str]
) -> dict[str, tuple[str, str, int | None]]:
    """Returns `event_id -> (internal_metadata, json, format_version)` for
    every id found in the embedded engine; a missing id is simply absent
    from the result (the caller falls back to SQL for it). `internal_metadata`
    and `json`/`format_version` are stored as two physically separate mtxdb
    records (see `event_json_put` on the Rust side); both must be present to
    count as a hit -- a partial write (which `event_json_put` never produces,
    since both are written in the same `put_many` call) would otherwise
    silently serve a mismatched pair.
    """
    with mirror_timing("event_json_get"):
        from synapse.synapse_rust.mtxdb_engine import event_json_get

        _et = time.monotonic()
        found = event_json_get(namespace, event_ids)
        ffi_timing("ffi_event_json_get", time.monotonic() - _et)
        result: dict[str, tuple[str, str, int | None]] = {}
        for event_id, metadata_record, body_record in found:
            if metadata_record is None or body_record is None:
                continue
            internal_metadata = _decode_event_json_metadata(metadata_record)
            json_str, format_version = _decode_event_json_body(body_record)
            result[event_id] = (internal_metadata, json_str, format_version)
    return result


def delete_event_json_batch(
    engine_name: str | None, namespace: str, event_ids: list[str]
) -> None:
    """Removes `event_id`s from the embedded mirror. Must be called wherever
    `event_json` rows are deleted from SQL (purge_events.py) so the mirror
    doesn't retain data the user asked to be purged -- see also
    `put_event_json_batch`, which is called wherever `event_json` is
    replaced in place (censor_events.py) rather than deleted.
    """
    if not event_ids:
        return
    from synapse.synapse_rust.mtxdb_engine import event_json_delete

    event_json_delete(namespace, event_ids)
    sync_now(pools=[Pool.EVENT_DAG])
