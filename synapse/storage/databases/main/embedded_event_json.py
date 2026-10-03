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

When the embedded engine is enabled it is the only store for new events'
`event_json`: `_persist_events_txn` does not insert into the SQL `event_json`
table (see events.py). The rows Synapse used to keep there are written here,
staged in the persist's mtxdb transaction and committed before the SQL COMMIT
so that a committed event row never points at JSON that is not yet visible.

Reads look here first. A miss falls back to a normal SQL `event_json` fetch,
but that fallback only finds rows written before the engine was enabled: for a
new event a miss means the id is absent, not that SQL will supply it. The read
path deliberately does not copy a SQL row back into mtxdb, because doing so
could race a concurrent censor/expiry and undo it (there is no version/CAS
scheme to prevent that). A row that predates the engine therefore keeps
coming from SQL until an explicit, serialised backfill moves it.

SQL still holds the event's index and metadata rows (`events`, `state_groups`,
edges and so on), and several code paths still query the SQL `event_json` table
directly with raw `JOIN`s (events_bg_updates.py, event_federation.py,
roommember.py, sticky_events.py, purge_events.py). Those are the reason the
table itself has not been dropped; they are not a sign that SQL is the source
of truth for new events.

Unlike the HAMT nodes/roots this shares a keyspace with, `event_json` rows are
NOT write-once in practice: censoring, expiry and re-signing replace a row's
`json` in place, and both paths write the new value here as part of the same
transaction that updates SQL (passing an empty prev list, so the write-once
edge record is preserved). Purge deletes the ids from here as well.

Reuses the same `embedded_db_engine`/`embedded_db_path` config and mtxdb
keyspace the state store already opens (one flat keyspace, prefixed keys --
`hamt:node:...`, `hamt:root:...`, `event_json:...` -- rather than a second
mtxdb directory/config knob), and is on whenever that is -- see
`open_embedded_event_json_engine`. Namespacing via `embedded_db_namespace`
keeps multiple homeservers sharing one mtxdb file from colliding on event_id.
"""

from __future__ import annotations

import logging
import os
import struct
import time
from typing import TYPE_CHECKING

from synapse.storage.databases.main.embedded_common import (
    Pool,
    ffi_timing,
    mirror_timing,
    sync_event_dag_now,
    sync_now,
)

if TYPE_CHECKING:
    from synapse.server import HomeServer
    from synapse.synapse_rust.mtxdb_engine import MtxdbTransaction

logger = logging.getLogger(__name__)

# THROWAWAY DIAGNOSTIC -- remove once the publication-timing question is
# settled. Forces a synchronous EVENT_DAG fsync after every `event_json`
# write, i.e. the write becomes durable before the persist transaction
# returns instead of waiting for the flush coalescer. Wired from
# `SYNAPSE_TEST_MTXDB_FORCE_SYNC_EVENT_JSON` in scripts-dev/complement.sh so
# the forced-sync half of the experiment matrix needs no rebuild. Never
# enable outside a diagnostic run: it is a whole-device cache flush per
# persisted event.
_FORCE_SYNC_EVENT_JSON = os.environ.get(
    "SYNAPSE_MTXDB_FORCE_SYNC_EVENT_JSON", ""
).strip().lower() not in ("", "0", "false", "no", "off")


def open_embedded_event_json_engine(hs: "HomeServer") -> bool:
    """Return whether the optional embedded event-JSON backend is enabled --
    on whenever the embedded engine itself is (`embedded_db_engine` +
    `embedded_db_path` configured), same as every other embedded mirror.

    Keys are namespaced by `embedded_db_namespace` (see `_event_json_key`),
    same scheme `embedded_event_to_state_group.py`/
    `embedded_event_auth_chain_links.py` already use, so multiple
    homeservers sharing one mtxdb file don't collide on event_id.
    """
    return bool(
        hs.config.database.embedded_db_engine and hs.config.database.embedded_db_path
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
    transaction: MtxdbTransaction | None = None,
) -> None:
    """`rows`: `(event_id, room_id, internal_metadata, json, format_version)`.

    With a `transaction`, the write is staged in it instead of applied: nothing
    is visible until the transaction commits, and it disappears if the
    transaction aborts. `sync` is then ignored (a staged write has nothing to
    fsync until it commits).
    Called from the event persister and from the censor/expiry paths,
    synchronously in the persisting transaction -- same reasoning as
    `_store_state_hamt_root_embedded_txn`: an mtxdb call is local, no
    network round-trip to justify deferring past commit.

    By default, does not call sync() after the write: this is the hottest
    embedded write path (one call per persisted event) and a synchronous
    fsync per event is a whole-device cache flush each time. The write
    becomes durable when the flush coalescer next syncs the EVENT_DAG pool
    (see `embedded_common._FlushCoalescer`).

    Visibility and durability are separate. A direct write is visible to
    readers as soon as it is journaled, and a staged write as soon as its
    transaction commits; the coalescer flush only makes it durable. In
    embedded-exclusive mode there is no SQL copy (`_persist_events_txn` skips
    the SQL `event_json` insert when `_embedded_event_json_enabled`), so a crash
    before the next flush can lose the writes made since the last one, and SQL
    cannot supply them. The SQL fallback in the read path only covers rows that
    predate the engine.

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
        if transaction is not None:
            transaction.event_json_put(namespace, tuples)
            ffi_timing("ffi_event_json_put", time.monotonic() - _et)
            return
        event_json_put(namespace, tuples)
        ffi_timing("ffi_event_json_put", time.monotonic() - _et)

        if sync or _FORCE_SYNC_EVENT_JSON:
            sync_event_dag_now()


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
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "[mtxdb-trace] event-json read requested=%d returned=%d missing=%s",
            len(event_ids),
            len(result),
            [event_id for event_id in event_ids if event_id not in result],
        )
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
