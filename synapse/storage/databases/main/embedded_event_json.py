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
format_version, keyed by event_id) into the same embedded mdbx keyspace the
state HAMT uses -- same rationale, same shape: write-once, immutable,
content-addressed by event_id, pure point lookups (`get_event`), no
aggregation/joins needed against the blob itself (see
scripts-dev/benchmark_event_json_storage.py for the measurements: mdbx
beat Postgres 23x at batch=1, 3.8x at batch=100).

Reuses the same `embedded_hamt_engine`/`embedded_hamt_path` config and mdbx
keyspace the state store already opens (one flat keyspace, prefixed keys --
`hamt:node:...`, `hamt:root:...`, `event_json:...` -- rather than a second
mdbx directory/config knob). `event_json` (Postgres) stays authoritative
and is always written; the embedded engine is consulted first on reads and
any event_id it's missing falls back to a normal SQL `event_json` fetch.

Unlike the HAMT nodes/roots this mirrors, `event_json` rows are NOT
write-once/immutable in practice: censoring and expiry both replace a
row's `json` in place (see `censor_events.py`'s `_censor_event_txn`,
called by both). Both of those paths explicitly re-mirror the new value
into mdbx as part of the same transaction that updates SQL. The read-path
SQL fallback in `events_worker.py`, however, deliberately does NOT write
back into mdbx on a miss -- doing so racing a concurrent censor/expiry
could land a stale pre-censor value in mdbx after the pruned one, quietly
undoing it, and there's no version/CAS scheme here to prevent that. So a
mirror gap (e.g. an id that predates this feature) stays a permanent SQL
fallback rather than self-healing; closing that gap needs an explicit,
serialized backfill job, not a read-path write.
"""

from __future__ import annotations

import logging
import struct
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from synapse.server import HomeServer

logger = logging.getLogger(__name__)


def open_embedded_event_json_engine(hs: "HomeServer") -> bool:
    """Return whether the optional embedded event-JSON backend is enabled.

    `embedded_hamt` configures only persistent state HAMT storage. It must not
    enable the independent event-JSON and event-chain stores: those keys are
    not part of the HAMT namespace and doing so lets separate homeservers
    sharing an MDBX file overwrite each other's event data.

    The event-JSON backend has no independent configuration yet, so it remains
    disabled.
    """
    return False


def _event_json_key(event_id: str) -> bytes:
    return b"event_json:" + event_id.encode("utf-8")


def _encode_event_json_record(
    internal_metadata: str, json: str, format_version: int | None
) -> bytes:
    internal_metadata_bytes = internal_metadata.encode("utf-8")
    json_bytes = json.encode("utf-8")
    # format_version is nullable in the schema (older rows); encode as a
    # signed int with -1 standing in for NULL rather than adding a presence
    # flag byte.
    return (
        b"\x01"
        + struct.pack(">i", -1 if format_version is None else format_version)
        + struct.pack(">I", len(internal_metadata_bytes))
        + internal_metadata_bytes
        + json_bytes
    )


def _decode_event_json_record(value: bytes) -> tuple[str, str, int | None]:
    if len(value) < 9 or value[0] != 1:
        raise RuntimeError("invalid or unsupported event_json record version")
    (format_version_raw,) = struct.unpack(">i", value[1:5])
    (metadata_len,) = struct.unpack(">I", value[5:9])
    metadata_start = 9
    json_start = metadata_start + metadata_len
    if len(value) < json_start:
        raise RuntimeError("truncated event_json record")
    internal_metadata = value[metadata_start:json_start].decode("utf-8")
    json_str = value[json_start:].decode("utf-8")
    format_version = None if format_version_raw == -1 else format_version_raw
    return internal_metadata, json_str, format_version


def put_event_json_batch(
    rows: list[tuple[str, str, str, int | None]],
) -> None:
    """`rows`: `(event_id, internal_metadata, json, format_version)`.
    Called from the event persister only (the sole writer of `event_json`),
    synchronously in the persisting transaction -- same reasoning as
    `_store_state_hamt_root_embedded_txn`: an mdbx call is local, no
    network round-trip to justify deferring past commit.
    """
    from synapse.synapse_rust import mdbx_engine

    pairs = [
        (
            _event_json_key(event_id),
            _encode_event_json_record(internal_metadata, json, format_version),
        )
        for event_id, internal_metadata, json, format_version in rows
    ]
    mdbx_engine.batch_put(pairs)


def get_event_json_batch(
    event_ids: list[str],
) -> dict[str, tuple[str, str, int | None]]:
    """Returns `event_id -> (internal_metadata, json, format_version)` for
    every id found in the embedded engine; a missing id is simply absent
    from the result (the caller falls back to SQL for it).
    """
    from synapse.synapse_rust import mdbx_engine

    keys = [_event_json_key(event_id) for event_id in event_ids]
    key_to_event_id = dict(zip(keys, event_ids))
    found = mdbx_engine.batch_get(keys)
    return {
        key_to_event_id[bytes(key)]: _decode_event_json_record(bytes(value))
        for key, value in found
    }


def delete_event_json_batch(event_ids: list[str]) -> None:
    """Removes `event_id`s from the embedded mirror. Must be called wherever
    `event_json` rows are deleted from SQL (purge_events.py) so the mirror
    doesn't retain data the user asked to be purged -- see also
    `put_event_json_batch`, which is called wherever `event_json` is
    replaced in place (censor_events.py) rather than deleted.
    """
    if not event_ids:
        return
    from synapse.synapse_rust import mdbx_engine

    keys = [_event_json_key(event_id) for event_id in event_ids]
    mdbx_engine.batch_delete(keys)
