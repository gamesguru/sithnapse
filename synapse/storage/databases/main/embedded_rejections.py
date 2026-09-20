#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 Element Creations Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Mirrors `rejections` (event_id -> (reason, last_check)) into the embedded
mtxdb State keyspace, alongside `event_json` and `event_to_state_group`. Flat
point lookups only -- see `_store_rejections_txn` in `events.py` (the write).

Modelled directly on `embedded_event_to_state_group.py`'s flat-KV pattern:
plain idempotent `batch_put`/`batch_get` against the generic KV surface, no new
Rust, no room-sharding/locator layer. `shard_type_for_key`
(`rust/src/database/mtxdb.rs:117`) routes the `rejection:` prefix -- like every
non-`event_json:`/`prev_event_edges:` key -- to the State pool's single global
flat-KV collection (`kv_room_id()`), not EventDag. SQL stays authoritative
through this phase, so writes are `SyncTier.CACHE`: a lost unflushed write only
costs a slower SQL-fallback read, never data loss.

`last_check` is stored as its text form (matching the SQL column, which is
TEXT and written as `str(time_msec)`), not re-encoded as an integer, so a
mirror round-trip is byte-identical to what SQL holds.

Every key is namespaced (`namespace_hash`) for the same reason as the other
mirrors: multiple homeservers sharing one mtxdb file must not collide on
event_id.
"""

from __future__ import annotations

import logging
import struct
import time

from synapse.storage.databases.embedded_engine import get_embedded_engine
from synapse.storage.databases.main.embedded_common import (
    ffi_timing,
    mirror_timing,
    namespace_hash,
)

logger = logging.getLogger(__name__)


def _rejection_key(namespace: str, event_id: str) -> bytes:
    return (
        b"rejection:"
        + namespace_hash(namespace).hex().encode("ascii")
        + b":"
        + event_id.encode("utf-8")
    )


def _encode_rejection(reason: str, last_check: str) -> bytes:
    encoded_reason = reason.encode("utf-8")
    encoded_last_check = last_check.encode("utf-8")
    return (
        struct.pack(">I", len(encoded_reason))
        + encoded_reason
        + struct.pack(">I", len(encoded_last_check))
        + encoded_last_check
    )


def _decode_rejection(value: bytes) -> tuple[str, str]:
    if len(value) < 8:
        raise RuntimeError("truncated rejection record")
    (reason_length,) = struct.unpack(">I", value[0:4])
    reason_end = 4 + reason_length
    if len(value) < reason_end + 4:
        raise RuntimeError("invalid rejection record")
    (last_check_length,) = struct.unpack(">I", value[reason_end : reason_end + 4])
    last_check_end = reason_end + 4 + last_check_length
    if len(value) != last_check_end:
        raise RuntimeError("invalid rejection record")
    reason = value[4:reason_end].decode("utf-8")
    last_check = value[reason_end + 4 : last_check_end].decode("utf-8")
    return reason, last_check


def put_rejection_batch(
    engine_name: str | None,
    namespace: str,
    rows: list[tuple[str, str, str]],
) -> None:
    """`rows`: `(event_id, reason, last_check)`. Plain idempotent `batch_put`:
    a re-persisted rejected event (a transaction retry, or a recheck that
    rewrites `last_check`) must be able to rewrite the same key.

    Does not sync() the embedded engine -- this is a CACHE-tier write (SQL
    remains authoritative), same reasoning as
    `embedded_event_json.put_event_json_batch`'s default.
    """
    if not rows:
        return
    with mirror_timing("put_rejection"):
        engine = get_embedded_engine(engine_name)
        pairs = [
            (
                _rejection_key(namespace, event_id),
                _encode_rejection(reason, last_check),
            )
            for event_id, reason, last_check in rows
        ]
        _et = time.monotonic()
        engine.batch_put(pairs)
        ffi_timing("ffi_batch_put", time.monotonic() - _et)


def get_rejections_batch(
    engine_name: str | None, namespace: str, event_ids: list[str]
) -> dict[str, tuple[str, str]]:
    """Returns `event_id -> (reason, last_check)` for every id found in the
    embedded engine; a missing id is simply absent from the result (the caller
    falls back to SQL for it).
    """
    if not event_ids:
        return {}
    with mirror_timing("get_rejections"):
        engine = get_embedded_engine(engine_name)
        keys = [_rejection_key(namespace, event_id) for event_id in event_ids]
        key_to_event_id = dict(zip(keys, event_ids))
        _et = time.monotonic()
        found = engine.batch_get(keys)
        ffi_timing("ffi_batch_get", time.monotonic() - _et)
        out: dict[str, tuple[str, str]] = {}
        for key, value in found:
            out[key_to_event_id[bytes(key)]] = _decode_rejection(bytes(value))
        return out


def delete_rejections_batch(
    engine_name: str | None, namespace: str, event_ids: list[str]
) -> None:
    """Removes `event_id`s from the embedded mirror. Must be called wherever
    `rejections` rows are deleted from SQL (purge_events.py) so the mirror
    doesn't retain a rejection the user asked to be purged.
    """
    if not event_ids:
        return
    engine = get_embedded_engine(engine_name)
    keys = [_rejection_key(namespace, event_id) for event_id in event_ids]
    engine.batch_delete(keys)
