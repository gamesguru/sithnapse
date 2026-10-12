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

"""Mirrors `redactions` (redacts_event_id -> the redaction event that targets
it) into the embedded mtxdb State keyspace, alongside `event_json` and
`event_to_state_group`. Flat point lookups only -- see
`_store_redaction`/`_store_event_txn` in `events.py` (writes) and
`have_censored_event` in `events_worker.py` (the read).

Deliberately keyed by the *redacted* event id (`redacts`), not the redaction
event's own id: the one read path, `have_censored_event(event_id)`, is a point
lookup "has the event `event_id` been censored" and in SQL selects by
`redacts = event_id` (see events_worker.py). Keying by the redaction event id
would make that lookup impossible without a secondary index.

The value carries the redaction event's id plus `have_censored`. Note that
`redactions.redacts` is NOT unique -- several redaction events can target the
same event, and `have_censored_event` ORs across all of them (`any(...)`). The
single flat slot represents that aggregate, and the two writers keep it
consistent with SQL:

- `put_redaction_batch` (the insert path, `_store_redaction`) is **create-only**
  and never overwrites an existing slot. This mirrors SQL exactly: the
  `simple_upsert_txn` there only sets `redacts`/`received_ts`/`recheck`, so an
  existing row's `have_censored` is preserved. Blindly overwriting would let a
  second redaction of an already-censored event reset the aggregate to False.
- `set_have_censored_batch` is an absolute setter for the aggregate: SQL's
  `:3017` bulk update resets every row for the target to False, and censoring
  sets one row True (so the OR is True) -- both map to a plain set here.

`received_ts` is intentionally dropped -- the plan
(`res/docs/2026-09-20-events-table-deprecation-plan.md`) treats it as derivable
from the event's internal_metadata.

`shard_type_for_key` (`rust/src/database/mtxdb.rs:117`) routes the `redaction:`
prefix -- like every non-`event_json:`/`prev_event_edges:` key, including
`event_to_state_group:` -- to the State pool's single global flat-KV collection
(`kv_room_id()`), not EventDag. That's the same generic `batch_put`/`batch_get`
surface `event_to_state_group` uses; no new Rust, no room-sharding/locator
layer. SQL stays authoritative through this phase, so writes are
`SyncTier.CACHE`: a lost unflushed write only costs a slower SQL-fallback read,
never data loss.

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


def _redaction_key(namespace: str, redacts_event_id: str) -> bytes:
    return (
        b"redaction:"
        + namespace_hash(namespace).hex().encode("ascii")
        + b":"
        + redacts_event_id.encode("utf-8")
    )


def _encode_redaction(redaction_event_id: str, have_censored: bool) -> bytes:
    encoded = redaction_event_id.encode("utf-8")
    return struct.pack(">I", len(encoded)) + encoded + struct.pack(">?", have_censored)


def _decode_redaction(value: bytes) -> tuple[str, bool]:
    if len(value) < 5:
        raise RuntimeError("truncated redaction record")
    (length,) = struct.unpack(">I", value[0:4])
    end = 4 + length
    if len(value) != end + 1:
        raise RuntimeError("invalid redaction record")
    redaction_event_id = value[4:end].decode("utf-8")
    (have_censored,) = struct.unpack(">?", value[end : end + 1])
    return redaction_event_id, have_censored


def _write_redaction_batch(
    engine_name: str | None,
    namespace: str,
    rows: list[tuple[str, str, bool]],
) -> None:
    """Raw unconditional write of `(redacts_event_id, redaction_event_id,
    have_censored)` records. Callers decide whether an existing slot may be
    overwritten -- see `put_redaction_batch` (create-only) and
    `set_have_censored_batch` (absolute overwrite).
    """
    if not rows:
        return
    with mirror_timing("put_redaction"):
        engine = get_embedded_engine(engine_name)
        pairs = [
            (
                _redaction_key(namespace, redacts_event_id),
                _encode_redaction(redaction_event_id, have_censored),
            )
            for redacts_event_id, redaction_event_id, have_censored in rows
        ]
        _et = time.monotonic()
        engine.batch_put(pairs)
        ffi_timing("ffi_batch_put", time.monotonic() - _et)


def put_redaction_batch(
    engine_name: str | None,
    namespace: str,
    rows: list[tuple[str, str, bool]],
) -> None:
    """`rows`: `(redacts_event_id, redaction_event_id, have_censored)`.

    Create-only: a slot that already exists is left untouched. This mirrors
    SQL's `_store_redaction` upsert (which never rewrites an existing row's
    `have_censored`) and keeps the aggregate correct when several redactions
    target the same event -- see the module docstring.

    Does not sync() the embedded engine -- this is a CACHE-tier write (SQL
    remains authoritative), same reasoning as
    `embedded_event_json.put_event_json_batch`'s default.
    """
    if not rows:
        return
    existing = get_redactions_batch(engine_name, namespace, [r[0] for r in rows])
    _write_redaction_batch(
        engine_name,
        namespace,
        [row for row in rows if row[0] not in existing],
    )


def get_redactions_batch(
    engine_name: str | None, namespace: str, redacts_event_ids: list[str]
) -> dict[str, tuple[str, bool]]:
    """Returns `redacts_event_id -> (redaction_event_id, have_censored)` for
    every id found in the embedded engine; a missing id is simply absent from
    the result (the caller falls back to SQL for it).
    """
    if not redacts_event_ids:
        return {}
    with mirror_timing("get_redactions"):
        engine = get_embedded_engine(engine_name)
        keys = [_redaction_key(namespace, event_id) for event_id in redacts_event_ids]
        key_to_event_id = dict(zip(keys, redacts_event_ids))
        _et = time.monotonic()
        found = engine.batch_get(keys)
        ffi_timing("ffi_batch_get", time.monotonic() - _et)
        out: dict[str, tuple[str, bool]] = {}
        for key, value in found:
            out[key_to_event_id[bytes(key)]] = _decode_redaction(bytes(value))
        return out


def set_have_censored_batch(
    engine_name: str | None,
    namespace: str,
    redacts_event_ids: list[str],
    have_censored: bool,
) -> None:
    """Read-modify-write `have_censored` for the listed redacted event ids,
    preserving each record's redaction event id. This is an absolute setter
    for the aggregate, not an OR -- see the module docstring.

    Ids with no mirror record are skipped: the redaction event that created
    them is written first (in `_store_redaction`), so a missing record means
    the mirror simply hasn't seen that redaction yet and SQL remains the
    source of truth for it.
    """
    if not redacts_event_ids:
        return
    existing = get_redactions_batch(engine_name, namespace, redacts_event_ids)
    if not existing:
        return
    _write_redaction_batch(
        engine_name,
        namespace,
        [
            (redacts_event_id, redaction_event_id, have_censored)
            for redacts_event_id, (redaction_event_id, _) in existing.items()
        ],
    )


def delete_redactions_batch(
    engine_name: str | None, namespace: str, redacts_event_ids: list[str]
) -> None:
    """Removes `redacts_event_id`s from the embedded mirror. Must be called
    wherever `redactions` rows are deleted from SQL (purge_events.py) so the
    mirror doesn't retain a redaction the user asked to be purged -- otherwise
    `have_censored_event` would keep reporting it as a cache hit.
    """
    if not redacts_event_ids:
        return
    engine = get_embedded_engine(engine_name)
    keys = [_redaction_key(namespace, event_id) for event_id in redacts_event_ids]
    engine.batch_delete(keys)
