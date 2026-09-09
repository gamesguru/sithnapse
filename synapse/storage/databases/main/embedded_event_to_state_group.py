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

"""Mirrors `event_to_state_groups` (event_id -> state_group, a pure point
lookup with no aggregation/joins needed against the forward mapping -- see
`_get_state_group_for_event(s)` in `state.py`) into the same embedded mtxdb
keyspace `event_json` and the state HAMT use. Exclusive by configured
engine, not a dual-write: when `embedded_hamt_engine` is configured, this
table is written/read here only, never SQL -- see `_store_event_state_mappings_txn`
et al. in `events.py`/`state.py`.

Every key here is namespaced (see `namespace` on each function, and
`hamt_namespace` on the state datastore) for the same reason the HAMT
node/root keys are: multiple homeservers/deployments can share one mtxdb
file (e.g. many trial test processes), and state_group ids restart at 1 for
each -- without a namespace prefix, two different deployments' state_group
9 would collide and silently share (and corrupt) each other's refcounts and
event mappings.

NOT write-once/immutable: partial-state events (fast-join) get their
`state_group` rewritten in place once resolution completes -- see
`state.py`'s `_update_state_for_partial_state_event_txn`, a real update, not
an insert. That path re-mirrors the new value here as part of the same
transaction.

`event_to_state_groups` also has a genuine reverse-lookup usage
(`get_referenced_state_groups`, "does any event still reference state_group
X" -- used by purge's safety check before deleting a state group) that a
flat event_id-keyed store can't answer without either a full scan or an
event-list index. An index that lists every referencing event per state
group would itself need updating (a list insert) on every single event
persisted and every purge -- real write cost that grows with room activity,
not O(1). A plain reference *count* per state group avoids that: each event
contributes exactly +1 to its state group's counter once, each purge
contributes exactly -1 per purged event, and both are O(1) mtxdb point
operations regardless of how many events share that state group. That's
what `increment_state_group_refcounts_batch`/`get_referenced_state_groups_batch`
below provide -- see `rust/src/database/mtxdb.rs`'s `increment_counters_batch`
for why this can't just be a Python read-then-write (it would race a
concurrent increment on the same key and lose an update).
"""

from __future__ import annotations

import hashlib
import logging
import struct

from synapse.storage.databases.embedded_engine import get_embedded_engine

logger = logging.getLogger(__name__)


def _namespace_hash(namespace: str) -> bytes:
    return hashlib.sha256(namespace.encode("utf-8")).digest()[:16]


def _event_to_state_group_key(namespace: str, event_id: str) -> bytes:
    return (
        b"event_to_state_group:"
        + _namespace_hash(namespace).hex().encode("ascii")
        + b":"
        + event_id.encode("utf-8")
    )


def _state_group_refcount_key(namespace: str, state_group: int) -> bytes:
    return (
        b"state_group_refcount:"
        + _namespace_hash(namespace).hex().encode("ascii")
        + b":"
        + struct.pack(">q", state_group)
    )


def put_event_to_state_group_batch(
    engine_name: str | None, namespace: str, rows: list[tuple[str, int]]
) -> None:
    """`rows`: `(event_id, state_group)`. Called both from the event
    persister (initial insert) and from `update_state_for_partial_state_event`
    (in-place rewrite once partial state resolves) -- synchronously, in the
    same transaction that would otherwise write SQL, same reasoning as
    `embedded_event_json.put_event_json_batch`.

    Does NOT touch the refcount -- callers that are inserting a *new*
    event_id -> state_group mapping (not rewriting an existing one) must
    also call `increment_state_group_refcounts_batch` for the same rows;
    `update_state_for_partial_state_event` deliberately does not, since it
    doesn't change which state groups are referenced overall (a partial
    event's state group is a placeholder, not an additional reference; see
    its caller for exactly how the refcount is kept accurate across the
    rewrite).

    Does not sync() the embedded engine itself -- callers that combine this
    with a refcount increment/decrement in the same logical update (e.g.
    `update_state_for_partial_state_event`, the initial-insert paths in
    `events.py`/`events_bg_updates.py`) are responsible for calling
    `maybe_sync(SyncTier.DURABLE)` once after all of their embedded writes,
    not once per helper call.
    """
    from synapse.synapse_rust.mtxdb_engine import batch_put

    pairs = [
        (
            _event_to_state_group_key(namespace, event_id),
            struct.pack(">q", state_group),
        )
        for event_id, state_group in rows
    ]
    batch_put(pairs)


def get_state_group_for_events_batch(
    engine_name: str | None, namespace: str, event_ids: list[str]
) -> dict[str, int]:
    """Returns `event_id -> state_group` for every id found in the embedded
    engine; a missing id is simply absent from the result.
    """
    from synapse.synapse_rust.mtxdb_engine import batch_get

    keys = [_event_to_state_group_key(namespace, event_id) for event_id in event_ids]
    key_to_event_id = dict(zip(keys, event_ids))
    found = batch_get(keys)
    out = {}
    for key, value in found:
        value = bytes(value)
        if len(value) != 8:
            raise RuntimeError("invalid event_to_state_group record")
        (state_group,) = struct.unpack(">q", value)
        out[key_to_event_id[bytes(key)]] = state_group
    return out


def delete_event_to_state_group_batch(
    engine_name: str | None, namespace: str, event_ids: list[str]
) -> None:
    """Removes `event_id`s from the embedded mirror. Must be called wherever
    `event_to_state_groups` rows are purged, alongside
    `decrement_state_group_refcounts_batch` for the state groups they
    referenced.

    Does not sync() the embedded engine itself -- see
    `put_event_to_state_group_batch`'s docstring; the caller syncs once
    after both this and the paired refcount decrement.
    """
    if not event_ids:
        return
    keys = [_event_to_state_group_key(namespace, event_id) for event_id in event_ids]
    from synapse.synapse_rust.mtxdb_engine import batch_delete

    batch_delete(keys)


def increment_state_group_refcounts_batch(
    engine_name: str | None, namespace: str, state_groups: list[int]
) -> None:
    """Adds +1 to each listed state group's reference count (repeats count
    multiple times, e.g. `[5, 5, 7]` adds 2 to group 5's count and 1 to
    group 7's). Call once per newly-inserted `(event_id, state_group)`
    mapping -- not for `update_state_for_partial_state_event`'s rewrite,
    which doesn't add a new reference.

    Does not sync() the embedded engine itself -- see
    `put_event_to_state_group_batch`'s docstring; the caller syncs once
    after this and the paired put/delete.
    """
    if not state_groups:
        return
    counts: dict[int, int] = {}
    for state_group in state_groups:
        counts[state_group] = counts.get(state_group, 0) + 1
    pairs = [
        (_state_group_refcount_key(namespace, state_group), delta)
        for state_group, delta in counts.items()
    ]
    get_embedded_engine(engine_name).increment_counters_batch(pairs)


def decrement_state_group_refcounts_batch(
    engine_name: str | None, namespace: str, state_groups: list[int]
) -> None:
    """The inverse of `increment_state_group_refcounts_batch` -- call once
    per purged `(event_id, state_group)` mapping. Never lets a counter go
    negative in practice (every decrement corresponds to a prior increment),
    but doesn't enforce that -- a mismatched call pair is a caller bug, not
    something this function can detect from a single counter value alone.

    Does not sync() the embedded engine itself -- see
    `put_event_to_state_group_batch`'s docstring; the caller syncs once
    after this and the paired delete.
    """
    if not state_groups:
        return
    counts: dict[int, int] = {}
    for state_group in state_groups:
        counts[state_group] = counts.get(state_group, 0) + 1
    pairs = [
        (_state_group_refcount_key(namespace, state_group), -delta)
        for state_group, delta in counts.items()
    ]
    get_embedded_engine(engine_name).increment_counters_batch(pairs)


def get_referenced_state_groups_batch(
    engine_name: str | None, namespace: str, state_groups: list[int]
) -> set[int]:
    """Returns the subset of `state_groups` whose reference count is > 0 --
    the embedded-engine equivalent of the SQL `get_referenced_state_groups`
    reverse lookup, backed by the counter instead of a scan/index.
    """
    from synapse.synapse_rust.mtxdb_engine import batch_get

    if not state_groups:
        return set()
    keys = [
        _state_group_refcount_key(namespace, state_group)
        for state_group in state_groups
    ]
    key_to_group = dict(zip(keys, state_groups))
    found = batch_get(keys)
    referenced = set()
    for key, value in found:
        value = bytes(value)
        if len(value) != 8:
            raise RuntimeError("invalid state_group_refcount record")
        (count,) = struct.unpack(">q", value)
        if count > 0:
            referenced.add(key_to_group[bytes(key)])
    return referenced
