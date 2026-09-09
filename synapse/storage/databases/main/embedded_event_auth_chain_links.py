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

"""Mirrors `event_auth_chain_links` (origin_chain_id, origin_sequence_number,
target_chain_id, target_sequence_number -- one directed edge in the auth
chain cover index's DAG of chains) into the embedded mtxdb engine. Exclusive
by configured engine, not a dual-write, same as `embedded_event_to_state_group.py`.

Unlike `event_to_state_groups`, this table is not a point lookup: its real
usage is `_get_chain_links`'s recursive walk ("all chains transitively
reachable from this set, and every edge out of each"), which SQL answers
with `WITH RECURSIVE`. mtxdb has no recursive-query primitive, so
`get_chain_links_batch` here is a flat batch-get -- one round trip per
*layer* of the walk, not per chain -- and `_get_chain_links` in
`event_federation.py` drives the BFS itself, adding each newly-discovered
`target_chain_id` back into the next batch. This keeps the per-item cost
out of the hot loop (see `batch_get_state_hamt_roots`'s docstring for the
same reasoning applied to HAMT root lookups) while still doing the
transitive walk in Python, since mtxdb can't do it in one call. This module
is accordingly a thin pass-through, not an independent implementation --
the key format only needs to agree with itself, in one place.

Every key is namespaced (see `namespace` on each function) for the same
reason as `embedded_event_to_state_group.py` -- multiple homeservers can
share one mtxdb file, and chain_ids restart at 1 for each.

Purge deletes by `(origin_chain_id, origin_sequence_number)` only, exactly
matching the existing SQL behaviour (see `_purge_room_txn`,
`purged_chain_cover_txn`) -- there is deliberately no index/scan on
`target_chain_id` for deletes; a link whose *target* chain gets purged but
whose *origin* chain doesn't is left dangling here exactly as it is in SQL
today (the existing code's own comment: "Hopefully any purged events are
due to a room being fully purged and they will be removed from the
origin_* searches").
"""

from __future__ import annotations

from synapse.storage.databases.embedded_engine import get_embedded_engine
from synapse.storage.databases.main.embedded_common import SyncTier, maybe_sync


def resolve_namespace(store: object) -> str | None:
    """The namespace to pass to `put_chain_links_batch`/`get_chain_links_batch`
    for `store`, or `None` if it should use SQL instead.

    This is the same `_embedded_hamt_namespace if _embedded_hamt_engine else
    None` check every chain-links call site needs; factored out here so it's
    defined once rather than re-typed at each of them. `getattr` (not a
    direct attribute access) because this is called from `@classmethod`s and
    other contexts where `store` isn't guaranteed to have set these -- see
    `events.py`/`events_worker.py`'s `__init__` for where they normally are.
    """
    return (
        store._embedded_hamt_namespace  # type: ignore[attr-defined]
        if getattr(store, "_embedded_hamt_engine", None)
        else None
    )


def put_chain_links_batch(
    engine_name: str | None,
    namespace: str,
    links: list[tuple[int, int, int, int]],
    sync: bool = False,
) -> None:
    """`links`: `(origin_chain_id, origin_sequence_number, target_chain_id,
    target_sequence_number)`.

    `sync`: defaults to `False` -- an fsync here is a whole-device cache
    flush (see the shard-sync discussion), not scoped to this write, so
    it's not paid on every batch. Pass `True` only at call sites that are
    standalone (not already covered by another sync in the same logical
    transaction) and where losing the last few seconds of these edges on a
    crash is unacceptable.
    """
    if not links:
        return
    get_embedded_engine(engine_name).put_auth_chain_links_batch(namespace, links)
    if sync:
        maybe_sync(SyncTier.DURABLE)


def get_chain_links_batch(
    engine_name: str | None, namespace: str, chain_ids: set[int]
) -> dict[int, list[tuple[int, int, int]]]:
    """Returns every edge out of exactly the given `chain_ids` (following
    `target_chain_id`), *not* the transitive closure: `chain_id ->
    [(origin_seq, target_chain_id, target_seq), ...]`. `_get_chain_links`
    drives the transitive walk by feeding each call's discovered
    `target_chain_id`s back in as the next call's `chain_ids`, mirroring
    what the SQL `WITH RECURSIVE` does in one query.

    Like the SQL version, this may return links not reachable from the
    *events* the caller ultimately cares about -- it returns everything
    reachable from the given chain IDs, which callers then filter down.
    """
    if not chain_ids:
        return {}
    return dict(
        get_embedded_engine(engine_name).get_auth_chain_links_batch(
            namespace, list(chain_ids)
        )
    )


def delete_chain_links_batch(
    engine_name: str | None,
    namespace: str,
    origin_chain_seq_pairs: list[tuple[int, int]],
    sync: bool = False,
) -> None:
    """Removes every edge whose `(origin_chain_id, origin_sequence_number)`
    matches one of `origin_chain_seq_pairs` -- the embedded-engine
    equivalent of `DELETE FROM event_auth_chain_links WHERE origin_chain_id
    = ? AND origin_sequence_number = ?`. Deliberately does not touch edges
    where these only appear as the *target* (see module docstring).

    `sync`: see `put_chain_links_batch` -- defaults to `False`, pass `True`
    at standalone call sites (purge, background migration) that aren't
    already covered by another sync in the same logical transaction.
    """
    if not origin_chain_seq_pairs:
        return
    get_embedded_engine(engine_name).delete_auth_chain_links_batch(
        namespace, origin_chain_seq_pairs
    )
    if sync:
        maybe_sync(SyncTier.DURABLE)
