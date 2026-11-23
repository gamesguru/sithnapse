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

"""Congruent-shaped auth-chain closure cache, embedded via mtxdb's
`AuthChain` pool. See `docs/docs/auth-chain-closures-plan.md` (mdb repo)
for the full design this implements.

Two data classes, per the plan's §4 split:

  * Durable, append-only while the room exists: short event ids (`u32`,
    room-scoped) and each event's direct `auth_events` edges (also
    room-scoped short ids). Embedded via the Rust primitives in
    `mtxdb.rs`'s "Auth Chain Closures" section --
    `get_or_create_short_ids`, `auth_chain_edges_get`/`_put`,
    `resolve_short_ids_to_event_ids`, `auth_chain_purge_room`.
  * Disposable, derived: each event's transitive ancestor closure, as a
    `pyroaring.BitMap` (32-bit -- short ids are capped at `u32`).
    RAM-only in v1 (never persisted to mtxdb): a bounded in-process LRU
    keyed by `(room_generation, event_short_id)`.

`room_generation` lives purely in this process (a plain `dict[str, int]`),
never in mtxdb -- a per-room record inside the `AuthChain` collection would
be deleted by `auth_chain_purge_room` itself, which is self-defeating (the
thing meant to invalidate the cache would vanish along with the data it's
invalidating). `bump_room_generation` must be called *before*
`purge_room`, not after -- see `purge_room`'s docstring.

Closure semantics (pinned, see plan §3): a cached closure for event `E` is
the set of short ids of `E`'s *ancestors only* (`E`'s direct auth_events
targets, unioned with their own cached ancestor closures) -- it does
*not* include `E` itself. Every call site must decide whether to add the
starting event's own short id after the lookup, matching Synapse's
existing `include_given` semantics at that call site.
"""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, cast

from pyroaring import BitMap

if TYPE_CHECKING:
    from synapse.synapse_rust.mtxdb_engine import MtxdbTransaction


logger = logging.getLogger(__name__)

# Short ids are `u32`; 0 is never assigned by the Rust side, so it's not a
# valid short id, but no code here relies on that -- it's just documented
# for anyone reading key encodings.
_MAX_SHORT_ID = (1 << 32) - 1


class IncompleteAuthGraph(Exception):
    """Raised internally when a closure walk finds an incomplete auth graph.

    This includes a missing auth-edge record (not an ordinary cache miss) or
    a cycle, which cannot occur in a valid event auth graph. In mtxdb mode
    this indicates corrupted or incompletely migrated data and must surface
    as an error; it must never trigger a SQL fallback. Never caught by a bare
    `except` anywhere in this module; a storage/(de)serialization bug must
    propagate instead of being mistaken for this.
    """


def resolve_namespace(store: object) -> str | None:
    """The namespace to use for this feature's mtxdb calls for `store`, or
    `None` if the legacy SQL backend is configured (no embedded engine).
    """
    return (
        store._embedded_db_namespace  # type: ignore[attr-defined]
        if getattr(store, "_embedded_db_engine", None)
        else None
    )


class AuthChainBackendMismatchError(Exception):
    """The configured auth-edge backend does not hold the migrated graph."""


def configured_backend(hs: object) -> str:
    """mtxdb is authoritative whenever its engine is configured."""
    return "mtxdb" if hs.config.database.embedded_db_engine else "sql"  # type: ignore[attr-defined]


async def check_backend(hs: object) -> None:
    """Refuse startup when the auth-edge data has not been migrated to the
    configured authoritative backend.

    An empty database can adopt either backend because it has no event graph
    to migrate. Once events exist, switching is an explicit offline operation.
    """
    store = hs.get_datastores().main  # type: ignore[attr-defined]
    configured = configured_backend(hs)

    def _txn(txn: Any) -> str:
        txn.execute("SELECT backend FROM auth_chain_backend WHERE lock = 'X'")
        row = txn.fetchone()
        stored = row[0] if row else "sql"
        if stored != configured:
            txn.execute("SELECT 1 FROM events LIMIT 1")
            if txn.fetchone() is None:
                txn.execute("DELETE FROM auth_chain_backend")
                txn.execute(
                    "INSERT INTO auth_chain_backend (lock, backend) VALUES ('X', ?)",
                    (configured,),
                )
                return configured
        return str(stored)

    stored = await store.db_pool.runInteraction("check_auth_chain_backend", _txn)
    if stored != configured:
        raise AuthChainBackendMismatchError(
            f"Auth edges are stored in {stored!r} but this server is configured "
            f"for {configured!r}. Refusing to start: stop Synapse and run "
            f"`synapse_auth_edges_migrate --to {configured}` with this config, "
            "or restore the previous embedded_db configuration."
        )


async def migrate_auth_chain_edges(
    hs: object, *, batch_size: int = 1000
) -> dict[str, int]:
    """Offline SQL -> mtxdb import. Stop every Synapse process first.

    Each room is cleared and rebuilt so an interrupted run can be repeated
    without retaining stale inbound children. The backend marker is changed
    only after outbound and inbound records have been checked against SQL.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if not hs.config.database.embedded_db_engine:  # type: ignore[attr-defined]
        raise ValueError("configure embedded_db.engine for the mtxdb destination")

    store = hs.get_datastores().main  # type: ignore[attr-defined]
    engine_name = hs.config.database.embedded_db_engine  # type: ignore[attr-defined]
    namespace = (
        hs.config.database.embedded_db_namespace  # type: ignore[attr-defined]
        or hs.hostname  # type: ignore[attr-defined]
    )

    def _read_marker(txn: Any) -> str:
        txn.execute("SELECT backend FROM auth_chain_backend WHERE lock = 'X'")
        row = txn.fetchone()
        return str(row[0]) if row else "sql"

    current_backend = await store.db_pool.runInteraction(
        "auth_chain_migrate_read_marker", _read_marker
    )
    if current_backend == "mtxdb":
        return {"already_migrated": 1, "rooms": 0, "state_events": 0, "auth_edges": 0}
    if current_backend != "sql":
        raise RuntimeError(
            f"Unrecognized auth-chain backend marker: {current_backend!r}"
        )

    def _rejected_metadata_pending(txn: Any) -> bool:
        txn.execute(
            "SELECT 1 FROM background_updates "
            "WHERE update_name = 'rejected_events_metadata'"
        )
        return txn.fetchone() is not None

    if await store.db_pool.runInteraction(
        "auth_chain_migrate_check_bg_updates", _rejected_metadata_pending
    ):
        raise RuntimeError(
            "The rejected_events_metadata background update is still queued, so "
            "SQL event_auth lacks some rejected state events' auth edges. Let it "
            "finish (run Synapse with the SQL backend) before migrating."
        )

    def _read_rooms(txn: Any) -> list[str]:
        txn.execute("SELECT room_id FROM rooms ORDER BY room_id")
        return [room_id for (room_id,) in txn.fetchall()]

    rooms = await store.db_pool.runInteraction("auth_chain_migrate_rooms", _read_rooms)
    stats = {"rooms": 0, "state_events": 0, "auth_edges": 0}

    from synapse.storage.database import make_in_list_sql_clause
    from synapse.storage.databases.main.embedded_common import Pool, sync_now

    for room_id in rooms:
        bump_room_generation(namespace, room_id)
        purge_room(engine_name, namespace, room_id)
        sync_now(pools=[Pool.STATE])
        last_event_id = ""

        while True:

            def _read_batch(txn: Any) -> tuple[list[str], dict[str, list[str]]]:
                # Union both sources: `event_auth` covers every imported edge
                # (including rejected state events, which may not be in
                # `state_events`), while `state_events` adds the durable empty
                # marker for state events whose auth_events list is empty
                # (e.g. the room create event).
                txn.execute(
                    "SELECT event_id FROM ("
                    "  SELECT event_id FROM state_events WHERE room_id = ?"
                    "  UNION"
                    "  SELECT event_id FROM event_auth WHERE room_id = ?"
                    ") AS ids WHERE event_id > ? "
                    "ORDER BY event_id LIMIT ?",
                    (room_id, room_id, last_event_id, batch_size),
                )
                event_ids = [event_id for (event_id,) in txn.fetchall()]
                if not event_ids:
                    return [], {}

                clause, args = make_in_list_sql_clause(
                    txn.database_engine, "event_id", event_ids
                )
                txn.execute(
                    "SELECT event_id, auth_id FROM event_auth WHERE room_id = ? AND "
                    + clause,
                    (room_id, *args),
                )
                auth_events: dict[str, list[str]] = {
                    event_id: [] for event_id in event_ids
                }
                for event_id, auth_id in txn.fetchall():
                    auth_events[event_id].append(auth_id)
                return event_ids, auth_events

            event_ids, auth_events = await store.db_pool.runInteraction(
                "auth_chain_migrate_batch", _read_batch
            )
            if not event_ids:
                break

            all_ids = list(
                dict.fromkeys(
                    event_ids
                    + [
                        auth_id
                        for event_id in event_ids
                        for auth_id in auth_events[event_id]
                    ]
                )
            )
            short_ids = get_or_create_short_ids(
                engine_name, namespace, room_id, all_ids
            )
            short_id_of = dict(zip(all_ids, short_ids))
            rows = [
                (short_id_of[event_id], [short_id_of[a] for a in auth_events[event_id]])
                for event_id in event_ids
            ]
            embed_auth_edges_batch(engine_name, namespace, room_id, rows)
            embedded = get_embedded_auth_edges_batch(
                engine_name, namespace, room_id, [short_id for short_id, _ in rows]
            )
            for (event_id, auth_ids), (short_id, _) in zip(
                ((event_id, auth_events[event_id]) for event_id in event_ids), rows
            ):
                expected = [short_id_of[auth_id] for auth_id in auth_ids]
                if embedded[short_id] != expected:
                    raise RuntimeError(
                        f"Auth-edge verification failed for {event_id} in {room_id}"
                    )

            stats["state_events"] += len(event_ids)
            stats["auth_edges"] += sum(
                len(auth_events[event_id]) for event_id in event_ids
            )
            last_event_id = event_ids[-1]

        def _read_dangling(txn: Any) -> list[str]:
            # Every auth target must itself have an outbound record, or the
            # runtime would later report IncompleteAuthGraph for it.
            txn.execute(
                "SELECT DISTINCT ea.auth_id FROM event_auth AS ea "
                "WHERE ea.room_id = ? "
                "AND NOT EXISTS (SELECT 1 FROM state_events AS se "
                "                WHERE se.event_id = ea.auth_id) "
                "AND NOT EXISTS (SELECT 1 FROM event_auth AS e2 "
                "                WHERE e2.event_id = ea.auth_id) "
                "LIMIT 5",
                (room_id,),
            )
            return [auth_id for (auth_id,) in txn.fetchall()]

        dangling = await store.db_pool.runInteraction(
            "auth_chain_migrate_dangling", _read_dangling
        )
        if dangling:
            raise RuntimeError(
                f"Room {room_id} has auth references with no outbound auth-event "
                f"record, e.g. {dangling}; refusing to migrate"
            )

        sync_now(pools=[Pool.STATE])
        stats["rooms"] += 1

    # Check that every SQL inbound edge is represented in mtxdb, and that no
    # stale child survived the room rebuild.
    for room_id in rooms:
        last_event_id = ""
        while True:

            def _read_parent_batch(txn: Any) -> tuple[list[str], dict[str, list[str]]]:
                txn.execute(
                    "SELECT event_id FROM ("
                    "  SELECT event_id FROM state_events WHERE room_id = ?"
                    "  UNION"
                    "  SELECT auth_id AS event_id FROM event_auth WHERE room_id = ?"
                    ") AS ids WHERE event_id > ? "
                    "ORDER BY event_id LIMIT ?",
                    (room_id, room_id, last_event_id, batch_size),
                )
                parent_ids = [event_id for (event_id,) in txn.fetchall()]
                if not parent_ids:
                    return [], {}
                clause, args = make_in_list_sql_clause(
                    txn.database_engine, "auth_id", parent_ids
                )
                txn.execute(
                    "SELECT auth_id, event_id FROM event_auth WHERE room_id = ? AND "
                    + clause,
                    (room_id, *args),
                )
                children: dict[str, list[str]] = {
                    event_id: [] for event_id in parent_ids
                }
                for parent_id, child_id in txn.fetchall():
                    children[parent_id].append(child_id)
                return parent_ids, children

            parent_ids, children = await store.db_pool.runInteraction(
                "auth_chain_migrate_verify_children", _read_parent_batch
            )
            if not parent_ids:
                break
            parent_short_ids = get_or_create_short_ids(
                engine_name, namespace, room_id, parent_ids
            )
            all_child_ids = list(
                dict.fromkeys(
                    child for parent in parent_ids for child in children[parent]
                )
            )
            child_short_ids = dict(
                zip(
                    all_child_ids,
                    get_or_create_short_ids(
                        engine_name, namespace, room_id, all_child_ids
                    ),
                )
            )
            actual = get_embedded_auth_children_batch(
                engine_name, namespace, room_id, parent_short_ids
            )
            for parent_id, parent_short_id in zip(parent_ids, parent_short_ids):
                expected = sorted(
                    child_short_ids[child_id] for child_id in children[parent_id]
                )
                if sorted(actual[parent_short_id]) != expected:
                    raise RuntimeError(
                        f"Auth-child verification failed for {parent_id} in {room_id}"
                    )
            last_event_id = parent_ids[-1]

    def _set_marker(txn: Any) -> None:
        txn.execute("UPDATE auth_chain_backend SET backend = 'mtxdb' WHERE lock = 'X'")
        if txn.rowcount == 0:
            txn.execute(
                "INSERT INTO auth_chain_backend (lock, backend) VALUES ('X', 'mtxdb')"
            )

    await store.db_pool.runInteraction("auth_chain_migrate_set_marker", _set_marker)
    return stats


def _engine(engine_name: str | None):  # type: ignore[no-untyped-def]
    from synapse.storage.databases.embedded_engine import get_embedded_engine

    return get_embedded_engine(engine_name)


# -----------------------------------------------------------------------------
# §1: Short event ids
# -----------------------------------------------------------------------------


def get_or_create_short_ids(
    engine_name: str | None, namespace: str, room_id: str, event_ids: list[str]
) -> list[int]:
    """Room-scoped `event_id -> u32 short_id`, allocating as needed.
    Thin pass-through to the Rust primitive -- see its docstring in
    `mtxdb.rs` for the crash-recovery protocol this relies on.
    """
    if not event_ids:
        return []
    return _engine(engine_name).get_or_create_short_ids(namespace, room_id, event_ids)


def resolve_event_ids_to_short_ids(
    engine_name: str | None, namespace: str, room_id: str, event_ids: list[str]
) -> list[int | None]:
    """Resolve existing short ids without allocating, for read-only workers."""
    if not event_ids:
        return []
    return _engine(engine_name).resolve_event_ids_to_short_ids(
        namespace, room_id, event_ids
    )


def resolve_short_ids_to_event_ids(
    engine_name: str | None, namespace: str, room_id: str, short_ids: list[int]
) -> list[str | None]:
    """Room-scoped batch reverse lookup. `None` for an unresolved/dangling
    short id (never an error) -- callers must filter these, not treat them
    as failures (plan §4).
    """
    if not short_ids:
        return []
    return _engine(engine_name).resolve_short_ids_to_event_ids(
        namespace, room_id, short_ids
    )


# -----------------------------------------------------------------------------
# §2: Direct auth edges
# -----------------------------------------------------------------------------


def embed_auth_edges_batch(
    engine_name: str | None,
    namespace: str,
    room_id: str,
    rows: list[tuple[int, list[int]]],
    *,
    transaction: MtxdbTransaction | None = None,
) -> None:
    """`rows`: `(event_short_id, [auth_short_id, ...])`. Idempotent: safe to
    call again with the same data (a retried persist, or a repeated offline
    import re-embedding an already-embedded event).

    Writes *both* directions of the graph: the outbound `event_short_id ->
    [auth_short_id, ...]` list this row describes, and -- for each
    `auth_short_id` -- appends `event_short_id` to that auth event's
    inbound (child) list. The inbound direction has no SQL source of its
    own; it's derived purely from the outbound edges as they're embedded,
    which is why it must be written here rather than lazily reconstructed
    later (there's no `event_auth`-shaped table to reconstruct it from
    that isn't just this same data transposed). See the V2.1 addendum
    below for why the inbound direction exists at all: ancestor bitmaps
    can't answer the forward-reachability question the V2.1
    conflicted-subgraph algorithm needs.

    A persistence caller may pass its staged mtxdb transaction so edges and
    event JSON become visible at the same boundary. Standalone imports write
    directly through the engine.
    """
    if not rows:
        return
    if transaction is not None:
        transaction.auth_chain_edges_put(namespace, room_id, rows)
        return

    engine = _engine(engine_name)
    engine.auth_chain_edges_put(namespace, room_id, rows)

    child_rows: list[tuple[int, list[int]]] = []
    for event_short_id, auth_short_ids in rows:
        for auth_short_id in auth_short_ids:
            child_rows.append((auth_short_id, [event_short_id]))
    if child_rows:
        engine.auth_chain_children_append(namespace, room_id, child_rows)


def get_embedded_auth_edges_batch(
    engine_name: str | None, namespace: str, room_id: str, short_ids: list[int]
) -> dict[int, list[int] | None]:
    """`None` for a short id with no embedded edge record (corruption after
    the cutover); an empty list is a genuine leaf (zero auth events).
    """
    if not short_ids:
        return {}
    results = _engine(engine_name).auth_chain_edges_get(namespace, room_id, short_ids)
    return dict(zip(short_ids, results))


def get_embedded_auth_children_batch(
    engine_name: str | None, namespace: str, room_id: str, short_ids: list[int]
) -> dict[int, list[int]]:
    """Inbound (child) edges: for each parent short id, the children known
    to directly name it as an auth event so far. Missing/`None` and
    `Some([])` are the same information here (see
    `auth_chain_children_get`'s docstring in `mtxdb.rs`) -- both normalize
    to `[]`. Outbound edges distinguish the two: `None` is a gap.
    """
    if not short_ids:
        return {}
    results = _engine(engine_name).auth_chain_children_get(
        namespace, room_id, short_ids
    )
    return {
        short_id: (children if children is not None else [])
        for short_id, children in zip(short_ids, results)
    }


# -----------------------------------------------------------------------------
# §4: Purge
# -----------------------------------------------------------------------------

# Purely in-process; deliberately never persisted to mtxdb (see module
# docstring). Keyed by `(namespace, room_id)` since one process may serve
# multiple namespaces (e.g. trial tests).
_room_generations: dict[tuple[str, str], int] = {}


def _room_generation(namespace: str, room_id: str) -> int:
    return _room_generations.get((namespace, room_id), 0)


def bump_room_generation(namespace: str, room_id: str) -> None:
    """Invalidates every RAM-cached closure for this room. Must be called
    *before* `purge_room`, not after: bumping first means a concurrent
    read that lands between the two calls at worst recomputes from data
    that's about to be deleted (wasted work, safe). Bumping after deleting
    would let a concurrent read serve an already-stale closure for the
    entire window between the delete completing and the generation bump
    landing.
    """
    key = (namespace, room_id)
    _room_generations[key] = _room_generations.get(key, 0) + 1


def purge_room(engine_name: str | None, namespace: str, room_id: str) -> None:
    """Deletes the entire durable skeleton (counter, short-id mappings,
    edges) for one room. Caller must have already called
    `bump_room_generation` for this room first -- this function does not
    do so itself, and does not touch any cache-generation state (that's
    purely in-process, per the module docstring).
    """
    _engine(engine_name).auth_chain_purge_room(namespace, room_id)


# -----------------------------------------------------------------------------
# §3: Closure bitmap cache (RAM-only for v1)
# -----------------------------------------------------------------------------


class ClosureCache:
    """A bounded in-process LRU of `(room_generation, event_short_id) ->
    BitMap`, plus the direct-edge BFS that populates it.

    Not thread-safe beyond the GIL's own serialization of individual
    dict/OrderedDict operations -- matches the rest of the embedded-engine
    code, which assumes single-process (`embedded_db_engine` rejects
    multi-worker deployments). A race between two callers computing the
    same closure just duplicates work (both compute the same
    content-deterministic bitmap); the last one to insert into the LRU
    wins, harmlessly.
    """

    def __init__(self, max_size: int = 10000) -> None:
        self._max_size = max_size
        # OrderedDict as a simple LRU: move-to-end on access, pop oldest
        # on overflow. Deliberately not synapse.util.caches.lrucache's
        # LruCache -- that one is wired to a HomeServer's Clock/metrics
        # registry, which this module (plain functions, no store/HS
        # handle) doesn't carry. A follow-up can size this via the
        # existing `*_cache_capacity` config convention (plan §6).
        #
        # Keyed by (namespace, room_id, generation, short_id) -- NOT just
        # (generation, short_id). short_id is only unique within one room's
        # own mtxdb collection (every room's counter independently starts
        # at 1), and generation defaults to 0 for every room that's never
        # been purged, so two different rooms' short_id=1, 2, 3... land on
        # the exact same (generation, short_id) pair. A single process-wide
        # ClosureCache instance keyed without namespace/room_id will serve
        # one room's cached ancestor closure for another room's identical
        # short_id -- silently, since a cache hit never re-walks or
        # re-verifies anything. Namespace and room_id must be part of the
        # key for this cache to be safe with more than one room per process.
        self._cache: OrderedDict[tuple[str, str, int, int], BitMap] = OrderedDict()
        # Reads run on database threads (see event_federation.py), so the
        # check-then-evict sequences below must be serialized.
        self._lock = threading.RLock()

    def _get(
        self, namespace: str, room_id: str, generation: int, short_id: int
    ) -> BitMap | None:
        key = (namespace, room_id, generation, short_id)
        value = self._cache.get(key)
        if value is not None:
            self._cache.move_to_end(key)
        return value

    def _put(
        self,
        namespace: str,
        room_id: str,
        generation: int,
        short_id: int,
        closure: BitMap,
    ) -> None:
        key = (namespace, room_id, generation, short_id)
        self._cache[key] = closure
        self._cache.move_to_end(key)
        while len(self._cache) > self._max_size:
            self._cache.popitem(last=False)

    def clear(self) -> None:
        """Test/debug helper -- production invalidation goes through
        `bump_room_generation`, not this.
        """
        with self._lock:
            self._cache.clear()

    def get_closure(
        self,
        engine_name: str | None,
        namespace: str,
        room_id: str,
        event_short_id: int,
    ) -> BitMap:
        """Returns event_short_id's ancestor closure (not including
        event_short_id itself), computing and caching it if necessary.

        Raises `IncompleteAuthGraph` if the walk hits an event with no
        embedded auth-edge record; after the cutover that is a data gap, not
        an ordinary cache miss.
        """
        return self.get_closures_batch(
            engine_name, namespace, room_id, [event_short_id]
        )[event_short_id]

    def get_closures_batch(
        self,
        engine_name: str | None,
        namespace: str,
        room_id: str,
        event_short_ids: list[int],
    ) -> dict[int, BitMap]:
        """Batch form of `get_closure`, sharing one memo dict across all
        requested roots so overlapping ancestor subgraphs are only walked
        once per call.
        """
        with self._lock:
            return self._get_closures_batch_locked(
                engine_name, namespace, room_id, event_short_ids
            )

    def _get_closures_batch_locked(
        self,
        engine_name: str | None,
        namespace: str,
        room_id: str,
        event_short_ids: list[int],
    ) -> dict[int, BitMap]:
        generation = _room_generation(namespace, room_id)
        # Closures found in the LRU while preloading. `_put` below may evict
        # them before the walk reaches them; the preload skipped loading their
        # edges because they were cached, so the walk must keep using these
        # request-local references rather than re-reading the LRU.
        pinned: dict[int, BitMap] = {}
        roots_to_load: list[int] = []
        for short_id in event_short_ids:
            cached = self._get(namespace, room_id, generation, short_id)
            if cached is None:
                roots_to_load.append(short_id)
            else:
                pinned[short_id] = cached
        direct_edges = self._load_direct_edges(
            engine_name, namespace, room_id, roots_to_load, generation, pinned
        )

        memo: dict[int, tuple[BitMap, bool]] = {}
        out: dict[int, BitMap] = {}
        for short_id in event_short_ids:
            cached = pinned.get(short_id)
            if cached is not None:
                out[short_id] = cached
                continue
            try:
                closure, is_complete = self._walk(
                    namespace, room_id, short_id, generation, memo, direct_edges, pinned
                )
            except IncompleteAuthGraph as e:
                missing = e.args[0] if e.args else short_id
                if isinstance(missing, int):
                    resolved = resolve_short_ids_to_event_ids(
                        engine_name, namespace, room_id, [missing]
                    )
                    if resolved and resolved[0] is not None:
                        missing = resolved[0]
                raise IncompleteAuthGraph(missing) from None
            if not is_complete:
                # Report the event id when we can, so the surfaced error names
                # the gap rather than an opaque per-room short id.
                resolved = resolve_short_ids_to_event_ids(
                    engine_name, namespace, room_id, [short_id]
                )
                event_id = resolved[0] if resolved else None
                raise IncompleteAuthGraph(
                    event_id if event_id is not None else short_id
                )
            self._put(namespace, room_id, generation, short_id, closure)
            out[short_id] = closure
        return out

    def _load_direct_edges(
        self,
        engine_name: str | None,
        namespace: str,
        room_id: str,
        root_short_ids: list[int],
        generation: int,
        pinned: dict[int, BitMap],
    ) -> dict[int, list[int] | None]:
        """Collect direct auth edges from mtxdb for the requested closures.

        An explicit empty list is the durable leaf marker. `None` means the
        event has no record at all, which the caller reports as an incomplete
        graph.
        """
        direct_edges: dict[int, list[int] | None] = {}
        pending = set(root_short_ids)
        seen: set[int] = set()

        while pending:
            layer = pending - seen
            pending.clear()
            if not layer:
                continue
            seen.update(layer)

            uncached: set[int] = set()
            for short_id in layer:
                cached = pinned.get(short_id)
                if cached is None:
                    cached = self._get(namespace, room_id, generation, short_id)
                if cached is None:
                    uncached.add(short_id)
                else:
                    pinned[short_id] = cached
            layer = uncached
            if not layer:
                continue

            embedded = get_embedded_auth_edges_batch(
                engine_name, namespace, room_id, list(layer)
            )
            for short_id, auth_short_ids in embedded.items():
                if auth_short_ids is None:
                    direct_edges[short_id] = None
                else:
                    direct_edges[short_id] = auth_short_ids
                    pending.update(auth_short_ids)

        return direct_edges

    def _walk(
        self,
        namespace: str,
        room_id: str,
        event_short_id: int,
        generation: int,
        memo: dict[int, tuple[BitMap, bool]],
        direct_edges: dict[int, list[int] | None],
        pinned: dict[int, BitMap],
    ) -> tuple[BitMap, bool]:
        """Iteratively walk direct auth edges and union child closures.

        Returns `(closure, is_complete)`; incomplete results are never cached
        by the caller. An iterative depth-first traversal avoids Python's
        recursion limit for long, valid auth chains.
        """
        if event_short_id in memo:
            return memo[event_short_id]

        def cached_closure(short_id: int) -> BitMap | None:
            cached = pinned.get(short_id)
            if cached is None:
                cached = self._get(namespace, room_id, generation, short_id)
            return cached

        root_cached = cached_closure(event_short_id)
        if root_cached is not None:
            memo[event_short_id] = (root_cached, True)
            return root_cached, True

        root_edges = direct_edges.get(event_short_id)
        if root_edges is None:
            logger.debug(
                "auth-chain closure walk: genuine gap at short_id=%s "
                "(room=%s namespace=%s) -- no embedded edges; treating as "
                "incomplete",
                event_short_id,
                room_id,
                namespace,
            )
            raise IncompleteAuthGraph(event_short_id)

        # Frames hold [event id, next child index, accumulated closure,
        # completeness]. `active` tracks only the current DFS path, so a cycle
        # is treated as an incomplete auth graph rather than recursing forever.
        stack: list[list[object]] = [[event_short_id, 0, BitMap(), True]]
        active = {event_short_id}
        while stack:
            frame = stack[-1]
            short_id = cast(int, frame[0])
            auth_short_ids = direct_edges.get(short_id)
            assert auth_short_ids is not None
            child_index = cast(int, frame[1])

            if child_index < len(auth_short_ids):
                child_id = auth_short_ids[child_index]
                frame[1] = child_index + 1
                closure = cast(BitMap, frame[2])
                closure.add(child_id)

                child_result = memo.get(child_id)
                if child_result is not None:
                    closure |= child_result[0]
                    frame[3] = cast(bool, frame[3]) and child_result[1]
                    continue

                child_cached = cached_closure(child_id)
                if child_cached is not None:
                    memo[child_id] = (child_cached, True)
                    closure |= child_cached
                    continue

                if child_id in active:
                    # Cycles cannot be produced by valid event auth edges. Use
                    # the established incomplete-graph path for corrupt data.
                    raise IncompleteAuthGraph(child_id)

                child_edges = direct_edges.get(child_id)
                if child_edges is None:
                    logger.debug(
                        "auth-chain closure walk: genuine gap at short_id=%s "
                        "(room=%s namespace=%s) -- no embedded edges; treating "
                        "as incomplete",
                        child_id,
                        room_id,
                        namespace,
                    )
                    raise IncompleteAuthGraph(child_id)

                logger.debug(
                    "auth-chain closure walk: short_id=%s has %d direct auth "
                    "edge(s): %s",
                    child_id,
                    len(child_edges),
                    child_edges,
                )
                active.add(child_id)
                stack.append([child_id, 0, BitMap(), True])
                continue

            closure = cast(BitMap, frame[2])
            complete = cast(bool, frame[3])
            memo[short_id] = (closure, complete)
            active.remove(short_id)
            stack.pop()
            if stack:
                parent = stack[-1]
                parent_closure = cast(BitMap, parent[2])
                parent_closure |= closure
                parent[3] = cast(bool, parent[3]) and complete

        return memo[event_short_id]


# -----------------------------------------------------------------------------
# §5 addendum: V2.1 conflicted-subgraph forward reachability
# -----------------------------------------------------------------------------
#
# The V2.1 state-res algorithm's conflicted subgraph needs *forward*
# reachability (descendants of a conflicted event), which an ancestor-only
# closure bitmap cannot answer. This is deliberately NOT a persisted
# descendant closure -- see ../rezzy's `reachability.rs`: a persisted
# forward transitive closure (its `ForwardReachabilityIndex`) is correct
# only for a sealed DAG snapshot, since appending one event invalidates
# the descendant closure of every one of its ancestors. For a live room,
# the right shape is an ephemeral, on-demand exact BFS over the embedded
# inbound (child) edges (`auth_chain_children_get`/`_append`), built fresh
# per request and discarded -- never cached, never persisted.


def get_forward_reachable_short_ids(
    engine_name: str | None,
    namespace: str,
    room_id: str,
    start_short_ids: list[int],
    candidate_short_ids: set[int] | None = None,
) -> set[int]:
    """Ephemeral BFS over mtxdb inbound (child) edges. This walk performs
    no SQL reads; outbound auth edges are imported when the backwards closure
    is built, and embedding each event appends its inbound child edges.

    Returns the set of short ids forward-reachable from `start_short_ids`,
    **including the starting events themselves**; when `candidate_short_ids`
    is given, the forward spread beyond the starts is restricted to its
    members. Not cached, not persisted; callers should treat the result as
    good for one request only.

    Two contract details, both required by the v2.1 conflicted-subgraph
    computation this feeds (see the §5 addendum above):

    1. *Seed inclusion*: the starting events are part of the result. The
       conflicted subgraph is `backwards ∩ forwards`, and both reference
       formulations -- the chain-cover v2.1 path in `event_federation.py`
       (whose forward-reachable set covers each conflicted event's own
       position) and rezzy's `compute_v2_1_conflicted_subgraph` (whose
       `forward_reachable_ids` is documented "seeds included") -- include
       the conflicted events in the forwards side. A seed-exclusive result
       would silently drop the conflicted events from the intersection:
       the very events state res v2.1 exists to isolate. `ClosureCache`'s
       ancestor closures are deliberately *not* changed (their call sites
       add the roots back via `include_given`); this forward walk needs the
       opposite default for the v2.1 path. Callers computing plain
       descendants can subtract the starts themselves.
    2. *Candidate pruning*: when `candidate_short_ids` is provided, children
       that are not members of it are neither returned nor expanded. For the
       subgraph's `backwards ∩ forwards`, a descendant that isn't an
       ancestor of *any* conflicted event provably can't be in the
       intersection (if it were in some conflicted event's ancestor set, its
       ancestor would be too, so it is already in the candidate set), so
       those children are dead weight -- skipping them bounds the walk by
       |candidate| instead of the whole room. Starts are always expanded and
       always in the result regardless of candidate membership.
    """
    starting = set(start_short_ids)
    result: set[int] = set(starting)
    seen: set[int] = set()
    frontier = list(starting)
    while frontier:
        current_layer = set(frontier) - seen
        frontier = []
        if not current_layer:
            continue

        children_by_parent = get_embedded_auth_children_batch(
            engine_name, namespace, room_id, list(current_layer)
        )
        seen.update(current_layer)
        for children in children_by_parent.values():
            for child in children:
                if child in seen:
                    continue
                if candidate_short_ids is not None and child not in candidate_short_ids:
                    continue
                result.add(child)
                frontier.append(child)
    return result
