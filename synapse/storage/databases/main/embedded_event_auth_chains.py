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
from collections import OrderedDict
from typing import Any, Protocol

from pyroaring import BitMap


class _AuthChainTxn(Protocol):
    """The minimal SQL-transaction surface the closure walk depends on.

    Structural (PEP 544) on purpose, so tests can drive the walk with a
    tiny fake txn instead of a full `LoggingTransaction`, and the walk
    itself stays decoupled from any concrete cursor type. The methods
    match what `_fetch_auth_event_ids_from_sql` /
    `_fetch_child_event_ids_from_sql` actually call, nothing more.
    """

    def execute(self, sql: str, parameters: tuple[Any, ...]) -> None: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...


logger = logging.getLogger(__name__)

# Short ids are `u32`; 0 is never assigned by the Rust side, so it's not a
# valid short id, but no code here relies on that -- it's just documented
# for anyone reading key encodings.
_MAX_SHORT_ID = (1 << 32) - 1


class IncompleteAuthGraph(Exception):
    """Raised internally when a closure walk hits a genuinely missing auth
    edge (not an ordinary cache miss) -- the narrow, explicit signal that
    should trigger falling back to the legacy SQL algorithm at the
    `event_federation.py` call sites. Never caught by a bare `except`
    anywhere in this module; a storage/(de)serialization bug must
    propagate instead of being mistaken for this.
    """


def resolve_namespace(store: object) -> str | None:
    """The namespace to use for this feature's mtxdb calls for `store`, or
    `None` if it should use SQL instead. Same shape as
    `embedded_event_auth_chain_links.resolve_namespace` -- kept as a
    separate function (not reused directly) because this feature has its
    own opt-in gate in principle, even though today it shares the same
    `embedded_hamt_engine`/`embedded_hamt_namespace` config surface.
    """
    return (
        store._embedded_hamt_namespace  # type: ignore[attr-defined]
        if getattr(store, "_embedded_hamt_engine", None)
        else None
    )


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
) -> None:
    """`rows`: `(event_short_id, [auth_short_id, ...])`. Idempotent: safe to
    call again with the same data (e.g. a retried post-commit dual-write,
    or the cold-import path in §3 re-embedding an already-embedded event).

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

    Callers must only invoke this strictly after the corresponding SQL
    `event_auth` insert has *committed* (plan §2's write-timing
    requirement) -- never from within that same transaction, since a
    rollback would otherwise leave ghost mtxdb records with no matching
    SQL row. A failure here is expected to happen occasionally and must
    not be treated as fatal: log/metric it and move on, since the
    cold-import path below repairs the gap the next time this event is
    visited by a closure walk.
    """
    if not rows:
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
    """`None` for a short id with no embedded edges yet (the cold-import
    signal); an empty list is a genuine leaf (zero auth events) and is
    final, not a signal to fetch SQL.
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
    to `[]`, never a cold-import signal the way outbound edges' `None` is.
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
    BitMap`, plus the cold-import-aware BFS that populates it.

    Not thread-safe beyond the GIL's own serialization of individual
    dict/OrderedDict operations -- matches the rest of the embedded-engine
    code, which assumes single-process (`embedded_hamt_engine` rejects
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
        self._cache.clear()

    def get_closure(
        self,
        txn: _AuthChainTxn,
        engine_name: str | None,
        namespace: str,
        room_id: str,
        event_short_id: int,
    ) -> BitMap:
        """Returns event_short_id's ancestor closure (not including
        event_short_id itself), computing and caching it if necessary.

        Raises `IncompleteAuthGraph` if the walk hits a genuinely missing
        auth edge for some event reachable from `event_short_id` that
        SQL's `event_auth` also has no row for (a real data gap, not an
        ordinary cache miss) -- callers must let this propagate up to
        `event_federation.py`'s narrow `_NoChainCoverIndex` fallback, not
        swallow it here.
        """
        generation = _room_generation(namespace, room_id)
        cached = self._get(namespace, room_id, generation, event_short_id)
        if cached is not None:
            return cached

        closure, is_complete = self._walk(
            txn, engine_name, namespace, room_id, event_short_id, generation, {}
        )
        if not is_complete:
            raise IncompleteAuthGraph(event_short_id)
        self._put(namespace, room_id, generation, event_short_id, closure)
        return closure

    def get_closures_batch(
        self,
        txn: _AuthChainTxn,
        engine_name: str | None,
        namespace: str,
        room_id: str,
        event_short_ids: list[int],
    ) -> dict[int, BitMap]:
        """Batch form of `get_closure`, sharing one memo dict across all
        requested roots so overlapping ancestor subgraphs are only walked
        once per call.
        """
        generation = _room_generation(namespace, room_id)
        memo: dict[int, tuple[BitMap, bool]] = {}
        out: dict[int, BitMap] = {}
        for short_id in event_short_ids:
            cached = self._get(namespace, room_id, generation, short_id)
            if cached is not None:
                out[short_id] = cached
                continue
            closure, is_complete = self._walk(
                txn, engine_name, namespace, room_id, short_id, generation, memo
            )
            if not is_complete:
                raise IncompleteAuthGraph(short_id)
            self._put(namespace, room_id, generation, short_id, closure)
            out[short_id] = closure
        return out

    def _walk(
        self,
        txn: _AuthChainTxn,
        engine_name: str | None,
        namespace: str,
        room_id: str,
        event_short_id: int,
        generation: int,
        memo: dict[int, tuple[BitMap, bool]],
    ) -> tuple[BitMap, bool]:
        """Depth-first walk over direct auth edges, unioning children's
        (cached or freshly-walked) ancestor closures. Returns
        `(closure, is_complete)`; an incomplete result (a genuinely
        missing auth event, per `_fetch_direct_edges`) is never cached by
        the caller.
        """
        if event_short_id in memo:
            return memo[event_short_id]

        cached = self._get(namespace, room_id, generation, event_short_id)
        if cached is not None:
            memo[event_short_id] = (cached, True)
            return cached, True

        auth_short_ids = self._fetch_direct_edges(
            txn, engine_name, namespace, room_id, event_short_id
        )
        if auth_short_ids is None:
            # A genuine gap: SQL has no event_auth rows for this event
            # either (see _fetch_direct_edges). Distinct from "not
            # embedded yet", which _fetch_direct_edges resolves via
            # cold-import before ever returning None.
            logger.debug(
                "auth-chain closure walk: genuine gap at short_id=%s "
                "(room=%s namespace=%s) -- no SQL event_auth rows and no "
                "embedded edges; treating as incomplete",
                event_short_id,
                room_id,
                namespace,
            )
            result = (BitMap(), False)
            memo[event_short_id] = result
            return result

        logger.debug(
            "auth-chain closure walk: short_id=%s has %d direct auth edge(s): %s",
            event_short_id,
            len(auth_short_ids),
            auth_short_ids,
        )

        closure = BitMap()
        complete = True
        for auth_short_id in auth_short_ids:
            closure.add(auth_short_id)
            child_closure, child_complete = self._walk(
                txn, engine_name, namespace, room_id, auth_short_id, generation, memo
            )
            closure |= child_closure
            complete = complete and child_complete

        memo[event_short_id] = (closure, complete)
        return closure, complete

    def _fetch_direct_edges(
        self,
        txn: _AuthChainTxn,
        engine_name: str | None,
        namespace: str,
        room_id: str,
        event_short_id: int,
    ) -> list[int] | None:
        """Returns `event_short_id`'s direct auth-edge short ids, embedding
        them from SQL first if they aren't in mtxdb yet (plan §3's
        cold-import path). Returns `None` only if SQL itself has no
        `event_auth` rows for this event AND it isn't a known short id at
        all -- a genuine gap, not an ordinary cache/embed miss.
        """
        embedded = get_embedded_auth_edges_batch(
            engine_name, namespace, room_id, [event_short_id]
        )[event_short_id]
        if embedded is not None:
            return embedded

        event_id = resolve_short_ids_to_event_ids(
            engine_name, namespace, room_id, [event_short_id]
        )[0]
        if event_id is None:
            # Dangling short id (its event was individually purged, or the
            # reverse mapping is missing for another reason) -- not
            # resolvable to anything SQL can help with either.
            return None

        auth_event_ids = _fetch_auth_event_ids_from_sql(txn, event_id)
        if auth_event_ids is None:
            # No event_auth rows at all for this event id -- treat as a
            # genuine leaf only if the event itself is known to SQL;
            # otherwise it's a real gap.
            return None

        auth_short_ids = (
            get_or_create_short_ids(
                engine_name, namespace, room_id, list(auth_event_ids)
            )
            if auth_event_ids
            else []
        )
        embed_auth_edges_batch(
            engine_name, namespace, room_id, [(event_short_id, auth_short_ids)]
        )
        return auth_short_ids


def _fetch_auth_event_ids_from_sql(
    txn: _AuthChainTxn, event_id: str
) -> list[str] | None:
    """Direct one-hop `auth_events` for `event_id`, from the durable SQL
    `event_auth` table (event_id, auth_id -- one row per direct edge,
    written at persist time; see `events.py`). Returns `[]` for a genuine
    leaf (e.g. the room create event), `None` if `event_id` is not known
    to SQL at all (a real gap, distinct from "leaf").
    """
    txn.execute("SELECT auth_id FROM event_auth WHERE event_id = ?", (event_id,))
    rows = txn.fetchall()
    if rows:
        return [auth_id for (auth_id,) in rows]

    txn.execute("SELECT 1 FROM events WHERE event_id = ?", (event_id,))
    exists = txn.fetchone()
    if exists is None:
        return None
    return []


def _fetch_child_event_ids_from_sql(txn: _AuthChainTxn, event_id: str) -> list[str]:
    """The reverse of `_fetch_auth_event_ids_from_sql`: every event that
    directly names `event_id` as one of its auth events, per SQL
    `event_auth` (`auth_id = event_id`). Used only to verify/repair the
    embedded inbound (child) edge list stays complete -- see
    `_fetch_and_verify_children` -- never to build a persisted index of
    its own.
    """
    txn.execute("SELECT event_id FROM event_auth WHERE auth_id = ?", (event_id,))
    return [child_id for (child_id,) in txn.fetchall()]


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


def _fetch_and_verify_children(
    txn: _AuthChainTxn,
    engine_name: str | None,
    namespace: str,
    room_id: str,
    parent_short_id: int,
) -> list[int]:
    """Returns `parent_short_id`'s known children, repairing the embedded
    inbound list against SQL `event_auth` first if needed. Unlike outbound
    edges (written once, atomically, alongside the event that owns them),
    inbound edges accumulate incrementally as *other* events get embedded
    -- so a child appended to SQL after this parent's own outbound-edge
    walk last touched it can be missing here even for an otherwise "warm"
    parent. This check is what keeps the forward BFS complete rather than
    silently under-approximating the conflicted subgraph.
    """
    embedded = get_embedded_auth_children_batch(
        engine_name, namespace, room_id, [parent_short_id]
    )[parent_short_id]

    parent_event_id = resolve_short_ids_to_event_ids(
        engine_name, namespace, room_id, [parent_short_id]
    )[0]
    if parent_event_id is None:
        # Dangling short id -- nothing in SQL to cross-check against either.
        return embedded

    sql_child_ids = _fetch_child_event_ids_from_sql(txn, parent_event_id)
    if not sql_child_ids:
        return embedded

    sql_child_short_ids = get_or_create_short_ids(
        engine_name, namespace, room_id, sql_child_ids
    )
    missing = [c for c in sql_child_short_ids if c not in embedded]
    if missing:
        _engine(engine_name).auth_chain_children_append(
            namespace, room_id, [(parent_short_id, missing)]
        )
        embedded = embedded + missing
    return embedded


def get_forward_reachable_short_ids(
    txn: _AuthChainTxn,
    engine_name: str | None,
    namespace: str,
    room_id: str,
    start_short_ids: list[int],
    candidate_short_ids: set[int] | None = None,
) -> set[int]:
    """Ephemeral BFS over embedded inbound (child) edges, verified/repaired
    against SQL as it goes (see `_fetch_and_verify_children`).

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
        current = frontier.pop()
        if current in seen:
            continue
        seen.add(current)
        for child in _fetch_and_verify_children(
            txn, engine_name, namespace, room_id, current
        ):
            if child in seen:
                continue
            if candidate_short_ids is not None and child not in candidate_short_ids:
                continue
            result.add(child)
            frontier.append(child)
    return result
