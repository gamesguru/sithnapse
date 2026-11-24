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

"""Unit tests for `embedded_event_auth_chains.py`'s closure-walk logic,
against a fake in-memory mtxdb engine -- these exercise the module's own
logic (walk correctness, gap handling, cache-generation invalidation)
independent of the real Rust engine or a running homeserver.
"""

from types import ModuleType
from unittest import TestCase

from synapse.storage.databases.main.embedded_event_auth_chains import (
    ClosureCache,
    IncompleteAuthGraph,
    bump_room_generation,
    embed_auth_edges_batch,
    get_forward_reachable_short_ids,
    get_or_create_short_ids,
)

ROOM_ID = "!room:example.org"


class FakeEngine(ModuleType):
    """A tiny in-memory stand-in for `synapse_rust.mtxdb_engine`, scoped to
    just the auth-chain-closure primitives this module calls. Subclasses
    `ModuleType` so it can stand in where `get_embedded_engine` promises a
    module.
    """

    def __init__(self, name: str = "fake_mtxdb_engine") -> None:
        super().__init__(name)
        self._counters: dict[str, int] = {}
        self._forward: dict[tuple[str, str], int] = {}
        self._reverse: dict[tuple[str, int], str] = {}
        self._edges: dict[tuple[str, int], list[int]] = {}
        self._children: dict[tuple[str, int], list[int]] = {}
        self._purged: set[str] = set()

    def get_or_create_short_ids(self, room_id: str, event_ids: list[str]) -> list[int]:
        out = []
        for event_id in event_ids:
            key = (room_id, event_id)
            if key in self._forward:
                out.append(self._forward[key])
                continue
            counter_key = room_id
            next_id = self._counters.get(counter_key, 0) + 1
            self._counters[counter_key] = next_id
            self._forward[key] = next_id
            self._reverse[(room_id, next_id)] = event_id
            out.append(next_id)
        return out

    def resolve_short_ids_to_event_ids(
        self, room_id: str, short_ids: list[int]
    ) -> list[str | None]:
        return [self._reverse.get((room_id, s)) for s in short_ids]

    def auth_chain_edges_get(
        self, room_id: str, short_ids: list[int]
    ) -> list[list[int] | None]:
        return [self._edges.get((room_id, s)) for s in short_ids]

    def auth_chain_edges_put(
        self, room_id: str, rows: list[tuple[int, list[int]]]
    ) -> None:
        for short_id, auth_short_ids in rows:
            self._edges[(room_id, short_id)] = list(auth_short_ids)

    def auth_chain_children_get(
        self, room_id: str, short_ids: list[int]
    ) -> list[list[int] | None]:
        return [self._children.get((room_id, s)) for s in short_ids]

    def auth_chain_children_append(
        self, room_id: str, rows: list[tuple[int, list[int]]]
    ) -> None:
        for parent_short_id, new_children in rows:
            key = (room_id, parent_short_id)
            existing = self._children.setdefault(key, [])
            for child in new_children:
                if child not in existing:
                    existing.append(child)

    def auth_chain_purge_room(self, room_id: str) -> None:
        self._forward = {k: v for k, v in self._forward.items() if k[0] != room_id}
        self._reverse = {k: v for k, v in self._reverse.items() if k[0] != room_id}
        self._edges = {k: v for k, v in self._edges.items() if k[0] != room_id}
        self._children = {k: v for k, v in self._children.items() if k[0] != room_id}
        self._counters.pop(room_id, None)


class ClosureWalkTests(TestCase):
    def setUp(self) -> None:
        self.engine = FakeEngine()
        import synapse.storage.databases.embedded_engine as embedded_engine_module

        self._real_get_embedded_engine = embedded_engine_module.get_embedded_engine
        embedded_engine_module.get_embedded_engine = lambda engine_name: self.engine
        self.cache = ClosureCache()

    def tearDown(self) -> None:
        import synapse.storage.databases.embedded_engine as embedded_engine_module

        embedded_engine_module.get_embedded_engine = self._real_get_embedded_engine

    def _embed_graph(self, graph: dict[str, list[str]]) -> dict[str, int]:
        event_ids = list(graph)
        short_ids = get_or_create_short_ids(None, ROOM_ID, event_ids)
        short_id_of = dict(zip(event_ids, short_ids))
        embed_auth_edges_batch(
            None,
            ROOM_ID,
            [
                (short_id_of[event_id], [short_id_of[parent] for parent in parents])
                for event_id, parents in graph.items()
            ],
        )
        return short_id_of

    def test_diamond_shaped_graph_and_leaf_with_zero_auth_events(self) -> None:
        # create -> (nothing)
        # a -> create
        # b -> create
        # c -> a, b   (diamond: c's closure should include create only once)
        ids = self._embed_graph(
            {
                "$create": [],
                "$a": ["$create"],
                "$b": ["$create"],
                "$c": ["$a", "$b"],
            }
        )
        create_id, a_id, b_id, c_id = (ids[k] for k in ("$create", "$a", "$b", "$c"))

        closure = self.cache.get_closure(None, ROOM_ID, c_id)
        self.assertEqual(set(closure), {a_id, b_id, create_id})
        # Ancestors only -- c itself is not included.
        self.assertNotIn(c_id, closure)

        # The leaf's own closure is empty (zero auth events), not an error.
        leaf_closure = self.cache.get_closure(None, ROOM_ID, create_id)
        self.assertEqual(len(leaf_closure), 0)

    def test_missing_edge_raises_and_is_not_cached(self) -> None:
        a_id, missing_id = get_or_create_short_ids(None, ROOM_ID, ["$a", "$missing"])
        embed_auth_edges_batch(None, ROOM_ID, [(a_id, [missing_id])])

        with self.assertRaises(IncompleteAuthGraph) as raised:
            self.cache.get_closure(None, ROOM_ID, a_id)

        self.assertEqual(raised.exception.args, ("$missing",))

        # An incomplete walk must not be cached: retrying must raise again,
        # not silently return a partial closure.
        with self.assertRaises(IncompleteAuthGraph):
            self.cache.get_closure(None, ROOM_ID, a_id)

    def test_deep_uncached_chain_does_not_recurse(self) -> None:
        chain_length = 1_500
        graph = {
            f"$event{i}": ([f"$event{i - 1}"] if i else []) for i in range(chain_length)
        }
        ids = self._embed_graph(graph)

        closure = self.cache.get_closure(
            None, ROOM_ID, ids[f"$event{chain_length - 1}"]
        )

        self.assertEqual(len(closure), chain_length - 1)
        self.assertNotIn(ids[f"$event{chain_length - 1}"], closure)

    def test_cycle_uses_incomplete_auth_graph_failure_path(self) -> None:
        ids = self._embed_graph({"$a": ["$b"], "$b": ["$a"]})

        with self.assertRaises(IncompleteAuthGraph):
            self.cache.get_closure(None, ROOM_ID, ids["$a"])

    def test_cache_generation_invalidation_on_purge(self) -> None:
        ids = self._embed_graph({"$create": [], "$a": ["$create"]})
        create_id, a_id = ids["$create"], ids["$a"]

        first = self.cache.get_closure(None, ROOM_ID, a_id)
        self.assertEqual(set(first), {create_id})

        # Purge the room: bump generation first (as purge_events.py must),
        # then delete the underlying skeleton.
        bump_room_generation(ROOM_ID)
        self.engine.auth_chain_purge_room(ROOM_ID)

        # Re-allocate short ids in the "new" room generation. An unused
        # placeholder alloc first shifts every subsequent id so
        # `create2_id` is guaranteed *not* to numerically equal
        # `create_id` -- otherwise a stale-cache bug could coincidentally
        # produce the right-looking set after the counter resets.
        get_or_create_short_ids(None, ROOM_ID, ["$placeholder"])
        # Allocate `$a` first so it reuses its pre-purge short id; the stale
        # cache entry for (old_generation, a_id) would be hit if the
        # generation were not part of the key.
        ids2 = self._embed_graph({"$a": ["$create2"], "$create2": []})
        create2_id, a2_id = ids2["$create2"], ids2["$a"]
        self.assertEqual(a2_id, a_id)
        self.assertNotEqual(create2_id, create_id)

        second = self.cache.get_closure(None, ROOM_ID, a2_id)
        # The post-purge lookup must recompute against the new graph, not
        # serve the pre-purge cached bitmap for (old_generation, a_id).
        self.assertEqual(set(second), {create2_id})
        self.assertNotEqual(set(second), set(first))

    def test_eviction_during_batch_does_not_look_like_a_gap(self) -> None:
        ids = self._embed_graph(
            {"$create": [], "$a": ["$create"], "$b": ["$a"], "$c": ["$a"]}
        )
        create_id, a_id, b_id, c_id = (ids[k] for k in ("$create", "$a", "$b", "$c"))
        # Cache only `$a`'s closure, then shrink the LRU so that computing `$b`
        # evicts it before `$c` is walked.
        self.cache.get_closure(None, ROOM_ID, a_id)
        self.cache._max_size = 1

        results = self.cache.get_closures_batch(None, ROOM_ID, [b_id, c_id])
        self.assertEqual(set(results[b_id]), {a_id, create_id})
        self.assertEqual(set(results[c_id]), {a_id, create_id})

    def test_get_closures_batch_shares_memo_across_roots(self) -> None:
        ids = self._embed_graph(
            {
                "$create": [],
                "$a": ["$create"],
                "$b": ["$a"],
                "$c": ["$a"],
            }
        )
        create_id, a_id, b_id, c_id = (ids[k] for k in ("$create", "$a", "$b", "$c"))

        results = self.cache.get_closures_batch(None, ROOM_ID, [b_id, c_id])
        self.assertEqual(set(results[b_id]), {a_id, create_id})
        self.assertEqual(set(results[c_id]), {a_id, create_id})


class ForwardReachabilityTests(TestCase):
    """Tests for the V2.1 mtxdb-only forward BFS."""

    def setUp(self) -> None:
        self.engine = FakeEngine()
        import synapse.storage.databases.embedded_engine as embedded_engine_module

        self._real_get_embedded_engine = embedded_engine_module.get_embedded_engine
        embedded_engine_module.get_embedded_engine = lambda engine_name: self.engine

    def tearDown(self) -> None:
        import synapse.storage.databases.embedded_engine as embedded_engine_module

        embedded_engine_module.get_embedded_engine = self._real_get_embedded_engine

    def _short_id(self, event_id: str) -> int:
        return get_or_create_short_ids(None, ROOM_ID, [event_id])[0]

    def _embed_graph(self, graph: dict[str, list[str]]) -> dict[str, int]:
        event_ids = list(graph)
        short_ids = get_or_create_short_ids(None, ROOM_ID, event_ids)
        short_id_of = dict(zip(event_ids, short_ids))
        embed_auth_edges_batch(
            None,
            ROOM_ID,
            [
                (short_id_of[event_id], [short_id_of[parent] for parent in parents])
                for event_id, parents in graph.items()
            ],
        )
        return short_id_of

    def test_linear_chain_forward_reachable(self) -> None:
        # create <- a <- b <- c  (each arrow: "auths")
        graph = {
            "$create": [],
            "$a": ["$create"],
            "$b": ["$a"],
            "$c": ["$b"],
        }
        ids = self._embed_graph(graph)
        create_id, a_id, b_id, c_id = (ids[k] for k in graph)

        reachable = get_forward_reachable_short_ids(None, ROOM_ID, [create_id])
        # Seed-inclusive (see the function docstring): the starting event
        # itself is part of the forward-reachable set, matching the v2.1
        # chain-cover/rezzy forward-pass contract the conflicted-subgraph
        # intersection depends on.
        self.assertEqual(reachable, {create_id, a_id, b_id, c_id})

    def test_diamond_forward_reachable(self) -> None:
        graph = {
            "$create": [],
            "$a": ["$create"],
            "$b": ["$create"],
            "$c": ["$a", "$b"],
        }
        ids = self._embed_graph(graph)
        create_id, a_id, b_id, c_id = (ids[k] for k in graph)

        reachable = get_forward_reachable_short_ids(None, ROOM_ID, [create_id])
        self.assertEqual(reachable, {create_id, a_id, b_id, c_id})

    def test_leaf_has_no_forward_reachable_events(self) -> None:
        graph = {"$create": [], "$a": ["$create"]}
        a_id = self._embed_graph(graph)["$a"]

        reachable = get_forward_reachable_short_ids(None, ROOM_ID, [a_id])
        self.assertEqual(reachable, {a_id})

    def test_candidate_prune_bounds_walk_to_intersectable_universe(self) -> None:
        """Children outside `candidate_short_ids` are dead weight (they
        cannot intersect a backwards-reachable set the caller computed
        first) and must be neither returned nor expanded -- bounding the
        walk by |candidate| rather than the whole room.
        """
        # create has children a and x; x has its own subtree (y) that must
        # not be reached when x is excluded from the candidate set.
        graph = {
            "$create": [],
            "$a": ["$create"],
            "$x": ["$create"],
            "$y": ["$x"],
        }
        ids = self._embed_graph(graph)
        create_id, a_id, x_id, y_id = (ids[k] for k in graph)

        candidate = {create_id, a_id}
        reachable = get_forward_reachable_short_ids(
            None,
            ROOM_ID,
            [create_id],
            candidate_short_ids=candidate,
        )
        self.assertEqual(reachable, {create_id, a_id})
        self.assertNotIn(x_id, reachable)
        self.assertNotIn(y_id, reachable)

    def test_candidate_prune_keeps_seeds_even_outside_candidate(self) -> None:
        """The starting events are always returned, regardless of candidate
        membership -- the conflict roots the subgraph intersection circles
        around must never vanish from the forwards side.
        """
        graph = {"$create": [], "$a": ["$create"]}
        ids = self._embed_graph(graph)
        a_id, create_id = ids["$a"], ids["$create"]

        reachable = get_forward_reachable_short_ids(
            None,
            ROOM_ID,
            [a_id],
            candidate_short_ids={create_id},
        )
        self.assertEqual(reachable, {a_id})

    def test_late_child_write_updates_embedded_children(self) -> None:
        """A child persisted (and short-id allocated) *after* the parent
        was already touched must still show up in the forward walk. The
        child's normal embed write appends its inbound edge in mtxdb.
        """
        graph = {"$create": [], "$a": ["$create"]}
        ids = self._embed_graph(graph)
        create_id, a_id = ids["$create"], ids["$a"]

        # First walk: only $a is a child of $create.
        first = get_forward_reachable_short_ids(None, ROOM_ID, [create_id])
        self.assertEqual(first, {create_id, a_id})

        # A new event "$b" is persisted, also authed by $create.
        b_id = self._short_id("$b")
        embed_auth_edges_batch(None, ROOM_ID, [(b_id, [create_id])])

        second = get_forward_reachable_short_ids(None, ROOM_ID, [create_id])
        self.assertEqual(second, {create_id, a_id, b_id})

    def test_candidate_walk_uses_only_mtxdb_edges(self) -> None:
        graph = {
            "$create": [],
            "$a": ["$create"],
            "$b": ["$a"],
            "$c": ["$b"],
            "$d": ["$c"],
        }
        ids = self._embed_graph(graph)
        candidate = set(ids.values())

        reachable = get_forward_reachable_short_ids(
            None,
            ROOM_ID,
            [ids["$create"]],
            candidate_short_ids=candidate,
        )
        self.assertEqual(reachable, candidate)
