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
against a fake in-memory mtxdb engine and a fake SQL transaction -- these
exercise the module's own logic (walk correctness, is_complete gating,
cold-import, cache-generation invalidation) independent of the real Rust
engine or a running homeserver, per the plan's Verification section.
"""

from types import ModuleType
from typing import Any
from unittest import TestCase

from synapse.storage.databases.main.embedded_event_auth_chains import (
    ClosureCache,
    IncompleteAuthGraph,
    bump_room_generation,
    embed_auth_edges_batch,
    get_embedded_auth_edges_batch,
    get_forward_reachable_short_ids,
    get_or_create_short_ids,
)

NAMESPACE = "test-ns"
ROOM_ID = "!room:example.org"


class FakeEngine(ModuleType):
    """A tiny in-memory stand-in for `synapse_rust.mtxdb_engine`, scoped to
    just the auth-chain-closure primitives this module calls. Subclasses
    `ModuleType` so it can stand in where `get_embedded_engine` promises a
    module.
    """

    def __init__(self, name: str = "fake_mtxdb_engine") -> None:
        super().__init__(name)
        self._counters: dict[tuple[str, str], int] = {}
        self._forward: dict[tuple[str, str, str], int] = {}
        self._reverse: dict[tuple[str, str, int], str] = {}
        self._edges: dict[tuple[str, str, int], list[int]] = {}
        self._children: dict[tuple[str, str, int], list[int]] = {}
        self._purged: set[tuple[str, str]] = set()

    def get_or_create_short_ids(
        self, namespace: str, room_id: str, event_ids: list[str]
    ) -> list[int]:
        out = []
        for event_id in event_ids:
            key = (namespace, room_id, event_id)
            if key in self._forward:
                out.append(self._forward[key])
                continue
            counter_key = (namespace, room_id)
            next_id = self._counters.get(counter_key, 0) + 1
            self._counters[counter_key] = next_id
            self._forward[key] = next_id
            self._reverse[(namespace, room_id, next_id)] = event_id
            out.append(next_id)
        return out

    def resolve_short_ids_to_event_ids(
        self, namespace: str, room_id: str, short_ids: list[int]
    ) -> list[str | None]:
        return [self._reverse.get((namespace, room_id, s)) for s in short_ids]

    def auth_chain_edges_get(
        self, namespace: str, room_id: str, short_ids: list[int]
    ) -> list[list[int] | None]:
        return [self._edges.get((namespace, room_id, s)) for s in short_ids]

    def auth_chain_edges_put(
        self, namespace: str, room_id: str, rows: list[tuple[int, list[int]]]
    ) -> None:
        for short_id, auth_short_ids in rows:
            self._edges[(namespace, room_id, short_id)] = list(auth_short_ids)

    def auth_chain_children_get(
        self, namespace: str, room_id: str, short_ids: list[int]
    ) -> list[list[int] | None]:
        return [self._children.get((namespace, room_id, s)) for s in short_ids]

    def auth_chain_children_append(
        self, namespace: str, room_id: str, rows: list[tuple[int, list[int]]]
    ) -> None:
        for parent_short_id, new_children in rows:
            key = (namespace, room_id, parent_short_id)
            existing = self._children.setdefault(key, [])
            for child in new_children:
                if child not in existing:
                    existing.append(child)

    def auth_chain_purge_room(self, namespace: str, room_id: str) -> None:
        key = (namespace, room_id)
        self._forward = {k: v for k, v in self._forward.items() if k[:2] != key}
        self._reverse = {k: v for k, v in self._reverse.items() if k[:2] != key}
        self._edges = {k: v for k, v in self._edges.items() if k[:2] != key}
        self._children = {k: v for k, v in self._children.items() if k[:2] != key}
        self._counters.pop(key, None)


class FakeTxn:
    """Fakes just the `execute(...).fetchall()`/`.fetchone()` surface
    `_fetch_auth_event_ids_from_sql` needs, backed by a hand-built
    `event_auth` graph.
    """

    def __init__(
        self, auth_edges: dict[str, list[str]], known_events: set[str] | None = None
    ) -> None:
        self._auth_edges = auth_edges
        self._known_events = (
            known_events if known_events is not None else set(auth_edges)
        )
        # Reverse index, derived from auth_edges -- what a real event_auth
        # table's WHERE auth_id = ? query would return.
        self._children_of: dict[str, list[str]] = {}
        for child, parents in auth_edges.items():
            for parent in parents:
                self._children_of.setdefault(parent, []).append(child)
        self._last_rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, parameters: tuple[Any, ...]) -> None:
        if sql.startswith("SELECT auth_id"):
            (event_id,) = parameters
            self._last_rows = [(a,) for a in self._auth_edges.get(event_id, [])]
        elif sql.startswith("SELECT event_id FROM event_auth WHERE auth_id"):
            (auth_id,) = parameters
            self._last_rows = [(c,) for c in self._children_of.get(auth_id, [])]
        elif sql.startswith("SELECT 1 FROM events"):
            (event_id,) = parameters
            self._last_rows = [(1,)] if event_id in self._known_events else []
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._last_rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._last_rows[0] if self._last_rows else None


def _install_fake_engine(
    monkeypatch_target: dict[str, Any], engine: FakeEngine
) -> None:
    import synapse.storage.databases.embedded_engine as embedded_engine_module

    embedded_engine_module.get_embedded_engine = lambda engine_name: engine


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

    def _short_id(self, event_id: str) -> int:
        return get_or_create_short_ids(None, NAMESPACE, ROOM_ID, [event_id])[0]

    def test_diamond_shaped_graph_and_leaf_with_zero_auth_events(self) -> None:
        # create -> (nothing)
        # a -> create
        # b -> create
        # c -> a, b   (diamond: c's closure should include create only once)
        graph = {
            "$create": [],
            "$a": ["$create"],
            "$b": ["$create"],
            "$c": ["$a", "$b"],
        }
        txn = FakeTxn(graph)

        create_id = self._short_id("$create")
        a_id = self._short_id("$a")
        b_id = self._short_id("$b")
        c_id = self._short_id("$c")

        closure = self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, c_id)
        self.assertEqual(set(closure), {a_id, b_id, create_id})
        # Ancestors only -- c itself is not included.
        self.assertNotIn(c_id, closure)

        # The leaf's own closure is empty (zero auth events), not an error.
        leaf_closure = self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, create_id)
        self.assertEqual(len(leaf_closure), 0)

    def test_cold_import_embeds_edges_from_sql_on_first_touch(self) -> None:
        graph = {"$create": [], "$a": ["$create"]}
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")

        # Nothing embedded yet.
        self.assertEqual(
            get_embedded_auth_edges_batch(None, NAMESPACE, ROOM_ID, [a_id])[a_id],
            None,
        )

        closure = self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, a_id)
        self.assertEqual(set(closure), {create_id})

        # Now embedded, idempotently re-embeddable.
        embedded = get_embedded_auth_edges_batch(None, NAMESPACE, ROOM_ID, [a_id])[a_id]
        self.assertEqual(embedded, [create_id])
        embed_auth_edges_batch(None, NAMESPACE, ROOM_ID, [(a_id, [create_id])])
        self.assertEqual(
            get_embedded_auth_edges_batch(None, NAMESPACE, ROOM_ID, [a_id])[a_id],
            [create_id],
        )

    def test_empty_embedded_edges_are_repaired_from_sql(self) -> None:
        """An early empty write must not permanently turn an event into a leaf."""
        graph = {"$create": [], "$a": ["$create"]}
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")

        # Model the race where the events row was visible before its SQL
        # event_auth rows, causing the first writer to record an empty list.
        embed_auth_edges_batch(None, NAMESPACE, ROOM_ID, [(a_id, [])])

        closure = self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, a_id)
        self.assertEqual(set(closure), {create_id})
        self.assertEqual(
            get_embedded_auth_edges_batch(None, NAMESPACE, ROOM_ID, [a_id])[a_id],
            [create_id],
        )

    def test_incomplete_graph_raises_and_is_not_cached(self) -> None:
        # "$a" claims "$missing" as an auth event, but SQL has no
        # event_auth rows *and* no events row for "$missing" -- a genuine
        # gap, not an ordinary miss.
        graph = {"$a": ["$missing"]}
        txn = FakeTxn(graph, known_events={"$a"})
        a_id = self._short_id("$a")

        with self.assertRaises(IncompleteAuthGraph):
            self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, a_id)

        # An incomplete walk must not be cached: retrying (even with the
        # same broken graph) must raise again, not silently return a
        # wrong/partial cached closure.
        with self.assertRaises(IncompleteAuthGraph):
            self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, a_id)

    def test_cache_generation_invalidation_on_purge(self) -> None:
        graph = {"$create": [], "$a": ["$create"]}
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")

        first = self.cache.get_closure(txn, None, NAMESPACE, ROOM_ID, a_id)
        self.assertEqual(set(first), {create_id})

        # Purge the room: bump generation first (as purge_events.py must),
        # then delete the underlying skeleton.
        bump_room_generation(NAMESPACE, ROOM_ID)
        self.engine.auth_chain_purge_room(NAMESPACE, ROOM_ID)

        # Re-allocate short ids in the "new" room generation. An unused
        # placeholder alloc first shifts every subsequent id so
        # `create2_id` is guaranteed *not* to numerically equal
        # `create_id` -- otherwise a stale-cache bug could coincidentally
        # produce the right-looking set after the counter resets.
        self._short_id("$placeholder")
        new_graph = {"$create2": [], "$a": ["$create2"]}
        txn2 = FakeTxn(new_graph)
        create2_id = self._short_id("$create2")
        a2_id = self._short_id("$a")
        self.assertNotEqual(create2_id, create_id)

        second = self.cache.get_closure(txn2, None, NAMESPACE, ROOM_ID, a2_id)
        # The post-purge lookup must recompute against the new graph, not
        # serve the pre-purge cached bitmap for (old_generation, a_id).
        self.assertEqual(set(second), {create2_id})
        self.assertNotEqual(set(second), set(first))

    def test_get_closures_batch_shares_memo_across_roots(self) -> None:
        graph = {
            "$create": [],
            "$a": ["$create"],
            "$b": ["$a"],
            "$c": ["$a"],
        }
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")
        b_id = self._short_id("$b")
        c_id = self._short_id("$c")

        results = self.cache.get_closures_batch(
            txn,
            None,
            NAMESPACE,
            ROOM_ID,
            [b_id, c_id],
        )
        self.assertEqual(set(results[b_id]), {a_id, create_id})
        self.assertEqual(set(results[c_id]), {a_id, create_id})


class ForwardReachabilityTests(TestCase):
    """Tests for the V2.1 addendum's on-demand forward BFS. Unlike
    `ClosureWalkTests`, these deliberately don't pre-warm the embedded
    inbound edges via `ClosureCache` -- `get_forward_reachable_short_ids`
    must be able to discover children straight from SQL's reverse
    `event_auth` query and repair the embedded list as it goes.
    """

    def setUp(self) -> None:
        self.engine = FakeEngine()
        import synapse.storage.databases.embedded_engine as embedded_engine_module

        self._real_get_embedded_engine = embedded_engine_module.get_embedded_engine
        embedded_engine_module.get_embedded_engine = lambda engine_name: self.engine

    def tearDown(self) -> None:
        import synapse.storage.databases.embedded_engine as embedded_engine_module

        embedded_engine_module.get_embedded_engine = self._real_get_embedded_engine

    def _short_id(self, event_id: str) -> int:
        return get_or_create_short_ids(None, NAMESPACE, ROOM_ID, [event_id])[0]

    def test_linear_chain_forward_reachable(self) -> None:
        # create <- a <- b <- c  (each arrow: "auths")
        graph = {
            "$create": [],
            "$a": ["$create"],
            "$b": ["$a"],
            "$c": ["$b"],
        }
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")
        b_id = self._short_id("$b")
        c_id = self._short_id("$c")

        reachable = get_forward_reachable_short_ids(
            txn, None, NAMESPACE, ROOM_ID, [create_id]
        )
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
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")
        b_id = self._short_id("$b")
        c_id = self._short_id("$c")

        reachable = get_forward_reachable_short_ids(
            txn, None, NAMESPACE, ROOM_ID, [create_id]
        )
        self.assertEqual(reachable, {create_id, a_id, b_id, c_id})

    def test_leaf_has_no_forward_reachable_events(self) -> None:
        graph = {"$create": [], "$a": ["$create"]}
        txn = FakeTxn(graph)
        a_id = self._short_id("$a")

        reachable = get_forward_reachable_short_ids(
            txn, None, NAMESPACE, ROOM_ID, [a_id]
        )
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
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")
        x_id = self._short_id("$x")
        y_id = self._short_id("$y")

        candidate = {create_id, a_id}
        reachable = get_forward_reachable_short_ids(
            txn,
            None,
            NAMESPACE,
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
        txn = FakeTxn(graph)
        a_id = self._short_id("$a")
        create_id = self._short_id("$create")

        reachable = get_forward_reachable_short_ids(
            txn,
            None,
            NAMESPACE,
            ROOM_ID,
            [a_id],
            candidate_short_ids={create_id},
        )
        self.assertEqual(reachable, {a_id})

    def test_repairs_embedded_children_discovered_late(self) -> None:
        """A child persisted (and short-id allocated) *after* the parent
        was already touched must still show up in the forward walk -- the
        embedded inbound list is verified/repaired against SQL each call,
        not trusted blindly once populated.
        """
        graph = {"$create": [], "$a": ["$create"]}
        txn = FakeTxn(graph)
        create_id = self._short_id("$create")
        a_id = self._short_id("$a")

        # First walk: only $a is a child of $create.
        first = get_forward_reachable_short_ids(
            txn, None, NAMESPACE, ROOM_ID, [create_id]
        )
        self.assertEqual(first, {create_id, a_id})

        # A new event "$b" is persisted, also authed by $create.
        graph2 = {"$create": [], "$a": ["$create"], "$b": ["$create"]}
        txn2 = FakeTxn(graph2)
        b_id = self._short_id("$b")

        second = get_forward_reachable_short_ids(
            txn2, None, NAMESPACE, ROOM_ID, [create_id]
        )
        self.assertEqual(second, {create_id, a_id, b_id})
