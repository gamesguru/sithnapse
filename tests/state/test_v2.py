#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2023 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#

import itertools
import json
import os
from typing import (
    Collection,
    Iterable,
    Mapping,
    TypeVar,
)

import attr

from twisted.internet import defer

from synapse.api.constants import EventTypes, JoinRules, Membership
from synapse.api.room_versions import RoomVersions
from synapse.event_auth import auth_types_for_event
from synapse.events import EventBase, make_event_from_dict
from synapse.state.v2 import (
    _get_auth_chain_difference,
    _get_power_level_for_sender,
    lexicographical_topological_sort,
    resolve_events_with_store,
)
from synapse.storage.databases.main.event_federation import StateDifference
from synapse.types import EventID, StateMap
from synapse.util.duration import Duration

from tests import unittest
from tests.test_utils.event_builders import make_test_event

ALICE = "@alice:example.com"
BOB = "@bob:example.com"
CHARLIE = "@charlie:example.com"
EVELYN = "@evelyn:example.com"
ZARA = "@zara:example.com"

ROOM_ID = "!test:example.com"

MEMBERSHIP_CONTENT_JOIN = {"membership": Membership.JOIN}
MEMBERSHIP_CONTENT_BAN = {"membership": Membership.BAN}


ORIGIN_SERVER_TS = 0


class FakeClock:
    async def sleep(self, duration: Duration) -> None:
        return None


class FakeEvent:
    """A fake event we use as a convenience.

    NOTE: Again as a convenience we use "node_ids" rather than event_ids to
    refer to events. The event_id has node_id as localpart and example.com
    as domain.
    """

    def __init__(
        self,
        id: str,
        sender: str,
        type: str,
        state_key: str | None,
        content: Mapping[str, object],
    ):
        self.node_id = id
        self.event_id = EventID(id, "example.com").to_string()
        self.sender = sender
        self.type = type
        self.state_key = state_key
        self.content = content
        self.room_id = ROOM_ID

    def to_event(self, auth_events: list[str], prev_events: list[str]) -> EventBase:
        """Given the auth_events and prev_events, convert to a Frozen Event

        Args:
            auth_events: list of event_ids
            prev_events: list of event_ids

        Returns:
            FrozenEvent
        """
        global ORIGIN_SERVER_TS

        ts = ORIGIN_SERVER_TS
        ORIGIN_SERVER_TS = ORIGIN_SERVER_TS + 1

        event_dict = {
            "auth_events": [(a, {}) for a in auth_events],
            "prev_events": [(p, {}) for p in prev_events],
            "event_id": self.event_id,
            "sender": self.sender,
            "type": self.type,
            "content": self.content,
            "origin_server_ts": ts,
            "room_id": ROOM_ID,
        }

        if self.state_key is not None:
            event_dict["state_key"] = self.state_key

        return make_test_event(event_dict)


# All graphs start with this set of events
INITIAL_EVENTS = [
    FakeEvent(
        id="CREATE",
        sender=ALICE,
        type=EventTypes.Create,
        state_key="",
        content={"creator": ALICE},
    ),
    FakeEvent(
        id="IMA",
        sender=ALICE,
        type=EventTypes.Member,
        state_key=ALICE,
        content=MEMBERSHIP_CONTENT_JOIN,
    ),
    FakeEvent(
        id="IPOWER",
        sender=ALICE,
        type=EventTypes.PowerLevels,
        state_key="",
        content={"users": {ALICE: 100}},
    ),
    FakeEvent(
        id="IJR",
        sender=ALICE,
        type=EventTypes.JoinRules,
        state_key="",
        content={"join_rule": JoinRules.PUBLIC},
    ),
    FakeEvent(
        id="IMB",
        sender=BOB,
        type=EventTypes.Member,
        state_key=BOB,
        content=MEMBERSHIP_CONTENT_JOIN,
    ),
    FakeEvent(
        id="IMC",
        sender=CHARLIE,
        type=EventTypes.Member,
        state_key=CHARLIE,
        content=MEMBERSHIP_CONTENT_JOIN,
    ),
    FakeEvent(
        id="IMZ",
        sender=ZARA,
        type=EventTypes.Member,
        state_key=ZARA,
        content=MEMBERSHIP_CONTENT_JOIN,
    ),
    FakeEvent(
        id="START", sender=ZARA, type=EventTypes.Message, state_key=None, content={}
    ),
    FakeEvent(
        id="END", sender=ZARA, type=EventTypes.Message, state_key=None, content={}
    ),
]

INITIAL_EDGES = ["START", "IMZ", "IMC", "IMB", "IJR", "IPOWER", "IMA", "CREATE"]


class StateTestCase(unittest.TestCase):
    def test_ban_vs_pl(self) -> None:
        events = [
            FakeEvent(
                id="PA",
                sender=ALICE,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50}},
            ),
            FakeEvent(
                id="MA",
                sender=ALICE,
                type=EventTypes.Member,
                state_key=ALICE,
                content={"membership": Membership.JOIN},
            ),
            FakeEvent(
                id="MB",
                sender=ALICE,
                type=EventTypes.Member,
                state_key=BOB,
                content={"membership": Membership.BAN},
            ),
            FakeEvent(
                id="PB",
                sender=BOB,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50}},
            ),
        ]

        edges = [["END", "MB", "MA", "PA", "START"], ["END", "PB", "PA"]]

        expected_state_ids = ["PA", "MA", "MB"]

        self.do_check(events, edges, expected_state_ids)

    def test_join_rule_evasion(self) -> None:
        events = [
            FakeEvent(
                id="JR",
                sender=ALICE,
                type=EventTypes.JoinRules,
                state_key="",
                content={"join_rules": JoinRules.PRIVATE},
            ),
            FakeEvent(
                id="ME",
                sender=EVELYN,
                type=EventTypes.Member,
                state_key=EVELYN,
                content={"membership": Membership.JOIN},
            ),
        ]

        edges = [["END", "JR", "START"], ["END", "ME", "START"]]

        expected_state_ids = ["JR"]

        self.do_check(events, edges, expected_state_ids)

    def test_offtopic_pl(self) -> None:
        events = [
            FakeEvent(
                id="PA",
                sender=ALICE,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50}},
            ),
            FakeEvent(
                id="PB",
                sender=BOB,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50, CHARLIE: 50}},
            ),
            FakeEvent(
                id="PC",
                sender=CHARLIE,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50, CHARLIE: 0}},
            ),
        ]

        edges = [["END", "PC", "PB", "PA", "START"], ["END", "PA"]]

        expected_state_ids = ["PC"]

        self.do_check(events, edges, expected_state_ids)

    def test_topic_basic(self) -> None:
        events = [
            FakeEvent(
                id="T1", sender=ALICE, type=EventTypes.Topic, state_key="", content={}
            ),
            FakeEvent(
                id="PA1",
                sender=ALICE,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50}},
            ),
            FakeEvent(
                id="T2", sender=ALICE, type=EventTypes.Topic, state_key="", content={}
            ),
            FakeEvent(
                id="PA2",
                sender=ALICE,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 0}},
            ),
            FakeEvent(
                id="PB",
                sender=BOB,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50}},
            ),
            FakeEvent(
                id="T3", sender=BOB, type=EventTypes.Topic, state_key="", content={}
            ),
        ]

        edges = [["END", "PA2", "T2", "PA1", "T1", "START"], ["END", "T3", "PB", "PA1"]]

        expected_state_ids = ["PA2", "T2"]

        self.do_check(events, edges, expected_state_ids)

    def test_topic_reset(self) -> None:
        events = [
            FakeEvent(
                id="T1", sender=ALICE, type=EventTypes.Topic, state_key="", content={}
            ),
            FakeEvent(
                id="PA",
                sender=ALICE,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50}},
            ),
            FakeEvent(
                id="T2", sender=BOB, type=EventTypes.Topic, state_key="", content={}
            ),
            FakeEvent(
                id="MB",
                sender=ALICE,
                type=EventTypes.Member,
                state_key=BOB,
                content={"membership": Membership.BAN},
            ),
        ]

        edges = [["END", "MB", "T2", "PA", "T1", "START"], ["END", "T1"]]

        expected_state_ids = ["T1", "MB", "PA"]

        self.do_check(events, edges, expected_state_ids)

    def test_topic(self) -> None:
        events = [
            FakeEvent(
                id="T1", sender=ALICE, type=EventTypes.Topic, state_key="", content={}
            ),
            FakeEvent(
                id="PA1",
                sender=ALICE,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50}},
            ),
            FakeEvent(
                id="T2", sender=ALICE, type=EventTypes.Topic, state_key="", content={}
            ),
            FakeEvent(
                id="PA2",
                sender=ALICE,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 0}},
            ),
            FakeEvent(
                id="PB",
                sender=BOB,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50}},
            ),
            FakeEvent(
                id="T3", sender=BOB, type=EventTypes.Topic, state_key="", content={}
            ),
            FakeEvent(
                id="MZ1",
                sender=ZARA,
                type=EventTypes.Message,
                state_key=None,
                content={},
            ),
            FakeEvent(
                id="T4", sender=ALICE, type=EventTypes.Topic, state_key="", content={}
            ),
        ]

        edges = [
            ["END", "T4", "MZ1", "PA2", "T2", "PA1", "T1", "START"],
            ["END", "MZ1", "T3", "PB", "PA1"],
        ]

        expected_state_ids = ["T4", "PA2"]

        self.do_check(events, edges, expected_state_ids)

    def test_mainline_sort(self) -> None:
        """Tests that the mainline ordering works correctly."""

        events = [
            FakeEvent(
                id="T1", sender=ALICE, type=EventTypes.Topic, state_key="", content={}
            ),
            FakeEvent(
                id="PA1",
                sender=ALICE,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50}},
            ),
            FakeEvent(
                id="T2", sender=ALICE, type=EventTypes.Topic, state_key="", content={}
            ),
            FakeEvent(
                id="PA2",
                sender=ALICE,
                type=EventTypes.PowerLevels,
                state_key="",
                content={
                    "users": {ALICE: 100, BOB: 50},
                    "events": {EventTypes.PowerLevels: 100},
                },
            ),
            FakeEvent(
                id="PB",
                sender=BOB,
                type=EventTypes.PowerLevels,
                state_key="",
                content={"users": {ALICE: 100, BOB: 50}},
            ),
            FakeEvent(
                id="T3", sender=BOB, type=EventTypes.Topic, state_key="", content={}
            ),
            FakeEvent(
                id="T4", sender=ALICE, type=EventTypes.Topic, state_key="", content={}
            ),
        ]

        edges = [
            ["END", "T3", "PA2", "T2", "PA1", "T1", "START"],
            ["END", "T4", "PB", "PA1"],
        ]

        # We expect T3 to be picked as the other topics are pointing at older
        # power levels. Note that without mainline ordering we'd pick T4 due to
        # it being sent *after* T3.
        expected_state_ids = ["T3", "PA2"]

        self.do_check(events, edges, expected_state_ids)

    def test_state_reset_join_rules_eviction(self) -> None:
        """Reproduces a V2 state reset bug observed in the wild (catgirl.cloud).

        Scenario: Eve joins a public room, then the room switches to
        invite-only. Later, a fork creates a conflict where one arm
        doesn't include Eve's membership (e.g., a server that was offline
        during Eve's join).

        In V2 state res, Eve's join is now in the conflicted set. The
        supplemental merge evaluates it against the resolved state, which
        has join_rules=invite. Eve's join fails auth (she was never invited)
        and is incorrectly evicted from state.

        This is a known V2 deficiency that V2.1 fixes by skipping the
        supplemental merge. Eve joined legitimately under public rules and
        should remain in state regardless of later join_rules changes.
        """
        events = [
            # Eve joins under the initial public join rules.
            FakeEvent(
                id="ME",
                sender=EVELYN,
                type=EventTypes.Member,
                state_key=EVELYN,
                content=MEMBERSHIP_CONTENT_JOIN,
            ),
            # Alice changes join rules to invite-only.
            FakeEvent(
                id="JR2",
                sender=ALICE,
                type=EventTypes.JoinRules,
                state_key="",
                content={"join_rule": JoinRules.INVITE},
            ),
            # Normal activity continues on the main fork.
            FakeEvent(
                id="MZ2",
                sender=ZARA,
                type=EventTypes.Message,
                state_key=None,
                content={},
            ),
            # A stale server sends an event forking from before Eve joined.
            # This server's state does NOT include Eve's membership.
            FakeEvent(
                id="MZ3",
                sender=ZARA,
                type=EventTypes.Message,
                state_key=None,
                content={},
            ),
        ]

        # Main branch: START -> ME -> JR2 -> MZ2 -> END
        # Stale fork:  START -> MZ3 -> END
        # The fork at END merges. State A has Eve joined + invite JR.
        # State B (stale fork) has neither Eve's join nor the JR change.
        # Eve's membership and JR are both conflicted.
        # V2 resolves JR first (control event), picks JR2 (newer ts).
        # Then evaluates ME against resolved state with invite JR.
        # Eve's join fails auth (not invited) -> evicted. This is the bug.
        edges = [
            ["END", "MZ2", "JR2", "ME", "START"],
            ["END", "MZ3", "START"],
        ]

        # BUG: V2 evicts Eve. The expected_state_ids only includes JR2.
        # Eve's membership (ME) is missing because the supplemental merge
        # caused her join to fail auth against invite-only join rules.
        # In a correct implementation (V2.1), ME would also be present.
        expected_state_ids = ["JR2"]

        self.do_check(events, edges, expected_state_ids)

    def test_v2_self_corrects_corrupted_state(self) -> None:
        """Proves that a server with corrupted state (missing a valid member)
        will self-correct when merging with a healthy server.

        Server A (Catgirl) has corrupted state: missing Eve's public join.
        Server B (Healthy) has correct state: includes Eve.

        When the two forks merge at END, Eve's join is in the conflicted
        set. Because the room is public, Eve's join passes auth checks
        in the supplemental merge, and she is RESTORED to the final state.

        This is the proof that heal-room / state re-resolution works.
        """
        events = [
            # Eve joins the public room (only visible to healthy server).
            FakeEvent(
                id="ME",
                sender=EVELYN,
                type=EventTypes.Member,
                state_key=EVELYN,
                content=MEMBERSHIP_CONTENT_JOIN,
            ),
            # Activity on the healthy fork (after Eve joined).
            FakeEvent(
                id="MZ2",
                sender=ZARA,
                type=EventTypes.Message,
                state_key=None,
                content={},
            ),
            # Activity on the corrupted fork (doesn't know about Eve).
            FakeEvent(
                id="MB2",
                sender=BOB,
                type=EventTypes.Message,
                state_key=None,
                content={},
            ),
        ]

        # Healthy fork: START -> ME -> MZ2 -> END (has Eve)
        # Corrupted fork: START -> MB2 -> END (missing Eve)
        # At END, Eve's membership is conflicted. Since join_rules=PUBLIC,
        # the supplemental merge accepts Eve's join. She is RESTORED.
        edges = [
            ["END", "MZ2", "ME", "START"],
            ["END", "MB2", "START"],
        ]

        # Eve's membership SHOULD be in the resolved state because
        # her join passes auth under public join rules.
        expected_state_ids = ["ME"]

        self.do_check(events, edges, expected_state_ids)

    def test_v2_unauthorized_event_not_restored(self) -> None:
        """Proves that state resolution won't blindly restore an event
        from a rogue server if that event is unauthorized.

        Join rules are changed to INVITE-only. Then a rogue fork injects
        Eve's join (which was never invited). When the forks merge, Eve's
        unauthorized join fails auth checks in the supplemental merge and
        is correctly EJECTED from the final state.

        This is the safety proof: self-correction won't accidentally
        restore forged or unauthorized memberships.
        """
        events = [
            # Alice changes join rules to invite-only.
            FakeEvent(
                id="JR2",
                sender=ALICE,
                type=EventTypes.JoinRules,
                state_key="",
                content={"join_rule": JoinRules.INVITE},
            ),
            # Activity on the healthy fork (after JR change).
            FakeEvent(
                id="MZ2",
                sender=ZARA,
                type=EventTypes.Message,
                state_key=None,
                content={},
            ),
            # Rogue fork: Eve joins without an invite (unauthorized).
            # This fork branches from before the JR change, so Eve's
            # join was technically "valid" on the public fork. But after
            # resolution, the JR2 invite-only rule wins (newer ts),
            # and Eve's join fails auth against it.
            FakeEvent(
                id="ME",
                sender=EVELYN,
                type=EventTypes.Member,
                state_key=EVELYN,
                content=MEMBERSHIP_CONTENT_JOIN,
            ),
        ]

        # Healthy fork: START -> JR2 -> MZ2 -> END (invite-only, no Eve)
        # Rogue fork:   START -> ME -> END (Eve joins on public fork)
        # At END, both JR2 and ME are conflicted. V2 resolves JR2 first
        # (control event), then evaluates ME against invite-only rules.
        # Eve's join fails auth (no invite) → EJECTED. Correct!
        edges = [
            ["END", "MZ2", "JR2", "START"],
            ["END", "ME", "START"],
        ]

        # Eve is NOT in the expected state — her unauthorized join
        # is correctly rejected by the supplemental merge.
        expected_state_ids = ["JR2"]

        self.do_check(events, edges, expected_state_ids)

    def do_check(
        self,
        events: list[FakeEvent],
        edges: list[list[str]],
        expected_state_ids: list[str],
    ) -> None:
        """Take a list of events and edges and calculate the state of the
        graph at END, and asserts it matches `expected_state_ids`

        Args:
            events
            edges: A list of chains of event edges, e.g.
                `[[A, B, C]]` are edges A->B and B->C.
            expected_state_ids: The expected state at END, (excluding
                the keys that haven't changed since START).
        """
        # We want to sort the events into topological order for processing.
        graph: dict[str, set[str]] = {}

        fake_event_map: dict[str, FakeEvent] = {}

        for ev in itertools.chain(INITIAL_EVENTS, events):
            graph[ev.node_id] = set()
            fake_event_map[ev.node_id] = ev

        for a, b in pairwise(INITIAL_EDGES):
            graph[a].add(b)

        for edge_list in edges:
            for a, b in pairwise(edge_list):
                graph[a].add(b)

        event_map: dict[str, EventBase] = {}
        state_at_event: dict[str, StateMap[str]] = {}

        # We copy the map as the sort consumes the graph
        graph_copy = {k: set(v) for k, v in graph.items()}

        for node_id in lexicographical_topological_sort(graph_copy, key=lambda e: e):
            fake_event = fake_event_map[node_id]
            event_id = fake_event.event_id

            prev_events = list(graph[node_id])

            state_before: StateMap[str]
            if len(prev_events) == 0:
                state_before = {}
            elif len(prev_events) == 1:
                state_before = dict(state_at_event[prev_events[0]])
            else:
                state_d = resolve_events_with_store(
                    FakeClock(),
                    ROOM_ID,
                    RoomVersions.V2,
                    [state_at_event[n] for n in prev_events],
                    event_map=event_map,
                    state_res_store=TestStateResolutionStore(event_map),
                )

                state_before = self.successResultOf(defer.ensureDeferred(state_d))

            state_after = dict(state_before)
            if fake_event.state_key is not None:
                state_after[(fake_event.type, fake_event.state_key)] = event_id

            # This type ignore is a bit sad. Things we have tried:
            # 1. Define a `GenericEvent` Protocol satisfied by FakeEvent, EventBase and
            #    EventBuilder. But this is Hard because the relevant attributes are
            #    DictProperty[T] descriptors on EventBase but normal Ts on FakeEvent.
            # 2. Define a `GenericEvent` Protocol describing `FakeEvent` only, and
            #    change this function to accept Event | EventBase | EventBuilder.
            #    This seems reasonable to me, but mypy isn't happy. I think that's
            #    a mypy bug, see https://github.com/python/mypy/issues/5570
            # Instead, resort to a type-ignore.
            auth_types = set(auth_types_for_event(RoomVersions.V6, fake_event))  # type: ignore[arg-type]

            auth_events = []
            for key in auth_types:
                if key in state_before:
                    auth_events.append(state_before[key])

            event = fake_event.to_event(auth_events, prev_events)

            state_at_event[node_id] = state_after
            event_map[event_id] = event

        expected_state = {}
        for node_id in expected_state_ids:
            # expected_state_ids are node IDs rather than event IDs,
            # so we have to convert
            event_id = EventID(node_id, "example.com").to_string()
            event = event_map[event_id]

            key = (event.type, event.state_key)

            expected_state[key] = event_id

        start_state = state_at_event["START"]
        end_state = {
            key: value
            for key, value in state_at_event["END"].items()
            if key in expected_state or start_state.get(key) != value
        }

        self.assertEqual(expected_state, end_state)


class LexicographicalTestCase(unittest.TestCase):
    def test_simple(self) -> None:
        graph: dict[str, set[str]] = {
            "l": {"o"},
            "m": {"n", "o"},
            "n": {"o"},
            "o": set(),
            "p": {"o"},
        }

        res = list(lexicographical_topological_sort(graph, key=lambda x: x))

        self.assertEqual(["o", "l", "n", "m", "p"], res)


class SimpleParamStateTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # We build up a simple DAG.

        event_map = {}

        create_event = FakeEvent(
            id="CREATE",
            sender=ALICE,
            type=EventTypes.Create,
            state_key="",
            content={"creator": ALICE},
        ).to_event([], [])
        event_map[create_event.event_id] = create_event

        alice_member = FakeEvent(
            id="IMA",
            sender=ALICE,
            type=EventTypes.Member,
            state_key=ALICE,
            content=MEMBERSHIP_CONTENT_JOIN,
        ).to_event([create_event.event_id], [create_event.event_id])
        event_map[alice_member.event_id] = alice_member

        join_rules = FakeEvent(
            id="IJR",
            sender=ALICE,
            type=EventTypes.JoinRules,
            state_key="",
            content={"join_rule": JoinRules.PUBLIC},
        ).to_event(
            auth_events=[create_event.event_id, alice_member.event_id],
            prev_events=[alice_member.event_id],
        )
        event_map[join_rules.event_id] = join_rules

        # Bob and Charlie join at the same time, so there is a fork
        bob_member = FakeEvent(
            id="IMB",
            sender=BOB,
            type=EventTypes.Member,
            state_key=BOB,
            content=MEMBERSHIP_CONTENT_JOIN,
        ).to_event(
            auth_events=[create_event.event_id, join_rules.event_id],
            prev_events=[join_rules.event_id],
        )
        event_map[bob_member.event_id] = bob_member

        charlie_member = FakeEvent(
            id="IMC",
            sender=CHARLIE,
            type=EventTypes.Member,
            state_key=CHARLIE,
            content=MEMBERSHIP_CONTENT_JOIN,
        ).to_event(
            auth_events=[create_event.event_id, join_rules.event_id],
            prev_events=[join_rules.event_id],
        )
        event_map[charlie_member.event_id] = charlie_member

        self.event_map = event_map
        self.create_event = create_event
        self.alice_member = alice_member
        self.join_rules = join_rules
        self.bob_member = bob_member
        self.charlie_member = charlie_member

        self.state_at_bob = {
            (e.type, e.state_key): e.event_id
            for e in [create_event, alice_member, join_rules, bob_member]
        }

        self.state_at_charlie = {
            (e.type, e.state_key): e.event_id
            for e in [create_event, alice_member, join_rules, charlie_member]
        }

        self.expected_combined_state = {
            (e.type, e.state_key): e.event_id
            for e in [
                create_event,
                alice_member,
                join_rules,
                bob_member,
                charlie_member,
            ]
        }

    def test_event_map_none(self) -> None:
        # Test that we correctly handle passing `None` as the event_map

        state_d = resolve_events_with_store(
            FakeClock(),
            ROOM_ID,
            RoomVersions.V2,
            [self.state_at_bob, self.state_at_charlie],
            event_map=None,
            state_res_store=TestStateResolutionStore(self.event_map),
        )

        state = self.successResultOf(defer.ensureDeferred(state_d))

        self.assert_dict(self.expected_combined_state, state)


class AuthChainDifferenceTestCase(unittest.TestCase):
    """We test that `_get_auth_chain_difference` correctly handles unpersisted
    events.
    """

    def test_simple(self) -> None:
        # Test getting the auth difference for a simple chain with a single
        # unpersisted event:
        #
        #  Unpersisted | Persisted
        #              |
        #           C -|-> B -> A

        a = FakeEvent(
            id="A",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([], [])

        b = FakeEvent(
            id="B",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([a.event_id], [])

        c = FakeEvent(
            id="C",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([b.event_id], [])

        persisted_events = {a.event_id: a, b.event_id: b}
        unpersited_events = {c.event_id: c}

        state_sets = [
            {("a", ""): a.event_id, ("b", ""): b.event_id},
            {("c", ""): c.event_id},
        ]

        store = TestStateResolutionStore(persisted_events)

        diff_d = _get_auth_chain_difference(
            ROOM_ID,
            state_sets,
            unpersited_events,
            store,
            None,
        )
        difference = self.successResultOf(defer.ensureDeferred(diff_d))

        self.assertEqual(difference, {c.event_id})

    def test_multiple_unpersisted_chain(self) -> None:
        # Test getting the auth difference for a simple chain with multiple
        # unpersisted events:
        #
        #  Unpersisted | Persisted
        #              |
        #      D -> C -|-> B -> A

        a = FakeEvent(
            id="A",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([], [])

        b = FakeEvent(
            id="B",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([a.event_id], [])

        c = FakeEvent(
            id="C",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([b.event_id], [])

        d = FakeEvent(
            id="D",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([c.event_id], [])

        persisted_events = {a.event_id: a, b.event_id: b}
        unpersited_events = {c.event_id: c, d.event_id: d}

        state_sets = [
            {("a", ""): a.event_id, ("b", ""): b.event_id},
            {("c", ""): c.event_id, ("d", ""): d.event_id},
        ]

        store = TestStateResolutionStore(persisted_events)

        diff_d = _get_auth_chain_difference(
            ROOM_ID,
            state_sets,
            unpersited_events,
            store,
            None,
        )
        difference = self.successResultOf(defer.ensureDeferred(diff_d))

        self.assertEqual(difference, {d.event_id, c.event_id})

    def test_unpersisted_events_different_sets(self) -> None:
        # Test getting the auth difference for with multiple unpersisted events
        # in different branches:
        #
        #  Unpersisted | Persisted
        #              |
        #     D --> C -|-> B -> A
        #     E ----^ -|---^
        #              |

        a = FakeEvent(
            id="A",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([], [])

        b = FakeEvent(
            id="B",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([a.event_id], [])

        c = FakeEvent(
            id="C",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([b.event_id], [])

        d = FakeEvent(
            id="D",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([c.event_id], [])

        e = FakeEvent(
            id="E",
            sender=ALICE,
            type=EventTypes.Member,
            state_key="",
            content={},
        ).to_event([c.event_id, b.event_id], [])

        persisted_events = {a.event_id: a, b.event_id: b}
        unpersited_events = {c.event_id: c, d.event_id: d, e.event_id: e}

        state_sets = [
            {("a", ""): a.event_id, ("b", ""): b.event_id, ("e", ""): e.event_id},
            {("c", ""): c.event_id, ("d", ""): d.event_id},
        ]

        store = TestStateResolutionStore(persisted_events)

        diff_d = _get_auth_chain_difference(
            ROOM_ID,
            state_sets,
            unpersited_events,
            store,
            None,
        )
        difference = self.successResultOf(defer.ensureDeferred(diff_d))

        self.assertEqual(difference, {d.event_id, e.event_id})

    def test_get_power_level_for_sender(self) -> None:
        """Test that we use the correct definition of `creator` depending
        on room version"""
        store = TestStateResolutionStore({})
        for room_version in [RoomVersions.V10, RoomVersions.V11]:
            create_event = make_test_event(
                {
                    "room_id": ROOM_ID,
                    "sender": ALICE,
                    "type": EventTypes.Create,
                    "state_key": "",
                    "content": {
                        "room_version": room_version.identifier,
                    }
                    # conditionally add 'creator' if the version doesn't use implicit room creators
                    | (
                        {"creator": ALICE}
                        if not room_version.implicit_room_creator
                        else {}
                    ),
                },
                room_version=room_version,
            )
            member_event = make_test_event(
                {
                    "room_id": ROOM_ID,
                    "sender": ALICE,
                    "type": EventTypes.Member,
                    "state_key": ALICE,
                    "content": {
                        "membership": "join",
                    },
                    "auth_events": [create_event.event_id],
                    "prev_events": [create_event.event_id],
                },
                room_version=room_version,
            )
            pl_event = make_test_event(
                {
                    "room_id": ROOM_ID,
                    "sender": ALICE,
                    "type": EventTypes.PowerLevels,
                    "state_key": "",
                    "content": {
                        "users": {
                            ALICE: 100,
                            BOB: 50,
                        },
                        "users_default": 10,
                    },
                    "auth_events": [create_event.event_id, member_event.event_id],
                    "prev_events": [member_event.event_id],
                },
                room_version=room_version,
            )

            event_map = {
                create_event.event_id: create_event,
                member_event.event_id: member_event,
                pl_event.event_id: pl_event,
            }
            want_pls = {
                ALICE: 100,
                BOB: 50,
                CHARLIE: 10,
            }
            for user_id, want_pl in want_pls.items():
                test_event = make_test_event(
                    {
                        "room_id": ROOM_ID,
                        "sender": user_id,
                        "type": EventTypes.Topic,
                        "state_key": "",
                        "content": {"topic": "Test"},
                        "auth_events": [
                            create_event.event_id,
                            member_event.event_id,
                            pl_event.event_id,
                        ],
                        "prev_events": [pl_event.event_id],
                    },
                    room_version=room_version,
                )
                event_map[test_event.event_id] = test_event
                got_pl = self.successResultOf(
                    defer.ensureDeferred(
                        _get_power_level_for_sender(
                            ROOM_ID, test_event.event_id, event_map, store
                        )
                    )
                )
                self.assertEqual(
                    got_pl,
                    want_pl,
                    f"wrong pl for {user_id} on v{room_version.identifier}",
                )

            # the creator alone without PL is 100, everyone else is 0
            want_pls = {
                ALICE: 100,
                BOB: 0,
                CHARLIE: 0,
            }
            for user_id, want_pl in want_pls.items():
                test_event = make_test_event(
                    {
                        "room_id": ROOM_ID,
                        "sender": user_id,
                        "type": EventTypes.Topic,
                        "state_key": "",
                        "content": {"topic": "Test"},
                        "auth_events": [
                            create_event.event_id,
                            member_event.event_id,
                            pl_event.event_id,
                        ],
                        "prev_events": [pl_event.event_id],
                    },
                    room_version=room_version,
                )
                got_pl = self.successResultOf(
                    defer.ensureDeferred(
                        _get_power_level_for_sender(
                            ROOM_ID,
                            test_event.event_id,
                            {
                                test_event.event_id: test_event,
                                create_event.event_id: create_event,
                            },
                            store,
                        )
                    )
                )
                self.assertEqual(
                    got_pl,
                    want_pl,
                    f"wrong pl for {user_id} with no PL event on v{room_version.identifier}",
                )


T = TypeVar("T")


def pairwise(iterable: Iterable[T]) -> Iterable[tuple[T, T]]:
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


@attr.s
class TestStateResolutionStore:
    event_map: dict[str, EventBase] = attr.ib()

    def get_events(
        self, event_ids: Collection[str], allow_rejected: bool = False
    ) -> "defer.Deferred[dict[str, EventBase]]":
        """Get events from the database

        Args:
            event_ids: The event_ids of the events to fetch
            allow_rejected: If True return rejected events.

        Returns:
            Dict from event_id to event.
        """

        return defer.succeed(
            {eid: self.event_map[eid] for eid in event_ids if eid in self.event_map}
        )

    def _get_auth_chain(self, event_ids: Iterable[str]) -> list[str]:
        """Gets the full auth chain for a set of events (including rejected
        events).

        Includes the given event IDs in the result.

        Note that:
            1. All events must be state events.
            2. For v1 rooms this may not have the full auth chain in the
               presence of rejected events

        Args:
            event_ids: The event IDs of the events to fetch the auth
                chain for. Must be state events.
        Returns:
            List of event IDs of the auth chain.
        """

        # Simple DFS for auth chain
        result = set()
        stack = list(event_ids)
        while stack:
            event_id = stack.pop()
            if event_id in result:
                continue

            result.add(event_id)

            event = self.event_map[event_id]
            for aid in event.auth_event_ids():
                stack.append(aid)

        return list(result)

    def get_auth_chain_difference(
        self,
        room_id: str,
        auth_sets: list[set[str]],
        conflicted_state: set[str] | None,
        additional_backwards_reachable_conflicted_events: set[str] | None,
    ) -> "defer.Deferred[StateDifference]":
        chains = [frozenset(self._get_auth_chain(a)) for a in auth_sets]

        common = set(chains[0]).intersection(*chains[1:])
        return defer.succeed(
            StateDifference(
                auth_difference=set(chains[0]).union(*chains[1:]) - common,
                conflicted_subgraph=set(),
            ),
        )


class DAGReplayTestCase(unittest.TestCase):
    """Replay a real-world JSONL DAG through V2 state resolution to reproduce
    the catgirl.cloud state reset where @bot:nutra.tk was evicted."""

    JSONL_PATH = os.path.join(
        os.path.dirname(__file__),
        "remote-dag-tgmfqAWaBc978M80V9_nutra.tk-v11-merged.jsonl",
    )

    def _load_events(self, path: str) -> list[EventBase]:
        """Load JSONL events natively as V11.

        Strips event_id, signatures, and unsigned (non-canonical fields) and
        lets Synapse recompute the event_id from the reference hash. The hashes
        field is preserved as it's part of the reference hash input.
        """
        raw_events = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    raw_events.append(json.loads(line))

        events = []
        for raw in raw_events:
            original_id = raw.pop("event_id", None)
            raw.pop("signatures", None)
            raw.pop("unsigned", None)
            ev = make_event_from_dict(raw, room_version=RoomVersions.V11)
            if original_id:
                assert ev.event_id == original_id, (
                    f"Event ID mismatch: {ev.event_id} != {original_id}"
                )
            events.append(ev)

        events.sort(key=lambda e: (e.depth, e.origin_server_ts))
        return events

    def test_replay_nutra_tk_dag_bot_membership(self) -> None:
        """Replay the full nutra.tk DAG through V2 state resolution.

        Traces @bot:nutra.tk's membership through every fork merge to find
        the exact depth where the bot is evicted from resolved state.

        This reproduces the state reset observed on catgirl.cloud where the
        bot disappeared despite being a legitimate member.
        """
        if not os.path.exists(self.JSONL_PATH):
            self.skipTest(f"JSONL not found: {self.JSONL_PATH}")

        events = self._load_events(self.JSONL_PATH)
        self.assertGreater(len(events), 0)

        room_id = events[0].room_id
        event_map: dict[str, EventBase] = {}
        state_at: dict[str, StateMap[str]] = {}

        target = "@bot:nutra.tk"
        bot_key = (EventTypes.Member, target)
        eviction_depths: list[int] = []
        merge_count = 0

        for ev in events:
            event_map[ev.event_id] = ev
            prev_ids = ev.prev_event_ids()

            state_before: StateMap[str]
            if not prev_ids:
                state_before = {}
            elif len(prev_ids) == 1:
                state_before = dict(state_at.get(prev_ids[0], {}))
            else:
                # FORK MERGE - run V2 state resolution
                state_sets = [state_at[pid] for pid in prev_ids if pid in state_at]
                if len(state_sets) < 2:
                    state_before = dict(state_sets[0]) if state_sets else {}
                else:
                    merge_count += 1
                    bot_in_any = any(bot_key in s for s in state_sets)
                    bot_in_all = all(bot_key in s for s in state_sets)

                    store = TestStateResolutionStore(event_map)
                    state_before = self.successResultOf(
                        defer.ensureDeferred(
                            resolve_events_with_store(
                                FakeClock(),
                                room_id,
                                RoomVersions.V2,
                                state_sets,
                                event_map=event_map,
                                state_res_store=store,
                            )
                        )
                    )

                    bot_survived = bot_key in state_before

                    if bot_in_any and not bot_survived:
                        print(
                            f"\n!!! BOT EVICTED at depth {ev.depth} "
                            f"(bot in {'all' if bot_in_all else 'some'} "
                            f"forks) !!!"
                        )
                        for i, s in enumerate(state_sets):
                            has = bot_key in s
                            print(
                                f"  Set {i}: {len(s)} keys, "
                                f"bot={'YES' if has else 'NO'}"
                            )
                        jr = state_before.get((EventTypes.JoinRules, ""))
                        if jr:
                            jr_ev = event_map.get(jr)
                            if jr_ev:
                                print(f"  Resolved join_rules: {jr_ev.content}")
                        eviction_depths.append(ev.depth)

            state_after = dict(state_before)
            if ev.is_state():
                state_after[(ev.type, ev.state_key)] = ev.event_id
            state_at[ev.event_id] = state_after

        final_state = state_at.get(events[-1].event_id, {})

        if eviction_depths:
            eviction_summary = f"BOT EVICTED at depths: {eviction_depths}"
        else:
            eviction_summary = "Bot was NEVER evicted with full DAG"

        bot_final = final_state.get(bot_key)
        if bot_final:
            bev = event_map[bot_final]
            final_summary = (
                f"Final: {bev.content.get('membership')} (depth={bev.depth})"
            )
        else:
            final_summary = "Final: NOT IN STATE"

        diagnostic_message = (
            f"REPLAY: {len(events)} events, {merge_count} merges; "
            f"{eviction_summary}; {final_summary}"
        )

        # Bot should never be evicted during state resolution merges.
        self.assertEqual(
            eviction_depths,
            [],
            f"Bot was evicted during merge resolution at depths: {eviction_depths}",
        )

        # Bot should be in the final state
        self.assertIn(bot_key, final_state, diagnostic_message)

    def test_replay_nutra_tk_dag_catgirl_perspective(self) -> None:
        """Simulate catgirl.cloud's state divergence.

        catgirl.cloud joined at depth 336 and would have received state
        via /state from whichever server it contacted. If that state
        snapshot was incomplete (missing @bot:nutra.tk's membership),
        all subsequent state resolution would build on that corrupted
        foundation.

        This test replays events starting from depth 336 with the bot
        deliberately removed from the initial state, simulating the
        exact divergence catgirl.cloud would experience.
        """
        if not os.path.exists(self.JSONL_PATH):
            self.skipTest(f"JSONL not found: {self.JSONL_PATH}")

        events = self._load_events(self.JSONL_PATH)
        self.assertGreater(len(events), 0)

        # First, replay the FULL DAG to get correct state at depth 336
        room_id = events[0].room_id
        event_map: dict[str, EventBase] = {}
        state_at: dict[str, StateMap[str]] = {}

        target = "@bot:nutra.tk"
        bot_key = (EventTypes.Member, target)

        # Phase 1: Build correct state up to depth 336
        catgirl_join_depth = 336
        pre_join_events = [ev for ev in events if ev.depth <= catgirl_join_depth]
        post_join_events = [ev for ev in events if ev.depth > catgirl_join_depth]

        for ev in pre_join_events:
            event_map[ev.event_id] = ev
            prev_ids = ev.prev_event_ids()

            if not prev_ids:
                state_before: StateMap[str] = {}
            elif len(prev_ids) == 1:
                state_before = dict(state_at.get(prev_ids[0], {}))
            else:
                state_sets = [state_at[pid] for pid in prev_ids if pid in state_at]
                if len(state_sets) < 2:
                    state_before = dict(state_sets[0]) if state_sets else {}
                else:
                    store = TestStateResolutionStore(event_map)
                    state_before = self.successResultOf(
                        defer.ensureDeferred(
                            resolve_events_with_store(
                                FakeClock(),
                                room_id,
                                RoomVersions.V2,
                                state_sets,
                                event_map=event_map,
                                state_res_store=store,
                            )
                        )
                    )

            state_after = dict(state_before)
            if ev.is_state():
                state_after[(ev.type, ev.state_key)] = ev.event_id
            state_at[ev.event_id] = state_after

        # Get the state at the last pre-join event
        last_pre = pre_join_events[-1]
        correct_state = state_at[last_pre.event_id]
        print(f"\nCorrect state at depth {catgirl_join_depth}:")
        print(f"  Bot in state: {bot_key in correct_state}")
        if bot_key in correct_state:
            bev = event_map[correct_state[bot_key]]
            print(
                f"  Bot membership: {bev.content.get('membership')} (depth={bev.depth})"
            )

        # Phase 2: Create CORRUPTED state (remove bot membership)
        corrupted_state = dict(correct_state)
        self.assertIn(
            bot_key,
            corrupted_state,
            "Expected bot membership to be present before corrupting state",
        )
        had_bot = True
        del corrupted_state[bot_key]
        print(f"  Corrupted state: removed bot (had_bot={had_bot})")

        # Reset state_at for the last event to use corrupted state
        state_at[last_pre.event_id] = corrupted_state

        # Phase 3: Replay remaining events with corrupted state
        recovery_depth = None

        for ev in post_join_events:
            event_map[ev.event_id] = ev
            prev_ids = ev.prev_event_ids()

            if not prev_ids:
                state_before = {}
            elif len(prev_ids) == 1:
                state_before = dict(state_at.get(prev_ids[0], {}))
            else:
                state_sets = [state_at[pid] for pid in prev_ids if pid in state_at]
                if len(state_sets) < 2:
                    state_before = dict(state_sets[0]) if state_sets else {}
                else:
                    store = TestStateResolutionStore(event_map)
                    state_before = self.successResultOf(
                        defer.ensureDeferred(
                            resolve_events_with_store(
                                FakeClock(),
                                room_id,
                                RoomVersions.V2,
                                state_sets,
                                event_map=event_map,
                                state_res_store=store,
                            )
                        )
                    )

                    if bot_key in state_before and not recovery_depth:
                        recovery_depth = ev.depth
                        print(f"\n  Bot RECOVERED at depth {ev.depth} via state res!")

            state_after = dict(state_before)
            if ev.is_state():
                state_after[(ev.type, ev.state_key)] = ev.event_id
            state_at[ev.event_id] = state_after

        final_state = state_at.get(events[-1].event_id, {})

        print(f"\n{'=' * 60}")
        print("CATGIRL SIMULATION:")
        print(f"  Bot removed from state at depth {catgirl_join_depth}")
        if recovery_depth:
            print(f"  Bot RECOVERED at depth {recovery_depth} via state resolution")
        else:
            print("  Bot NEVER recovered")
        bot_final = final_state.get(bot_key)
        if bot_final:
            bev = event_map[bot_final]
            print(f"  Final: {bev.content.get('membership')} (depth={bev.depth})")
        else:
            print("  Final: NOT IN STATE")
        print(f"{'=' * 60}")

        # V2 state res shouldn't recover dropped member from corrupt state.
        # The member is absent from affected server's view and never enters
        # the conflict set, so no merge can surface it.
        self.assertIsNone(
            recovery_depth,
            "Bot should not have been recovered via V2 state resolution",
        )

    def test_replay_nutra_tk_dag_catgirl_v21(self) -> None:
        """Prove V2.1 self-heals corrupted ingestion state.

        The catgirl bug: bot's membership was missing from the corrupted
        /state response at depth 336. Under V2, the bot never recovers
        because the supplemental merge poisons auth checks.

        Under V2.1 (V12+), the bot RECOVERS at the first fork merge
        where its membership appears in at least one fork. Without the
        supplemental merge, the iterative auth checks start from {} and
        the bot's join auths cleanly against its own auth chain.

        This proves that upgrading to V12 is sufficient to self-heal
        rooms affected by the catgirl bug — no surgical repair needed.
        """
        if not os.path.exists(self.JSONL_PATH):
            self.skipTest(f"JSONL not found: {self.JSONL_PATH}")

        events = self._load_events(self.JSONL_PATH)
        self.assertGreater(len(events), 0)

        room_id = events[0].room_id
        event_map: dict[str, EventBase] = {}
        state_at: dict[str, StateMap[str]] = {}

        target = "@bot:nutra.tk"
        bot_key = (EventTypes.Member, target)

        catgirl_join_depth = 336
        pre_join_events = [ev for ev in events if ev.depth <= catgirl_join_depth]
        post_join_events = [ev for ev in events if ev.depth > catgirl_join_depth]

        # Phase 1: Build correct state up to depth 336
        for ev in pre_join_events:
            event_map[ev.event_id] = ev
            prev_ids = ev.prev_event_ids()

            if not prev_ids:
                state_before: StateMap[str] = {}
            elif len(prev_ids) == 1:
                state_before = dict(state_at.get(prev_ids[0], {}))
            else:
                state_sets = [state_at[pid] for pid in prev_ids if pid in state_at]
                if len(state_sets) < 2:
                    state_before = dict(state_sets[0]) if state_sets else {}
                else:
                    store = TestStateResolutionStore(event_map)
                    state_before = self.successResultOf(
                        defer.ensureDeferred(
                            resolve_events_with_store(
                                FakeClock(),
                                room_id,
                                RoomVersions.V12,
                                state_sets,
                                event_map=event_map,
                                state_res_store=store,
                            )
                        )
                    )

            state_after = dict(state_before)
            if ev.is_state():
                state_after[(ev.type, ev.state_key)] = ev.event_id
            state_at[ev.event_id] = state_after

        # Phase 2: Corrupt state (remove bot)
        last_pre = pre_join_events[-1]
        corrupted_state = dict(state_at[last_pre.event_id])
        self.assertIn(bot_key, corrupted_state)
        del corrupted_state[bot_key]
        state_at[last_pre.event_id] = corrupted_state

        # Phase 3: Replay with V2.1 (V12) state resolution
        recovery_depth = None

        for ev in post_join_events:
            event_map[ev.event_id] = ev
            prev_ids = ev.prev_event_ids()

            if not prev_ids:
                state_before = {}
            elif len(prev_ids) == 1:
                state_before = dict(state_at.get(prev_ids[0], {}))
            else:
                state_sets = [state_at[pid] for pid in prev_ids if pid in state_at]
                if len(state_sets) < 2:
                    state_before = dict(state_sets[0]) if state_sets else {}
                else:
                    store = TestStateResolutionStore(event_map)
                    state_before = self.successResultOf(
                        defer.ensureDeferred(
                            resolve_events_with_store(
                                FakeClock(),
                                room_id,
                                RoomVersions.V12,
                                state_sets,
                                event_map=event_map,
                                state_res_store=store,
                            )
                        )
                    )

                    if bot_key in state_before and not recovery_depth:
                        recovery_depth = ev.depth

            state_after = dict(state_before)
            if ev.is_state():
                state_after[(ev.type, ev.state_key)] = ev.event_id
            state_at[ev.event_id] = state_after

        # V2.1 DOES recover the bot! Without the supplemental merge,
        # the bot's membership passes auth at the first fork merge
        # where it appears in at least one state set.
        self.assertIsNotNone(
            recovery_depth,
            "Bot should recover via V2.1 — self-healing proves the fix works",
        )

        # Verify bot is in final state
        final_state = state_at.get(events[-1].event_id, {})
        self.assertIn(
            bot_key,
            final_state,
            "Bot should be in final state after V2.1 self-healing",
        )

        # Phase 4: Prove this ONLY works because of the V2.1 code change.
        # Re-run the exact same corrupted state through V2 resolution —
        # the bot should NOT recover, proving V2.1 is the differentiator.
        event_map_v2: dict[str, EventBase] = {}
        state_at_v2: dict[str, StateMap[str]] = {}

        for ev in pre_join_events:
            event_map_v2[ev.event_id] = ev
            prev_ids = ev.prev_event_ids()
            if not prev_ids:
                sb: StateMap[str] = {}
            elif len(prev_ids) == 1:
                sb = dict(state_at_v2.get(prev_ids[0], {}))
            else:
                sets = [state_at_v2[p] for p in prev_ids if p in state_at_v2]
                if len(sets) < 2:
                    sb = dict(sets[0]) if sets else {}
                else:
                    sb = self.successResultOf(
                        defer.ensureDeferred(
                            resolve_events_with_store(
                                FakeClock(),
                                room_id,
                                RoomVersions.V2,
                                sets,
                                event_map=event_map_v2,
                                state_res_store=TestStateResolutionStore(event_map_v2),
                            )
                        )
                    )
            sa = dict(sb)
            if ev.is_state():
                sa[(ev.type, ev.state_key)] = ev.event_id
            state_at_v2[ev.event_id] = sa

        # Apply the same corruption
        corrupted_v2 = dict(state_at_v2[last_pre.event_id])
        del corrupted_v2[bot_key]
        state_at_v2[last_pre.event_id] = corrupted_v2

        recovery_v2 = None
        for ev in post_join_events:
            event_map_v2[ev.event_id] = ev
            prev_ids = ev.prev_event_ids()
            if not prev_ids:
                sb = {}
            elif len(prev_ids) == 1:
                sb = dict(state_at_v2.get(prev_ids[0], {}))
            else:
                sets = [state_at_v2[p] for p in prev_ids if p in state_at_v2]
                if len(sets) < 2:
                    sb = dict(sets[0]) if sets else {}
                else:
                    sb = self.successResultOf(
                        defer.ensureDeferred(
                            resolve_events_with_store(
                                FakeClock(),
                                room_id,
                                RoomVersions.V2,
                                sets,
                                event_map=event_map_v2,
                                state_res_store=TestStateResolutionStore(event_map_v2),
                            )
                        )
                    )
                    if bot_key in sb and not recovery_v2:
                        recovery_v2 = ev.depth
            sa = dict(sb)
            if ev.is_state():
                sa[(ev.type, ev.state_key)] = ev.event_id
            state_at_v2[ev.event_id] = sa

        # V2 does NOT recover — proving the self-healing is solely
        # due to the V2.1 code change in _iterative_auth_checks.
        self.assertIsNone(
            recovery_v2,
            "V2 must NOT recover the bot — only V2.1 self-heals",
        )
