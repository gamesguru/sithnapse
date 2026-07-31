from typing import Mapping

from twisted.internet import defer

from synapse.api.constants import EventTypes, Membership
from synapse.api.room_versions import RoomVersions
from synapse.state.v2 import resolve_events_with_store
from synapse.types import EventID
from synapse.util.duration import Duration

from tests.unittest import TestCase

ALICE = "@alice:example.com"
BOB = "@bob:example.com"
ROOM_ID = "!test:example.com"

MEMBERSHIP_CONTENT_JOIN = {"membership": Membership.JOIN}

class FakeClock:
    async def sleep(self, duration: Duration) -> None:
        pass

class FakeEvent:
    def __init__(
        self,
        id: str,
        sender: str,
        type: str,
        state_key: str | None,
        content: Mapping[str, object],
        room_version: str,
        auth_events: list[str] = None,
    ):
        self.node_id = id
        self.event_id = EventID(id, "example.com").to_string()
        self.sender = sender
        self.type = type
        self.state_key = state_key
        self.content = content
        self.room_id = ROOM_ID
        self._auth_events = auth_events or []
        self.room_version = room_version

        if self.type == EventTypes.Member:
            self.membership = self.content.get("membership")

    def auth_event_ids(self):
        return [EventID(aid, "example.com").to_string() for aid in self._auth_events]

    def prev_event_ids(self):
        return []

    def is_state(self):
        return self.state_key is not None

    def get_state_key(self):
        return self.state_key

    @property
    def origin_server_ts(self):
        return 0

    @property
    def rejected_reason(self):
        return None

class PerfStateResTestCase(TestCase):
    def test_performance_gains_wide_dag(self) -> None:
        # Test across modern room versions
        for rv_name in ["V10", "V11", "V12"]:
            rv = getattr(RoomVersions, rv_name)
            with self.subTest(room_version=rv_name):
                self._do_test_wide_dag(rv)

    def _do_test_wide_dag(self, room_version) -> None:
        num_events = 50
        events_dict = {}

        # Create a "wide" auth chain: 50 conflicted events, each with its own auth event.
        # Use ALICE as creator and BOB as the sender of PL events to avoid V11+ creator PL restrictions.
        events_dict["CREATE"] = FakeEvent("CREATE", ALICE, EventTypes.Create, "", {"creator": ALICE}, room_version)

        # Join BOB
        events_dict["JOIN_BOB"] = FakeEvent("JOIN_BOB", BOB, EventTypes.Member, BOB, MEMBERSHIP_CONTENT_JOIN, room_version, auth_events=["CREATE"])

        conflicted_ids = set()
        for i in range(num_events):
            auth_eid = f"AUTH{i}"
            # Auth event for each PL: ensure sender is in room
            events_dict[auth_eid] = FakeEvent(auth_eid, BOB, EventTypes.Member, BOB, MEMBERSHIP_CONTENT_JOIN, room_version, auth_events=["CREATE"])

            eid = f"PL{i}"
            # Power level event by BOB
            events_dict[eid] = FakeEvent(eid, BOB, EventTypes.PowerLevels, "", {"users": {BOB: 100}}, room_version, auth_events=[auth_eid, "CREATE"])
            conflicted_ids.add(events_dict[eid].event_id)

        # state sets
        state_sets = [
            {(EventTypes.PowerLevels, ""): events_dict["PL0"].event_id},
            {(EventTypes.PowerLevels, ""): events_dict["PL1"].event_id},
        ]

        call_count = 0

        class MockStore:
            async def get_events(self, event_ids, allow_rejected=False):
                nonlocal call_count
                call_count += 1
                return {eid: events_dict[EventID.from_string(eid).localpart] for eid in event_ids if EventID.from_string(eid).localpart in events_dict}

            async def get_auth_chain_difference(self, room_id, state_sets, conflicted_state, additional_backwards_reachable_conflicted_events):
                from synapse.storage.databases.main.event_federation import (
                    StateDifference,
                )
                return StateDifference(
                    auth_difference=conflicted_ids,
                    conflicted_subgraph=None
                )

        store = MockStore()
        clock = FakeClock()

        d = defer.ensureDeferred(resolve_events_with_store(
            clock,
            ROOM_ID,
            room_version,
            state_sets,
            event_map={},
            state_res_store=store
        ))
        self.successResultOf(d)

        print(f"\nOptimization Results (Wide DAG, {room_version.identifier}):")
        print(f"DB Call count: {call_count}")

        self.assertLess(call_count, 10, f"Should have very few DB calls on {room_version.identifier}")

    def test_parallel_sorting(self) -> None:
        for rv_name in ["V10", "V11", "V12"]:
            rv = getattr(RoomVersions, rv_name)
            with self.subTest(room_version=rv_name):
                self._do_test_parallel_sorting(rv)

    def _do_test_parallel_sorting(self, room_version) -> None:
        mainline_len = 100
        events_dict = {}
        events_dict["CREATE"] = FakeEvent("CREATE", ALICE, EventTypes.Create, "", {"creator": ALICE}, room_version)

        # Use BOB for PL events
        events_dict["JOIN_BOB"] = FakeEvent("JOIN_BOB", BOB, EventTypes.Member, BOB, MEMBERSHIP_CONTENT_JOIN, room_version, auth_events=["CREATE"])

        prev = "CREATE"
        mainline = []
        for i in range(mainline_len):
            eid = f"M{i}"
            events_dict[eid] = FakeEvent(eid, BOB, EventTypes.PowerLevels, "", {"users": {BOB: 100}}, room_version, auth_events=[prev, "CREATE", "JOIN_BOB"])
            prev = eid
            mainline.append(events_dict[eid].event_id)

        # Events to sort
        num_to_sort = 20
        to_sort_ids = set()
        for i in range(num_to_sort):
            eid = f"S{i}"
            mainline_target = f"M{i * 2}"
            events_dict[eid] = FakeEvent(eid, BOB, EventTypes.PowerLevels, "", {"users": {BOB: 100}}, room_version, auth_events=[mainline_target, "CREATE", "JOIN_BOB"])
            to_sort_ids.add(events_dict[eid].event_id)

        call_count = 0

        class MockStore:
            async def get_events(self, event_ids, allow_rejected=False):
                nonlocal call_count
                call_count += 1
                return {eid: events_dict[EventID.from_string(eid).localpart] for eid in event_ids if EventID.from_string(eid).localpart in events_dict}

            async def get_auth_chain_difference(self, room_id, state_sets, conflicted_state, additional_backwards_reachable_conflicted_events):
                from synapse.storage.databases.main.event_federation import (
                    StateDifference,
                )
                return StateDifference(
                    auth_difference=to_sort_ids | set(mainline),
                    conflicted_subgraph=None
                )

        store = MockStore()
        clock = FakeClock()

        state_sets = [
            {(EventTypes.PowerLevels, ""): mainline[-1]},
            {(EventTypes.PowerLevels, ""): list(to_sort_ids)[0]},
        ]

        d = defer.ensureDeferred(resolve_events_with_store(
            clock,
            ROOM_ID,
            room_version,
            state_sets,
            event_map={},
            state_res_store=store
        ))
        self.successResultOf(d)

        print(f"\nOptimization Results (Parallel Sorting, {room_version.identifier}):")
        print(f"DB Call count: {call_count}")

        self.assertLess(call_count, 10, f"Should have very few DB calls on {room_version.identifier}")
