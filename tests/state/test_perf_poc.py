import time
import asyncio
from typing import Collection, Mapping
from synapse.api.constants import EventTypes, Membership
from synapse.api.room_versions import RoomVersions
from synapse.state.v2 import resolve_events_with_store
from synapse.types import EventID
from tests import unittest
from tests.test_utils.event_builders import make_test_event
from synapse.util.duration import Duration

ALICE = "@alice:example.com"
ROOM_ID = "!test:example.com"

class FakeClock:
    async def sleep(self, duration: Duration) -> None:
        await asyncio.sleep(duration.as_seconds())

class FakeEvent:
    def __init__(
        self,
        id: str,
        sender: str,
        type: str,
        state_key: str | None,
        content: Mapping[str, object],
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

    def auth_event_ids(self):
        return [EventID(aid, "example.com").to_string() for aid in self._auth_events]
    
    def prev_event_ids(self):
        return []

    def is_state(self):
        return self.state_key is not None
    
    @property
    def origin_server_ts(self):
        return 0
    
    @property
    def rejected_reason(self):
        return None

class PerfStateResTestCase(unittest.TestCase):
    def test_performance_gains(self) -> None:
        # We create a large number of power events.
        # Each power event has another power event as its auth event.
        # This creates a long auth chain that would traditionally be fetched sequentially.
        
        num_events = 50
        events_dict = {}
        
        # Create an auth chain of power events
        prev_auth = "CREATE"
        events_dict["CREATE"] = FakeEvent("CREATE", ALICE, EventTypes.Create, "", {"creator": ALICE})
        
        for i in range(num_events):
            eid = f"PL{i}"
            events_dict[eid] = FakeEvent(eid, ALICE, EventTypes.PowerLevels, "", {"users": {ALICE: 100}}, auth_events=[prev_auth])
            prev_auth = eid
            
        # conflicted state sets
        state_sets = [
            {(EventTypes.PowerLevels, ""): EventID(f"PL{num_events-1}", "example.com").to_string()},
            {(EventTypes.PowerLevels, ""): EventID("PL0", "example.com").to_string()},
        ]
        
        call_count = 0
        
        class MockStore:
            async def get_events(self, event_ids, allow_rejected=False):
                nonlocal call_count
                call_count += 1
                # Simulate 10ms DB latency
                await asyncio.sleep(0.01)
                return {eid: events_dict[EventID.from_string(eid).localpart] for eid in event_ids if EventID.from_string(eid).localpart in events_dict}

            async def get_auth_chain_difference(self, room_id, state_sets, conflicted_state, additional_backwards_reachable_conflicted_events):
                from synapse.storage.databases.main.event_federation import StateDifference
                # For simplicity, return all PL events as auth difference
                return StateDifference(
                    auth_difference={EventID(eid, "example.com").to_string() for eid in events_dict if eid.startswith("PL")},
                    conflicted_subgraph=None
                )

        store = MockStore()
        clock = FakeClock()
        
        start_time = time.perf_counter()
        
        # This will now use the optimized code:
        # 1. Bulk pre-fetch of the entire auth chain.
        # 2. Parallel mainline depth calculation.
        # 3. Synchronous iterative auth checks (since everything is pre-fetched).
        self.get_success(resolve_events_with_store(
            clock,
            ROOM_ID,
            RoomVersions.V2,
            state_sets,
            event_map={},
            state_res_store=store
        ))
        
        end_time = time.perf_counter()
        duration = end_time - start_time
        
        print(f"\nOptimization Results:")
        print(f"Total time: {duration:.4f}s")
        print(f"DB Call count: {call_count}")
        
        # With the optimizations:
        # - The auth chain pre-fetch should take a few rounds (depending on how it walks), 
        #   but it's much better than 50 sequential calls.
        # - Mainline depth calculation is parallelized.
        
        # If it were sequential, it would take AT LEAST num_events * 0.01s = 0.5s.
        # We expect it to be much faster.
        # Also call_count should be small.
        
        self.assertLess(call_count, 10, "Should have few DB calls due to bulk fetching")
        self.assertLess(duration, 0.3, "Should be fast due to concurrency")

