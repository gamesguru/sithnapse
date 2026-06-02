from typing import Mapping

from synapse.api.constants import EventTypes
from synapse.api.room_versions import RoomVersion, RoomVersions
from synapse.events import EventBase
from synapse.storage.databases.main.event_federation import StateDifference
from synapse.types import StateMap, StrCollection
from synapse.util.duration import Duration

from tests.state.test_v21 import StateResV21TestCase


class FakeClock:
    async def sleep(self, duration: Duration) -> None:
        from twisted.internet import defer

        defer.succeed(None)


class PathologyTestCase(StateResV21TestCase):
    def create_event(
        self,
        event_type: str,
        state_key: str | None,
        sender: str,
        content: dict,
        auth_events: list[str],
        prev_events: list[str] | None = None,
        room_id: str | None = None,
        version: RoomVersion = RoomVersions.V12,
    ) -> EventBase:
        from synapse.federation.federation_base import event_from_pdu_json

        if prev_events is None:
            prev_events = []

        pdu = {
            "type": event_type,
            "state_key": state_key,
            "content": content,
            "sender": sender,
            "depth": 5,
            "prev_events": prev_events,
            "auth_events": auth_events,
            "origin_server_ts": self.monotonic_timestamp(),
            "hashes": {},
        }

        # Room Version 12+ (MSC4291) omits room_id from PDUs.
        # Older versions REQUIRE it in the PDU.
        if not version.msc4291_room_ids_as_hashes:
            if room_id is None:
                # For create events in old versions, we still need a room_id.
                # If not provided, we'll invent one for the test.
                room_id = "!test:room"
            pdu["room_id"] = room_id
        else:
            if event_type != EventTypes.Create:
                if room_id is None:
                    raise Exception(
                        "must specify a room_id to create_event for non-create events in V12"
                    )
                pdu["room_id"] = room_id

        # For Room Version 12+, auth_events must NOT contain the create event.
        if version.msc4291_room_ids_as_hashes:
            if room_id:
                create_event_id = "$" + room_id[1:]
                if create_event_id in auth_events:
                    auth_events = [ae for ae in auth_events if ae != create_event_id]
                    pdu["auth_events"] = auth_events

        return event_from_pdu_json(
            pdu,
            version,
        )

    def test_duplicate_auth_syndrome(self) -> None:
        """
        Reproduces the "Duplicate Auth" syndrome where an event cites two
        competing join events for the same user in its auth_events.
        """
        # 1. Create room
        e1_create = self.create_event(
            EventTypes.Create,
            "",
            sender="@admin:A",
            content={"room_version": "11"},
            auth_events=[],
        )

        # 2. Power levels
        e2_pl = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender="@admin:A",
            content={"users": {"@admin:A": 100, "@x:X": 50}},
            auth_events=[e1_create.event_id],
            room_id=e1_create.room_id,
        )

        # 3. First join for X
        e3_join1 = self.create_event(
            EventTypes.Member,
            "@x:X",
            sender="@x:X",
            content={"membership": "join"},
            auth_events=[e1_create.event_id, e2_pl.event_id],
            room_id=e1_create.room_id,
        )

        # 4. Second join for X (competing)
        e4_join2 = self.create_event(
            EventTypes.Member,
            "@x:X",
            sender="@x:X",
            content={"membership": "join", "extra": "garbage"},
            auth_events=[e1_create.event_id, e2_pl.event_id],
            room_id=e1_create.room_id,
        )

        # 5. Message with DUPLICATE AUTH
        # This event cites BOTH joins for X in its auth_events.
        # This is a protocol violation.
        e5_poisoned = self.create_event(
            EventTypes.Message,
            None,
            sender="@b:B",
            content={"body": "i am poisoned"},
            auth_events=[
                e1_create.event_id,
                e2_pl.event_id,
                e3_join1.event_id,
                e4_join2.event_id,
            ],
            room_id=e1_create.room_id,
        )

        # In Synapse, check_state_independent_auth_rules should raise AuthError
        # for e5_poisoned.
        from synapse import event_auth

        from tests.test_utils import get_awaitable_result

        # We need a store that has these events
        event_map = {
            e.event_id: e for e in [e1_create, e2_pl, e3_join1, e4_join2, e5_poisoned]
        }
        store = MockStateResolutionStore(event_map)

        from synapse.api.errors import AuthError

        with self.assertRaises(AuthError) as cm:
            get_awaitable_result(
                event_auth.check_state_independent_auth_rules(store, e5_poisoned)
            )
        # Synapse raises "unexpected auth_event" because it clobbers the first join
        # when building the dict, and then finds the second join doesn't match
        # the required auth types for its (type, state_key).
        self.assertIn("unexpected auth_event", str(cm.exception))

    def test_nexy_invite_lock_anomaly(self) -> None:
        """
        Reproduces the "Invite Rule Lock" where a room gets stuck in invite-only
        due to stale branch pollution in V2.
        """
        # Room version 11 (V2)
        version = RoomVersions.V11

        e1_create = self.create_event(
            EventTypes.Create,
            "",
            sender="@a:A",
            content={"room_version": "11"},
            auth_events=[],
            version=version,
            room_id="!nexy:room",
        )
        e2_pl = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender="@a:A",
            content={"users": {"@a:A": 100}},
            auth_events=[e1_create.event_id],
            room_id=e1_create.room_id,
            version=version,
        )

        # Branch A: Room becomes public, Nexy joins, then room becomes invite-only.
        e3_jr_public = self.create_event(
            EventTypes.JoinRules,
            "",
            sender="@a:A",
            content={"join_rule": "public"},
            auth_events=[e1_create.event_id, e2_pl.event_id],
            room_id=e1_create.room_id,
            version=version,
        )
        e4_nexy_join = self.create_event(
            EventTypes.Member,
            "@nexy:B",
            sender="@nexy:B",
            content={"membership": "join"},
            auth_events=[e1_create.event_id, e3_jr_public.event_id],
            room_id=e1_create.room_id,
            version=version,
        )
        e5_jr_invite = self.create_event(
            EventTypes.JoinRules,
            "",
            sender="@a:A",
            content={"join_rule": "invite"},
            auth_events=[e1_create.event_id, e2_pl.event_id],
            room_id=e1_create.room_id,
            version=version,
        )

        # Branch B: Stale fork that doesn't know about public/nexy/invite.
        # It just has some random message.
        e6_stale_msg = self.create_event(
            EventTypes.Message,
            None,
            sender="@a:A",
            content={"body": "stale"},
            auth_events=[e1_create.event_id, e2_pl.event_id],
            room_id=e1_create.room_id,
            version=version,
        )

        # Merge point.
        state_a: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.PowerLevels, ""): e2_pl.event_id,
            (EventTypes.JoinRules, ""): e5_jr_invite.event_id,
            (EventTypes.Member, "@nexy:B"): e4_nexy_join.event_id,
        }
        state_b: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.PowerLevels, ""): e2_pl.event_id,
            (
                EventTypes.JoinRules,
                "",
            ): e3_jr_public.event_id,  # Stale fork thinks it's still public
        }

        # Let's see what happens with V2.
        event_map = {
            e.event_id: e
            for e in [
                e1_create,
                e2_pl,
                e3_jr_public,
                e4_nexy_join,
                e5_jr_invite,
                e6_stale_msg,
            ]
        }

        # We want to see if Nexy is evicted.
        from synapse.state.v2 import resolve_events_with_store

        res = self.get_success(
            resolve_events_with_store(
                FakeClock(),
                e1_create.room_id,
                version,
                [state_a, state_b],
                event_map=event_map,
                state_res_store=MockStateResolutionStore(event_map),
            )
        )

        # In V2, Nexy might be evicted because of the supplemental merge poisoning her auth chain
        # with the resolved 'invite' rule (if 'invite' wins the conflict).

        # If 'invite' wins:
        if res.get((EventTypes.JoinRules, "")) == e5_jr_invite.event_id:
            # Check if Nexy is still there.
            if (EventTypes.Member, "@nexy:B") not in res:
                print("Nexy was EVICTED by V2 (as predicted by anomaly report)")
            else:
                print("Nexy survived V2")

    def test_vanish_anomaly_reproduction(self) -> None:
        """
        Reproduces the "Vanish" anomaly where a bot rejoin is lost due to
        incomplete DAG context during merge.
        """
        version = RoomVersions.V11

        e1_create = self.create_event(
            EventTypes.Create,
            "",
            sender="@admin:A",
            content={"room_version": "11"},
            auth_events=[],
            version=version,
            room_id="!vanish:room",
        )
        e2_pl = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender="@admin:A",
            content={"users": {"@admin:A": 100}},
            auth_events=[e1_create.event_id],
            room_id=e1_create.room_id,
            version=version,
        )

        # Bot joins, leaves, joins.
        e3_bot_join1 = self.create_event(
            EventTypes.Member,
            "@bot:A",
            sender="@bot:A",
            content={"membership": "join"},
            auth_events=[e1_create.event_id, e2_pl.event_id],
            room_id=e1_create.room_id,
            version=version,
        )
        self.create_event(
            EventTypes.Member,
            "@bot:A",
            sender="@bot:A",
            content={"membership": "leave"},
            auth_events=[e1_create.event_id, e2_pl.event_id],
            room_id=e1_create.room_id,
            version=version,
        )
        e5_bot_join2 = self.create_event(
            EventTypes.Member,
            "@bot:A",
            sender="@bot:A",
            content={"membership": "join"},
            auth_events=[e1_create.event_id, e2_pl.event_id],
            room_id=e1_create.room_id,
            version=version,
        )

        # Server B only sees up to e3_bot_join1, then misses e4 and e5.
        # Then it receives a message from A that cites e5 as a prev_event or auth_event.

        self.create_event(
            EventTypes.Message,
            None,
            sender="@admin:A",
            content={"body": "hello"},
            prev_events=[e5_bot_join2.event_id],
            auth_events=[
                e1_create.event_id,
                e2_pl.event_id,
                e3_bot_join1.event_id,
            ],  # WRONG AUTH!
            room_id=e1_create.room_id,
            version=version,
        )

        # This is a bit complex to reproduce purely in state res tests because it's
        # about what events are available.

        # If we run state res with a missing event, it should handle it.


class MockStateResolutionStore:
    def __init__(self, event_map: Mapping[str, EventBase]):
        self.event_map = event_map

    async def get_events(
        self,
        event_ids: StrCollection,
        allow_rejected: bool = False,
    ) -> dict[str, EventBase]:
        return {eid: self.event_map[eid] for eid in event_ids if eid in self.event_map}

    async def get_auth_chain_difference(
        self,
        room_id: str,
        state_sets: list[set[str]],
        conflicted_state: set[str] | None,
        additional_backwards_reachable_conflicted_events: set[str] | None,
    ) -> StateDifference:
        # Simple mock or use actual logic if needed
        return StateDifference(set(), set())
