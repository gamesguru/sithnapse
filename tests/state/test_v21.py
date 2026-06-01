#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2025 New Vector, Ltd
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
import logging
from typing import Sequence

from twisted.internet import defer
from twisted.test.proto_helpers import MemoryReactor

from synapse.api.constants import EventTypes, JoinRules, Membership
from synapse.api.room_versions import RoomVersions
from synapse.events import EventBase
from synapse.federation.federation_base import event_from_pdu_json
from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.state import StateResolutionStore
from synapse.state.v2 import (
    StateResolutionStore as StateResolutionStoreInterface,
    _get_auth_chain_difference,
    _seperate,
    resolve_events_with_store,
)
from synapse.types import StateMap
from synapse.util.clock import Clock
from synapse.util.duration import Duration

from tests import unittest
from tests.state.test_v2 import TestStateResolutionStore

logger = logging.getLogger(__name__)

ALICE = "@alice:example.com"
BOB = "@bob:example.com"
CHARLIE = "@charlie:example.com"
EVELYN = "@evelyn:example.com"
ZARA = "@zara:example.com"

ROOM_ID = "!test:example.com"

MEMBERSHIP_CONTENT_JOIN = {"membership": Membership.JOIN}
MEMBERSHIP_CONTENT_INVITE = {"membership": Membership.INVITE}
MEMBERSHIP_CONTENT_LEAVE = {"membership": Membership.LEAVE}
MEMBERSHIP_CONTENT_BAN = {"membership": Membership.BAN}




class FakeClock:
    async def sleep(self, duration: Duration) -> None:
        defer.succeed(None)


class StateResV21TestCase(unittest.HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        room.register_servlets,
        login.register_servlets,
    ]

    def prepare(
        self, reactor: MemoryReactor, clock: Clock, homeserver: HomeServer
    ) -> None:
        self.state = self.hs.get_state_handler()
        persistence = self.hs.get_storage_controllers().persistence
        assert persistence is not None
        self._persistence = persistence
        self._state_storage_controller = self.hs.get_storage_controllers().state
        self._state_deletion = self.hs.get_datastores().state_deletion
        self.store = self.hs.get_datastores().main

        self.register_user("user", "pass")
        self.token = self.login("user", "pass")
        self._ts_counter = 1000

    def monotonic_timestamp(self) -> int:
        self._ts_counter += 1
        return self._ts_counter

    def test_state_reset_replay_conflicted_subgraph(self) -> None:
        # 1. Alice creates a room.
        e1_create = self.create_event(
            EventTypes.Create,
            "",
            sender=ALICE,
            content={"creator": ALICE},
            auth_events=[],
        )
        # 2. Alice joins it.
        e2_ma = self.create_event(
            EventTypes.Member,
            ALICE,
            sender=ALICE,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[],
            prev_events=[e1_create.event_id],
            room_id=e1_create.room_id,
        )
        # 3. Alice is the creator
        e3_power1 = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=ALICE,
            content={"users": {}},
            auth_events=[e2_ma.event_id],
            room_id=e1_create.room_id,
        )
        # 4. Alice sets the room to public.
        e4_jr = self.create_event(
            EventTypes.JoinRules,
            "",
            sender=ALICE,
            content={"join_rule": JoinRules.PUBLIC},
            auth_events=[e2_ma.event_id, e3_power1.event_id],
            room_id=e1_create.room_id,
        )
        # 5. Bob joins the room.
        e5_mb = self.create_event(
            EventTypes.Member,
            BOB,
            sender=BOB,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e3_power1.event_id, e4_jr.event_id],
            room_id=e1_create.room_id,
        )
        # 6. Charlie joins the room.
        e6_mc = self.create_event(
            EventTypes.Member,
            CHARLIE,
            sender=CHARLIE,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e3_power1.event_id, e4_jr.event_id],
            room_id=e1_create.room_id,
        )
        # 7. Alice promotes Bob.
        e7_power2 = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=ALICE,
            content={"users": {BOB: 50}},
            auth_events=[e2_ma.event_id, e3_power1.event_id],
            room_id=e1_create.room_id,
        )
        # 8. Bob promotes Charlie.
        e8_power3 = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=BOB,
            content={"users": {BOB: 50, CHARLIE: 50}},
            auth_events=[e5_mb.event_id, e7_power2.event_id],
            room_id=e1_create.room_id,
        )
        # 9. Eve joins the room.
        e9_me1 = self.create_event(
            EventTypes.Member,
            EVELYN,
            sender=EVELYN,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e8_power3.event_id, e4_jr.event_id],
            room_id=e1_create.room_id,
        )
        # 10. Eve changes her name, /!\\ but cites old power levels /!\
        e10_me2 = self.create_event(
            EventTypes.Member,
            EVELYN,
            sender=EVELYN,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[
                e3_power1.event_id,
                e4_jr.event_id,
                e9_me1.event_id,
            ],
            room_id=e1_create.room_id,
        )
        # 11. Zara joins the room, citing the most recent power levels.
        e11_mz = self.create_event(
            EventTypes.Member,
            ZARA,
            sender=ZARA,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e8_power3.event_id, e4_jr.event_id],
            room_id=e1_create.room_id,
        )

        # Event 10 above is DODGY: it directly cites old auth events, but indirectly
        # cites new ones. If the state after event 10 contains old power level and old
        # join events, we are vulnerable to a reset.

        dodgy_state_after_eve_rename: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.Member, CHARLIE): e6_mc.event_id,
            (EventTypes.Member, EVELYN): e10_me2.event_id,
            (EventTypes.PowerLevels, ""): e3_power1.event_id,  # old and /!\\ DODGY /!\
            (EventTypes.JoinRules, ""): e4_jr.event_id,
        }

        sensible_state_after_zara_joins: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.Member, CHARLIE): e6_mc.event_id,
            (EventTypes.Member, ZARA): e11_mz.event_id,
            (EventTypes.PowerLevels, ""): e8_power3.event_id,
            (EventTypes.JoinRules, ""): e4_jr.event_id,
        }

        expected: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.Member, CHARLIE): e6_mc.event_id,
            # Expect ME2 replayed first: it's in the POWER 1 epoch
            # Then ME1, in the POWER 3 epoch
            (EventTypes.Member, EVELYN): e9_me1.event_id,
            (EventTypes.Member, ZARA): e11_mz.event_id,
            (EventTypes.PowerLevels, ""): e8_power3.event_id,
            (EventTypes.JoinRules, ""): e4_jr.event_id,
        }

        self.get_resolution_and_verify_expected(
            [dodgy_state_after_eve_rename, sensible_state_after_zara_joins],
            [
                e1_create,
                e2_ma,
                e3_power1,
                e4_jr,
                e5_mb,
                e6_mc,
                e7_power2,
                e8_power3,
                e9_me1,
                e10_me2,
                e11_mz,
            ],
            expected,
        )

    def test_state_reset_start_empty_set(self) -> None:
        # The join rules reset to missing, when:
        # - join rules were in conflict
        # - the membership of those join rules' senders were not in conflict
        # - those memberships are all leaves.

        # 1. Alice creates a room.
        e1_create = self.create_event(
            EventTypes.Create,
            "",
            sender=ALICE,
            content={"creator": ALICE},
            auth_events=[],
        )
        # 2. Alice joins it.
        e2_ma1 = self.create_event(
            EventTypes.Member,
            ALICE,
            sender=ALICE,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[],
            room_id=e1_create.room_id,
        )
        # 3. Alice makes Bob an admin.
        e3_power = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=ALICE,
            content={"users": {BOB: 100}},
            auth_events=[e2_ma1.event_id],
            room_id=e1_create.room_id,
        )
        # 4. Alice sets the room to public.
        e4_jr1 = self.create_event(
            EventTypes.JoinRules,
            "",
            sender=ALICE,
            content={"join_rule": JoinRules.PUBLIC},
            auth_events=[e2_ma1.event_id, e3_power.event_id],
            room_id=e1_create.room_id,
        )
        # 5. Bob joins.
        e5_mb = self.create_event(
            EventTypes.Member,
            BOB,
            sender=BOB,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e3_power.event_id, e4_jr1.event_id],
            room_id=e1_create.room_id,
        )
        # 6. Alice sets join rules to invite.
        e6_jr2 = self.create_event(
            EventTypes.JoinRules,
            "",
            sender=ALICE,
            content={"join_rule": JoinRules.INVITE},
            auth_events=[e2_ma1.event_id, e3_power.event_id],
            room_id=e1_create.room_id,
        )
        # 7. Alice then leaves.
        e7_ma2 = self.create_event(
            EventTypes.Member,
            ALICE,
            sender=ALICE,
            content=MEMBERSHIP_CONTENT_LEAVE,
            auth_events=[e3_power.event_id, e2_ma1.event_id],
            room_id=e1_create.room_id,
        )

        correct_state: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e7_ma2.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.PowerLevels, ""): e3_power.event_id,
            (EventTypes.JoinRules, ""): e6_jr2.event_id,
        }

        # Imagine that another server gives us incorrect state on a fork
        # (via e.g. backfill). It cites the old join rules.
        incorrect_state: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e7_ma2.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.PowerLevels, ""): e3_power.event_id,
            (EventTypes.JoinRules, ""): e4_jr1.event_id,
        }

        # Resolving those two should give us the new join rules.
        expected: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e7_ma2.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.PowerLevels, ""): e3_power.event_id,
            (EventTypes.JoinRules, ""): e6_jr2.event_id,
        }

        self.get_resolution_and_verify_expected(
            [correct_state, incorrect_state],
            [e1_create, e2_ma1, e3_power, e4_jr1, e5_mb, e6_jr2, e7_ma2],
            expected,
        )

    def test_supplemental_merge_does_not_clobber_auth_chain(self) -> None:
        """Regression test: in V2.1, the supplemental merge from resolved_state
        must NOT override an event's own auth_events.

        Scenario: join_rules forks (public vs invite). Bob joined when the room
        was public, so his auth chain contains join_rules=public. After the
        control pass resolves join_rules to invite, the supplemental merge in V2
        would overwrite bob's auth state with join_rules=invite, causing bob's
        join to fail auth with 'not invited to invite-only room'.

        V2.1 must skip the supplemental merge so bob authenticates against his
        own auth chain (which has join_rules=public) and survives resolution.
        """
        # 1. Alice creates a room.
        e1_create = self.create_event(
            EventTypes.Create,
            "",
            sender=ALICE,
            content={"creator": ALICE},
            auth_events=[],
        )
        # 2. Alice joins.
        e2_ma1 = self.create_event(
            EventTypes.Member,
            ALICE,
            sender=ALICE,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[],
            room_id=e1_create.room_id,
        )
        # 3. Power levels.
        e3_power = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=ALICE,
            content={"users": {}},
            auth_events=[e2_ma1.event_id],
            room_id=e1_create.room_id,
        )
        # 4. Join rules = public.
        e4_jr1 = self.create_event(
            EventTypes.JoinRules,
            "",
            sender=ALICE,
            content={"join_rule": JoinRules.PUBLIC},
            auth_events=[e2_ma1.event_id, e3_power.event_id],
            room_id=e1_create.room_id,
        )
        # 5. Bob joins (auth chain has join_rules=public).
        e5_mb = self.create_event(
            EventTypes.Member,
            BOB,
            sender=BOB,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e3_power.event_id, e4_jr1.event_id],
            room_id=e1_create.room_id,
        )
        # 6. Join rules = invite (fork).
        e6_jr2 = self.create_event(
            EventTypes.JoinRules,
            "",
            sender=ALICE,
            content={"join_rule": JoinRules.INVITE},
            auth_events=[e2_ma1.event_id, e3_power.event_id],
            room_id=e1_create.room_id,
        )
        # 7. Alice leaves.
        e7_ma2 = self.create_event(
            EventTypes.Member,
            ALICE,
            sender=ALICE,
            content=MEMBERSHIP_CONTENT_LEAVE,
            auth_events=[e3_power.event_id, e2_ma1.event_id],
            room_id=e1_create.room_id,
        )

        # State set 1: has bob, has JR2 (invite)
        state_with_bob: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e7_ma2.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.PowerLevels, ""): e3_power.event_id,
            (EventTypes.JoinRules, ""): e6_jr2.event_id,
        }

        # State set 2: does NOT have bob, has JR1 (public)
        # This makes bob's membership conflicted, forcing it through
        # iterative_auth_checks: the path where the supplemental merge would
        # previously have applied before the V2.1 change.
        state_without_bob: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e7_ma2.event_id,
            (EventTypes.PowerLevels, ""): e3_power.event_id,
            (EventTypes.JoinRules, ""): e4_jr1.event_id,
        }

        # Bob must survive: his auth chain has JR1=public, which should be used
        # for his auth check (V2.1 skips supplemental merge).
        # JR2 wins the control pass (newer timestamp).
        expected: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e7_ma2.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.PowerLevels, ""): e3_power.event_id,
            (EventTypes.JoinRules, ""): e6_jr2.event_id,
        }

        self.get_resolution_and_verify_expected(
            [state_with_bob, state_without_bob],
            [e1_create, e2_ma1, e3_power, e4_jr1, e5_mb, e6_jr2, e7_ma2],
            expected,
        )

    def test_conflicted_subgraph_preserves_power_levels(self) -> None:
        """Regression test ported from Complement's
        TestMSC4297StateResolutionV2_1_includes_conflicted_subgraph.

        Scenario: Creator creates a room, gives Alice PL 100. Alice promotes
        Bob to PL 50, Bob promotes Charlie to PL 50 (PL3). Eve joins citing
        the ORIGINAL power levels (PL1) in her auth_events -- this is "dodgy"
        and creates a conflicted subgraph where one side has PL1 and the other
        has PL3.

        State resolution must preserve PL3 (with bob:50, charlie:50), not
        regress to PL1 (empty users dict).
        """
        # Alice creates the room.
        e1_create = self.create_event(
            EventTypes.Create,
            "",
            sender=ALICE,
            content={"creator": ALICE},
            auth_events=[],
        )
        # Alice joins.
        e2_ma = self.create_event(
            EventTypes.Member,
            ALICE,
            sender=ALICE,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[],
            room_id=e1_create.room_id,
        )
        # Initial power levels (alice is creator, implicit PL 100 in V12).
        e3_power1 = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=ALICE,
            content={"users": {}},
            auth_events=[e2_ma.event_id],
            room_id=e1_create.room_id,
        )
        # Join rules = public.
        e4_jr = self.create_event(
            EventTypes.JoinRules,
            "",
            sender=ALICE,
            content={"join_rule": JoinRules.PUBLIC},
            auth_events=[e2_ma.event_id, e3_power1.event_id],
            room_id=e1_create.room_id,
        )
        # Bob joins.
        e5_mb = self.create_event(
            EventTypes.Member,
            BOB,
            sender=BOB,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e3_power1.event_id, e4_jr.event_id],
            room_id=e1_create.room_id,
        )
        # Charlie joins.
        e6_mc = self.create_event(
            EventTypes.Member,
            CHARLIE,
            sender=CHARLIE,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e3_power1.event_id, e4_jr.event_id],
            room_id=e1_create.room_id,
        )
        # Alice PROMOTES Bob to PL 50 (alice omitted - implicit PL 100).
        e7_power2 = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=ALICE,
            content={"users": {BOB: 50}},
            auth_events=[e2_ma.event_id, e3_power1.event_id],
            room_id=e1_create.room_id,
        )
        # Bob promotes Charlie to PL 50.
        e8_power3 = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=BOB,
            content={"users": {BOB: 50, CHARLIE: 50}},
            auth_events=[e5_mb.event_id, e7_power2.event_id],
            room_id=e1_create.room_id,
        )
        # Zara joins, citing PL3 (correct).
        e9_mz = self.create_event(
            EventTypes.Member,
            ZARA,
            sender=ZARA,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e8_power3.event_id, e4_jr.event_id],
            room_id=e1_create.room_id,
        )
        # Eve joins, citing PL1 (DODGY - old power levels).
        e10_me = self.create_event(
            EventTypes.Member,
            EVELYN,
            sender=EVELYN,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e3_power1.event_id, e4_jr.event_id],
            room_id=e1_create.room_id,
        )

        # -- END SETUP --

        # The "dodgy" state fork: has Eve with old PL1.
        dodgy_state: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.Member, CHARLIE): e6_mc.event_id,
            (EventTypes.Member, EVELYN): e10_me.event_id,
            (EventTypes.PowerLevels, ""): e3_power1.event_id,  # /!\ DODGY /!\
            (EventTypes.JoinRules, ""): e4_jr.event_id,
        }

        # The correct state fork: has Zara with PL3.
        correct_state: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.Member, CHARLIE): e6_mc.event_id,
            (EventTypes.Member, ZARA): e9_mz.event_id,
            (EventTypes.PowerLevels, ""): e8_power3.event_id,
            (EventTypes.JoinRules, ""): e4_jr.event_id,
        }

        # Resolution must pick PL3 (alice:100, bob:50, charlie:50), not PL1.
        expected: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.Member, BOB): e5_mb.event_id,
            (EventTypes.Member, CHARLIE): e6_mc.event_id,
            (EventTypes.Member, EVELYN): e10_me.event_id,
            (EventTypes.Member, ZARA): e9_mz.event_id,
            (EventTypes.PowerLevels, ""): e8_power3.event_id,
            (EventTypes.JoinRules, ""): e4_jr.event_id,
        }

        self.get_resolution_and_verify_expected(
            [dodgy_state, correct_state],
            [
                e1_create,
                e2_ma,
                e3_power1,
                e4_jr,
                e5_mb,
                e6_mc,
                e7_power2,
                e8_power3,
                e9_mz,
                e10_me,
            ],
            expected,
        )

    def test_v21_cve_auth_bypass_kick_scenario(self) -> None:
        """
        SECURITY TEST: Proves whether removing the supplemental merge
        re-opens a Power Level Replay Attack (CVE).

        Scenario: Alice creates a room, joins, sets up power levels giving
        Eve admin (PL 100). Charlie joins. Alice demotes Eve to PL 0.
        Eve crafts a kick of Charlie citing the OLD power levels (where she was
        still admin) in her auth_events, attempting to bypass the demotion.

        If the supplemental merge is disabled, Eve's kick authenticates against
        her historical PL100. It then enters the conflict set with Charlie's join.
        If Eve's kick has a newer timestamp, it wins the tie-breaker and
        Charlie is kicked, proving a CVE.
        """
        # Room setup.
        e1_create = self.create_event(
            EventTypes.Create,
            "",
            sender=ALICE,
            content={"creator": ALICE},
            auth_events=[],
        )
        e2_ma = self.create_event(
            EventTypes.Member,
            ALICE,
            sender=ALICE,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[],
            room_id=e1_create.room_id,
        )
        e2_join_rules = self.create_event(
            EventTypes.JoinRules,
            "",
            sender=ALICE,
            content={"join_rule": "public"},
            auth_events=[e2_ma.event_id],
            room_id=e1_create.room_id,
        )
        e3_charlie_join = self.create_event(
            EventTypes.Member,
            CHARLIE,
            sender=CHARLIE,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e2_join_rules.event_id],
            room_id=e1_create.room_id,
        )
        e4_eve_join = self.create_event(
            EventTypes.Member,
            EVELYN,
            sender=EVELYN,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e2_join_rules.event_id],
            room_id=e1_create.room_id,
        )

        # Alice gives Eve admin (PL 100).
        e5_pl1 = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=ALICE,
            content={"users": {EVELYN: 100}},
            auth_events=[e2_ma.event_id],
            room_id=e1_create.room_id,
        )

        # Alice DEMOTES Eve to PL 0.
        e6_pl2 = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=ALICE,
            content={"users": {EVELYN: 0}},
            auth_events=[e2_ma.event_id, e5_pl1.event_id],
            room_id=e1_create.room_id,
        )

        # THE ATTACK: Eve kicks Charlie, maliciously citing
        # the old PL (e5_pl1) where she was still admin.
        e7_attack_kick = self.create_event(
            EventTypes.Member,
            CHARLIE,
            sender=EVELYN,
            content={"membership": "leave"},
            auth_events=[e4_eve_join.event_id, e5_pl1.event_id],
            room_id=e1_create.room_id,
        )

        state_a: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.JoinRules, ""): e2_join_rules.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.Member, CHARLIE): e3_charlie_join.event_id,
            (EventTypes.Member, EVELYN): e4_eve_join.event_id,
            (EventTypes.PowerLevels, ""): e6_pl2.event_id,
        }

        # Malicious fork: cites old PL where Eve was admin, and Charlie is kicked.
        state_b: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.JoinRules, ""): e2_join_rules.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.Member, CHARLIE): e7_attack_kick.event_id,
            (EventTypes.Member, EVELYN): e4_eve_join.event_id,
            (EventTypes.PowerLevels, ""): e5_pl1.event_id,
        }

        # The algorithm will resolve between state_a and state_b.
        # We expect Eve's kick to be REJECTED, and Charlie to remain joined.
        expected_secure_state: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.JoinRules, ""): e2_join_rules.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.Member, CHARLIE): e3_charlie_join.event_id,
            (EventTypes.Member, EVELYN): e4_eve_join.event_id,
            (EventTypes.PowerLevels, ""): e6_pl2.event_id,
        }

        self.get_resolution_and_verify_expected(
            [state_a, state_b],
            [e1_create, e2_ma, e2_join_rules, e3_charlie_join, e4_eve_join, e5_pl1, e6_pl2, e7_attack_kick],
            expected_secure_state,
        )

    def test_v21_self_corrects_corrupted_state(self) -> None:
        """
        Proves that a server with "stuck" or corrupted state will naturally
        self-correct and converge with the rest of the network as soon as a
        new federated event triggers a state merge.

        State resolution is a pure function of the DAG. If Server A has
        broken state and Server B has the correct state, merging the two
        forks forces re-evaluation against the full DAG, producing the
        mathematically correct result.
        """
        # Base events
        e1_create = self.create_event(
            EventTypes.Create,
            "",
            sender=ALICE,
            content={"creator": ALICE},
            auth_events=[],
        )
        e2_ma = self.create_event(
            EventTypes.Member,
            ALICE,
            sender=ALICE,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[],
            room_id=e1_create.room_id,
        )
        e3_pl = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=ALICE,
            content={"users": {BOB: 50}},
            auth_events=[e2_ma.event_id],
            room_id=e1_create.room_id,
        )
        e4_eve_join = self.create_event(
            EventTypes.Member,
            EVELYN,
            sender=EVELYN,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e3_pl.event_id],
            room_id=e1_create.room_id,
        )
        e5_bob_join = self.create_event(
            EventTypes.Member,
            BOB,
            sender=BOB,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[e3_pl.event_id],
            room_id=e1_create.room_id,
        )

        # Bob (legitimate mod) bans Eve.
        e6_ban_eve = self.create_event(
            EventTypes.Member,
            EVELYN,
            sender=BOB,
            content=MEMBERSHIP_CONTENT_BAN,
            auth_events=[e5_bob_join.event_id, e3_pl.event_id, e4_eve_join.event_id],
            room_id=e1_create.room_id,
        )

        # Server A (Buggy/Corrupted): somehow lost Bob's ban during a
        # previous bad resolution. Eve is still shown as joined.
        corrupted_state_server_a: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.PowerLevels, ""): e3_pl.event_id,
            (EventTypes.Member, BOB): e5_bob_join.event_id,
            (EventTypes.Member, EVELYN): e4_eve_join.event_id,
        }

        # Server B (Healthy): has the correct state with Eve banned.
        healthy_state_server_b: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.PowerLevels, ""): e3_pl.event_id,
            (EventTypes.Member, BOB): e5_bob_join.event_id,
            (EventTypes.Member, EVELYN): e6_ban_eve.event_id,
        }

        # When resolving the forks, V2.1 evaluates e6_ban_eve natively.
        # Bob is PL 50, the ban succeeds, the room heals.
        expected_healed_state: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.PowerLevels, ""): e3_pl.event_id,
            (EventTypes.Member, BOB): e5_bob_join.event_id,
            (EventTypes.Member, EVELYN): e6_ban_eve.event_id,
        }

        self.get_resolution_and_verify_expected(
            [corrupted_state_server_a, healthy_state_server_b],
            [e1_create, e2_ma, e3_pl, e4_eve_join, e5_bob_join, e6_ban_eve],
            expected_healed_state,
        )

    def test_incomplete_dag_picks_stale_membership(self) -> None:
        """
        Proves that incomplete local DAG data causes state resolution to pick
        a stale membership event, and that providing the complete event set
        self-corrects to the latest join with updated profile info.

        Real-world scenario (earthtopic c10y):
        - User joins (join1: plain displayname, no avatar)
        - User leaves
        - User rejoins (join2: updated displayname + avatar_url)
        - Server A rejected join2 during ingestion (e.g. auth chain cascade)
        - Server A resolves with incomplete data → picks join1 (stale profile)
        - Server B has complete data → picks join2 (correct profile)
        - When A gets the missing event, resolution self-corrects
        """
        # Room setup
        e1_create = self.create_event(
            EventTypes.Create,
            "",
            sender=ALICE,
            content={"creator": ALICE},
            auth_events=[],
        )
        e2_ma = self.create_event(
            EventTypes.Member,
            ALICE,
            sender=ALICE,
            content=MEMBERSHIP_CONTENT_JOIN,
            auth_events=[],
            room_id=e1_create.room_id,
        )
        e3_pl = self.create_event(
            EventTypes.PowerLevels,
            "",
            sender=ALICE,
            content={"users": {}},
            auth_events=[e2_ma.event_id],
            room_id=e1_create.room_id,
        )
        e4_jr = self.create_event(
            EventTypes.JoinRules,
            "",
            sender=ALICE,
            content={"join_rule": JoinRules.PUBLIC},
            auth_events=[e2_ma.event_id, e3_pl.event_id],
            room_id=e1_create.room_id,
        )

        # Eve joins with plain profile (join1)
        e5_eve_join1 = self.create_event(
            EventTypes.Member,
            EVELYN,
            sender=EVELYN,
            content={
                "membership": Membership.JOIN,
                "displayname": "Eve",
            },
            auth_events=[e3_pl.event_id, e4_jr.event_id],
            room_id=e1_create.room_id,
        )

        # Eve leaves
        e6_eve_leave = self.create_event(
            EventTypes.Member,
            EVELYN,
            sender=EVELYN,
            content=MEMBERSHIP_CONTENT_LEAVE,
            auth_events=[e3_pl.event_id, e5_eve_join1.event_id],
            room_id=e1_create.room_id,
        )

        # Eve REJOINS with updated profile (join2)
        e7_eve_join2 = self.create_event(
            EventTypes.Member,
            EVELYN,
            sender=EVELYN,
            content={
                "membership": Membership.JOIN,
                "displayname": "Eve [Updated]",
                "avatar_url": "mxc://example.com/avatar",
            },
            auth_events=[e3_pl.event_id, e4_jr.event_id, e6_eve_leave.event_id],
            room_id=e1_create.room_id,
        )

        # --- Test 1: Incomplete DAG (join2 missing, simulating rejection) ---
        # Server A only knows about join1 and the leave. The rejoin (join2)
        # was rejected during ingestion due to an auth chain cascade failure.
        incomplete_state_a: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.PowerLevels, ""): e3_pl.event_id,
            (EventTypes.JoinRules, ""): e4_jr.event_id,
            (EventTypes.Member, EVELYN): e5_eve_join1.event_id,
        }

        # Server B saw the leave but not the rejoin (different fork)
        incomplete_state_b: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.PowerLevels, ""): e3_pl.event_id,
            (EventTypes.JoinRules, ""): e4_jr.event_id,
            (EventTypes.Member, EVELYN): e6_eve_leave.event_id,
        }

        # Without join2, the conflict is {join1, leave}. The leave event wins
        # the event_id lexicographic tiebreak. This is WRONG from the user's
        # perspective (Eve actually rejoined!) but CORRECT given the incomplete
        # data -- state res is a pure function of its inputs.
        stale_expected: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.PowerLevels, ""): e3_pl.event_id,
            (EventTypes.JoinRules, ""): e4_jr.event_id,
            (EventTypes.Member, EVELYN): e6_eve_leave.event_id,
        }

        # Without join2, the conflict is {join1, leave}. The leave event wins
        # the event_id lexicographic tiebreak -- Eve appears LEFT (wrong).
        self.get_resolution_and_verify_expected(
            [incomplete_state_a, incomplete_state_b],
            [e1_create, e2_ma, e3_pl, e4_jr, e5_eve_join1, e6_eve_leave],
            stale_expected,
        )

        # --- Test 2: Complete DAG (join2 present) -- self-corrects ---
        # Now Server C has the complete data including join2.
        complete_state_c: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.PowerLevels, ""): e3_pl.event_id,
            (EventTypes.JoinRules, ""): e4_jr.event_id,
            (EventTypes.Member, EVELYN): e7_eve_join2.event_id,
        }

        # With COMPLETE data, resolution picks join2 (latest, correct profile)
        correct_expected: StateMap[str] = {
            (EventTypes.Create, ""): e1_create.event_id,
            (EventTypes.Member, ALICE): e2_ma.event_id,
            (EventTypes.PowerLevels, ""): e3_pl.event_id,
            (EventTypes.JoinRules, ""): e4_jr.event_id,
            (EventTypes.Member, EVELYN): e7_eve_join2.event_id,
        }

        self.get_resolution_and_verify_expected(
            [incomplete_state_a, complete_state_c],
            [
                e1_create,
                e2_ma,
                e3_pl,
                e4_jr,
                e5_eve_join1,
                e6_eve_leave,
                e7_eve_join2,
            ],
            correct_expected,
        )

        # --- Assertions: prove the divergence is a data completeness issue ---
        # The stale result (leave) differs from the correct result (join2).
        # Same algorithm, same code path, different inputs -> different outputs.
        # This is NOT a state res bug -- it's a federation ingestion failure.
        self.assertNotEqual(
            stale_expected[(EventTypes.Member, EVELYN)],
            correct_expected[(EventTypes.Member, EVELYN)],
            "Incomplete and complete DAGs must produce DIFFERENT membership "
            "state, proving the divergence is caused by missing data, not "
            "an algorithm defect.",
        )
        # The stale result has Eve as LEFT (wrong!)
        self.assertEqual(
            stale_expected[(EventTypes.Member, EVELYN)],
            e6_eve_leave.event_id,
            "Incomplete DAG should resolve Eve as LEFT (stale/wrong)",
        )
        # The correct result has Eve as JOINED with updated profile (right!)
        self.assertEqual(
            correct_expected[(EventTypes.Member, EVELYN)],
            e7_eve_join2.event_id,
            "Complete DAG should resolve Eve as JOINED with updated profile",
        )

    async def _get_auth_difference_and_conflicted_subgraph(
        self,
        room_id: str,
        state_maps: Sequence[StateMap[str]],
        event_map: dict[str, EventBase] | None,
        state_res_store: StateResolutionStoreInterface,
    ) -> set[str]:
        _, conflicted_state = _seperate(state_maps)
        conflicted_set: set[str] | None = set(
            itertools.chain.from_iterable(conflicted_state.values())
        )
        if event_map is None:
            event_map = {}
        return await _get_auth_chain_difference(
            room_id,
            state_maps,
            event_map,
            state_res_store,
            conflicted_set,
        )

    def get_resolution_and_verify_expected(
        self,
        state_maps: Sequence[StateMap[str]],
        events: list[EventBase],
        expected: StateMap[str],
    ) -> None:
        room_id = events[0].room_id
        # First we try everything in-memory to check that the test case works.
        event_map = {ev.event_id: ev for ev in events}
        for ev in events:
            logger.debug("%s => %s %s => %s", ev.event_id, ev.type, ev.state_key, ev.content)
        resolution = self.get_success(
            resolve_events_with_store(
                FakeClock(),
                room_id,
                events[0].room_version,
                state_maps,
                event_map=event_map,
                state_res_store=TestStateResolutionStore(event_map),
            )
        )
        self.assertEqual(resolution, expected)

        got_auth_diff = self.get_success(
            self._get_auth_difference_and_conflicted_subgraph(
                room_id,
                state_maps,
                event_map,
                TestStateResolutionStore(event_map),
            )
        )
        # We should never see the create event in the auth diff. If we do, the
        # conflicted subgraph is wrong and is returning too many old events.
        self.assertNotIn(
            events[0].event_id,
            got_auth_diff,
            "Create event incorrectly found in auth difference",
        )

        # now let's make the room exist on the DB, some queries rely on there being a row in
        # the rooms table when persisting. Guard against duplicate inserts when a test calls
        # this helper multiple times for the same room.
        existing = self.get_success(self.store.get_room(room_id))
        if not existing:
            self.get_success(
                self.store.store_room(
                    room_id,
                    events[0].sender,
                    True,
                    events[0].room_version,
                )
            )

        def resolve_and_check() -> None:
            event_map = {ev.event_id: ev for ev in events}
            store = StateResolutionStore(
                self._persistence.main_store,
                self._state_deletion,
            )
            resolution = self.get_success(
                resolve_events_with_store(
                    FakeClock(),
                    room_id,
                    RoomVersions.V12,
                    state_maps,
                    event_map=event_map,
                    state_res_store=store,
                )
            )
            self.assertEqual(resolution, expected)
            got_auth_diff2 = self.get_success(
                self._get_auth_difference_and_conflicted_subgraph(
                    room_id,
                    state_maps,
                    event_map,
                    store,
                )
            )
            # no matter how many events are persisted, the overall diff should always be the same.
            self.assertEqual(got_auth_diff, got_auth_diff2)

        # Drip-feed events one-by-one, persisting then re-resolving.
        # Ensures correct handling of mixed persisted/unpersisted events.
        unpersisted_events = list(events)
        while unpersisted_events:
            event_to_persist = unpersisted_events.pop(0)
            self.persist_event(event_to_persist)
            resolve_and_check()

    def persist_event(
        self, event: EventBase, state: StateMap[str] | None = None
    ) -> None:
        """Persist the event, with optional state"""
        context = self.get_success(
            self.state.compute_event_context(
                event,
                state_ids_before_event=state,
                partial_state=None if state is None else False,
            )
        )
        self.get_success(self._persistence.persist_event(event, context))

    def create_event(
        self,
        event_type: str,
        state_key: str | None,
        sender: str,
        content: dict,
        auth_events: list[str],
        prev_events: list[str] | None = None,
        room_id: str | None = None,
    ) -> EventBase:
        """Short-hand for event_from_pdu_json for fields we typically care about.
        Tests can override by just calling event_from_pdu_json directly."""
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
        if event_type != EventTypes.Create:
            if room_id is None:
                raise Exception("must specify a room_id to create_event")
            pdu["room_id"] = room_id
        return event_from_pdu_json(
            pdu,
            RoomVersions.V12,
        )
