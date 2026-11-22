#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.

"""
Tests for how `EventBuilder.build` selects `prev_state_events` in MSC4242 rooms.

MSC4242 says an event cites the room's current state DAG forward extremities, and
separately caps `prev_state_events` at 20 entries. It does not say which 20 to cite
when there are more than 20 extremities. `EventBuilder.build` currently keeps the
20 lexicographically smallest event IDs (see the FIXME in `builder.py`).

Event IDs are hashes, so a sender can pick a content which makes its event sort
last. Those events are then never cited by honest servers for as long as there are
20 or more extremities which sort before them.

These tests document the *current* behaviour so that the gap is visible, and then
state the property a fixed implementation must have. They deliberately do not bless
lexicographic truncation: `test_late_sorting_extremity_is_eventually_cited` is the
requirement, and is expected to fail until a fair selection policy exists.
"""

from unittest.mock import AsyncMock, patch

from twisted.test.proto_helpers import MemoryReactor

from synapse.api.constants import EventTypes, StateDag
from synapse.api.room_versions import RoomVersions
from synapse.rest.client import room
from synapse.server import HomeServer
from synapse.util.clock import Clock

from tests.unittest import HomeserverTestCase, override_config


class MSC4242BuilderPrevStateEventsTestCase(HomeserverTestCase):
    user_id = "@user1:server"
    servlets = [room.register_servlets]

    def make_homeserver(self, reactor: MemoryReactor, clock: Clock) -> HomeServer:
        return self.setup_test_homeserver("server")

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.room_id = self.helper.create_room_as(
            self.user_id, room_version=RoomVersions.MSC4242v12.identifier
        )
        self.store = hs.get_datastores().main
        self.factory = hs.get_event_builder_factory()

    def _build_citing(self, extremities: list[str]) -> list[str]:
        """Build a message event as if the room had the given state DAG extremities,
        returning the `prev_state_events` the builder chose.
        """
        latest = self.get_success(self.store.get_latest_event_ids_in_room(self.room_id))
        builder = self.factory.for_room_version(
            RoomVersions.MSC4242v12,
            {
                "type": EventTypes.Message,
                "room_id": self.room_id,
                "sender": self.user_id,
                "content": {"msgtype": "m.text", "body": "hi"},
            },
        )
        # The extremities are synthetic, so the builder cannot compute state after them.
        # An empty result takes the "batched event" path where the caller supplies the
        # auth events. That path is not under test, only the parent selection is.
        with patch.object(
            self.hs.get_state_handler(),
            "compute_state_after_events",
            new=AsyncMock(return_value={}),
        ):
            event = self.get_success(
                builder.build(
                    prev_event_ids=list(latest),
                    auth_event_ids=["$" + self.room_id[1:]],
                    prev_state_events=extremities,
                )
            )
        return list(event.prev_state_events)  # type: ignore[attr-defined]

    @staticmethod
    def _ids(n: int) -> list[str]:
        # Distinct, fixed-width IDs which sort in numeric order.
        return [f"$e{i:04d}" for i in range(n)]

    @override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_at_or_below_limit_cites_every_extremity(self) -> None:
        extremities = self._ids(StateDag.MAX_PREV_STATE_EVENTS)
        self.assertEqual(sorted(self._build_citing(extremities)), extremities)

    @override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_over_limit_is_capped(self) -> None:
        extremities = self._ids(StateDag.MAX_PREV_STATE_EVENTS + 10)
        cited = self._build_citing(extremities)
        self.assertEqual(len(cited), StateDag.MAX_PREV_STATE_EVENTS)
        self.assertEqual(len(set(cited)), len(cited))

    @override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_truncation_is_by_event_id_today(self) -> None:
        """Characterisation test: records what Synapse does today, not what it should do.

        The extremity which sorts last is the one which is dropped, regardless of how
        long it has been an extremity.
        """
        extremities = self._ids(StateDag.MAX_PREV_STATE_EVENTS + 5)
        cited = self._build_citing(extremities)
        self.assertNotIn(extremities[-1], cited)
        self.assertEqual(sorted(cited), extremities[: StateDag.MAX_PREV_STATE_EVENTS])

    @override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_late_sorting_extremity_is_eventually_cited(self) -> None:
        """The requirement. Expected to FAIL until a fair selection policy exists.

        Model the attack: one extremity ("$zz_target") has an ID which sorts after
        everything else. Each round the honest server builds an event, and the
        attacker then adds enough fresh extremities that there are still more than 20.
        Whichever 20 the server cites are merged away (they stop being extremities).

        A fair policy (oldest first, round robin, random, ...) must cite the target
        within a bounded number of rounds. Lexicographic truncation never does.
        """
        target = "$zz_target"
        rounds = 10
        fresh = 0
        extremities = {target}
        for _ in range(StateDag.MAX_PREV_STATE_EVENTS + 5):
            extremities.add(f"$a{fresh:06d}")
            fresh += 1

        for round_no in range(rounds):
            cited = set(self._build_citing(sorted(extremities)))
            if target in cited:
                return
            # Cited events are merged; the new event is itself an extremity; the
            # attacker tops the set back up so it stays above the limit.
            extremities -= cited
            extremities.add(f"$h{round_no:06d}")
            while len(extremities) <= StateDag.MAX_PREV_STATE_EVENTS + 5:
                extremities.add(f"$a{fresh:06d}")
                fresh += 1

        self.fail(
            f"{target} was never cited in {rounds} rounds while the room had more than "
            f"{StateDag.MAX_PREV_STATE_EVENTS} state DAG extremities: it is starved "
            "and stays concurrent with every later event"
        )

    # Trial's todo marker records this known failure as an expected failure,
    # while the cap and truncation tests above remain ordinary passing tests.
    test_late_sorting_extremity_is_eventually_cited.__dict__["todo"] = (
        "A fair prev_state_events selection policy has not been implemented"
    )
