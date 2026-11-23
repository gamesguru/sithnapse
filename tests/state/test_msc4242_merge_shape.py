#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""
Does the way the 20-parent cap groups a merge change the resolved state?

Branches off a shared topic event T0 (room version MSC4242v12, i.e. state resolution
V2.2, with `prev_state_events` as the graph edges):

    X   (dave, PL 50)        sets the topic to "X"             ts 10
    PL_B (bob, PL 90)        promotes carol to PL 50           ts 15
      `- Y (carol)           sets the topic to "Y"             ts 20
    PL_A (admin, PL 100)     demotes bob to PL 0               ts 12

Shape A: the whole frontier {X, Y, PL_A} is resolved at once.
Shape B: {X, Y} are merged first by M1 (what a capped selection could do), and PL_A is
only merged in afterwards, so the frontier is {M1, PL_A}.

Every event goes through the production federation ingestion path, so the calculated
auth events come from Synapse itself rather than from the fixture.
"""

from synapse.api.room_versions import RoomVersion, RoomVersions
from synapse.events import EventBase
from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.types import JsonDict

from tests import unittest
from tests.test_utils import event_injection
from tests.test_utils.event_builders import make_test_event

OTHER = "other.example.com"
ADMIN = f"@admin:{OTHER}"
BOB = f"@bob:{OTHER}"
CAROL = f"@carol:{OTHER}"
DAVE = f"@dave:{OTHER}"

PL_EVENTS = {"m.room.power_levels": 90, "m.room.topic": 50}


class MSC4242MergeShapeTestCase(unittest.FederatingHomeserverTestCase):
    OTHER_SERVER_NAME = OTHER
    servlets = [
        admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
    ]

    def _make_room(self, name: str) -> tuple[str, str]:
        user_id = self.register_user(name, "test")
        tok = self.login(name, "test")
        room_id = self.helper.create_room_as(
            room_creator=user_id,
            tok=tok,
            room_version=RoomVersions.MSC4242v12.identifier,
        )
        # kermit is the v12 creator, so is absent from `users`.
        self.helper.send_state(
            room_id,
            "m.room.power_levels",
            {
                "users": {ADMIN: 100, BOB: 90, DAVE: 50},
                "events": PL_EVENTS,
                "state_default": 50,
                "events_default": 0,
            },
            tok=tok,
        )
        for u in (ADMIN, BOB, CAROL, DAVE):
            self.get_success(
                event_injection.inject_member_event(self.hs, room_id, u, "join")
            )
        t0 = self.helper.send_state(room_id, "m.room.topic", {"topic": "T0"}, tok=tok)
        return room_id, t0["event_id"]

    def _remote(
        self,
        room_id: str,
        room_version: RoomVersion,
        kind: str,
        state_key: str,
        sender: str,
        parents: list[str],
        ts: int,
        depth: int,
        content: JsonDict,
    ) -> EventBase:
        return make_test_event(
            self.add_hashes_and_signatures_from_other_server(
                {
                    "type": kind,
                    "state_key": state_key,
                    "room_id": room_id,
                    "sender": sender,
                    "prev_events": parents,
                    "prev_state_events": parents,
                    "origin_server_ts": ts,
                    "depth": depth,
                    "content": content,
                },
                room_version,
            ),
            room_version,
        )

    def _topic(self, room_id: str) -> str:
        store = self.hs.get_datastores().main
        state = self.get_success(store.get_partial_current_state_ids(room_id))
        event = self.get_success(store.get_event(state[("m.room.topic", "")]))
        return event.content["topic"]

    def _build(self, merge_first: bool) -> str:
        store = self.hs.get_datastores().main
        handler = self.hs.get_federation_event_handler()
        room_id, t0 = self._make_room("shape_b" if merge_first else "shape_a")
        rv = self.get_success(store.get_room_version(room_id))
        self.assertTrue(rv.msc4242_state_dags)

        def mk(
            kind: str,
            state_key: str,
            sender: str,
            parents: list[str],
            ts: int,
            depth: int,
            content: JsonDict,
        ) -> EventBase:
            return self._remote(
                room_id, rv, kind, state_key, sender, parents, ts, depth, content
            )

        d = 100
        x = mk("m.room.topic", "", DAVE, [t0], 10, d, {"topic": "X"})
        pl_b = mk(
            "m.room.power_levels",
            "",
            BOB,
            [t0],
            15,
            d,
            {
                "users": {ADMIN: 100, BOB: 90, DAVE: 50, CAROL: 50},
                "events": PL_EVENTS,
                "state_default": 50,
                "events_default": 0,
            },
        )
        y = mk("m.room.topic", "", CAROL, [pl_b.event_id], 20, d + 1, {"topic": "Y"})
        pl_a = mk(
            "m.room.power_levels",
            "",
            ADMIN,
            [t0],
            12,
            d,
            {
                "users": {ADMIN: 100, DAVE: 50},
                "events": PL_EVENTS,
                "state_default": 50,
                "events_default": 0,
            },
        )
        events = [x, pl_b, y]
        if merge_first:
            m1 = mk(
                "org.example.merge", "m1", DAVE, [x.event_id, y.event_id], 25, d + 2, {}
            )
            events.append(m1)
            # Persist M1 before PL_A is known, so the capped merge is a real prior step.
            self.get_success(
                handler._process_pulled_events(OTHER, events, backfilled=True)
            )
            self.assertEqual(
                self._topic(room_id), "Y", "M1 alone should resolve {X, Y} to Y"
            )
            batch = [pl_a]
        else:
            batch = events + [pl_a]
        self.get_success(handler._process_pulled_events(OTHER, batch, backfilled=True))
        events = events + [pl_a] if merge_first else batch
        for e in events:
            self.assertIsNone(
                self.get_success(store.get_rejection_reason(e.event_id)),
                f"{e.event_id} ({e.type}) was rejected",
            )
        if merge_first:
            # Exercise the incoming-event state_before path over the persisted
            # {M1, PL_A} extremities, in addition to current-state resolution.
            m2 = mk(
                "org.example.merge",
                "m2",
                ADMIN,
                [m1.event_id, pl_a.event_id],
                30,
                d + 3,
                {},
            )
            self.get_success(
                handler._process_pulled_events(OTHER, [m2], backfilled=True)
            )
            self.assertIsNone(
                self.get_success(store.get_rejection_reason(m2.event_id)),
                f"{m2.event_id} ({m2.type}) was rejected",
            )
            self.assertEqual(self._topic(room_id), "X")
        return self._topic(room_id)

    @unittest.override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_merge_shape_does_not_change_resolved_topic(self) -> None:
        all_at_once = self._build(merge_first=False)
        stepwise = self._build(merge_first=True)
        self.assertEqual(all_at_once, "X", "all-at-once should resolve to X")
        self.assertEqual(
            all_at_once,
            stepwise,
            "the resolved topic depends on how the frontier was merged",
        )

    test_merge_shape_does_not_change_resolved_topic.__dict__["todo"] = (
        "MSC4242 state resolution currently depends on merge shape"
    )

    @unittest.override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_twenty_parent_cap_can_drop_the_auth_relevant_head(self) -> None:
        """Exercise the same state loss with the real builder's 20-parent cap."""
        store = self.hs.get_datastores().main
        handler = self.hs.get_federation_event_handler()
        room_id, t0 = self._make_room("capped")
        rv = self.get_success(store.get_room_version(room_id))
        tok = self.login("capped", "test")
        depth = 100

        # Grind PL_A into the late-sorting part of the event-ID space. The 20
        # other heads are then ground below it, so Synapse's sorted(... )[:20]
        # selection necessarily leaves PL_A out.
        pl_a = None
        for nonce in range(1000):
            candidate = self._remote(
                room_id,
                rv,
                "m.room.power_levels",
                "",
                ADMIN,
                [t0],
                12,
                depth,
                {
                    "users": {ADMIN: 100, DAVE: 50},
                    "events": PL_EVENTS,
                    "state_default": 50,
                    "events_default": 0,
                    "nonce": nonce,
                },
            )
            if candidate.event_id.startswith("$z"):
                pl_a = candidate
                break
        self.assertIsNotNone(pl_a, "failed to grind a late-sorting PL_A event")
        assert pl_a is not None

        def grind_below(
            kind: str,
            state_key: str,
            sender: str,
            parents: list[str],
            ts: int,
            base_content: JsonDict,
        ) -> EventBase:
            for nonce in range(1000):
                candidate = self._remote(
                    room_id,
                    rv,
                    kind,
                    state_key,
                    sender,
                    parents,
                    ts,
                    depth,
                    {**base_content, "nonce": nonce},
                )
                if candidate.event_id < pl_a.event_id:
                    return candidate
            self.fail(f"failed to grind {kind} below the PL_A event ID")

        x = grind_below("m.room.topic", "", DAVE, [t0], 10, {"topic": "X"})
        pl_b = self._remote(
            room_id,
            rv,
            "m.room.power_levels",
            "",
            BOB,
            [t0],
            15,
            depth,
            {
                "users": {ADMIN: 100, BOB: 90, DAVE: 50, CAROL: 50},
                "events": PL_EVENTS,
                "state_default": 50,
                "events_default": 0,
            },
        )
        y = grind_below("m.room.topic", "", CAROL, [pl_b.event_id], 20, {"topic": "Y"})
        fillers = [
            grind_below(
                "org.example.filler",
                f"filler-{i}",
                ADMIN,
                [t0],
                40 + i,
                {"n": i},
            )
            for i in range(18)
        ]
        heads = [x, y, pl_a, *fillers]
        self.assertEqual(len(heads), 21)
        self.get_success(
            handler._process_pulled_events(
                OTHER, [x, pl_b, y, pl_a, *fillers], backfilled=True
            )
        )
        self.assertEqual(
            len(self.get_success(store.get_state_dag_extremities(room_id))), 21
        )
        self.assertEqual(
            self._topic(room_id), "X", "the full 21-head frontier should resolve to X"
        )

        # This goes through EventBuilder.build, which selects the sorted lowest
        # 20 extremities. Its signed event therefore omits the late PL_A head.
        response = self.helper.send_state(
            room_id, "org.example.merge", {}, tok=tok, state_key="m1"
        )
        m1 = self.get_success(store.get_event(response["event_id"]))
        self.assertEqual(len(m1.prev_state_events), 20)  # type: ignore[attr-defined]
        self.assertNotIn(pl_a.event_id, m1.prev_state_events)  # type: ignore[attr-defined]
        self.assertIn(x.event_id, m1.prev_state_events)  # type: ignore[attr-defined]
        self.assertIn(y.event_id, m1.prev_state_events)  # type: ignore[attr-defined]

        # The omitted branch remains concurrent, but the state-group resolver
        # loses X when resolving the new M1 head with PL_A.
        self.assertEqual(self._topic(room_id), "X")

    test_twenty_parent_cap_can_drop_the_auth_relevant_head.__dict__["todo"] = (
        "MSC4242 merge-shape state loss under Synapse's 20-parent selection is under investigation"
    )
