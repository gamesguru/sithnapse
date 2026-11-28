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
#

"""History purge in MSC4242 rooms must not lose the state of state DAG events.

A server which loses it judges later events differently from one which did not
purge, so each verdict here is checked both with and without a purge.

PRIVATE until disclosure is agreed: do not push to a public remote.

    prev_state_events graph        A = ban of the sender, or the sender's join
    base <- A <- T                 T makes A a non-tip
       `--- B                      B forks from before A
    X cites [A, B]
"""

import attr

from twisted.internet.testing import MemoryReactor

from synapse.api.room_versions import RoomVersions
from synapse.events.py_protocol import supports_msc4242_state_dag
from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.types import create_requester
from synapse.util.clock import Clock
from synapse.util.duration import Duration

from tests import unittest
from tests.test_utils import event_injection
from tests.test_utils.event_builders import make_test_event


@attr.s(auto_attribs=True)
class _Room:
    room_id: str
    token: str
    sender: str
    carol: str
    a: str
    b: str
    t: str
    cutoff: str


class PurgeStateDagTestCase(unittest.FederatingHomeserverTestCase):
    servlets = [
        admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
    ]

    def default_config(self) -> dict:
        config = super().default_config()
        config["experimental_features"] = {"msc4242_enabled": True}
        return config

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self.state_storage = hs.get_storage_controllers().state
        self.alice = self.register_user("alice", "pw")
        self.tok = self.login("alice", "pw")

    def _local_state(
        self, room_id: str, type_: str, content: dict, prev: list[str] | None = None
    ) -> str:
        event, _ = self.get_success(
            self.hs.get_event_creation_handler().create_and_send_nonmember_event(
                create_requester(self.alice),
                {
                    "type": type_,
                    "state_key": "",
                    "content": content,
                    "room_id": room_id,
                    "sender": self.alice,
                },
                prev_state_events=prev,
            )
        )
        return event.event_id

    def _make_room(
        self,
        branch_a: str,
        fork: bool = True,
        merging_message: bool = False,
        a_stays_tip: bool = False,
    ) -> _Room:
        """Builds the graph in the module docstring.

        Args:
            branch_a: "ban" if A bans X's sender, "join" if A is the only record
                that X's sender joined
            fork: if False, there is no B or T, so A stays the only tip
            merging_message: if True, send a message before the cutoff while the
                state DAG has two tips. The state resolved for it belongs to no
                state DAG event, so the purge leaves its state group unused.
            a_stays_tip: if True, fork B but send no T, so A and B are both tips
        """
        sender = f"@sender:{self.OTHER_SERVER_NAME}"
        carol = f"@carol:{self.OTHER_SERVER_NAME}"
        room_id = self.helper.create_room_as(
            self.alice,
            tok=self.tok,
            is_public=True,
            room_version=RoomVersions.MSC4242v12.identifier,
        )
        self.helper.send_state(
            room_id,
            "m.room.power_levels",
            {"events_default": 0, "state_default": 0},
            tok=self.tok,
        )

        def inject_member(user: str, membership: str, by: str) -> str:
            return self.get_success(
                event_injection.inject_member_event(
                    self.hs, room_id, by, membership, target=user
                )
            ).event_id

        inject_member(carol, "join", carol)
        if branch_a == "ban":
            inject_member(sender, "join", sender)
        (base,) = self.get_success(self.store.get_state_dag_extremities(room_id))

        if branch_a == "ban":
            a = inject_member(sender, "ban", self.alice)
        else:
            a = inject_member(sender, "join", sender)
        for i in range(3):
            self.helper.send(room_id, f"m{i}", tok=self.tok)
        if merging_message:
            tips = list(self.get_success(self.store.get_state_dag_extremities(room_id)))
            self._local_state(room_id, "test.one", {}, prev=tips)
            self._local_state(room_id, "test.two", {}, prev=tips)
            self.helper.send(room_id, "merging", tok=self.tok)
            # Bring the state DAG back to a single tip.
            self._local_state(room_id, "test.merge", {})
        cutoff = self.helper.send(room_id, "cutoff", tok=self.tok)["event_id"]

        if a_stays_tip:
            b = self._local_state(room_id, "m.room.name", {"name": "fork"}, prev=[base])
            t = a
        elif fork:
            t = self._local_state(room_id, "m.room.topic", {"topic": "after A"})
            b = self._local_state(room_id, "m.room.name", {"name": "fork"}, prev=[base])
        else:
            self.helper.send(room_id, "after", tok=self.tok)
            t = b = a
        self.assertEqual(
            set(self.get_success(self.store.get_state_dag_extremities(room_id))),
            {t, b},
        )
        return _Room(room_id, self.tok, sender, carol, a, b, t, cutoff)

    def _purge(self, r: _Room) -> None:
        token = self.get_success(self.store.get_topological_token_for_event(r.cutoff))
        error = self.get_success(
            self.hs.get_pagination_handler().purge_history(
                r.room_id,
                self.get_success(token.to_string(self.store)),
                delete_local_events=True,
            )
        )
        self.assertIsNone(error)
        # Check that the purge really did reach A.
        event = self.get_success(self.store.get_event(r.a))
        self.assertTrue(event.internal_metadata.outlier)

    def _receive(self, r: _Room, fields: dict) -> str:
        room_version = self.get_success(self.store.get_room_version(r.room_id))
        latest = self.get_success(self.store.get_latest_event_ids_in_room(r.room_id))
        pdu = make_test_event(
            self.add_hashes_and_signatures_from_other_server(
                {"room_id": r.room_id, "prev_events": list(latest), **fields},
                room_version,
            ),
            room_version,
        )
        self.get_success(
            self.hs.get_federation_event_handler().on_receive_pdu(
                self.OTHER_SERVER_NAME, pdu
            )
        )
        return pdu.event_id

    def _judge_x(self, r: _Room) -> str | None:
        """Delivers X, which cites A and B, and returns its `rejected_reason`."""
        # Ordinary inbound traffic citing only B. After a purge this used to
        # leave B as the only parent whose state could be found.
        self._receive(
            r,
            {
                "type": "m.room.message",
                "sender": r.carol,
                "content": {"msgtype": "m.text", "body": "hi"},
                "prev_state_events": [r.b],
                "origin_server_ts": 1,
                "depth": 99,
            },
        )
        x = self._receive(
            r,
            {
                "type": "test.state",
                "state_key": "",
                "sender": r.sender,
                "content": {},
                "prev_state_events": [r.a, r.b],
                "origin_server_ts": 2,
                "depth": 100,
            },
        )
        event = self.get_success(self.store.get_event(x, allow_rejected=True))
        return event.rejected_reason

    def _state_at_dag_events(self, room_id: str) -> dict[str, dict]:
        """The state after every event in the state DAG, read from the database."""
        event_ids = self.get_success(
            self.store.db_pool.simple_select_onecol(
                table="msc4242_state_dag_edges",
                keyvalues={"room_id": room_id},
                retcol="event_id",
                desc="test",
            )
        )
        self.store._get_state_group_for_event_sql.invalidate_all()
        self.hs.get_datastores().state._state_group_cache.invalidate_all()
        self.hs.get_datastores().state._state_group_members_cache.invalidate_all()
        return {
            event_id: dict(
                self.get_success(self.state_storage.get_state_ids_for_event(event_id))
            )
            for event_id in set(event_ids)
        }

    def test_banned_sender_rejected_without_purge(self) -> None:
        r = self._make_room("ban")
        self.assertEqual(self._judge_x(r), "auth_error")

    def test_banned_sender_rejected_after_purge(self) -> None:
        r = self._make_room("ban")
        self._purge(r)
        self.assertEqual(self._judge_x(r), "auth_error")

    def test_joined_sender_accepted_without_purge(self) -> None:
        r = self._make_room("join")
        self.assertIsNone(self._judge_x(r))

    def test_joined_sender_accepted_after_purge(self) -> None:
        r = self._make_room("join")
        self._purge(r)
        self.assertIsNone(self._judge_x(r))

    def test_can_send_state_after_purging_a_tip(self) -> None:
        r = self._make_room("ban", fork=False)
        self._purge(r)
        event_id = self._local_state(r.room_id, "m.room.name", {"name": "after"})
        state = self.get_success(self.state_storage.get_current_state_ids(r.room_id))
        self.assertEqual(state[("m.room.name", "")], event_id)
        self.assertEqual(state[("m.room.member", r.sender)], r.a)

    def test_can_send_state_merging_a_purged_tip(self) -> None:
        r = self._make_room("ban", a_stays_tip=True)
        self._purge(r)
        merge = self._local_state(r.room_id, "m.room.avatar", {"url": "mxc://test/a"})
        # The new event really did merge the purged tip.
        event = self.get_success(self.store.get_event(merge))
        assert supports_msc4242_state_dag(event)
        self.assertEqual(set(event.prev_state_events), {r.a, r.b})
        state = self.get_success(self.state_storage.get_current_state_ids(r.room_id))
        self.assertEqual(state[("m.room.member", r.sender)], r.a)
        self.assertEqual(state[("m.room.name", "")], r.b)

    def _state_groups(self, room_id: str) -> set[int]:
        return set(
            self.get_success(
                self.hs.get_datastores().state.db_pool.simple_select_onecol(
                    table="state_groups",
                    keyvalues={"room_id": room_id},
                    retcol="id",
                    desc="test",
                )
            )
        )

    def test_state_survives_purge_and_state_group_deletion(self) -> None:
        """The state at every state DAG event is unchanged once the state groups
        which the purge left unreferenced have really been deleted."""
        r = self._make_room("ban", merging_message=True)
        before = self._state_at_dag_events(r.room_id)
        self.assertIn(r.a, before)
        groups_before = self._state_groups(r.room_id)

        self._purge(r)
        self.assertEqual(self._state_at_dag_events(r.room_id), before)

        # State groups pending deletion are only deleted after a delay, by a
        # looping background task.
        for _ in range(5):
            self.reactor.advance(Duration(days=1).as_secs())
            self.get_success(
                self.hs.get_storage_controllers().purge_events._delete_state_groups_loop()
            )
        # Without this the check below would prove nothing.
        self.assertLess(
            self._state_groups(r.room_id), groups_before, "no state group was deleted"
        )
        self.assertEqual(self._state_at_dag_events(r.room_id), before)

    def test_retention_purge_keeps_state(self) -> None:
        """The same through the entry point of the retention job."""
        r = self._make_room("ban")
        before = self._state_at_dag_events(r.room_id)
        # Make the whole room older than the lifetime, then add a recent message
        # so there is something to keep.
        self.reactor.advance(Duration(days=30).as_secs())
        self.helper.send(r.room_id, "recent", tok=r.token)
        self.helper.send_state(
            r.room_id,
            "m.room.retention",
            {"max_lifetime": Duration(days=1).as_millis()},
            tok=r.token,
        )
        pagination = self.hs.get_pagination_handler()
        self.get_success(pagination.purge_history_for_rooms_in_range(None, None))
        self.pump(1)

        event = self.get_success(self.store.get_event(r.a))
        self.assertTrue(event.internal_metadata.outlier, "retention did not reach A")
        after = self._state_at_dag_events(r.room_id)
        self.assertEqual({k: after[k] for k in before}, before)
        self.assertEqual(self._judge_x(r), "auth_error")

    def test_rejected_state_event_keeps_its_state_mapping_after_purge(self) -> None:
        """Rejected state events are retained too, so their mapping must remain."""
        r = self._make_room("ban")
        rejected_id = self._receive(
            r,
            {
                "type": "test.state",
                "state_key": "",
                "sender": r.sender,
                "content": {},
                "prev_state_events": [r.a, r.b],
                "origin_server_ts": 1,
                "depth": 100,
            },
        )
        rejected = self.get_success(
            self.store.get_event(rejected_id, allow_rejected=True)
        )
        self.assertEqual(rejected.rejected_reason, "auth_error")

        state_group_before = self.get_success(
            self.store._get_state_group_for_event(rejected_id)
        )
        self.assertIsNotNone(state_group_before)
        state_before = self.get_success(
            self.state_storage.get_state_ids_for_event(rejected_id)
        )

        # Advance the topological cutoff past the rejected event. Its rejected
        # status means it does not itself provide a later timeline extremity.
        cutoff_id = self._receive(
            r,
            {
                "type": "m.room.message",
                "sender": r.carol,
                "content": {"msgtype": "m.text", "body": "cutoff"},
                "prev_state_events": [r.b],
                "origin_server_ts": 2,
                "depth": 101,
            },
        )
        cutoff = self.get_success(self.store.get_topological_token_for_event(cutoff_id))
        error = self.get_success(
            self.hs.get_pagination_handler().purge_history(
                r.room_id,
                self.get_success(cutoff.to_string(self.store)),
                delete_local_events=True,
            )
        )
        self.assertIsNone(error)

        # Force the post-purge reads back to storage rather than the values
        # cached before the purge.
        self.store._get_state_group_for_event_sql.invalidate_all()
        self.hs.get_datastores().state._state_group_cache.invalidate_all()
        self.hs.get_datastores().state._state_group_members_cache.invalidate_all()
        state_group_after = self.get_success(
            self.store._get_state_group_for_event(rejected_id)
        )
        self.assertEqual(state_group_after, state_group_before)
        self.assertEqual(
            self.get_success(self.state_storage.get_state_ids_for_event(rejected_id)),
            state_before,
        )
