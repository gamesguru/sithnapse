#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""History purge in MSC4242 rooms must not change how later events are judged.

PRIVATE until disclosure is agreed: do not push to a public remote.

A purge keeps old state events. If it drops their state, a server can no longer
authorise events which cite them in `prev_state_events`, and may disagree with a
server which did not purge. Each scenario runs with and without a purge; both
must reach the same, correct, verdict.

    prev_state_events graph        A = ban of mallory, or dave's join
    base <- A <- T                 T makes A a non-tip
       `--- B                      B forks from before A
    X cites [A, B]
"""

from synapse.api.room_versions import RoomVersions
from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.types import create_requester

from tests import unittest
from tests.test_utils import event_injection
from tests.test_utils.event_builders import make_test_event


class PurgeStateDagTestCase(unittest.FederatingHomeserverTestCase):
    servlets = [
        admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
    ]

    def _judge(self, purge: bool, branch_a: str) -> str | None:
        """Returns the `rejected_reason` of X.

        Args:
            purge: whether to purge history past A before X arrives
            branch_a: "ban" if A bans X's sender, "join" if A is the only record
                that X's sender joined
        """
        store = self.hs.get_datastores().main
        sender = f"@sender:{self.OTHER_SERVER_NAME}"
        carol = f"@carol:{self.OTHER_SERVER_NAME}"
        alice = self.register_user("alice", "pw")
        tok = self.login("alice", "pw")
        room_id = self.helper.create_room_as(
            alice,
            tok=tok,
            is_public=True,
            room_version=RoomVersions.MSC4242v12.identifier,
        )
        room_version = self.get_success(store.get_room_version(room_id))
        self.helper.send_state(
            room_id,
            "m.room.power_levels",
            {"events_default": 0, "state_default": 0},
            tok=tok,
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
        (base,) = self.get_success(store.get_state_dag_extremities(room_id))

        def local_state(
            type_: str, content: dict, prev: list[str] | None = None
        ) -> str:
            event, _ = self.get_success(
                self.hs.get_event_creation_handler().create_and_send_nonmember_event(
                    create_requester(alice),
                    {
                        "type": type_,
                        "state_key": "",
                        "content": content,
                        "room_id": room_id,
                        "sender": alice,
                    },
                    prev_state_events=prev,
                )
            )
            return event.event_id

        if branch_a == "ban":
            a = inject_member(sender, "ban", alice)
        else:
            a = inject_member(sender, "join", sender)
        for i in range(3):
            self.helper.send(room_id, f"m{i}", tok=tok)
        cutoff = self.helper.send(room_id, "cutoff", tok=tok)["event_id"]
        t = local_state("m.room.topic", {"topic": "after A"})
        b = local_state("m.room.name", {"name": "fork"}, prev=[base])
        self.assertEqual(
            set(self.get_success(store.get_state_dag_extremities(room_id))), {t, b}
        )

        if purge:
            token = self.get_success(store.get_topological_token_for_event(cutoff))
            error = self.get_success(
                self.hs.get_pagination_handler().purge_history(
                    room_id,
                    self.get_success(token.to_string(store)),
                    delete_local_events=True,
                )
            )
            self.assertIsNone(error)
            # State groups queued by the purge are only deleted after a delay,
            # so let that happen before judging anything against them.
            deletion_store = self.hs.get_datastores().state_deletion
            assert deletion_store is not None
            self.reactor.advance(deletion_store.DELAY_BEFORE_DELETION_MS / 1000 + 1)
            for _ in range(10):
                self.reactor.advance(60)

        def receive(fields: dict) -> str:
            latest = self.get_success(store.get_latest_event_ids_in_room(room_id))
            pdu = make_test_event(
                self.add_hashes_and_signatures_from_other_server(
                    {"room_id": room_id, "prev_events": list(latest), **fields},
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

        # Ordinary inbound traffic citing only the surviving tip.
        receive(
            {
                "type": "m.room.message",
                "sender": carol,
                "content": {"msgtype": "m.text", "body": "hi"},
                "prev_state_events": [b],
                "origin_server_ts": 1,
                "depth": 99,
            }
        )
        x = receive(
            {
                "type": "test.state",
                "state_key": "",
                "sender": sender,
                "content": {},
                "prev_state_events": [a, b],
                "origin_server_ts": 2,
                "depth": 100,
            }
        )
        return self.get_success(store.get_event(x, allow_rejected=True)).rejected_reason

    @unittest.override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_banned_sender_rejected_without_purge(self) -> None:
        self.assertEqual(self._judge(purge=False, branch_a="ban"), "auth_error")

    @unittest.override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_banned_sender_rejected_after_purge(self) -> None:
        self.assertEqual(self._judge(purge=True, branch_a="ban"), "auth_error")

    @unittest.override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_joined_sender_accepted_without_purge(self) -> None:
        self.assertIsNone(self._judge(purge=False, branch_a="join"))

    @unittest.override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_joined_sender_accepted_after_purge(self) -> None:
        self.assertIsNone(self._judge(purge=True, branch_a="join"))
