#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

from twisted.internet.testing import MemoryReactor

from synapse._scripts.state_repair import _repair_member
from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.types.state import StateFilter
from synapse.util.clock import Clock

from tests import unittest


class RepairMemberTestCase(unittest.HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self.controllers = hs.get_storage_controllers()

        alice = self.register_user("alice", "pass")
        self.alice_tok = self.login("alice", "pass")
        bob = self.register_user("bob", "pass")
        bob_tok = self.login("bob", "pass")
        self.room_id = self.helper.create_room_as(alice, tok=self.alice_tok)
        self.key = ("m.room.member", bob)

        self.helper.join(self.room_id, bob, tok=bob_tok)
        join = self.get_success(
            self.controllers.state.get_current_state_ids(
                self.room_id, StateFilter.from_types([self.key])
            )
        )[self.key]
        self.join_id = join
        self.helper.leave(self.room_id, bob, tok=bob_tok)
        self.leave_id = self.get_success(
            self.controllers.state.get_current_state_ids(
                self.room_id, StateFilter.from_types([self.key])
            )
        )[self.key]
        self.assertNotEqual(self.join_id, self.leave_id)

    def _current(self) -> str | None:
        return self.get_success(
            self.controllers.state.get_current_state_ids(
                self.room_id, StateFilter.from_types([self.key])
            )
        ).get(self.key)

    def _repair(self, event_id: str, publish: bool) -> dict:
        return self.get_success(
            _repair_member(self.hs, self.room_id, event_id, "unused", publish)
        )

    def _rewind_to_join(self) -> None:
        """Simulate the bug: state lacks the leave and has the old join."""
        extremities = self.get_success(
            self.store.get_latest_event_ids_in_room(self.room_id)
        )
        groups = self.get_success(self.store._get_state_group_for_events(extremities))
        for extremity, group in groups.items():
            new_group = self.get_success(
                self.controllers.state.store_state_group(
                    extremity,
                    self.room_id,
                    self.get_success(self.store.get_room_version(self.room_id)),
                    prev_group=group,
                    delta_ids={self.key: self.join_id},
                    current_state_ids=None,
                )
            )
            self.get_success(self.store.repoint_event_state_group(extremity, new_group))
        persistence = self.controllers.persistence
        assert persistence is not None
        self.get_success(persistence.update_current_state(self.room_id))
        self.assertEqual(self._current(), self.join_id)

    def test_dry_run_changes_nothing(self) -> None:
        self._rewind_to_join()
        report = self._repair(self.leave_id, publish=False)
        self.assertIsNone(report["refused"])
        self.assertEqual(report["plan"][0]["from"], self.join_id)
        self.assertEqual(report["plan"][0]["to"], self.leave_id)
        self.assertFalse(report["published"])
        self.assertEqual(self._current(), self.join_id)

    def test_publish_repairs_and_sticks(self) -> None:
        self._rewind_to_join()
        report = self._repair(self.leave_id, publish=True)
        self.assertIsNone(report["refused"], report)
        self.assertTrue(report["published"])
        self.assertEqual(self._current(), self.leave_id)

        # The extremities now carry the repaired state, so the next event in the
        # room must not put the old join back.
        self.helper.send(self.room_id, "after repair", tok=self.alice_tok)
        self.assertEqual(self._current(), self.leave_id)

        # Idempotent.
        again = self._repair(self.leave_id, publish=True)
        self.assertEqual(again["plan"], [])

    def test_refuses_event_that_does_not_supersede(self) -> None:
        # The current value is the leave; the join is not in the leave's
        # auth_events chain as a replacement for itself, so repairing "to" the
        # join must be refused.
        report = self._repair(self.join_id, publish=False)
        self.assertIn("does not supersede", report["refused"])
