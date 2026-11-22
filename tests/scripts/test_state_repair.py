#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

from twisted.internet.testing import MemoryReactor

from synapse._scripts.state_repair import _import_state, _repair_member
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
        self.bob_tok = bob_tok
        self.bob = bob
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

        # The correct full state at the tip, captured before we rewind.
        self.correct_state = self.get_success(
            self.controllers.state.get_current_state_ids(self.room_id)
        )

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

    def _import(self, publish: bool, max_diff: int = 500) -> dict:
        federation = self.hs.get_federation_client()

        async def fake_state_ids(
            destination: str, room_id: str, event_id: str
        ) -> tuple[list[str], list[str]]:
            self.assertEqual(destination, "remote.example")
            self.assertEqual(room_id, self.room_id)
            return list(self.correct_state.values()), []

        self.patch(federation, "get_room_state_ids", fake_state_ids)
        return self.get_success(
            _import_state(
                self.hs,
                self.room_id,
                "remote.example",
                None,
                max_diff,
                publish,
            )
        )

    def test_import_dry_run_reports_key_diff(self) -> None:
        self._rewind_to_join()
        report = self._import(publish=False)

        self.assertIsNone(report["refused"], report)
        self.assertEqual(report["counts"]["only_remote"], 1)
        self.assertEqual(report["counts"]["only_local"], 1)
        self.assertFalse(report["published"])
        self.assertEqual(self._current(), self.join_id)

        actions = {
            (change["type"], change["state_key"]): change["action"]
            for change in report["changes"]
        }
        self.assertEqual(actions[self.key], "replace")
        self.assertEqual(len(report["plan"]), 1)
        self.assertEqual(
            report["plan"][0],
            [
                {
                    "type": self.key[0],
                    "state_key": self.key[1],
                    "from": self.join_id,
                    "to": self.leave_id,
                }
            ],
        )

    def test_import_publish_replaces_and_sticks(self) -> None:
        self._rewind_to_join()
        report = self._import(publish=True)

        self.assertIsNone(report["refused"], report)
        self.assertTrue(report["published"])
        self.assertEqual(self._current(), self.leave_id)

        self.helper.send(self.room_id, "after import", tok=self.alice_tok)
        self.assertEqual(self._current(), self.leave_id)

        # Idempotent: the local and remote state now agree.
        again = self._import(publish=True)
        self.assertEqual(again["changes"], [])

    def test_import_refuses_large_diff(self) -> None:
        self._rewind_to_join()
        report = self._import(publish=False, max_diff=0)
        self.assertIn("exceeds --max-diff", report["refused"])

    def _bug_scenario(self) -> tuple[str, dict]:
        """Create a join after the leave, then rewind it to the old join.

        This mirrors the production bug: the local state at a later join lacks
        the earlier leave, while the remote state at that join has it.
        """
        charlie = self.register_user("charlie", "pass")
        charlie_tok = self.login("charlie", "pass")
        self.helper.join(self.room_id, charlie, tok=charlie_tok)

        remote_state = dict(
            self.get_success(self.controllers.state.get_current_state_ids(self.room_id))
        )
        seed_event_id = remote_state[("m.room.member", charlie)]
        seed_group = self.get_success(
            self.store._get_state_group_for_events([seed_event_id])
        )[seed_event_id]

        bad_group = self.get_success(
            self.controllers.state.store_state_group(
                seed_event_id,
                self.room_id,
                self.get_success(self.store.get_room_version(self.room_id)),
                prev_group=seed_group,
                delta_ids={self.key: self.join_id},
                current_state_ids=None,
            )
        )
        self.get_success(self.store.repoint_event_state_group(seed_event_id, bad_group))
        persistence = self.controllers.persistence
        assert persistence is not None
        self.get_success(persistence.update_current_state(self.room_id))

        # Make the seed a non-extremity and give propagation a second group.
        self.helper.send(self.room_id, "after seed", tok=self.alice_tok)
        self.helper.send_state(
            self.room_id, "m.room.topic", {"topic": "after seed"}, tok=self.alice_tok
        )
        self.assertEqual(self._current(), self.join_id)
        return seed_event_id, remote_state

    def _import_at(
        self,
        at_event: str,
        remote_state: dict,
        publish: bool,
        propagate: bool,
    ) -> dict:
        federation = self.hs.get_federation_client()

        async def fake_state_ids(
            destination: str, room_id: str, event_id: str
        ) -> tuple[list[str], list[str]]:
            self.assertEqual(destination, "remote.example")
            self.assertEqual(room_id, self.room_id)
            self.assertEqual(event_id, at_event)
            return list(remote_state.values()), []

        self.patch(federation, "get_room_state_ids", fake_state_ids)
        return self.get_success(
            _import_state(
                self.hs,
                self.room_id,
                "remote.example",
                at_event,
                500,
                publish,
                propagate=propagate,
            )
        )

    def test_at_event_outside_extremity_requires_propagate(self) -> None:
        seed, remote_state = self._bug_scenario()
        report = self._import_at(seed, remote_state, publish=False, propagate=False)
        self.assertIn("must be a forward extremity", report["refused"])

    def test_propagate_repairs_seed_and_descendant_groups(self) -> None:
        seed, remote_state = self._bug_scenario()

        report = self._import_at(seed, remote_state, publish=False, propagate=True)
        self.assertIsNone(report["refused"], report)
        self.assertFalse(report["published"])
        self.assertEqual(self._current(), self.join_id)

        # The seed group and the topic group both still have the old join.
        self.assertEqual(report["propagation"]["group_count"], 2)
        self.assertGreaterEqual(report["propagation"]["event_count"], 2)
        self.assertEqual(report["propagation"]["stops"], [])

        report = self._import_at(seed, remote_state, publish=True, propagate=True)
        self.assertIsNone(report["refused"], report)
        self.assertTrue(report["published"])
        self.assertEqual(self._current(), self.leave_id)

        # Historical state maps now carry the leave too: the seed and the
        # descendant state event both descend from the repaired group.
        tip = next(
            iter(
                self.get_success(self.store.get_latest_event_ids_in_room(self.room_id))
            )
        )
        for event_id in (seed, tip):
            state = self.get_success(
                self.controllers.state.get_state_ids_for_event(
                    event_id, StateFilter.from_types([self.key])
                )
            )
            self.assertEqual(state.get(self.key), self.leave_id, event_id)

        # Idempotent: local and remote state now agree at the seed.
        again = self._import_at(seed, remote_state, publish=True, propagate=True)
        self.assertEqual(again["changes"], [])

    def test_propagate_stops_at_later_membership_change(self) -> None:
        seed, remote_state = self._bug_scenario()

        # A later real membership change must not be overwritten.
        self.helper.leave(self.room_id, self.bob, tok=self.bob_tok)
        self.helper.join(self.room_id, self.bob, tok=self.bob_tok)
        later_state = self.get_success(
            self.controllers.state.get_current_state_ids(
                self.room_id, StateFilter.from_types([self.key])
            )
        )
        later_join = later_state[self.key]
        self.assertEqual(self._current(), later_join)

        report = self._import_at(seed, remote_state, publish=False, propagate=True)
        self.assertIsNone(report["refused"], report)
        stops = report["propagation"]["stops"]
        self.assertTrue(any(stop["current"] == later_join for stop in stops), report)

        report = self._import_at(seed, remote_state, publish=True, propagate=True)
        self.assertIsNone(report["refused"], report)
        self.assertTrue(report["published"])
        self.assertEqual(self._current(), later_join)
