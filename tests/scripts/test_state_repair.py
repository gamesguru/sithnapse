#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

from collections.abc import Collection
from typing import Any, cast

from twisted.internet.testing import MemoryReactor

from synapse._scripts.state_repair import _import_state, _repair_member
from synapse.api.room_versions import RoomVersion
from synapse.events import EventBase
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
        self.alice = alice
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
            report["plan"][0]["changes"],
            [
                {
                    "type": self.key[0],
                    "state_key": self.key[1],
                    "from": self.join_id,
                    "to": self.leave_id,
                }
            ],
        )
        self.assertIn("old_state_group", report["plan"][0])
        self.assertIn("extremities", report["plan"][0])

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

    def _pre_event_state(self, event_id: str, key: tuple[str, str]) -> dict:
        """Approximate `/state_ids`: the snapshot immediately before `event_id`."""
        state = dict(
            self.get_success(self.controllers.state.get_current_state_ids(self.room_id))
        )
        event = self.get_success(self.store.get_event(event_id))
        replaces = event.unsigned.get("replaces_state")
        if replaces is None:
            state.pop(key, None)
        else:
            state[key] = replaces
        return state

    def test_empty_diff_reports_nothing_and_changes_nothing(self) -> None:
        """A seed whose state already matches the remote gets no plan."""
        topic_key = ("m.room.topic", "")
        seed = self.helper.send_state(
            self.room_id, "m.room.topic", {"topic": "tip"}, tok=self.alice_tok
        )["event_id"]
        remote_state = self._pre_event_state(seed, topic_key)

        report = self._import_at(seed, remote_state, publish=True, propagate=False)

        self.assertIsNone(report["refused"], report)
        self.assertEqual(report["plan"], [])
        self.assertFalse(report.get("published", False))
        current = self.get_success(
            self.controllers.state.get_current_state_ids(self.room_id)
        )
        self.assertEqual(current[topic_key], seed)

    def _bug_scenario(self) -> tuple[str, dict]:
        """Create a join after the leave, then rewind it to the old join.

        This mirrors the production bug: the local state at a later join lacks
        the earlier leave, while the remote state at that join has it.
        """
        charlie = self.register_user("charlie", "pass")
        charlie_tok = self.login("charlie", "pass")
        self.helper.invite(
            self.room_id, src=self.alice, targ=charlie, tok=self.alice_tok
        )

        # /state_ids at Charlie's join returns this before-join snapshot,
        # including the invitation that the join supersedes.
        remote_state = dict(
            self.get_success(self.controllers.state.get_current_state_ids(self.room_id))
        )
        self.helper.join(self.room_id, charlie, tok=charlie_tok)
        after_join = self.get_success(
            self.controllers.state.get_current_state_ids(
                self.room_id, StateFilter.from_types([("m.room.member", charlie)])
            )
        )
        seed_event_id = after_join[("m.room.member", charlie)]
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
        max_diff: int = 500,
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
                max_diff,
                publish,
                propagate=propagate,
            )
        )

    def test_at_event_outside_extremity_requires_propagate(self) -> None:
        seed, remote_state = self._bug_scenario()
        report = self._import_at(seed, remote_state, publish=False, propagate=False)
        self.assertIn("must be a forward extremity", report["refused"])

    def test_superseded_predecessor_does_not_consume_diff_budget(self) -> None:
        seed, remote_state = self._bug_scenario()

        # The before-event snapshot contains Charlie's invite, but the join
        # supersedes it. Only Bob's actual state difference should count.
        report = self._import_at(
            seed, remote_state, publish=True, propagate=True, max_diff=1
        )

        self.assertIsNone(report["refused"], report)
        self.assertTrue(report["published"])

    def test_unfetchable_superseded_predecessor_is_ignored(self) -> None:
        """A predecessor the remote no longer serves must not block publish.

        `/state_ids` names the invite that Charlie's join supersedes, but the
        join is the after-event value: losing the invite costs nothing.
        """
        seed, remote_state = self._bug_scenario()
        seed_event = self.get_success(self.store.get_event(seed))
        invite_id = seed_event.unsigned["replaces_state"]
        self.assertIn(invite_id, remote_state.values())

        # Pretend the invite is neither held locally nor served by the remote.
        store = self.store
        real_get_event = store.get_event

        async def fake_get_event(
            event_id: str, *args: Any, **kwargs: Any
        ) -> EventBase | None:
            if event_id == invite_id:
                return None
            return await cast(Any, real_get_event)(event_id, *args, **kwargs)

        self.patch(store, "get_event", fake_get_event)

        federation = self.hs.get_federation_client()

        pdu_requests: list[str] = []

        async def fake_pdu(
            destinations: Collection[str], event_id: str, room_version: RoomVersion
        ) -> None:
            pdu_requests.append(event_id)
            return None

        self.patch(federation, "get_pdu", fake_pdu)

        report = self._import_at(
            seed, remote_state, publish=True, propagate=True, max_diff=1
        )

        self.assertIsNone(report["refused"], report)
        self.assertEqual(report["errors"], [])
        self.assertNotIn(invite_id, pdu_requests)
        self.assertTrue(report["published"])

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
        for step in report["plan"]:
            self.assertIn("old_state_group", step)
            self.assertIn("events", step)
            self.assertIn("changes", step)
            self.assertNotIn("delta", step)

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

    def test_empty_propagation_diff_preserves_later_extremity_state(self) -> None:
        seed, remote_state = self._bug_scenario()
        repaired = self._import_at(seed, remote_state, publish=True, propagate=True)
        self.assertTrue(repaired["published"])

        # A legitimate later membership update diverges from the historical
        # seed snapshot. An empty seed diff must not push that old snapshot to
        # the forward extremity.
        self.helper.join(self.room_id, self.bob, tok=self.bob_tok)
        later_join = self._current()
        self.assertNotEqual(later_join, self.leave_id)

        again = self._import_at(seed, remote_state, publish=True, propagate=True)

        self.assertEqual(again["changes"], [])
        self.assertEqual(again["plan"], [])
        self.assertEqual(self._current(), later_join)

    def test_propagation_reports_failed_event_repoint_as_partial(self) -> None:
        seed, remote_state = self._bug_scenario()
        real_repoint = self.store.repoint_event_state_group
        failed_event: str | None = None

        async def fail_first_repoint(event_id: str, state_group: int) -> None:
            nonlocal failed_event
            if failed_event is None:
                failed_event = event_id
                raise RuntimeError("injected repoint failure")
            await real_repoint(event_id, state_group)

        self.patch(self.store, "repoint_event_state_group", fail_first_repoint)
        report = self._import_at(seed, remote_state, publish=True, propagate=True)

        self.assertIsNone(report["refused"], report)
        # Some writes landed, but `published` alone must not read as success.
        self.assertFalse(report["published"])
        self.assertTrue(report["partial"])
        self.assertTrue(report["incomplete"])
        self.assertEqual(
            report["note"], "partially published; see propagation.incomplete_work"
        )
        self.assertIsNotNone(failed_event)
        self.assertTrue(
            any(
                failed_event in work.get("events", [])
                for work in report["propagation"]["incomplete_work"]
            ),
            report,
        )

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
