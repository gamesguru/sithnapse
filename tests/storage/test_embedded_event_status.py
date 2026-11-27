#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""mtxdb event statuses, and the extremity traversal that reads them."""

from twisted.internet.testing import MemoryReactor

from synapse.api.constants import EventTypes
from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.storage.databases.main.embedded_event_json import delete_event_json_batch
from synapse.storage.databases.main.embedded_event_status import (
    get_event_statuses,
    patch_event_statuses,
)
from synapse.util.clock import Clock

from tests.storage.event_status import assert_statuses_match_sql
from tests.unittest import HomeserverTestCase


class EmbeddedEventStatusTestCase(HomeserverTestCase):
    servlets = [
        admin.register_servlets_for_client_rest_resource,
        room.register_servlets,
        login.register_servlets,
    ]

    def prepare(
        self, reactor: MemoryReactor, clock: Clock, homeserver: HomeServer
    ) -> None:
        self.store = homeserver.get_datastores().main
        persist_events = homeserver.get_datastores().persist_events
        assert persist_events is not None
        self.persist_events = persist_events

        self.register_user("status", "pass")
        self.tok = self.login("status", "pass")
        self.room_id = self.helper.create_room_as("status", tok=self.tok)
        # A linear chain: each event's only prev is the one before it.
        self.chain = [
            self.helper.send_event(
                self.room_id,
                EventTypes.Message,
                {"msgtype": "m.text", "body": f"message {i}"},
                tok=self.tok,
            )["event_id"]
            for i in range(4)
        ]

    def _prevs(self, include_soft_failed: bool = True) -> set[str]:
        return self.get_success(
            self.persist_events._get_prevs_before_rejected(
                [self.chain[3]], include_soft_failed
            )
        )

    def _which_are_prevs(self, event_ids: list[str]) -> list[str]:
        return self.get_success(
            self.persist_events._get_events_which_are_prevs(event_ids, self.room_id)
        )

    def _mark_rejected(self, event_id: str, reason: str | None) -> None:
        self.get_success(
            self.store.db_pool.runInteraction(
                "mark_rejected",
                self.store.mark_event_rejected_txn,
                event_id,
                reason,
            )
        )

    def test_persist_writes_a_status(self) -> None:
        statuses = get_event_statuses(self.chain)
        self.assertEqual(set(statuses), set(self.chain))
        for status in statuses.values():
            self.assertFalse(status.outlier)
            self.assertFalse(status.rejected)
            self.assertFalse(status.soft_failed)

    def test_patching_an_absent_locator_is_a_no_op(self) -> None:
        # A delete can win the race against a purge or sticky update. There is
        # no locator left to carry status, so patching must not raise.
        patch_event_statuses(["$missing:example.com"], outlier=True)
        self.assertEqual(get_event_statuses(["$missing:example.com"]), {})

    def test_accepted_start_is_not_traversed(self) -> None:
        self.assertEqual(self._prevs(), set())

    def test_follows_rejected_chain(self) -> None:
        self._mark_rejected(self.chain[3], "auth_error")
        self._mark_rejected(self.chain[2], "a reason mtxdb has no code for")
        self.assertEqual(self._prevs(), {self.chain[2], self.chain[1]})

        # Accepting an event again stops the traversal there.
        self._mark_rejected(self.chain[2], None)
        self.assertEqual(self._prevs(), {self.chain[2]})

    def test_soft_failed_only_when_included(self) -> None:
        patch_event_statuses([self.chain[3]], soft_failed=True)
        self.assertEqual(self._prevs(), {self.chain[2]})
        self.assertEqual(self._prevs(include_soft_failed=False), set())

    def test_which_are_prevs_uses_statuses(self) -> None:
        # chain[0] is a prev of the accepted chain[1], and chain[3] has no child.
        self.assertEqual(self._which_are_prevs([self.chain[0]]), [self.chain[0]])
        self.assertEqual(self._which_are_prevs([self.chain[3]]), [])

        self._mark_rejected(self.chain[1], "auth_error")
        self.assertEqual(self._which_are_prevs([self.chain[0]]), [])

        self._mark_rejected(self.chain[1], None)
        patch_event_statuses([self.chain[1]], outlier=True)
        self.assertEqual(self._which_are_prevs([self.chain[0]]), [])

        patch_event_statuses([self.chain[1]], outlier=False, soft_failed=True)
        self.assertEqual(self._which_are_prevs([self.chain[0]]), [])

        patch_event_statuses([self.chain[1]], soft_failed=False)
        self.assertEqual(self._which_are_prevs([self.chain[0]]), [self.chain[0]])

        # An edge row with no status is not a stored child.
        delete_event_json_batch(
            self.hs.config.database.embedded_db_engine, [self.chain[1]]
        )
        self.assertEqual(self._which_are_prevs([self.chain[0]]), [])

    def test_outlier_and_missing_events_are_not_traversed(self) -> None:
        self._mark_rejected(self.chain[3], "auth_error")
        patch_event_statuses([self.chain[3]], outlier=True)
        self.assertEqual(self._prevs(), set())

        delete_event_json_batch(
            self.hs.config.database.embedded_db_engine, [self.chain[3]]
        )
        self.assertEqual(self._prevs(), set())
        self.assertEqual(get_event_statuses([self.chain[3]]), {})

    def test_history_purge_updates_statuses(self) -> None:
        token = self.get_success(
            self.store.get_topological_token_for_event(self.chain[3])
        )
        token_str = self.get_success(token.to_string(self.store))
        self.get_success(
            self.hs.get_storage_controllers().purge_events.purge_history(
                self.room_id, token_str, True
            )
        )
        statuses = get_event_statuses(self.chain)
        # Messages before the token are deleted; the one at it is untouched.
        self.assertEqual(set(statuses), {self.chain[3]})
        self.assertFalse(statuses[self.chain[3]].outlier)
        # The purge keeps state events but turns the old ones into outliers.
        assert_statuses_match_sql(self, self.hs, self.room_id)
        self.assertTrue(self._room_has_outlier())

    def _room_has_outlier(self) -> bool:
        event_ids = self.get_success(
            self.store.db_pool.simple_select_onecol(
                "events", {"room_id": self.room_id}, "event_id"
            )
        )
        return any(v.outlier for v in get_event_statuses(event_ids).values())
