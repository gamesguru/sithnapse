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

"""The mtxdb room timeline index against the pagination SQL it mirrors."""

import os
from typing import Any
from unittest import mock

from twisted.internet.testing import MemoryReactor

from synapse.api.constants import Direction, EventTypes
from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.storage.databases.main import embedded_event_timeline, stream
from synapse.types import RoomStreamToken
from synapse.util.clock import Clock

from tests.unittest import HomeserverTestCase


class EmbeddedEventTimelineTestCase(HomeserverTestCase):
    servlets = [
        admin.register_servlets_for_client_rest_resource,
        room.register_servlets,
        login.register_servlets,
    ]

    def setUp(self) -> None:
        # Index from the first persisted event, so every room is complete.
        patcher = mock.patch.dict(os.environ, {"SYNAPSE_MTXDB_TIMELINE": "compare"})
        patcher.start()
        self.addCleanup(patcher.stop)
        super().setUp()

    def prepare(
        self, reactor: MemoryReactor, clock: Clock, homeserver: HomeServer
    ) -> None:
        self.store = homeserver.get_datastores().main
        self.user_id = self.register_user("timeline", "pass")
        self.tok = self.login("timeline", "pass")
        self.room_id = self.helper.create_room_as(self.user_id, tok=self.tok)
        self.sent = [
            self.helper.send_event(
                self.room_id,
                EventTypes.Message,
                {"msgtype": "m.text", "body": f"message {i}"},
                tok=self.tok,
            )["event_id"]
            for i in range(25)
        ]
        self.results: list[tuple[str, bool]] = []
        self.answered: list[str] = []
        original_report = embedded_event_timeline.report_mismatch

        def record(label: str, room_id: str, sql_rows: Any, index_rows: Any) -> bool:
            matched = original_report(label, room_id, sql_rows, index_rows)
            if index_rows is not None:
                self.answered.append(label)
            self.results.append((label, matched))
            return matched

        patcher = mock.patch.object(stream, "report_mismatch", side_effect=record)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _live_token(self) -> RoomStreamToken:
        """A live (stream-only) token for "now"."""
        return self.store.get_room_max_token()

    def _walk_topological(self, direction: Direction) -> list[str]:
        """Page the whole room in `direction`, 4 events per page."""
        if direction == Direction.BACKWARDS:
            from_key = self._live_token()
        else:
            from_key = RoomStreamToken(topological=0, timestamp=0, stream=0)
        seen: list[str] = []
        for _ in range(40):
            events, from_key, _ = self.get_success(
                self.store.paginate_room_events_by_topological_ordering(
                    room_id=self.room_id,
                    from_key=from_key,
                    direction=direction,
                    limit=4,
                )
            )
            if not events:
                break
            seen.extend(event.event_id for event in events)
        return seen

    def _walk_stream(self, direction: Direction) -> list[str]:
        live = self._live_token()
        if direction == Direction.BACKWARDS:
            from_key, to_key = live, None
        else:
            from_key, to_key = RoomStreamToken(stream=0), live
        seen: list[str] = []
        for _ in range(40):
            events, from_key, _ = self.get_success(
                self.store.paginate_room_events_by_stream_ordering(
                    room_id=self.room_id,
                    from_key=from_key,
                    to_key=to_key,
                    direction=direction,
                    limit=4,
                )
            )
            if not events:
                break
            seen.extend(event.event_id for event in events)
        return seen

    def _walk_all(self) -> list[list[str]]:
        return [
            self._walk_topological(Direction.BACKWARDS),
            self._walk_topological(Direction.FORWARDS),
            self._walk_stream(Direction.BACKWARDS),
            self._walk_stream(Direction.FORWARDS),
        ]

    def test_compare_mode_finds_no_mismatch(self) -> None:
        walks = self._walk_all()
        for walk in walks:
            # The room's creation events plus the 25 messages.
            self.assertGreater(len(walk), 25)
            self.assertEqual(len(walk), len(set(walk)))
        self.assertEqual(walks[0][: len(self.sent)], list(reversed(self.sent)))
        self.assertIn("paginate_room_events_by_topological_ordering", self.answered)
        self.assertIn("paginate_room_events_by_stream_ordering", self.answered)
        mismatches = [label for label, matched in self.results if not matched]
        self.assertEqual(mismatches, [])

    def test_read_mode_returns_the_sql_pages(self) -> None:
        sql_walks = self._walk_all()
        answers: list[object] = []

        def recording(page: Any) -> Any:
            def call(*args: Any) -> Any:
                result = page(*args)
                answers.append(result)
                return result

            return call

        with (
            mock.patch.dict(os.environ, {"SYNAPSE_MTXDB_TIMELINE": "read"}),
            mock.patch.object(
                stream, "page_topological", recording(stream.page_topological)
            ),
            mock.patch.object(stream, "page_stream", recording(stream.page_stream)),
        ):
            self.results.clear()
            index_walks = self._walk_all()
            # Read mode returns index rows without running the SQL, so there
            # is nothing to compare against.
            self.assertEqual(self.results, [])
        self.assertEqual(index_walks, sql_walks)
        # Every page came from the index: none fell back to SQL.
        self.assertGreater(len(answers), 8)
        self.assertNotIn(None, answers)

    def test_room_purge_removes_the_room_from_the_index(self) -> None:
        self.assertIsNotNone(
            embedded_event_timeline.page_stream(self.room_id, None, None, True, 5)
        )
        self.get_success(self.store.purge_room(self.room_id))
        self.assertEqual(
            embedded_event_timeline.page_stream(self.room_id, None, None, True, 5), []
        )
