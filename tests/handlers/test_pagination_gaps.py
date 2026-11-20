#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 Element Creations Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Tests for `/messages` pagination across a hole in the stored timeline.

A room joined over federation starts out with its older history stored as
outliers (the `send_join` state), which the timeline query excludes. Backfill
then de-outliers that history in batches. These tests model that by hiding a run
of ordinary events (`outlier = TRUE`) behind a backward extremity and letting a
stand-in for `maybe_backfill` reveal them a batch at a time.
"""

from http import HTTPStatus
from unittest.mock import AsyncMock, patch

from twisted.internet.testing import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, room, sync
from synapse.server import HomeServer
from synapse.util.clock import Clock

from tests import unittest

TOTAL_MESSAGES = 20
# Indexes (into the sent messages) of the run of events hidden behind the gap.
HIDDEN = list(range(5, 15))


class PaginationGapTestCase(unittest.HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
        sync.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self.user_id = self.register_user("alice", "pass")
        self.token = self.login("alice", "pass")
        self.room_id = self.helper.create_room_as(self.user_id, tok=self.token)

        self.event_ids = [
            self.helper.send(self.room_id, f"message {i}", tok=self.token)["event_id"]
            for i in range(TOTAL_MESSAGES)
        ]

        # Hide the middle of the room and record the gap's backward extremity
        # (the newest hidden event), exactly as persisting a child whose
        # prev_event is missing would.
        self._set_outlier([self.event_ids[i] for i in HIDDEN], True)
        self._add_backward_extremity(self.event_ids[HIDDEN[-1]])

        # How many hidden events each stand-in backfill call reveals.
        self.backfill_batch = 3
        self.backfill_calls = 0

        # A gap is only "still being fetched" if some other server could supply
        # the history, which is true of a room joined over federation. Model
        # that by putting a remote server in the room; a room with only local
        # users (for example after a purge) has nobody to backfill from.
        remote_hosts = patch.object(
            self.store,
            "get_current_hosts_in_room",
            AsyncMock(return_value={hs.hostname, "remote.example.com"}),
        )
        remote_hosts.start()
        self.addCleanup(remote_hosts.stop)

    # -- helpers modelling the join / backfill state -----------------------

    async def _a_set_outlier(self, event_ids: list[str], outlier: bool) -> None:
        def txn(t) -> None:  # type: ignore[no-untyped-def]
            for event_id in event_ids:
                t.execute(
                    "UPDATE events SET outlier = ? WHERE event_id = ?",
                    (outlier, event_id),
                )

        await self.store.db_pool.runInteraction("set_outlier", txn)
        for event_id in event_ids:
            self.store._invalidate_local_get_event_cache(event_id)

    async def _a_set_extremity(self, event_id: str | None) -> None:
        """Make `event_id` the room's only backward extremity (or clear them)."""
        await self.store.db_pool.simple_delete(
            "event_backward_extremities",
            {"room_id": self.room_id},
            desc="clear",
        )
        if event_id is not None:
            await self.store.db_pool.simple_insert(
                "event_backward_extremities",
                {"room_id": self.room_id, "event_id": event_id},
            )

    async def _a_hidden_event_ids(self) -> list[str]:
        """Event ids still hidden, newest first."""
        run = self.event_ids[HIDDEN[0] : HIDDEN[-1] + 1]
        rows = await self.store.db_pool.simple_select_many_batch(
            table="events",
            column="event_id",
            iterable=run,
            retcols=("event_id", "outlier"),
            keyvalues={},
            desc="hidden",
        )
        hidden = {event_id for event_id, outlier in rows if outlier}
        return [e for e in reversed(run) if e in hidden]

    def _set_outlier(self, event_ids: list[str], outlier: bool) -> None:
        self.get_success(self._a_set_outlier(event_ids, outlier))

    def _add_backward_extremity(self, event_id: str) -> None:
        self.get_success(self._a_set_extremity(event_id))

    def _hidden_event_ids(self) -> list[str]:
        return self.get_success(self._a_hidden_event_ids())

    async def _fake_backfill(
        self, room_id: str, *args: object, **kwargs: object
    ) -> bool:
        """Stand-in for `FederationHandler.maybe_backfill`: reveal the next
        batch of hidden events, newest first, and move the extremity down."""
        self.backfill_calls += 1
        hidden = await self._a_hidden_event_ids()
        if not hidden:
            return False
        batch = hidden[: self.backfill_batch]
        remaining = hidden[self.backfill_batch :]
        await self._a_set_outlier(batch, False)
        await self._a_set_extremity(remaining[0] if remaining else None)
        return True

    async def _unreachable_backfill(
        self, room_id: str, *args: object, **kwargs: object
    ) -> bool:
        """Stand-in for backfill when the remote server cannot be reached."""
        self.backfill_calls += 1
        return False

    def _messages(self, direction: str, from_token: str, limit: int = 5) -> dict:
        channel = self.make_request(
            "GET",
            f"/rooms/{self.room_id}/messages?dir={direction}&from={from_token}"
            f"&limit={limit}",
            access_token=self.token,
        )
        retry_after = channel.headers.getRawHeaders("Retry-After")
        return {
            "code": channel.code,
            "body": channel.json_body,
            "retry_after": retry_after[0] if retry_after else None,
        }

    def _live_token(self) -> str:
        channel = self.make_request("GET", "/sync", access_token=self.token)
        self.assertEqual(channel.code, HTTPStatus.OK, channel.json_body)
        return channel.json_body["next_batch"]

    # -- tests -------------------------------------------------------------

    def test_backward_pagination_returns_every_event_once_gap_is_filled(self) -> None:
        """Paging back across a hole must keep going until the hidden history
        has been revealed, and return every event exactly once, in order."""
        handler = self.hs.get_federation_handler()
        seen: list[str] = []
        with patch.object(handler, "maybe_backfill", self._fake_backfill):
            token = self._live_token()
            for _ in range(60):
                result = self._messages("b", token)
                self.assertEqual(result["code"], HTTPStatus.OK, result["body"])
                seen.extend(e["event_id"] for e in result["body"]["chunk"])
                token = result["body"].get("end")
                if token is None:
                    break
                self.reactor.advance(0.1)
            else:
                self.fail("pagination never terminated")

        sent_in_room = [self.event_ids.index(e) for e in seen if e in self.event_ids]
        self.assertEqual(
            sent_in_room,
            list(range(TOTAL_MESSAGES - 1, -1, -1)),
            "every message, including those that were hidden behind the gap, "
            "must be returned once and in order",
        )
        self.assertEqual(self._hidden_event_ids(), [])
        self.assertGreater(
            self.backfill_calls,
            1,
            "pagination must continue triggering backfill across batches",
        )

    def test_forward_pagination_returns_every_event_once_gap_is_filled(self) -> None:
        """The mirror image: paging forwards from the start of the room must
        also reveal the hidden history and return all of it, in order."""
        handler = self.hs.get_federation_handler()
        first = self.get_success(
            self.store.get_topological_token_for_event(self.event_ids[0])
        )
        seen: list[str] = []
        with patch.object(handler, "maybe_backfill", self._fake_backfill):
            token = self.get_success(first.to_string(self.store))
            for _ in range(60):
                result = self._messages("f", token)
                if result["code"] == HTTPStatus.TOO_MANY_REQUESTS:
                    # "History is still being fetched": the client retries.
                    self.reactor.advance(0.1)
                    continue
                self.assertEqual(result["code"], HTTPStatus.OK, result["body"])
                seen.extend(e["event_id"] for e in result["body"]["chunk"])
                token = result["body"].get("end")
                if token is None:
                    break
                self.reactor.advance(0.1)
            else:
                self.fail("pagination never terminated")

        sent_in_room = [self.event_ids.index(e) for e in seen if e in self.event_ids]
        self.assertEqual(
            sent_in_room,
            list(range(1, TOTAL_MESSAGES)),
            "forward pagination must return the whole hidden run exactly once "
            "and in order (no repeats, no reordering)",
        )
        self.assertGreater(
            self.backfill_calls,
            1,
            "forward pagination must continue triggering backfill across batches",
        )

    def test_backward_pagination_attempts_the_gap_before_ending(self) -> None:
        """`/messages` must not end the timeline over a gap it has not tried to
        fill.

        The newest hidden event is a backward extremity, so the room is known to
        be incomplete. Ending without ever asking backfill makes clients render
        "beginning of conversation" over a hole. Once backfill has been
        attempted and added nothing, ending means "the end of what this server
        can fetch right now": a walk over a gap nothing can fill must
        terminate, not hand back the same cursor forever.
        """
        handler = self.hs.get_federation_handler()
        with patch.object(handler, "maybe_backfill", self._unreachable_backfill):
            token = self._live_token()
            for _ in range(60):
                result = self._messages("b", token)
                self.assertEqual(result["code"], HTTPStatus.OK, result["body"])
                token = result["body"].get("end")
                if token is None:
                    self.assertGreater(
                        self.backfill_calls,
                        0,
                        "pagination ended over an open gap without trying to "
                        "backfill it",
                    )
                    break
                self.reactor.advance(0.1)
            else:
                self.fail("pagination never terminated over an unfillable gap")

        self.assertTrue(self._hidden_event_ids(), "the gap should still be open")

    def test_backward_pagination_ends_when_nobody_can_fill_the_gap(self) -> None:
        """A gap with no remote server to ask is the end of what exists.

        A purge deliberately leaves backward extremities at the surviving
        boundary. In a room with only local users nothing can ever be backfilled,
        so answering "history is still being fetched, retry" would never resolve.
        """
        handler = self.hs.get_federation_handler()
        local_only = patch.object(
            self.store,
            "get_current_hosts_in_room",
            AsyncMock(return_value={self.hs.hostname}),
        )
        with (
            local_only,
            patch.object(handler, "maybe_backfill", self._unreachable_backfill),
        ):
            token = self._live_token()
            pages: list[list[int]] = []
            for _ in range(60):
                result = self._messages("b", token)
                self.assertEqual(
                    result["code"],
                    HTTPStatus.OK,
                    "an unfillable gap in a room with no remote servers must not "
                    f"be reported as retryable: {result['body']}",
                )
                pages.append(
                    [
                        self.event_ids.index(e["event_id"])
                        if e["event_id"] in self.event_ids
                        else -1
                        for e in result["body"]["chunk"]
                    ]
                )
                token = result["body"].get("end")
                if token is None:
                    break
                self.reactor.advance(0.1)
            else:
                self.fail(f"pagination never terminated; first pages: {pages[:6]}")


class StaleOutlierCacheTestCase(unittest.HomeserverTestCase):
    """`/messages` must survive a worker still holding an event as an outlier.

    When a backfill running on one worker de-outliers an event, the new
    `outlier = FALSE` row is visible immediately but the event cache
    invalidation reaches the other workers a moment later. A `/messages`
    request that backfills in the foreground can then page over events whose
    cached copy still says `outlier`, which used to trip the assertion in
    `is_event_next_to_backward_gap` and answer 500.
    """

    servlets = [
        admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
        sync.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self.user_id = self.register_user("alice", "pass")
        self.token = self.login("alice", "pass")
        self.room_id = self.helper.create_room_as(self.user_id, tok=self.token)
        self.event_ids = [
            self.helper.send(self.room_id, f"message {i}", tok=self.token)["event_id"]
            for i in range(6)
        ]

    def _page_back(self) -> tuple[int, list[str]]:
        channel = self.make_request("GET", "/sync", access_token=self.token)
        token = channel.json_body["next_batch"]
        channel = self.make_request(
            "GET",
            f"/rooms/{self.room_id}/messages?dir=b&from={token}&limit=20",
            access_token=self.token,
        )
        chunk = channel.json_body.get("chunk", [])
        return channel.code, [e["event_id"] for e in chunk]

    def test_backward_page_with_stale_outlier_flag(self) -> None:
        # Warm the cache, then make the cached copies claim to be outliers while
        # the database row (what the page query reads) is not, which is what a
        # worker that has not yet processed the de-outlier invalidation holds.
        for event_id in self.event_ids[1:4]:
            event = self.get_success(self.store.get_event(event_id))
            event.internal_metadata.outlier = True

        code, seen = self._page_back()

        self.assertEqual(code, HTTPStatus.OK)
        for event_id in self.event_ids:
            self.assertIn(event_id, seen)
        self.assertEqual(len(seen), len(set(seen)))
        # The page must hand back current copies, not the stale cached ones.
        for event_id in self.event_ids[1:4]:
            event = self.get_success(self.store.get_event(event_id))
            self.assertFalse(event.internal_metadata.is_outlier())
