#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2014 - 2016 OpenMarket Ltd
# Copyright (C) 2023 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#
import logging
from typing import TYPE_CHECKING, cast

import attr

from twisted.python.failure import Failure

from synapse.api.constants import MAX_DEPTH, Direction, EventTypes, Membership
from synapse.api.errors import SynapseError
from synapse.api.filtering import Filter
from synapse.events import EventBase
from synapse.events.utils import FilteredEvent
from synapse.handlers.relations import BundledAggregations
from synapse.handlers.worker_lock import NEW_EVENT_DURING_PURGE_LOCK_NAME
from synapse.logging.opentracing import trace
from synapse.rest.admin._base import assert_user_is_admin
from synapse.storage.databases.main.stream import generate_next_token
from synapse.streams.config import PaginationConfig
from synapse.types import (
    JsonMapping,
    Requester,
    RoomStreamToken,
    ScheduledTask,
    StreamKeyType,
    StreamToken,
    TaskStatus,
)
from synapse.types.handlers import ShutdownRoomParams, ShutdownRoomResponse
from synapse.types.state import StateFilter
from synapse.util.async_helpers import ReadWriteLock
from synapse.util.duration import Duration
from synapse.visibility import filter_and_transform_events_for_client

if TYPE_CHECKING:
    from synapse.server import HomeServer


logger = logging.getLogger(__name__)

# How many single event gaps we tolerate returning in a `/messages` response before we
# backfill and try to fill in the history. This is an arbitrarily picked number so feel
# free to tune it in the future.
BACKFILL_BECAUSE_TOO_MANY_GAPS_THRESHOLD = 3

# `Retry-After` (seconds) sent with the 429 that tells a forward-paginating client
# the history it needs is still being fetched.
FORWARD_BACKFILL_RETRY_AFTER_SECONDS = 2


# This is used to avoid purging a room several time at the same moment,
# and also paginating during a purge. Pagination can trigger backfill,
# which would create old events locally, and would potentially clash with the room delete.
PURGE_PAGINATION_LOCK_NAME = "purge_pagination_lock"


PURGE_HISTORY_ACTION_NAME = "purge_history"

PURGE_ROOM_ACTION_NAME = "purge_room"

SHUTDOWN_AND_PURGE_ROOM_ACTION_NAME = "shutdown_and_purge_room"


@attr.s(slots=True, frozen=True, auto_attribs=True)
class GetMessagesResult:
    """
    Everything needed to serialize a `/messages` response.
    """

    messages_chunk: list[FilteredEvent]
    """
    A list of room events.

     - When the request is `Direction.FORWARDS`, events will be in the range:
       `start_token` < x <= `end_token`, (ascending topological_order)
     - When the request is `Direction.BACKWARDS`, events will be in the range:
       `start_token` >= x > `end_token`, (descending topological_order)

    Note that an empty chunk does not necessarily imply that no more events are
    available. Clients should continue to paginate until no `end_token` property is returned.
    """

    bundled_aggregations: dict[str, BundledAggregations]
    """
    A map of event ID to the bundled aggregations for the events in the chunk.

    If an event doesn't have any bundled aggregations, it may not appear in the map.
    """

    state: list[EventBase] | None
    """
    A list of state events relevant to showing the chunk. For example, if
    lazy_load_members is enabled in the filter then this may contain the membership
    events for the senders of events in the chunk.

    Omitted from the response when `None`.
    """

    start_token: StreamToken
    """
    Token corresponding to the start of chunk. This will be the same as the value given
    in `from` query parameter of the `/messages` request.
    """

    end_token: StreamToken | None
    """
    A token corresponding to the end of chunk. This token can be passed back to this
    endpoint to request further events.

    If no further events are available (either because we have reached the start of the
    timeline, or because the user does not have permission to see any more events), this
    property is omitted from the response.
    """


class PaginationHandler:
    """Handles pagination and purge history requests.

    These are in the same handler due to the fact we need to block clients
    paginating during a purge.
    """

    def __init__(self, hs: "HomeServer"):
        self.hs = hs
        self.server_name = hs.hostname
        self.auth = hs.get_auth()
        self.store = hs.get_datastores().main
        self._storage_controllers = hs.get_storage_controllers()
        self._state_storage_controller = self._storage_controllers.state
        self.clock = hs.get_clock()
        self._room_shutdown_handler = hs.get_room_shutdown_handler()
        self._relations_handler = hs.get_relations_handler()
        self._worker_locks = hs.get_worker_locks_handler()
        self._task_scheduler = hs.get_task_scheduler()

        self.pagination_lock = ReadWriteLock()
        # IDs of rooms in which there currently an active purge *or delete* operation.
        self._purges_in_progress_by_room: set[str] = set()
        self._event_serializer = hs.get_event_client_serializer()

        self._retention_default_max_lifetime = (
            hs.config.retention.retention_default_max_lifetime
        )

        self._retention_allowed_lifetime_min = (
            hs.config.retention.retention_allowed_lifetime_min
        )
        self._retention_allowed_lifetime_max = (
            hs.config.retention.retention_allowed_lifetime_max
        )
        self._forgotten_room_retention_period = (
            hs.config.server.forgotten_room_retention_period
        )
        self._is_master = hs.config.worker.worker_app is None

        if hs.config.retention.retention_enabled and self._is_master:
            # Run the purge jobs described in the configuration file.
            for job in hs.config.retention.retention_purge_jobs:
                logger.info("Setting up purge job with config: %s", job)

                self.clock.looping_call(
                    self.hs.run_as_background_process,
                    Duration(milliseconds=job.interval),
                    "purge_history_for_rooms_in_range",
                    self.purge_history_for_rooms_in_range,
                    job.shortest_max_lifetime,
                    job.longest_max_lifetime,
                )

        self._task_scheduler.register_action(
            self._purge_history, PURGE_HISTORY_ACTION_NAME
        )
        self._task_scheduler.register_action(self._purge_room, PURGE_ROOM_ACTION_NAME)
        self._task_scheduler.register_action(
            self._shutdown_and_purge_room, SHUTDOWN_AND_PURGE_ROOM_ACTION_NAME
        )

    async def purge_history_for_rooms_in_range(
        self, min_ms: int | None, max_ms: int | None
    ) -> None:
        """Purge outdated events from rooms within the given retention range.

        If a default retention policy is defined in the server's configuration and its
        'max_lifetime' is within this range, also targets rooms which don't have a
        retention policy.

        Args:
            min_ms: Duration in milliseconds that define the lower limit of
                the range to handle (exclusive). If None, it means that the range has no
                lower limit.
            max_ms: Duration in milliseconds that define the upper limit of
                the range to handle (inclusive). If None, it means that the range has no
                upper limit.
        """
        # We want the storage layer to include rooms with no retention policy in its
        # return value only if a default retention policy is defined in the server's
        # configuration and that policy's 'max_lifetime' is either lower (or equal) than
        # max_ms or higher than min_ms (or both).
        if self._retention_default_max_lifetime is not None:
            include_null = True

            if min_ms is not None and min_ms >= self._retention_default_max_lifetime:
                # The default max_lifetime is lower than (or equal to) min_ms.
                include_null = False

            if max_ms is not None and max_ms < self._retention_default_max_lifetime:
                # The default max_lifetime is higher than max_ms.
                include_null = False
        else:
            include_null = False

        logger.info(
            "[purge] Running retention purge job for %s < max_lifetime <= %s (include NULLs = %s)",
            min_ms,
            max_ms,
            include_null,
        )

        rooms = await self.store.get_rooms_for_retention_period_in_range(
            min_ms, max_ms, include_null
        )

        logger.debug("[purge] Rooms to purge: %s", rooms)

        for room_id, retention_policy in rooms.items():
            logger.info("[purge] Attempting to purge messages in room %s", room_id)

            if len(await self.get_delete_tasks_by_room(room_id, only_active=True)) > 0:
                logger.warning(
                    "[purge] not purging room %s for retention as there's an ongoing purge"
                    " running for this room",
                    room_id,
                )
                continue

            # If max_lifetime is None, it means that the room has no retention policy.
            # Given we only retrieve such rooms when there's a default retention policy
            # defined in the server's configuration, we can safely assume that's the
            # case and use it for this room.
            max_lifetime = (
                retention_policy.max_lifetime or self._retention_default_max_lifetime
            )

            # Cap the effective max_lifetime to be within the range allowed in the
            # config.
            # We do this in two steps:
            #   1. Make sure it's higher or equal to the minimum allowed value, and if
            #      it's not replace it with that value. This is because the server
            #      operator can be required to not delete information before a given
            #      time, e.g. to comply with freedom of information laws.
            #   2. Make sure the resulting value is lower or equal to the maximum allowed
            #      value, and if it's not replace it with that value. This is because the
            #      server operator can be required to delete any data after a specific
            #      amount of time.
            if self._retention_allowed_lifetime_min is not None:
                max_lifetime = max(self._retention_allowed_lifetime_min, max_lifetime)

            if self._retention_allowed_lifetime_max is not None:
                max_lifetime = min(max_lifetime, self._retention_allowed_lifetime_max)

            logger.debug("[purge] max_lifetime for room %s: %s", room_id, max_lifetime)

            # Figure out what token we should start purging at.
            ts = self.clock.time_msec() - max_lifetime

            stream_ordering = await self.store.find_first_stream_ordering_after_ts(ts)

            r = await self.store.get_room_event_before_stream_ordering(
                room_id,
                stream_ordering,
            )
            if not r:
                logger.warning(
                    "[purge] purging events not possible: No event found "
                    "(ts %i => stream_ordering %i)",
                    ts,
                    stream_ordering,
                )
                continue

            (stream, topo, _event_id, origin_ts) = r
            token = "t%d.%d.%d" % (topo, origin_ts, stream)

            logger.info("Starting purging events in room %s", room_id)

            # We want to purge everything, including local events, and to run the purge in
            # the background so that it's not blocking any other operation apart from
            # other purges in the same room.
            self.hs.run_as_background_process(
                PURGE_HISTORY_ACTION_NAME,
                self.purge_history,
                room_id,
                token,
                True,
            )

    async def start_purge_history(
        self, room_id: str, token: str, delete_local_events: bool = False
    ) -> str:
        """Start off a history purge on a room.

        Args:
            room_id: The room to purge from
            token: topological token to delete events before
            delete_local_events: True to delete local events as well as
                remote ones

        Returns:
            unique ID for this purge transaction.
        """
        purge_id = await self._task_scheduler.schedule_task(
            PURGE_HISTORY_ACTION_NAME,
            resource_id=room_id,
            params={"token": token, "delete_local_events": delete_local_events},
        )

        # we log the purge_id here so that it can be tied back to the
        # request id in the log lines.
        logger.info("[purge] starting purge_id %s", purge_id)

        return purge_id

    async def _purge_history(
        self,
        task: ScheduledTask,
    ) -> tuple[TaskStatus, JsonMapping | None, str | None]:
        """
        Scheduler action to purge some history of a room.
        """
        if (
            task.resource_id is None
            or task.params is None
            or "token" not in task.params
            or "delete_local_events" not in task.params
        ):
            return (
                TaskStatus.FAILED,
                None,
                "Not enough parameters passed to _purge_history",
            )
        err = await self.purge_history(
            task.resource_id,
            task.params["token"],
            task.params["delete_local_events"],
        )
        if err is not None:
            return TaskStatus.FAILED, None, err
        return TaskStatus.COMPLETE, None, None

    async def purge_history(
        self,
        room_id: str,
        token: str,
        delete_local_events: bool,
    ) -> str | None:
        """Carry out a history purge on a room.

        Args:
            room_id: The room to purge from
            token: topological token to delete events before
            delete_local_events: True to delete local events as well as remote ones
        """
        try:
            async with self._worker_locks.acquire_read_write_lock(
                PURGE_PAGINATION_LOCK_NAME, room_id, write=True
            ):
                await self._storage_controllers.purge_events.purge_history(
                    room_id, token, delete_local_events
                )
            logger.info("[purge] complete")
            return None
        except Exception:
            f = Failure()
            logger.error(
                "[purge] failed", exc_info=(f.type, f.value, f.getTracebackObject())
            )
            return f.getErrorMessage()

    async def get_delete_task(self, delete_id: str) -> ScheduledTask | None:
        """Get the current status of an active deleting

        Args:
            delete_id: delete_id returned by start_shutdown_and_purge_room
                or start_purge_history.
        """
        return await self._task_scheduler.get_task(delete_id)

    async def get_delete_tasks_by_room(
        self, room_id: str, only_active: bool | None = False
    ) -> list[ScheduledTask]:
        """Get complete, failed or active delete tasks by room

        Args:
            room_id: room_id that is deleted
            only_active: if True, completed&failed tasks will be omitted
        """
        statuses = [TaskStatus.ACTIVE]
        if not only_active:
            statuses += [TaskStatus.COMPLETE, TaskStatus.FAILED]

        return await self._task_scheduler.get_tasks(
            actions=[PURGE_ROOM_ACTION_NAME, SHUTDOWN_AND_PURGE_ROOM_ACTION_NAME],
            resource_id=room_id,
            statuses=statuses,
        )

    async def _purge_room(
        self,
        task: ScheduledTask,
    ) -> tuple[TaskStatus, JsonMapping | None, str | None]:
        """
        Scheduler action to purge a room.
        """
        if not task.resource_id:
            raise Exception("No room id passed to purge_room task")
        params = task.params if task.params else {}
        await self.purge_room(task.resource_id, params.get("force", False))
        return TaskStatus.COMPLETE, None, None

    async def purge_room(
        self,
        room_id: str,
        force: bool,
    ) -> None:
        """Purge the given room from the database.

        Args:
            room_id: room to be purged
            force: set true to skip checking for joined users.
        """
        logger.info("starting purge room_id=%s force=%s", room_id, force)

        async with self._worker_locks.acquire_multi_read_write_lock(
            [
                (PURGE_PAGINATION_LOCK_NAME, room_id),
                (NEW_EVENT_DURING_PURGE_LOCK_NAME, room_id),
            ],
            write=True,
        ):
            # first check that we have no users in this room
            joined = await self.store.is_host_joined(room_id, self.server_name)
            if joined:
                if force:
                    logger.info(
                        "force-purging room %s with some local users still joined",
                        room_id,
                    )
                else:
                    raise SynapseError(400, "Users are still joined to this room")

            await self._storage_controllers.purge_events.purge_room(room_id)

        logger.info("purge complete for room_id %s", room_id)

    @trace
    async def _reload_stale_outliers(self, events: list[EventBase]) -> list[EventBase]:
        """Replace cached copies of events that are no longer outliers.

        The timeline queries only select rows with `outlier = FALSE`, so an event
        that still reports `is_outlier()` here is a stale in-memory copy: another
        worker de-outliered it (typically a backfill running during this very
        request) and the cache invalidation has not replicated to this worker
        yet. Reload those from the database so the page, and the gap detection
        that follows, see the current state instead of tripping over it.
        """
        stale = [e.event_id for e in events if e.internal_metadata.is_outlier()]
        if not stale:
            return events

        for event_id in stale:
            self.store._invalidate_local_get_event_cache(event_id)
        fresh = {
            e.event_id: e
            for e in await self.store.get_events_as_list(stale, get_prev_content=True)
        }
        return [fresh.get(e.event_id, e) for e in events]

    async def get_messages(
        self,
        requester: Requester,
        room_id: str,
        pagin_config: PaginationConfig,
        as_client_event: bool = True,
        event_filter: Filter | None = None,
        use_admin_priviledge: bool = False,
    ) -> GetMessagesResult:
        """Get messages in a room.

        Args:
            requester: The user requesting messages.
            room_id: The room they want messages from.
            pagin_config: The pagination config rules to apply, if any.
            as_client_event: True to get events in client-server format.
            event_filter: Filter to apply to results or None
            use_admin_priviledge: if `True`, return all events, regardless
                of whether `user` has access to them. To be used **ONLY**
                from the admin API.

        Returns:
            Pagination API results
        """
        if use_admin_priviledge:
            await assert_user_is_admin(self.auth, requester)

        user_id = requester.user.to_string()

        if pagin_config.from_token:
            from_token = pagin_config.from_token
        elif pagin_config.direction == Direction.FORWARDS:
            from_token = (
                await self.hs.get_event_sources().get_start_token_for_pagination(
                    room_id
                )
            )
        else:
            from_token = (
                await self.hs.get_event_sources().get_current_token_for_pagination(
                    room_id
                )
            )
            # We expect `/messages` to use historic pagination tokens by default but
            # `/messages` should still works with live tokens when manually provided.
            assert from_token.room_key.topological is not None

        pagination_from_token = from_token.room_key
        live_from_token: RoomStreamToken | None = None
        if pagin_config.direction == Direction.FORWARDS:
            if (
                from_token.room_key.topological is None
                and not from_token.room_key.instance_map
            ):
                # A stream-only token (e.g. a sync `prev_batch`) cannot see
                # backfilled events, whose stream orderings are negative. Pin a
                # separate query token to a topological position so forward
                # pages can include them, while the original token's stream
                # position still bounds live events.
                #
                # Vector clock tokens (with an instance map) already encode each
                # writer's position and are filtered per instance, so leave them
                # alone: discarding the map would let through events the client
                # has already synced.
                pinned = await self.store.get_topological_token_for_stream_position(
                    room_id, from_token.room_key.stream
                )
                if pinned is not None:
                    pagination_from_token = pinned
                    live_from_token = from_token.room_key
            # A token with a topological part is already a full
            # (depth, timestamp, stream) cursor in the same order the query
            # sorts by. Bounding live events by its stream position would skip
            # events on forks, where a later-depth event has a lower stream
            # ordering than an earlier-depth one.

        room_token = pagination_from_token

        (membership, member_event_id) = (None, None)
        if not use_admin_priviledge:
            (
                membership,
                member_event_id,
            ) = await self.auth.check_user_in_room_or_world_readable(
                room_id, requester, allow_departed_users=True
            )

        if pagin_config.direction == Direction.BACKWARDS:
            # if we're going backwards, we might need to backfill. This
            # requires that we have a topo token.
            if room_token.topological:
                curr_topo = room_token.topological
            else:
                curr_topo = await self.store.get_current_topological_token(
                    room_id, room_token.stream
                )

        # If they have left the room then clamp the token to be before
        # they left the room, to save the effort of loading from the
        # database.
        if (
            pagin_config.direction == Direction.BACKWARDS
            and not use_admin_priviledge
            and membership == Membership.LEAVE
        ):
            # This is only None if the room is world_readable, in which case
            # "Membership.JOIN" would have been returned and we should never hit
            # this branch.
            assert member_event_id

            leave_token = await self.store.get_topological_token_for_event(
                member_event_id
            )
            assert leave_token.topological is not None

            if leave_token.topological < curr_topo:
                from_token = from_token.copy_and_replace(
                    StreamKeyType.ROOM, leave_token
                )
                # The database query uses `pagination_from_token`, which was
                # captured from the original token above. Clamp it too, or a
                # departed user can still page past their leave event.
                pagination_from_token = leave_token
                # Gap detection and backfill must use the clamped cursor too, or
                # gaps above the leave event (which this user can never see) would
                # keep triggering backfill and retry tokens.
                curr_topo = leave_token.topological

        to_room_key = None
        if pagin_config.to_token:
            to_room_key = pagin_config.to_token.room_key

        # The token the client sent (or the start of this pagination run) is
        # what we echo back, even when the query uses a pinned topological
        # anchor to find backfilled events.
        response_start_token = from_token

        # Initially fetch the events from the database. With any luck, we can return
        # these without blocking on backfill (handled below).
        (
            events,
            next_key,
            limited,
        ) = await self.store.paginate_room_events_by_topological_ordering(
            room_id=room_id,
            from_key=pagination_from_token,
            to_key=to_room_key,
            direction=pagin_config.direction,
            limit=pagin_config.limit,
            event_filter=event_filter,
            live_from_key=live_from_token,
        )
        events = await self._reload_stale_outliers(events)

        async def find_gap_index(
            page_events: list[EventBase] | None = None,
        ) -> int | None:
            """Return the first event in this page which is next to an open gap."""
            for i, event in enumerate(events if page_events is None else page_events):
                if event.internal_metadata.is_outlier():
                    # Outliers have no backward extremity bookkeeping. The page
                    # only holds non-outliers, so this can only be a copy that
                    # went stale after the reload above.
                    continue
                if await self.store.is_event_next_to_backward_gap(event):
                    return i
            return None

        async def gap_boundary_token(event: EventBase) -> RoomStreamToken:
            """Build a token at an event without advancing a live stream bound."""
            if (
                pagin_config.direction == Direction.FORWARDS
                and live_from_token is not None
                and live_from_token.stream >= 0
                and event.internal_metadata.stream_ordering is not None
                and event.internal_metadata.stream_ordering < 0
            ):
                # Forward pagination can be anchored by both a topological
                # position and a live stream position. Advancing past a
                # backfilled event must keep the latter at its original bound.
                return RoomStreamToken(
                    topological=event.depth,
                    timestamp=event.origin_server_ts,
                    stream=live_from_token.stream,
                )
            stream_ordering = event.internal_metadata.stream_ordering
            if stream_ordering is None:
                return await self.store.get_topological_token_for_event(event.event_id)
            # Same convention as every other page: a backward token must point
            # past the last event returned, or that event is returned again by
            # the next request.
            return generate_next_token(
                pagin_config.direction,
                event.depth,
                event.origin_server_ts,
                stream_ordering,
            )

        async def get_forward_unfiltered_events(
            endpoint_event_id: str | None,
        ) -> tuple[list[EventBase], RoomStreamToken, bool]:
            """Scan the unfiltered range up to the filtered page's endpoint.

            Reads bounded chunks, resuming each from the previous chunk's
            cursor so no event is loaded twice. It stops at the first gap or at
            the endpoint, and retains only the filtered events seen before a
            gap (plus the chunk containing it), not the whole scanned prefix.
            """
            chunk_limit = max(pagin_config.limit, 1)
            cursor = room_token
            relevant_ids = {event.event_id for event in events}
            before_gap: list[EventBase] = []
            while True:
                (
                    chunk,
                    next_key,
                    chunk_limited,
                ) = await self.store.paginate_room_events_by_topological_ordering(
                    room_id=room_id,
                    from_key=cursor,
                    to_key=to_room_key,
                    direction=Direction.FORWARDS,
                    limit=chunk_limit,
                    event_filter=None,
                    live_from_key=live_from_token,
                )
                chunk = await self._reload_stale_outliers(chunk)

                # A gap beyond the filtered page's endpoint belongs to a later
                # page, so do not scan past that endpoint.
                endpoint_index = None
                if endpoint_event_id is not None:
                    endpoint_index = next(
                        (
                            index
                            for index, event in enumerate(chunk)
                            if event.event_id == endpoint_event_id
                        ),
                        None,
                    )
                scanned = (
                    chunk if endpoint_index is None else chunk[: endpoint_index + 1]
                )

                gap_index = await find_gap_index(scanned)
                if gap_index is not None:
                    return (
                        before_gap + scanned[: gap_index + 1],
                        next_key,
                        chunk_limited,
                    )

                # Only filtered events can appear in the eventual cut. Keeping
                # just those bounds this prefix by the page size, not by how far
                # the unfiltered scan has travelled.
                before_gap.extend(
                    event for event in scanned if event.event_id in relevant_ids
                )

                if endpoint_index is not None or not chunk_limited:
                    return (
                        before_gap if before_gap else scanned,
                        next_key,
                        chunk_limited,
                    )
                if not chunk:
                    return [], next_key, chunk_limited
                cursor = next_key

        async def has_retryable_gap(nearby_depth: int) -> bool:
            """Whether a gap is still eligible to be retried right now.

            ``get_backfill_points_in_room`` applies ``event_failed_pull_attempts``
            backoff, so an extremity a remote recently refused is excluded until
            the backoff expires. That is a deferral, not a permanent verdict: it
            only means this request must not block on (or cut the page at) that
            extremity again. Continuing past it lets reachable history on the
            other side be exposed without asking the remote again.
            """
            backfill_points = await self.store.get_backfill_points_in_room(
                room_id,
                nearby_depth=nearby_depth,
                limit=50,
            )
            return bool(backfill_points)

        async def gap_can_be_filled() -> bool:
            """Whether some other server in the room could supply missing history.

            A "history is still being fetched, retry" answer only makes sense if
            someone can be asked. In a room with no remote servers (for example
            after a purge, which deliberately leaves backward extremities at the
            surviving boundary) there is nobody to backfill from, so the gap is
            the end of what exists and must not be reported as retryable.
            """
            hosts = await self.store.get_current_hosts_in_room(room_id)
            return any(host != self.server_name for host in hosts)

        backward_gap_index = (
            await find_gap_index()
            if pagin_config.direction == Direction.BACKWARDS
            else None
        )

        # A cursor returned at a gap boundary points at its successor event. On the
        # next request that event is excluded by the pagination bounds, so detect a
        # still-open extremity at the cursor itself and try to fill it before
        # returning anything from the far side.
        backward_gap_at_cursor = False
        if pagin_config.direction == Direction.BACKWARDS and curr_topo:
            cursor_backfill_points = await self.store.get_backfill_points_in_room(
                room_id,
                nearby_depth=curr_topo,
                limit=50,
                include_failed=True,
            )
            backward_gap_at_cursor = (
                any(depth == curr_topo for _, depth in cursor_backfill_points)
                and await gap_can_be_filled()
            )

        # Whether a foreground backfill made progress during this request. A
        # continuation token is only worth returning while the fill is moving:
        # a pass that adds nothing must let the walk end instead of handing back
        # the same cursor forever.
        backward_backfill_progress = False

        if pagin_config.direction == Direction.BACKWARDS:
            # We use a `Set` because there can be multiple events at a given depth
            # and we only care about looking at the unique continum of depths to
            # find gaps.
            event_depths: set[int] = {event.depth for event in events}
            sorted_event_depths = sorted(event_depths)

            # Inspect the depths of the returned events to see if there are any gaps
            found_big_gap = False
            number_of_gaps = 0
            previous_event_depth = (
                sorted_event_depths[0] if len(sorted_event_depths) > 0 else 0
            )
            for event_depth in sorted_event_depths:
                # We don't expect a negative depth but we'll just deal with it in
                # any case by taking the absolute value to get the true gap between
                # any two integers.
                depth_gap = abs(event_depth - previous_event_depth)
                # A `depth_gap` of 1 is a normal continuous chain to the next event
                # (1 <-- 2 <-- 3) so anything larger indicates a missing event (it's
                # also possible there is no event at a given depth but we can't ever
                # know that for sure)
                if depth_gap > 1:
                    number_of_gaps += 1

                # We only tolerate a small number single-event long gaps in the
                # returned events because those are most likely just events we've
                # failed to pull in the past. Anything longer than that is probably
                # a sign that we're missing a decent chunk of history and we should
                # try to backfill it.
                #
                # XXX: It's possible we could tolerate longer gaps if we checked
                # that a given events `prev_events` is one that has failed pull
                # attempts and we could just treat it like a dead branch of history
                # for now or at least something that we don't need the block the
                # client on to try pulling.
                #
                # XXX: If we had something like MSC3871 to indicate gaps in the
                # timeline to the client, we could also get away with any sized gap
                # and just have the client refetch the holes as they see fit.
                if depth_gap > 2:
                    found_big_gap = True
                    break
                previous_event_depth = event_depth

            # Backfill in the foreground if we found a big gap, have too many holes,
            # or we don't have enough events to fill the limit that the client asked
            # for.
            #
            # A page of one event is not enough to establish that the timeline is
            # continuous: direct predecessors alone do not cover concurrent branches
            # or other missing events in the topological range. Returning its token
            # before foreground backfill could make those events unreachable to this
            # pagination run.
            missing_too_many_events = (
                number_of_gaps > BACKFILL_BECAUSE_TOO_MANY_GAPS_THRESHOLD
            )
            # A filtered page can legitimately contain fewer events than the
            # requested limit. That alone does not show that history is
            # missing, so do not synchronously federate in that case.
            not_enough_events_to_fill_response = (
                event_filter is None and len(events) < pagin_config.limit
            )

            # Keep the pagination cursor as the backfill point. Using the
            # shallowest event in a sparse page can advance past a gap attached
            # to a newer event in the same page.
            backfill_depth = curr_topo

            if (
                found_big_gap
                or backward_gap_index is not None
                or backward_gap_at_cursor
                or missing_too_many_events
                or not_enough_events_to_fill_response
                or pagin_config.limit == 1
            ):
                did_backfill = await self.hs.get_federation_handler().maybe_backfill(
                    room_id,
                    backfill_depth,
                    limit=pagin_config.limit,
                    force=True,
                    direction=Direction.BACKWARDS,
                )

                # If we did backfill something, refetch the events from the database to
                # catch anything new that might have been added since we last fetched.
                if did_backfill:
                    (
                        events,
                        next_key,
                        limited,
                    ) = await self.store.paginate_room_events_by_topological_ordering(
                        room_id=room_id,
                        from_key=pagination_from_token,
                        to_key=to_room_key,
                        direction=pagin_config.direction,
                        limit=pagin_config.limit,
                        event_filter=event_filter,
                        live_from_key=live_from_token,
                    )
                    events = await self._reload_stale_outliers(events)
                backward_backfill_progress = did_backfill
                # The cursor sits on a gap and the fill is moving but nothing
                # new is visible yet: return no events and keep the cursor so the
                # next request resumes the same gap instead of skipping it. If
                # the attempt added nothing, the page continues past the hole so
                # reachable history on the other side is still exposed and the
                # walk can end.
                if (
                    backward_gap_at_cursor
                    and did_backfill
                    and not events
                    and await has_retryable_gap(curr_topo)
                ):
                    limited = True
                    next_key = from_token.room_key

                backward_gap_index = await find_gap_index()
            else:
                # Otherwise, we can backfill in the background for eventual
                # consistency's sake but we don't need to block the client
                # waiting for a costly federation call and processing.
                self.hs.run_as_background_process(
                    "maybe_backfill_in_the_background",
                    self.hs.get_federation_handler().maybe_backfill,
                    room_id,
                    backfill_depth,
                    limit=pagin_config.limit,
                    direction=Direction.BACKWARDS,
                )

            if (
                backward_gap_index is not None
                and await gap_can_be_filled()
                and backward_backfill_progress
                and await has_retryable_gap(events[backward_gap_index].depth)
            ):
                # A backward page is newest first. Include the event at the edge
                # but nothing older than it; the returned token is immediately
                # before that event, where the next request will retry backfill.
                # Without a remote server to ask the gap cannot close, and
                # cutting the page here would hand back the same boundary event
                # on every request, so return the page as it is instead.
                events = events[: backward_gap_index + 1]
                if events:
                    next_key = await gap_boundary_token(events[-1])
                    limited = True

        if pagin_config.direction == Direction.FORWARDS:
            # Events we accepted as a "forward gap" have prev_events we could
            # not fetch when they arrived. Those gaps are ahead of a forward
            # cursor and can only be filled by backfilling from the backward
            # extremities they created. A filtered empty page is included so
            # that a client can still drive the fill when no stored event is
            # visible.
            page_hits_a_gap = False
            # Inspect the actual requested range before applying the client's
            # filter. A filter can hide the event adjacent to a gap while still
            # returning later events, which would otherwise let pagination skip
            # the missing history entirely.
            (
                forward_unfiltered_events,
                forward_unfiltered_next_key,
                forward_unfiltered_limited,
            ) = await get_forward_unfiltered_events(
                events[-1].event_id if events else None
            )
            page_hits_a_gap = (
                await find_gap_index(forward_unfiltered_events) is not None
            )
            backfill_progress = False

            if page_hits_a_gap:
                # Any extremity ahead of the cursor is eligible: we are walking
                # forwards, so there is no "already paged past" boundary.
                did_backfill = await self.hs.get_federation_handler().maybe_backfill(
                    room_id,
                    MAX_DEPTH,
                    limit=pagin_config.limit,
                    force=True,
                    direction=Direction.FORWARDS,
                )
                backfill_progress = did_backfill
                if did_backfill:
                    # `did_backfill` only says a request was made. Re-asking for
                    # the same cursor is only worthwhile if that request changed
                    # what this page contains; otherwise a gap the remote cannot
                    # fill would return the same cursor forever.
                    (
                        events,
                        next_key,
                        limited,
                    ) = await self.store.paginate_room_events_by_topological_ordering(
                        room_id=room_id,
                        from_key=pagination_from_token,
                        to_key=to_room_key,
                        direction=pagin_config.direction,
                        limit=pagin_config.limit,
                        event_filter=event_filter,
                        live_from_key=live_from_token,
                    )
                    events = await self._reload_stale_outliers(events)
                    (
                        forward_unfiltered_events,
                        forward_unfiltered_next_key,
                        forward_unfiltered_limited,
                    ) = await get_forward_unfiltered_events(
                        events[-1].event_id if events else None
                    )

            # A forward page is oldest first. Do not advance the cursor past the
            # event at a gap boundary: its missing predecessor may still be
            # backfilled, and events revealed behind the cursor would otherwise
            # never be returned by this run.
            forward_gap_index = await find_gap_index(forward_unfiltered_events)
            forward_cut_at_gap = False
            if (
                forward_gap_index is not None
                and await gap_can_be_filled()
                and await has_retryable_gap(
                    forward_unfiltered_events[forward_gap_index].depth
                )
                # Cutting at the very first event hands back an empty page, which
                # only makes sense while the fill is moving. Otherwise return the
                # page as it is: cutting it and advancing the cursor would skip
                # the events at the gap boundary.
                and (forward_gap_index > 0 or backfill_progress)
            ):
                events_before_gap = {
                    event.event_id
                    for event in forward_unfiltered_events[:forward_gap_index]
                }
                events = [
                    event for event in events if event.event_id in events_before_gap
                ]
                if events:
                    next_key = await gap_boundary_token(events[-1])
                    limited = True
                    forward_cut_at_gap = True
                elif backfill_progress:
                    # The gap is the first event in the page and a fill is
                    # moving. Returning the unchanged cursor as an empty 200
                    # page makes clients spin; tell them this page is deferred.
                    raise SynapseError(
                        429,
                        "History is still being fetched, please retry",
                        headers={
                            "Retry-After": str(FORWARD_BACKFILL_RETRY_AFTER_SECONDS)
                        },
                    )
                elif forward_gap_index > 0:
                    # The filter hid every event before this gap, but may match
                    # events after it. Resume after the last unfiltered event
                    # before the gap so the next request can return those later
                    # matches without skipping across the gap.
                    next_key = await gap_boundary_token(
                        forward_unfiltered_events[forward_gap_index - 1]
                    )
                    limited = True
                    forward_cut_at_gap = True

            if page_hits_a_gap and not events:
                # A failed or backed-off fill must not return an empty page with
                # the same cursor. Retry only while a backfill point is currently
                # eligible; once all points are in backoff, let pagination end
                # until that backoff expires (or another request discovers new
                # history).
                if (
                    backfill_progress
                    and await gap_can_be_filled()
                    and await has_retryable_gap(MAX_DEPTH)
                ):
                    raise SynapseError(
                        429,
                        "History is still being fetched, please retry",
                        headers={
                            "Retry-After": str(FORWARD_BACKFILL_RETRY_AFTER_SECONDS)
                        },
                    )
                if not forward_cut_at_gap:
                    limited = False
                    if not backfill_progress:
                        # With the gap at the start of the range, an empty
                        # filtered page contains no matching events to skip.
                        next_key = forward_unfiltered_next_key
                        limited = forward_unfiltered_limited
            elif not events and forward_unfiltered_events:
                # A filter may remove the whole page. Keep pagination moving
                # using the unfiltered range's cursor so later matching events
                # (and gaps) remain reachable.
                next_key = forward_unfiltered_next_key
                limited = forward_unfiltered_limited

        # An empty backward page at the room's oldest known event is not the
        # beginning of the room if a missing predecessor is still recorded.
        if pagin_config.direction == Direction.BACKWARDS and not events and curr_topo:
            if backward_backfill_progress and await has_retryable_gap(curr_topo):
                # The room is not finished just because this page is empty: the
                # fill advanced during this request. Return a continuation token
                # instead of claiming the start of the room.
                limited = True

        next_token = from_token.copy_and_replace(StreamKeyType.ROOM, next_key)

        # if no events are returned from pagination (this page is empty)
        # and there aren't any more pages (not limited),
        # that implies we have reached the end of the available events.
        # In that case we do not return end, to tell the client
        # there is no need for further queries.
        if not limited and not events:
            return GetMessagesResult(
                messages_chunk=[],
                bundled_aggregations={},
                state=None,
                start_token=response_start_token,
                end_token=None,
            )

        if event_filter:
            events = await event_filter.filter(events)

        if not use_admin_priviledge:
            filtered_events = await filter_and_transform_events_for_client(
                self._storage_controllers,
                user_id,
                events,
                is_peeking=(member_event_id is None),
            )
        else:
            filtered_events = [FilteredEvent.admin_override(e) for e in events]

        # if after the filter applied there are no more events
        # return immediately - but there might be more in next_token batch
        if not filtered_events:
            return GetMessagesResult(
                messages_chunk=[],
                bundled_aggregations={},
                state=None,
                start_token=response_start_token,
                end_token=next_token,
            )

        state = None
        if event_filter and event_filter.lazy_load_members and len(filtered_events) > 0:
            # TODO: remove redundant members

            # FIXME: we also care about invite targets etc.
            state_filter = StateFilter.from_types(
                (EventTypes.Member, event.event.sender) for event in filtered_events
            )

            state_ids = await self._state_storage_controller.get_state_ids_for_event(
                filtered_events[0].event.event_id, state_filter=state_filter
            )

            if state_ids:
                state_dict = await self.store.get_events(list(state_ids.values()))
                state = list(state_dict.values())

        aggregations = await self._relations_handler.get_bundled_aggregations(
            filtered_events, user_id
        )

        return GetMessagesResult(
            messages_chunk=filtered_events,
            bundled_aggregations=aggregations,
            state=state,
            start_token=response_start_token,
            end_token=next_token,
        )

    async def _shutdown_and_purge_room(
        self,
        task: ScheduledTask,
    ) -> tuple[TaskStatus, JsonMapping | None, str | None]:
        """
        Scheduler action to shutdown and purge a room.
        """
        if task.resource_id is None or task.params is None:
            raise Exception(
                "No room id and/or no parameters passed to shutdown_and_purge_room task"
            )

        room_id = task.resource_id

        async def update_result(result: JsonMapping | None) -> None:
            await self._task_scheduler.update_task(task.id, result=result)

        shutdown_result = (
            cast(ShutdownRoomResponse, task.result) if task.result else None
        )

        shutdown_result = await self._room_shutdown_handler.shutdown_room(
            room_id,
            cast(ShutdownRoomParams, task.params),
            shutdown_result,
            update_result,
        )

        if task.params.get("purge", False):
            await self.purge_room(
                room_id,
                task.params.get("force_purge", False),
            )

        return (TaskStatus.COMPLETE, shutdown_result, None)

    async def start_shutdown_and_purge_room(
        self,
        room_id: str,
        shutdown_params: ShutdownRoomParams,
    ) -> str:
        """Start off shut down and purge on a room.

        Args:
            room_id: The ID of the room to shut down.
            shutdown_params: parameters for the shutdown

        Returns:
            unique ID for this delete transaction.
        """
        if len(await self.get_delete_tasks_by_room(room_id, only_active=True)) > 0:
            raise SynapseError(400, "Purge already in progress for %s" % (room_id,))

        # This check is double to `RoomShutdownHandler.shutdown_room`
        # But here the requester get a direct response / error with HTTP request
        # and do not have to check the purge status
        new_room_user_id = shutdown_params["new_room_user_id"]
        if new_room_user_id is not None:
            if not self.hs.is_mine_id(new_room_user_id):
                raise SynapseError(
                    400, "User must be our own: %s" % (new_room_user_id,)
                )

        delete_id = await self._task_scheduler.schedule_task(
            SHUTDOWN_AND_PURGE_ROOM_ACTION_NAME,
            resource_id=room_id,
            params=shutdown_params,
        )

        # we log the delete_id here so that it can be tied back to the
        # request id in the log lines.
        logger.info(
            "starting shutdown room_id %s with delete_id %s",
            room_id,
            delete_id,
        )

        return delete_id
