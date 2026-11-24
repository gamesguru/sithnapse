# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.

import logging
from http import HTTPStatus
from typing import TYPE_CHECKING

from synapse.api.constants import Direction
from synapse.http.server import DirectServeJsonResource
from synapse.http.site import SynapseRequest
from synapse.storage.databases.embedded_engine import (
    get_embedded_db_disk_bytes,
    get_embedded_db_index_bytes,
    get_embedded_db_wal_bytes,
)
from synapse.storage.databases.main.transactions import DestinationSortOrder
from synapse.types import JsonDict
from synapse.util.memory import get_process_rss_bytes

logger = logging.getLogger(__name__)

# The endpoint is anonymous and runs several counts, so serve a recent result
# instead of querying on every request.
_CACHE_LIFETIME_MS = 60 * 1000

if TYPE_CHECKING:
    from synapse.server import HomeServer


class PublicStatsResource(DirectServeJsonResource):
    """Opt-in public endpoint exposing only aggregate counts.

    Returns the total registered users, known rooms, federation peers, the
    number of distinct servers present in room state, local-user and joined
    membership counts, event/state-event counts, the on-disk size of the
    PostgreSQL and embedded databases, and this process's resident memory.
    No identifiers or per-user/room details are exposed. Any value that fails
    to load is returned as `null`.
    """

    def __init__(self, hs: "HomeServer"):
        super().__init__(clock=hs.get_clock())
        self.store = hs.get_datastores().main
        self._clock = hs.get_clock()
        self._reactor = hs.get_reactor()
        self._hs = hs
        self._embedded_db_path = hs.config.database.embedded_db_path
        self._cached: JsonDict | None = None
        self._cached_at_ms = 0
        # The ever-seen user count is the most expensive part of a refresh.
        # Keep the last successful value so a failed query serves the previous
        # number instead of dropping the field from the page.
        self._last_ever_seen_users: int | None = None

    def _uptime(self) -> int | None:
        """Seconds since this process started, or `None` before setup."""
        start = self._hs.start_time
        if start is None:
            return None
        return max(0, int(self._clock.time()) - start)

    async def _async_render_GET(self, request: SynapseRequest) -> tuple[int, JsonDict]:
        now_ms = self._clock.time_msec()
        if (
            self._cached is not None
            and 0 <= now_ms - self._cached_at_ms < _CACHE_LIFETIME_MS
        ):
            return HTTPStatus.OK, {**self._cached, "uptime_seconds": self._uptime()}

        try:
            total_users = await self.store.count_all_users()
        except Exception:
            logger.exception("Failed to count users for public server statistics")
            total_users = None

        try:
            total_rooms = await self.store.get_room_count()
        except Exception:
            logger.exception("Failed to count rooms for public server statistics")
            total_rooms = None

        try:
            _, total_destinations = await self.store.get_destinations_paginate(
                start=0,
                limit=1,
                destination=None,
                order_by=DestinationSortOrder.DESTINATION.value,
                direction=Direction.FORWARDS,
            )
        except Exception:
            logger.exception("Failed to count peers for public server statistics")
            total_destinations = None

        try:
            total_known_servers = await self.store.get_known_servers_count()
        except Exception:
            logger.exception(
                "Failed to count known servers for public server statistics"
            )
            total_known_servers = None

        try:
            total_events, total_state_events = await self.store.get_event_counts()
        except Exception:
            logger.exception("Failed to count events for public server statistics")
            total_events = None
            total_state_events = None

        try:
            local_users = await self.store.count_local_users()
        except Exception:
            logger.exception("Failed to count local users for public server statistics")
            local_users = None

        try:
            (
                joined_members,
                local_joined_members,
            ) = await self.store.get_joined_member_counts()
        except Exception:
            logger.exception(
                "Failed to count joined members for public server statistics"
            )
            joined_members = None
            local_joined_members = None

        counts_fresh = True
        ever_seen_users: int | None
        try:
            ever_seen_users = await self.store.get_ever_seen_user_count()
        except Exception:
            logger.exception(
                "Failed to count ever-seen users for public server statistics"
            )
            counts_fresh = False
            ever_seen_users = self._last_ever_seen_users
        else:
            self._last_ever_seen_users = ever_seen_users

        try:
            postgres_bytes = await self.store.get_postgres_disk_bytes()
        except Exception:
            logger.exception("Failed to read database size for public statistics")
            postgres_bytes = None

        try:
            postgres_pss_bytes = await self.store.get_postgres_process_pss_bytes()
        except Exception:
            logger.exception("Failed to read PostgreSQL memory for public statistics")
            postgres_pss_bytes = None

        database: JsonDict = {
            "events": total_events,
            "state_events": total_state_events,
            "local_users": local_users,
            "joined_members": joined_members,
            "local_joined_members": local_joined_members,
            "ever_seen_users": ever_seen_users,
            "postgres_bytes": postgres_bytes,
            "postgres_pss_bytes": postgres_pss_bytes,
            "mtxdb_bytes": await get_embedded_db_disk_bytes(
                self._reactor, self._embedded_db_path
            ),
            "mtxdb_index_bytes": await get_embedded_db_index_bytes(
                self._reactor, self._embedded_db_path
            ),
            "mtxdb_wal_bytes": await get_embedded_db_wal_bytes(
                self._reactor, self._embedded_db_path
            ),
        }

        result: JsonDict = {
            "total_users": total_users,
            "total_rooms": total_rooms,
            "total_destinations": total_destinations,
            "total_known_servers": total_known_servers,
            "database": database,
            "memory": {"rss_bytes": get_process_rss_bytes()},
            "uptime_seconds": self._uptime(),
        }
        # Only cache complete results so a transient failure isn't served for
        # the whole lifetime. The storage sizes are optional: PostgreSQL and
        # the embedded engine may legitimately be absent, so they don't gate
        # caching.
        required = (
            total_users,
            total_rooms,
            total_destinations,
            total_known_servers,
            total_events,
            total_state_events,
            local_users,
            joined_members,
            local_joined_members,
        )
        # A degraded refresh (last-good count) must not extend the cache, or
        # the failure would be hidden for another lifetime.
        if counts_fresh and all(value is not None for value in required):
            self._cached = result
            self._cached_at_ms = now_ms
        return HTTPStatus.OK, result
