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
from synapse.storage.databases.embedded_engine import get_embedded_db_disk_bytes
from synapse.storage.databases.main.transactions import DestinationSortOrder
from synapse.types import JsonDict

logger = logging.getLogger(__name__)

# The endpoint is anonymous and runs several counts, so serve a recent result
# instead of querying on every request.
_CACHE_LIFETIME_MS = 60 * 1000

if TYPE_CHECKING:
    from synapse.server import HomeServer


class PublicStatsResource(DirectServeJsonResource):
    """Opt-in public endpoint exposing only aggregate counts.

    Returns the total registered users, known rooms, federation peers, the
    number of distinct servers present in room state, event/state-event counts
    and the embedded engine's on-disk size. No identifiers or per-user/room
    details are exposed. Any count that fails to load is returned as `null`.
    """

    def __init__(self, hs: "HomeServer"):
        super().__init__(clock=hs.get_clock())
        self.store = hs.get_datastores().main
        self._clock = hs.get_clock()
        self._embedded_db_path = hs.config.database.embedded_db_path
        self._cached: JsonDict | None = None
        self._cached_at_ms = 0

    async def _async_render_GET(self, request: SynapseRequest) -> tuple[int, JsonDict]:
        now_ms = self._clock.time_msec()
        if (
            self._cached is not None
            and 0 <= now_ms - self._cached_at_ms < _CACHE_LIFETIME_MS
        ):
            return HTTPStatus.OK, self._cached

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

        database: JsonDict = {
            "events": total_events,
            "state_events": total_state_events,
            "mtxdb_bytes": get_embedded_db_disk_bytes(self._embedded_db_path),
        }

        result: JsonDict = {
            "total_users": total_users,
            "total_rooms": total_rooms,
            "total_destinations": total_destinations,
            "total_known_servers": total_known_servers,
            "database": database,
        }
        # Only cache complete results so a transient failure isn't served for
        # the whole lifetime.
        if None not in result.values() and None not in database.values():
            self._cached = result
            self._cached_at_ms = now_ms
        return HTTPStatus.OK, result
