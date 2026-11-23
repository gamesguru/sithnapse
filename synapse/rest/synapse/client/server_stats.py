# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 Element Creations Ltd.
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
from synapse.api.errors import AuthError
from synapse.http.server import DirectServeJsonResource
from synapse.http.site import SynapseRequest
from synapse.storage.databases.embedded_engine import (
    get_embedded_db_disk_bytes,
    get_embedded_db_index_bytes,
)
from synapse.storage.databases.main.transactions import DestinationSortOrder
from synapse.types import JsonDict
from synapse.util.memory import get_process_rss_bytes

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from synapse.server import HomeServer


class ServerStatsResource(DirectServeJsonResource):
    """Authenticated server statistics endpoint for monitoring."""

    def __init__(self, hs: "HomeServer"):
        super().__init__(clock=hs.get_clock())
        self.hs = hs
        self.store = hs.get_datastores().main
        self._auth = hs.get_auth()

    async def _async_render_GET(self, request: SynapseRequest) -> tuple[int, JsonDict]:
        requester = await self._auth.get_user_by_req(request)
        if not await self._auth.is_server_admin(requester):
            raise AuthError(403, "Server admin access required")
        try:
            total_users = await self.store.count_all_users()
        except Exception:
            logger.exception("Failed to count users for server statistics")
            total_users = None

        try:
            public_rooms = await self.store.count_public_rooms(
                network_tuple=None,
                ignore_non_federatable=False,
                search_filter=None,
            )
        except Exception:
            logger.exception("Failed to count public rooms for server statistics")
            public_rooms = None
        try:
            total_rooms = await self.store.get_room_count()
        except Exception:
            logger.exception("Failed to count rooms for server statistics")
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
            logger.exception("Failed to count destinations for server statistics")
            total_destinations = None

        try:
            total_known_servers = await self.store.get_known_servers_count()
        except Exception:
            logger.exception("Failed to count known servers for server statistics")
            total_known_servers = None

        try:
            total_events, total_state_events = await self.store.get_event_counts()
        except Exception:
            logger.exception("Failed to count events for server statistics")
            total_events = None
            total_state_events = None

        try:
            local_users = await self.store.count_local_users()
        except Exception:
            logger.exception("Failed to count local users for server statistics")
            local_users = None

        try:
            (
                joined_members,
                local_joined_members,
            ) = await self.store.get_joined_member_counts()
        except Exception:
            logger.exception("Failed to count joined members for server statistics")
            joined_members = None
            local_joined_members = None

        postgres_bytes = await self.store.get_postgres_disk_bytes()
        mtxdb_bytes = await get_embedded_db_disk_bytes(
            self.hs.get_reactor(), self.hs.config.database.embedded_db_path
        )
        mtxdb_index_bytes = await get_embedded_db_index_bytes(
            self.hs.get_reactor(), self.hs.config.database.embedded_db_path
        )

        return HTTPStatus.OK, {
            "total_users": total_users,
            "total_rooms": total_rooms,
            "public_rooms": public_rooms,
            "total_destinations": total_destinations,
            "total_known_servers": total_known_servers,
            "database": {
                "events": total_events,
                "state_events": total_state_events,
                "local_users": local_users,
                "joined_members": joined_members,
                "local_joined_members": local_joined_members,
                "postgres_bytes": postgres_bytes,
                "mtxdb_bytes": mtxdb_bytes,
                "mtxdb_index_bytes": mtxdb_index_bytes,
            },
            "memory": {"rss_bytes": get_process_rss_bytes()},
            "server_version": self.hs.version_string,
        }
