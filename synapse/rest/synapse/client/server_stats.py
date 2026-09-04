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
from synapse.storage.databases.main.transactions import DestinationSortOrder
from synapse.types import JsonDict

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

        return HTTPStatus.OK, {
            "total_users": total_users,
            "total_rooms": total_rooms,
            "public_rooms": public_rooms,
            "total_destinations": total_destinations,
            "server_version": self.hs.version_string,
        }
