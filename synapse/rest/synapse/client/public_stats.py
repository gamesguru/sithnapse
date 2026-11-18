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

from synapse.http.server import DirectServeJsonResource
from synapse.http.site import SynapseRequest
from synapse.types import JsonDict

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from synapse.server import HomeServer


class PublicUserCountResource(DirectServeJsonResource):
    """Opt-in public endpoint exposing only the total registered user count."""

    def __init__(self, hs: "HomeServer"):
        super().__init__(clock=hs.get_clock())
        self.store = hs.get_datastores().main

    async def _async_render_GET(self, request: SynapseRequest) -> tuple[int, JsonDict]:
        try:
            total_users = await self.store.count_all_users()
        except Exception:
            logger.exception("Failed to count users for public server statistics")
            total_users = None

        return HTTPStatus.OK, {"total_users": total_users}
