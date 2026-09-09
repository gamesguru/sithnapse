#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2021 The Matrix.org Foundation C.I.C.
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
from typing import cast

from synapse.api.errors import SynapseError
from synapse.storage.database import LoggingTransaction
from synapse.storage.databases.main import CacheInvalidationWorkerStore
from synapse.storage.databases.main.embedded_event_to_state_group import (
    get_state_group_for_events_batch,
)
from synapse.storage.databases.main.event_federation import EventFederationWorkerStore

logger = logging.getLogger(__name__)


class EventForwardExtremitiesStore(
    EventFederationWorkerStore,
    CacheInvalidationWorkerStore,
):
    async def delete_forward_extremities_for_room(self, room_id: str) -> int:
        """Delete any extra forward extremities for a room.

        Invalidates the "get_latest_event_ids_in_room" cache if any forward
        extremities were deleted.

        Returns count deleted.
        """

        def delete_forward_extremities_for_room_txn(txn: LoggingTransaction) -> int:
            # First we need to get the event_id to not delete
            sql = """
                SELECT event_id FROM event_forward_extremities
                INNER JOIN events USING (room_id, event_id)
                WHERE room_id = ?
                ORDER BY stream_ordering DESC
                LIMIT 1
            """
            txn.execute(sql, (room_id,))
            rows = txn.fetchall()
            try:
                event_id = rows[0][0]
                logger.debug(
                    "Found event_id %s as the forward extremity to keep for room %s",
                    event_id,
                    room_id,
                )
            except KeyError:
                msg = "No forward extremity event found for room %s" % room_id
                logger.warning(msg)
                raise SynapseError(400, msg)

            # Now delete the extra forward extremities
            sql = """
                DELETE FROM event_forward_extremities
                WHERE event_id != ? AND room_id = ?
            """

            txn.execute(sql, (event_id, room_id))

            deleted_count = txn.rowcount
            logger.info(
                "Deleted %s extra forward extremities for room %s",
                deleted_count,
                room_id,
            )

            if deleted_count > 0:
                # Invalidate the cache
                self._invalidate_cache_and_stream(
                    txn,
                    self.get_latest_event_ids_in_room,
                    (room_id,),
                )

            return deleted_count

        return await self.db_pool.runInteraction(
            "delete_forward_extremities_for_room",
            delete_forward_extremities_for_room_txn,
        )

    async def get_forward_extremities_for_room(
        self, room_id: str
    ) -> list[tuple[str, int, int, int | None]]:
        """
        Get list of forward extremities for a room.

        Returns:
            A list of tuples of event_id, state_group, depth, and received_ts.
        """

        def get_forward_extremities_for_room_txn(
            txn: LoggingTransaction,
        ) -> list[tuple[str, int, int | None]]:
            # No JOIN against event_to_state_groups here -- that table is
            # exclusive to SQL vs. the embedded engine (see
            # embedded_event_to_state_group.py), so it's empty whenever the
            # embedded engine is configured and this JOIN would silently
            # drop every row. state_group is looked up per event_id below
            # instead, via the same helper every other call site uses that
            # already knows which backend is authoritative.
            sql = """
                SELECT event_id, depth, received_ts
                FROM event_forward_extremities
                INNER JOIN events USING (room_id, event_id)
                WHERE room_id = ?
            """

            txn.execute(sql, (room_id,))
            return cast(list[tuple[str, int, int | None]], txn.fetchall())

        rows = await self.db_pool.runInteraction(
            "get_forward_extremities_for_room",
            get_forward_extremities_for_room_txn,
        )
        # One batched lookup for every event_id at once -- mtxdb has no
        # range/ordered-scan API (keys are content hashes, nothing to scan
        # in order), but get_many/get_state_group_for_events_batch is
        # exactly the batch-point-lookup-by-known-keys primitive this
        # wants, same backend-aware split _get_state_group_for_events uses
        # (state.py) -- reimplemented tolerantly here since that helper
        # raises on any missing event_id, whereas the old INNER JOIN this
        # replaces silently dropped non-matching rows instead.
        event_ids = [event_id for event_id, _depth, _received_ts in rows]
        if getattr(self, "_embedded_event_json_enabled", False):
            state_groups = get_state_group_for_events_batch(
                self._embedded_hamt_engine, self._embedded_hamt_namespace, event_ids
            )
        else:
            sql_rows = cast(
                list[tuple[str, int]],
                await self.db_pool.simple_select_many_batch(
                    table="event_to_state_groups",
                    column="event_id",
                    iterable=event_ids,
                    keyvalues={},
                    retcols=("event_id", "state_group"),
                    desc="get_forward_extremities_for_room_state_groups",
                ),
            )
            state_groups = dict(sql_rows)
        return [
            (event_id, state_groups[event_id], depth, received_ts)
            for event_id, depth, received_ts in rows
            if event_id in state_groups
        ]
