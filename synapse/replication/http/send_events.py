#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2022 The Matrix.org Foundation C.I.C.
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
from typing import TYPE_CHECKING, Any

from twisted.web.server import Request

from synapse.api.room_versions import KNOWN_ROOM_VERSIONS
from synapse.events import make_event_from_dict
from synapse.events.snapshot import (
    EventContext,
    EventPersistencePair,
    _decode_state_dict,
)
from synapse.http.server import HttpServer
from synapse.replication.http._base import ReplicationEndpoint
from synapse.storage.databases.state.store import MAX_MIRROR_STATE_ENTRIES
from synapse.types import JsonDict, Requester, UserID
from synapse.util.metrics import Measure

if TYPE_CHECKING:
    from synapse.server import HomeServer
    from synapse.storage.databases.main import DataStore

logger = logging.getLogger(__name__)


class ReplicationSendEventsRestServlet(ReplicationEndpoint):
    """Handles batches of newly created events on workers, including persisting and
    notifying.

    The API looks like:

        POST /_synapse/replication/send_events/:txn_id

        {
            "events": [{
                "event": { .. serialized event .. },
                "room_version": .., // "1", "2", "3", etc: the version of the room
                            // containing the event
                "event_format_version": .., // 1,2,3 etc: the event format version
                "internal_metadata": { .. serialized internal_metadata .. },
                "outlier": true|false,
                "rejected_reason": ..,   // The event.rejected_reason field
                "context": { .. serialized event context .. },
                "requester": { .. serialized requester .. },
                "ratelimit": true,
            }]
        }

        200 OK

        { "stream_id": 12345, "event_id": "$abcdef..." }

    Responds with a 409 when a `PartialStateConflictError` is raised due to an event
    context that needs to be recomputed due to the un-partial stating of a room.

    """

    NAME = "send_events"
    PATH_ARGS = ()

    def __init__(self, hs: "HomeServer"):
        super().__init__(hs)

        self.server_name = hs.hostname
        self.event_creation_handler = hs.get_event_creation_handler()
        self.store = hs.get_datastores().main
        self._state_store = hs.get_datastores().state
        self._storage_controllers = hs.get_storage_controllers()
        self.clock = hs.get_clock()

    @staticmethod
    async def _serialize_payload(  # type: ignore[override]
        events_and_context: list[EventPersistencePair],
        store: "DataStore",
        requester: Requester,
        ratelimit: bool,
        extra_users: list[UserID],
    ) -> JsonDict:
        """
        Args:
            store
            requester
            events_and_ctx
            ratelimit
        """
        serialized_events = []

        for event, context in events_and_context:
            serialized_context = await context.serialize(event, store)
            serialized_event = {
                "event": event.get_pdu_json(),
                "room_version": event.room_version.identifier,
                "event_format_version": event.format_version,
                "internal_metadata": event.internal_metadata.get_dict(),
                "outlier": event.internal_metadata.is_outlier(),
                "rejected_reason": event.rejected_reason,
                "context": serialized_context,
                "requester": requester.serialize(),
                "ratelimit": ratelimit,
                "extra_users": [u.to_string() for u in extra_users],
            }
            serialized_events.append(serialized_event)

        payload = {"events": serialized_events}

        return payload

    async def _handle_request(  # type: ignore[override]
        self, request: Request, payload: JsonDict
    ) -> tuple[int, JsonDict]:
        with Measure(
            self.clock, name="repl_send_events_parse", server_name=self.server_name
        ):
            events_and_context = []
            events = payload["events"]
            rooms = set()

            for event_payload in events:
                event_dict = event_payload["event"]
                room_ver = KNOWN_ROOM_VERSIONS[event_payload["room_version"]]
                internal_metadata = event_payload["internal_metadata"]
                rejected_reason = event_payload["rejected_reason"]

                event = make_event_from_dict(
                    event_dict, room_ver, internal_metadata, rejected_reason
                )
                event.internal_metadata.outlier = event_payload["outlier"]

                requester = Requester.deserialize(event_payload["requester"])
                context = EventContext.deserialize(
                    self._storage_controllers, event_payload["context"]
                )

                if context.pending_embedded_hamt_mirror_roots is not None:
                    # The instance that created this event's state group (or its
                    # predecessors) opened mtxdb read-only and could not mirror-write
                    # it -- redo those writes here, now that we're on the events
                    # writer. Sort by state group so that the predecessor is always
                    # mirror-written before the child group that depends on it.
                    replays: list[dict[str, Any]] = []
                    for sg, pending_payload in sorted(
                        context.pending_embedded_hamt_mirror_roots.items()
                    ):
                        prev_sg = None
                        delta = None

                        if isinstance(pending_payload, bytes):
                            if len(pending_payload) != 32:
                                raise RuntimeError(
                                    f"Invalid legacy root length {len(pending_payload)} for state group {sg}"
                                )
                            expected_root = pending_payload
                            full_state_map = None
                            expected_lattice = None
                            expected_prefix = None
                            version = 0
                            state_count = None
                        else:
                            payload = pending_payload
                            version = payload.get("version", 0)
                            if version != 1:
                                if "expected_root" not in payload:
                                    raise RuntimeError(
                                        f"Missing expected_root in mirror payload for state group {sg}"
                                    )
                                expected_root = bytes.fromhex(payload["expected_root"])
                                if len(expected_root) != 32:
                                    raise RuntimeError(
                                        f"Invalid expected_root length {len(expected_root)} for state group {sg}"
                                    )
                                full_state_map = None
                                expected_lattice = None
                                expected_prefix = None
                                state_count = None
                            else:
                                if payload.get("state_group") != sg:
                                    raise RuntimeError(
                                        "Pending HAMT payload state-group mismatch"
                                    )
                                if (
                                    "state" not in payload
                                    or "state_count" not in payload
                                    or "expected_root" not in payload
                                    or "lattice" not in payload
                                    or "room_prefix" not in payload
                                ):
                                    raise RuntimeError(
                                        "Incomplete version-1 pending HAMT payload"
                                    )
                                state_count = payload["state_count"]
                                if state_count > MAX_MIRROR_STATE_ENTRIES:
                                    raise RuntimeError(
                                        "Pending HAMT payload exceeds state limit"
                                    )
                                expected_root = bytes.fromhex(payload["expected_root"])
                                if len(expected_root) != 32:
                                    raise RuntimeError(
                                        f"Invalid expected_root length {len(expected_root)} for state group {sg}"
                                    )
                                expected_lattice = bytes.fromhex(payload["lattice"])
                                if len(expected_lattice) != 2048:
                                    raise RuntimeError(
                                        f"Invalid expected_lattice length {len(expected_lattice)} for state group {sg}"
                                    )
                                expected_prefix = bytes.fromhex(payload["room_prefix"])
                                if len(expected_prefix) != 32:
                                    raise RuntimeError(
                                        f"Invalid room_prefix length {len(expected_prefix)} for state group {sg}"
                                    )

                                from synapse.synapse_rust import state_hamt

                                event_room_prefix = state_hamt.room_hamt_prefix(
                                    event.room_id,
                                    event.room_version.msc4291_room_ids_as_hashes,
                                )
                                if expected_prefix != event_room_prefix:
                                    raise RuntimeError(
                                        f"Room prefix mismatch in mirror payload for state group {sg}: "
                                        f"expected {expected_prefix.hex()} but room computed {event_room_prefix.hex()}"
                                    )

                                full_state_map = _decode_state_dict(payload["state"])
                                if full_state_map is None:
                                    raise RuntimeError(
                                        "Version-1 pending HAMT payload has no state"
                                    )
                                if len(full_state_map) != state_count:
                                    raise RuntimeError(
                                        f"State count mismatch in mirror payload for state group {sg}: "
                                        f"payload specified {state_count} entries, found {len(full_state_map)}"
                                    )

                        if sg == context._state_group:
                            prev_sg = context.state_group_before_event
                            delta = context._state_delta_due_to_event or {}
                        else:
                            for (
                                p_sg,
                                c_sg,
                            ), d_map in context.state_group_deltas.items():
                                if c_sg == sg:
                                    prev_sg = p_sg
                                    delta = d_map
                                    break
                            if delta is None:
                                raise RuntimeError(
                                    f"Could not find predecessor for state group {sg} in deltas"
                                )

                        updates = [
                            (event_type, state_key, ev_id)
                            for (event_type, state_key), ev_id in delta.items()
                        ]
                        replays.append(
                            {
                                "state_group": sg,
                                "prev_state_group": prev_sg,
                                "updates": updates,
                                "expected_root_hash": expected_root,
                                "expected_lattice": expected_lattice,
                                "expected_room_prefix": expected_prefix,
                                "state_map": full_state_map,
                                "state_count": state_count,
                                "version": version,
                            }
                        )

                    if replays:
                        await self._state_store.redo_embedded_hamt_mirror_writes_batch(
                            event.room_id,
                            event.room_version,
                            replays,
                        )

                ratelimit = event_payload["ratelimit"]
                events_and_context.append((event, context))

                extra_users = [
                    UserID.from_string(u) for u in event_payload["extra_users"]
                ]

                # all the rooms *should* be the same, but we'll log separately to be
                # sure.
                rooms.add(event.room_id)

            logger.info(
                "Got batch of %i events to persist to rooms %s", len(events), rooms
            )

            last_event = (
                await self.event_creation_handler.persist_and_notify_client_events(
                    requester, events_and_context, ratelimit, extra_users
                )
            )

        return (
            200,
            {
                "stream_id": last_event.internal_metadata.stream_ordering,
                "event_id": last_event.event_id,
            },
        )


def register_servlets(hs: "HomeServer", http_server: HttpServer) -> None:
    ReplicationSendEventsRestServlet(hs).register(http_server)
