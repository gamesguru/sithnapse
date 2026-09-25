#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2020 The Matrix.org Foundation C.I.C.
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

from twisted.internet.testing import MemoryReactor

from synapse.events import EventBase
from synapse.events.snapshot import EventContext
from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.util.clock import Clock

from tests import unittest
from tests.test_utils.event_injection import create_event


class TestEventContext(unittest.HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        login.register_servlets,
        room.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self._storage_controllers = hs.get_storage_controllers()

        self.user_id = self.register_user("u1", "pass")
        self.user_tok = self.login("u1", "pass")
        self.room_id = self.helper.create_room_as(tok=self.user_tok)

    def test_serialize_deserialize_msg(self) -> None:
        """Test that an EventContext for a message event is the same after
        serialize/deserialize.
        """

        event, context = self.get_success(
            create_event(
                self.hs,
                room_id=self.room_id,
                type="m.test",
                sender=self.user_id,
            )
        )

        self._check_serialize_deserialize(event, context)

    def test_serialize_deserialize_state_no_prev(self) -> None:
        """Test that an EventContext for a state event (with not previous entry)
        is the same after serialize/deserialize.
        """
        event, context = self.get_success(
            create_event(
                self.hs,
                room_id=self.room_id,
                type="m.test",
                sender=self.user_id,
                state_key="",
            )
        )

        self._check_serialize_deserialize(event, context)

    def test_serialize_deserialize_state_prev(self) -> None:
        """Test that an EventContext for a state event (which replaces a
        previous entry) is the same after serialize/deserialize.
        """
        event, context = self.get_success(
            create_event(
                self.hs,
                room_id=self.room_id,
                type="m.room.member",
                sender=self.user_id,
                state_key=self.user_id,
                content={"membership": "leave"},
            )
        )

        self._check_serialize_deserialize(event, context)

    def _check_serialize_deserialize(
        self, event: EventBase, context: EventContext
    ) -> None:
        serialized = self.get_success(context.serialize(event, self.store))

        d_context = EventContext.deserialize(self._storage_controllers, serialized)

        self.assertEqual(context.state_group, d_context.state_group)
        self.assertEqual(context.rejected, d_context.rejected)
        self.assertEqual(
            context.state_group_before_event, d_context.state_group_before_event
        )
        self.assertEqual(context.state_group_deltas, d_context.state_group_deltas)
        self.assertEqual(context.app_service, d_context.app_service)

        self.assertEqual(
            self.get_success(context.get_current_state_ids()),
            self.get_success(d_context.get_current_state_ids()),
        )
        self.assertEqual(
            self.get_success(context.get_prev_state_ids()),
            self.get_success(d_context.get_prev_state_ids()),
        )

    def test_serialize_uses_historical_pending_mirror_root_key(self) -> None:
        """The deferred state-HAMT mirror roots travel under their historical
        wire key even though the storage engine is now `embedded_db`, so
        mixed-version workers keep interoperating during a rolling upgrade."""
        event, context = self.get_success(
            create_event(
                self.hs,
                room_id=self.room_id,
                type="m.test",
                sender=self.user_id,
            )
        )
        context.pending_embedded_hamt_mirror_roots = {1: {"expected_root": "abcd"}}

        serialized = self.get_success(context.serialize(event, self.store))

        self.assertEqual(
            serialized["pending_embedded_hamt_mirror_roots"],
            {"1": {"expected_root": "abcd"}},
        )
        self.assertNotIn("pending_embedded_db_mirror_roots", serialized)

        d_context = EventContext.deserialize(self._storage_controllers, serialized)
        self.assertEqual(
            d_context.pending_embedded_hamt_mirror_roots,
            {1: {"expected_root": "abcd"}},
        )

    def test_deserialize_accepts_legacy_singular_mirror_root(self) -> None:
        """Even older workers send a single hex string, not the plural map."""
        event, context = self.get_success(
            create_event(
                self.hs,
                room_id=self.room_id,
                type="m.test",
                sender=self.user_id,
            )
        )
        serialized = self.get_success(context.serialize(event, self.store))
        serialized.pop("pending_embedded_hamt_mirror_roots")
        serialized["pending_embedded_hamt_mirror_root"] = "abcd"

        d_context = EventContext.deserialize(self._storage_controllers, serialized)
        self.assertEqual(
            d_context.pending_embedded_hamt_mirror_roots,
            {context.state_group: {"expected_root": "abcd"}},
        )
