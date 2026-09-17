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
from twisted.internet.defer import ensureDeferred

from synapse.rest.client import room

from tests.replication._base import BaseMultiWorkerStreamTestCase


class PartialStateStreamsTestCase(BaseMultiWorkerStreamTestCase):
    servlets = [room.register_servlets]
    hijack_auth = True
    user_id = "@bob:test"

    def setUp(self) -> None:
        super().setUp()
        self.store = self.hs.get_datastores().main

    def test_un_partial_stated_room_unblocks_over_replication(self) -> None:
        """
        Tests that, when a room is un-partial-stated on another worker,
        pending calls to `await_full_state` get unblocked.
        """

        # Make a room.
        room_id = self.helper.create_room_as("@bob:test")
        # Mark the room as partial-stated.
        self.get_success(
            self.store.store_partial_state_room(room_id, {"serv1", "serv2"}, 0, "serv1")
        )

        worker = self.make_worker_hs("synapse.app.generic_worker")

        # On the worker, attempt to get the current hosts in the room
        d = ensureDeferred(
            worker.get_storage_controllers().state.get_current_hosts_in_room(room_id)
        )

        self.reactor.advance(0.1)

        # This should block
        self.assertFalse(
            d.called, "get_current_hosts_in_room/await_full_state did not block"
        )

        # On the master, clear the partial state flag.
        self.get_success(self.store.clear_partial_state_room(room_id))

        self.reactor.advance(0.1)

        # The worker should have unblocked
        self.assertTrue(
            d.called, "get_current_hosts_in_room/await_full_state did not unblock"
        )

    def test_un_partial_stated_event_cache_invalidation_over_replication(self) -> None:
        """
        Tests that when an event is un-partial-stated on the writer, the worker's
        un-partial-stated cache gets invalidated over replication and re-queries SQL.
        """
        room_id = self.helper.create_room_as("@bob:test")
        res = self.helper.send(room_id, "body", "@bob:test")
        event_id = res["event_id"]
        event = self.get_success(self.store.get_event(event_id))

        worker = self.make_worker_hs("synapse.app.generic_worker")
        worker_store = worker.get_datastores().main

        # 1. Worker checks if event is un-partial-stated -> False, and caches it
        is_un = self.get_success(worker_store.is_un_partial_stated_event(event_id))
        self.assertFalse(is_un)
        cached_val = worker_store.is_un_partial_stated_event.cache.get_immediate(
            (event_id,), None
        )
        self.assertFalse(cached_val)

        # 2. Mark event as partial state, then writer de-partial-states it
        self.get_success(
            self.store.store_partial_state_room(
                room_id=room_id,
                servers={"test"},
                device_lists_stream_id=0,
                joined_via="test",
            )
        )
        self.get_success(
            self.store.db_pool.simple_insert(
                table="partial_state_events",
                values={"room_id": room_id, "event_id": event_id},
            )
        )
        context = self.get_success(
            self.hs.get_state_handler().compute_event_context(event)
        )
        self.get_success(
            self.store.update_state_for_partial_state_event(event, context)
        )
        self.hs.get_replication_notifier().notify_replication()

        # 3. Advance reactor to deliver replication stream to worker
        self.reactor.advance(0.1)

        # 4. Worker cache should be invalidated and now return True
        self.assertIsNone(
            worker_store.is_un_partial_stated_event.cache.get_immediate(
                (event_id,), None
            )
        )
        is_un_now = self.get_success(worker_store.is_un_partial_stated_event(event_id))
        self.assertTrue(is_un_now)

        # 5. Batch cache lookup also works correctly
        batch_map = self.get_success(
            worker_store.get_un_partial_stated_events([event_id, "$nonexistent:test"])
        )
        self.assertEqual(batch_map, {event_id: True, "$nonexistent:test": False})
