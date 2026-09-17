#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

import shutil
import tempfile

from twisted.test.proto_helpers import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.storage.databases.main.embedded_event_edges import (
    delete_event_edges_batch,
    get_event_edges_backward_batch,
    get_event_edges_forward_batch,
    put_event_edges_batch,
)
from synapse.synapse_rust import mtxdb_engine
from synapse.util.clock import Clock

from tests import unittest
from tests.unittest import HomeserverTestCase


class EmbeddedEventEdgesTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp(prefix="test-embedded-event-edges-")
        mtxdb_engine.open_client(self.tmpdir)
        self.namespace = "test-edges-ns"
        self.room_id = "!room:example.org"

    def tearDown(self) -> None:
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_put_get_backward_and_forward(self) -> None:
        # e1 has prev_events p1 and p2
        rows = [
            (self.room_id, "$e1", "$p1", False),
            (self.room_id, "$e1", "$p2", True),
        ]
        put_event_edges_batch(self.namespace, rows)

        # Backward edges for $e1
        backward = get_event_edges_backward_batch(
            self.namespace, ["$e1", "$nonexistent"]
        )
        self.assertIn("$e1", backward)
        self.assertEqual(backward["$e1"], [("$p1", False), ("$p2", True)])
        self.assertIsNone(backward["$nonexistent"])

        # Forward edges for $p1 and $p2
        forward = get_event_edges_forward_batch(
            self.namespace, ["$p1", "$p2", "$nonexistent"]
        )
        self.assertEqual(forward["$p1"], ["$e1"])
        self.assertEqual(forward["$p2"], ["$e1"])
        self.assertIsNone(forward["$nonexistent"])

        # Append another event $e2 whose prev_event is $p1
        put_event_edges_batch(self.namespace, [(self.room_id, "$e2", "$p1", False)])
        forward2 = get_event_edges_forward_batch(self.namespace, ["$p1"])
        self.assertCountEqual(forward2["$p1"] or [], ["$e1", "$e2"])

        # Delete $e1 tombstones backward edge
        delete_event_edges_batch(self.namespace, ["$e1"])
        backward_after = get_event_edges_backward_batch(self.namespace, ["$e1"])
        self.assertIsNone(backward_after["$e1"])


class EventEdgesStorageIntegrationTestCase(HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        room.register_servlets,
        login.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self.user_id = self.register_user("alice", "test")
        self.tok = self.login("alice", "test")
        self.room_id = self.helper.create_room_as(
            room_creator=self.user_id, tok=self.tok
        )

    def test_event_edges_mirrored_on_persistence(self) -> None:
        """When events are persisted, event_edges are dual-written to mtxdb."""
        if not getattr(self.store, "_embedded_event_edges_enabled", False):
            return

        res1 = self.helper.send(self.room_id, "first", tok=self.tok)
        e1_id = res1["event_id"]
        res2 = self.helper.send(self.room_id, "second", tok=self.tok)
        e2_id = res2["event_id"]

        # e2 should have e1 in its backward edges in mtxdb
        backward = get_event_edges_backward_batch(
            self.store._embedded_hamt_namespace, [e2_id]
        )
        self.assertIsNotNone(backward[e2_id])
        prev_ids = [p for p, _ in backward[e2_id] or []]
        self.assertIn(e1_id, prev_ids)

        # e1 should have e2 in its forward successors in mtxdb
        successors = self.get_success(self.store.get_successor_events(e1_id))
        self.assertIn(e2_id, successors)
