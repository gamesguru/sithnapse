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

from twisted.web.resource import Resource

from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.rest.synapse.client import build_synapse_client_resource_tree

from tests import unittest


class PublicStatsTests(unittest.HomeserverTestCase):
    servlets = [
        admin.register_servlets_for_client_rest_resource,
        login.register_servlets,
        room.register_servlets,
    ]

    def create_resource_dict(self) -> dict[str, Resource]:
        base = super().create_resource_dict()
        base.update(build_synapse_client_resource_tree(self.hs))
        return base

    def test_disabled_by_default(self) -> None:
        channel = self.make_request(
            "GET", "/_synapse/client/public_stats", shorthand=False
        )
        self.assertEqual(channel.code, 404)

    @unittest.override_config({"stats": {"public_stats_enabled": True}})
    def test_returns_only_aggregate_counts_without_auth(self) -> None:
        user = self.register_user("user", "password")
        tok = self.login("user", "password")
        self.helper.create_room_as(user, tok=tok)

        channel = self.make_request(
            "GET", "/_synapse/client/public_stats", shorthand=False
        )

        self.assertEqual(channel.code, 200, channel.result)
        self.assertEqual(
            set(channel.json_body),
            {
                "total_users",
                "total_rooms",
                "total_destinations",
                "total_known_servers",
                "database",
                "memory",
            },
        )
        self.assertEqual(channel.json_body["total_users"], 1)
        self.assertEqual(channel.json_body["total_rooms"], 1)
        self.assertIsInstance(channel.json_body["total_destinations"], int)
        self.assertIsInstance(channel.json_body["total_known_servers"], int)

        database = channel.json_body["database"]
        self.assertEqual(
            set(database),
            {
                "events",
                "state_events",
                "local_users",
                "joined_members",
                "local_joined_members",
                "postgres_bytes",
                "mtxdb_bytes",
            },
        )
        self.assertIsInstance(database["events"], int)
        self.assertIsInstance(database["state_events"], int)
        self.assertEqual(database["local_users"], 1)
        self.assertEqual(database["joined_members"], 1)
        self.assertEqual(database["local_joined_members"], 1)
        # PostgreSQL is only reported when it is the main database engine.
        self.assertTrue(
            database["postgres_bytes"] is None
            or isinstance(database["postgres_bytes"], int)
        )

        self.assertIsInstance(channel.json_body["memory"]["rss_bytes"], int)
