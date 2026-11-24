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

"""Boot-time checks for the authoritative auth-edge backend.

mtxdb is authoritative whenever `embedded_db_engine` is configured. An
existing SQL database whose auth edges have not been imported must refuse to
start rather than silently read an incomplete graph.
"""

from twisted.internet.testing import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.storage.database import LoggingTransaction
from synapse.storage.databases.main.embedded_event_auth_chains import (
    AuthChainBackendMismatchError,
    check_backend,
    get_embedded_auth_edges_batch,
    get_or_create_short_ids,
    migrate_auth_chain_edges,
    resolve_short_ids_to_event_ids,
)
from synapse.util.clock import Clock

from tests.unittest import HomeserverTestCase, skip_unless
from tests.utils import EMBEDDED_DB_ENGINE


class AuthChainBackendCheckTestCase(HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        room.register_servlets,
        login.register_servlets,
    ]

    def prepare(
        self, reactor: MemoryReactor, clock: Clock, homeserver: HomeServer
    ) -> None:
        self.store = homeserver.get_datastores().main

    def _set_marker(self, backend: str) -> None:
        def _txn(txn: LoggingTransaction) -> None:
            txn.execute("DELETE FROM auth_chain_backend")
            txn.execute(
                "INSERT INTO auth_chain_backend (lock, backend) VALUES ('X', ?)",
                (backend,),
            )

        self.get_success(
            self.store.db_pool.runInteraction("set_auth_chain_backend", _txn)
        )

    def _read_marker(self) -> str:
        return self.get_success(
            self.store.db_pool.simple_select_one_onecol(
                table="auth_chain_backend",
                keyvalues={"lock": "X"},
                retcol="backend",
                desc="read_auth_chain_backend",
            )
        )

    def _create_event(self) -> None:
        user = self.register_user("backend_user", "pass")
        token = self.login("backend_user", "pass")
        self.helper.create_room_as(user, tok=token)

    def test_empty_database_adopts_configured_mtxdb(self) -> None:
        """A fresh database with no events can switch to mtxdb on boot."""
        self.hs.config.database.embedded_db_engine = "mtxdb"
        self._set_marker("sql")

        self.get_success(check_backend(self.hs))

        self.assertEqual(self._read_marker(), "mtxdb")

    def test_existing_sql_graph_refuses_mtxdb_boot(self) -> None:
        """Events with a SQL marker must not boot as mtxdb: the import is
        offline and explicit."""
        self._create_event()
        self.hs.config.database.embedded_db_engine = "mtxdb"
        self._set_marker("sql")

        self.get_failure(check_backend(self.hs), AuthChainBackendMismatchError)

        # The marker is not silently flipped by a failed boot.
        self.assertEqual(self._read_marker(), "sql")

    def test_existing_mtxdb_graph_boots(self) -> None:
        self._create_event()
        self.hs.config.database.embedded_db_engine = "mtxdb"
        self._set_marker("mtxdb")

        self.get_success(check_backend(self.hs))

        self.assertEqual(self._read_marker(), "mtxdb")

    def test_sql_config_refuses_mtxdb_marker(self) -> None:
        """Switching the config back to SQL after the cutover is also a
        mismatch, not a silent fallback."""
        self._create_event()
        self.hs.config.database.embedded_db_engine = None
        self._set_marker("mtxdb")

        self.get_failure(check_backend(self.hs), AuthChainBackendMismatchError)

    def test_empty_database_adopts_configured_sql(self) -> None:
        self.hs.config.database.embedded_db_engine = None
        self._set_marker("mtxdb")

        self.get_success(check_backend(self.hs))

        self.assertEqual(self._read_marker(), "sql")

    @skip_unless(bool(EMBEDDED_DB_ENGINE), "requires embedded DB engine")
    def test_migration_rebuilds_mtxdb_and_flips_marker(self) -> None:
        """The offline import reads SQL, rebuilds mtxdb, verifies both edge
        directions, and only then flips the marker."""
        user = self.register_user("migrate_user", "pass")
        token = self.login("migrate_user", "pass")
        room_id = self.helper.create_room_as(user, tok=token)

        state_ids = self.get_success(
            self.hs.get_storage_controllers().state.get_current_state_ids(room_id)
        )
        expected: dict[str, set[str]] = {}
        rows: list[tuple[str, str, str]] = []
        for event_id in set(state_ids.values()):
            event = self.get_success(self.store.get_event(event_id))
            auth_ids = set(event.auth_event_ids())
            expected[event_id] = auth_ids
            rows.extend((event_id, room_id, auth_id) for auth_id in auth_ids)

        # Simulate the legacy SQL graph and a database that has not switched.
        self.get_success(
            self.store.db_pool.simple_insert_many(
                table="event_auth",
                keys=("event_id", "room_id", "auth_id"),
                values=rows,
                desc="seed_legacy_event_auth",
            )
        )
        self._set_marker("sql")

        result = self.get_success(migrate_auth_chain_edges(self.hs, batch_size=2))
        self.assertEqual(result["rooms"], 1)
        self.assertEqual(self._read_marker(), "mtxdb")

        engine = self.hs.config.database.embedded_db_engine
        event_ids = list(expected)
        short_ids = get_or_create_short_ids(engine, room_id, event_ids)
        edges = get_embedded_auth_edges_batch(engine, room_id, short_ids)
        for event_id, short_id in zip(event_ids, short_ids):
            self.assertIsNotNone(edges[short_id])
            resolved = resolve_short_ids_to_event_ids(
                engine, room_id, list(edges[short_id] or [])
            )
            self.assertEqual(set(resolved), expected[event_id])
