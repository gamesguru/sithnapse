#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
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

"""Tests for the SQL -> mtxdb `event_to_state_groups` migration and its
interaction with purge. The migration deletes each SQL row as it copies it, so
every case checks both stores."""

import shutil
import struct
import tempfile

from twisted.internet.testing import MemoryReactor

from synapse.rest.client import room
from synapse.server import HomeServer
from synapse.storage.databases.main.embedded_event_to_state_group import (
    _state_group_refcount_key,
    get_state_group_for_events_batch,
    increment_state_group_refcounts_batch,
    put_event_to_state_group_batch,
)
from synapse.util.clock import Clock

from tests.unittest import HomeserverTestCase


class EventToStateGroupMigrationTests(HomeserverTestCase):
    user_id = "@red:server"
    servlets = [room.register_servlets]

    def make_homeserver(self, reactor: MemoryReactor, clock: Clock) -> HomeServer:
        return self.setup_test_homeserver("server")

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        from synapse.synapse_rust import mtxdb_engine

        self.store = hs.get_datastores().main
        self.persist_store = hs.get_datastores().persist_events
        assert self.persist_store is not None
        self.controllers = hs.get_storage_controllers()

        tmpdir = tempfile.mkdtemp(prefix="test-state-group-migration-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        mtxdb_engine.open_client(tmpdir)
        self.mtxdb = mtxdb_engine
        # The engine is process-wide; a unique namespace isolates this test.
        self.engine = "mtxdb"
        self.namespace = tmpdir
        for store in (self.store, self.persist_store):
            store._embedded_event_json_enabled = True
            store._embedded_db_engine = self.engine
            store._embedded_db_namespace = self.namespace

    def _refcount(self, state_group: int) -> int:
        key = _state_group_refcount_key(self.namespace, state_group)
        found = self.mtxdb.batch_get([key])
        if not found:
            return 0
        return struct.unpack(">q", bytes(found[0][1]))[0]

    def _mtxdb_mapping(self, event_id: str) -> int | None:
        return get_state_group_for_events_batch(
            self.engine, self.namespace, [event_id]
        ).get(event_id)

    def _sql_mapping(self, event_id: str) -> int | None:
        rows = self.get_success(
            self.store.db_pool.simple_select_list(
                table="event_to_state_groups",
                keyvalues={"event_id": event_id},
                retcols=("state_group",),
            )
        )
        return rows[0][0] if rows else None

    def _insert_sql_mapping(self, event_id: str, state_group: int) -> None:
        self.get_success(
            self.store.db_pool.simple_upsert(
                table="event_to_state_groups",
                keyvalues={"event_id": event_id},
                values={"state_group": state_group},
            )
        )

    def _migrate(self) -> None:
        """Run the migration to completion through the real background updater
        (the handler is only registered when the engine is configured, which
        the test homeserver doesn't do)."""
        updates = self.store.db_pool.updates
        name = self.store.EMBEDDED_EVENT_TO_STATE_GROUP_MIGRATION_UPDATE_NAME
        updates.register_background_update_handler(
            name, self.store._background_migrate_event_to_state_groups_to_embedded
        )
        self.get_success(
            self.store.db_pool.simple_upsert(
                table="background_updates",
                keyvalues={"update_name": name},
                values={},
                insertion_values={"progress_json": "{}"},
            )
        )
        updates._all_done = False
        while not self.get_success(updates.has_completed_background_update(name)):
            self.get_success(updates.do_next_background_update(False))

    def test_migration_copies_sql_only_mapping_and_counts_it(self) -> None:
        self._insert_sql_mapping("$sqlonly1", 9001)
        self._insert_sql_mapping("$sqlonly2", 9001)
        self._insert_sql_mapping("$sqlonly3", 9002)

        self._migrate()

        self.assertEqual(self._mtxdb_mapping("$sqlonly1"), 9001)
        self.assertEqual(self._mtxdb_mapping("$sqlonly2"), 9001)
        self.assertEqual(self._mtxdb_mapping("$sqlonly3"), 9002)
        self.assertEqual(self._refcount(9001), 2)
        self.assertEqual(self._refcount(9002), 1)
        # mtxdb is now the only copy: migrated SQL rows are deleted.
        for event_id in ("$sqlonly1", "$sqlonly2", "$sqlonly3"):
            self.assertIsNone(self._sql_mapping(event_id), event_id)

    def test_migration_does_not_overwrite_existing_mtxdb_mapping(self) -> None:
        # mtxdb has a newer mapping (e.g. a partial-state rewrite after the
        # engine was turned on) than the retained SQL row.
        self._insert_sql_mapping("$rewritten", 9101)
        put_event_to_state_group_batch(
            self.engine, self.namespace, [("$rewritten", 9102)]
        )
        increment_state_group_refcounts_batch(self.engine, self.namespace, [9102])

        self._migrate()

        self.assertEqual(self._mtxdb_mapping("$rewritten"), 9102)
        # The stale SQL group is never counted, and the new one isn't
        # counted a second time.
        self.assertEqual(self._refcount(9101), 0)
        self.assertEqual(self._refcount(9102), 1)
        # The stale SQL row is still dropped, so it can't be read back later.
        self.assertIsNone(self._sql_mapping("$rewritten"))

    def test_migration_replay_does_not_double_count(self) -> None:
        self._insert_sql_mapping("$replayed", 9201)

        self._migrate()
        # A crash between the writes and the progress update replays the batch.
        self._migrate()

        self.assertEqual(self._mtxdb_mapping("$replayed"), 9201)
        self.assertEqual(self._refcount(9201), 1)

    def test_sql_fallback_stops_once_sql_rows_are_migrated(self) -> None:
        self._insert_sql_mapping("$gone", 9401)
        self.assertTrue(self.get_success(self.store._embedded_sql_state_groups_remain()))

        self._migrate()

        self.assertFalse(
            self.get_success(self.store._embedded_sql_state_groups_remain())
        )
        # Reads are served from mtxdb alone from here on.
        self.assertEqual(
            self.get_success(self.store._get_state_group_for_events(["$gone"])),
            {"$gone": 9401},
        )

    def test_state_reads_fall_back_to_sql_before_migration(self) -> None:
        self._insert_sql_mapping("$unmigrated", 9301)

        res = self.get_success(
            self.store._get_state_group_for_events(["$unmigrated"])
        )
        self.assertEqual(res, {"$unmigrated": 9301})
        # Only SQL-referenced, so reference lookups must still see it.
        self.assertEqual(
            self.get_success(self.store.get_referenced_state_groups([9301])), {9301}
        )

    def _prepare_purgeable_room(self) -> tuple[str, str, str, str]:
        """Create a room, and return a room ID and a purge token, with `first`
        SQL-only (as before migration) and `second` in both stores."""
        room_id = self.helper.create_room_as(self.user_id)
        first = self.helper.send(room_id, body="first")["event_id"]
        second = self.helper.send(room_id, body="second")["event_id"]
        last = self.helper.send(room_id, body="last")["event_id"]

        first_group = self._mtxdb_mapping(first)
        second_group = self._mtxdb_mapping(second)
        assert first_group is not None and second_group is not None

        # `first`: back into the pre-migration shape (SQL only, uncounted).
        from synapse.storage.databases.main.embedded_event_to_state_group import (
            decrement_state_group_refcounts_batch,
            delete_event_to_state_group_batch,
        )

        delete_event_to_state_group_batch(self.engine, self.namespace, [first])
        decrement_state_group_refcounts_batch(
            self.engine, self.namespace, [first_group]
        )
        self._insert_sql_mapping(first, first_group)
        # `second`: migrated (retained SQL row plus the mtxdb mapping).
        self._insert_sql_mapping(second, second_group)

        token = self.get_success(self.store.get_topological_token_for_event(last))
        token_str = self.get_success(token.to_string(self.store))
        return room_id, first, second, token_str

    def _survivors(self, state_group: int) -> int:
        """How many events still map to `state_group` in mtxdb."""
        rows = self.get_success(
            self.store.db_pool.simple_select_list(
                table="events", keyvalues={}, retcols=("event_id",)
            )
        )
        ids = [event_id for (event_id,) in rows]
        mapping = get_state_group_for_events_batch(self.engine, self.namespace, ids)
        return sum(1 for group in mapping.values() if group == state_group)

    def test_purge_removes_sql_and_mtxdb_mappings(self) -> None:
        room_id, first, second, token_str = self._prepare_purgeable_room()
        second_group = self._mtxdb_mapping(second)
        assert second_group is not None

        self.get_success(
            self.controllers.purge_events.purge_history(room_id, token_str, True)
        )

        for event_id in (first, second):
            self.assertIsNone(self._sql_mapping(event_id), event_id)
            self.assertIsNone(self._mtxdb_mapping(event_id), event_id)
        # Only mtxdb-counted mappings are decremented (the SQL-only one never
        # was counted): nothing is left in the group but surviving `last`.
        self.assertEqual(self._refcount(second_group), self._survivors(second_group))

    def test_migration_after_purge_cannot_resurrect_mapping(self) -> None:
        """The race outcome the row locks rule out: a migration batch that
        runs after purge must not find (or re-insert) a purged mapping."""
        room_id, first, second, token_str = self._prepare_purgeable_room()
        first_group = self._sql_mapping(first)
        assert first_group is not None

        self.get_success(
            self.controllers.purge_events.purge_history(room_id, token_str, True)
        )
        self._migrate()

        self.assertIsNone(self._mtxdb_mapping(first))
        self.assertIsNone(self._sql_mapping(first))
        self.assertEqual(self._refcount(first_group), self._survivors(first_group))

    def test_purge_after_migration_removes_migrated_mapping(self) -> None:
        """The other lock ordering: purge waits for the migration batch, then
        sees and removes what it wrote."""
        room_id, first, second, token_str = self._prepare_purgeable_room()
        first_group = self._sql_mapping(first)
        assert first_group is not None

        self._migrate()
        self.assertEqual(self._mtxdb_mapping(first), first_group)

        self.get_success(
            self.controllers.purge_events.purge_history(room_id, token_str, True)
        )

        self.assertIsNone(self._mtxdb_mapping(first))
        self.assertIsNone(self._sql_mapping(first))
        self.assertEqual(self._refcount(first_group), self._survivors(first_group))
