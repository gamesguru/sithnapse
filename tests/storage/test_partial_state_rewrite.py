#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 Element Creations Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/agpl-3.0.html>.

"""Regression test for the partial-state rewrite's coordination with the
SQL -> mtxdb `event_to_state_groups` migration.

`_update_state_for_partial_state_event_txn` must drop any not-yet-migrated
SQL row *before* it reads/writes mtxdb: that delete takes the same row lock
the migration batch holds while it copies the row, so the two interleave only
in the safe orders. Without it, a migration batch that probed the event
before the rewrite can then write its stale SQL placeholder over the resolved
mtxdb mapping, and subsequent reads use the placeholder state group forever.

The interleaving itself needs two database connections (the migration holds
its row lock across the mtxdb probe), which the single-connection SQLite test
pool can't express, so the probe is forced to miss instead -- that is exactly
the state the migration is in when its probe ran before the rewrite put the
resolved mapping.
"""

import atexit
import shutil
import struct
import tempfile
from typing import Any
from unittest import mock

from twisted.internet.testing import MemoryReactor

from synapse.events.snapshot import EventContext
from synapse.rest.client import room
from synapse.server import HomeServer
from synapse.storage.databases.main import events_bg_updates
from synapse.storage.databases.main.embedded_event_to_state_group import (
    _state_group_refcount_key,
    decrement_state_group_refcounts_batch,
    delete_event_to_state_group_batch,
    get_state_group_for_events_batch,
)
from synapse.util.clock import Clock

from tests.unittest import HomeserverTestCase

_MTXDB_DIR: str | None = None


class PartialStateRewriteMigrationRaceTests(HomeserverTestCase):
    user_id = "@red:server"
    servlets = [room.register_servlets]

    def make_homeserver(self, reactor: MemoryReactor, clock: Clock) -> HomeServer:
        return self.setup_test_homeserver("server")

    def prepare(
        self, reactor: MemoryReactor, clock: Clock, homeserver: HomeServer
    ) -> None:
        from synapse.synapse_rust import mtxdb_engine

        self.store = homeserver.get_datastores().main
        self.persist_store = homeserver.get_datastores().persist_events
        assert self.persist_store is not None

        # The engine is process-global and `open_client` only honours the
        # first path, so the directory must outlive every test in the process.
        global _MTXDB_DIR
        if _MTXDB_DIR is None:
            _MTXDB_DIR = tempfile.mkdtemp(prefix="test-partial-state-rewrite-")
            atexit.register(shutil.rmtree, _MTXDB_DIR, ignore_errors=True)
        mtxdb_engine.open_client(_MTXDB_DIR)
        # Isolate each test with its own namespace within the shared store.
        tmpdir = tempfile.mkdtemp(prefix="ns-", dir=_MTXDB_DIR)
        self.mtxdb = mtxdb_engine
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
        """Run the migration to completion through the real background
        updater (the handler is only registered when the engine is
        configured, which the test homeserver doesn't do)."""
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

    def test_rewrite_survives_migration_that_probed_before_it(self) -> None:
        room_id = self.helper.create_room_as(self.user_id)
        event_id = self.helper.send(room_id, body="resolve me")["event_id"]
        real_group = self.get_success(self.store._get_state_group_for_event(event_id))
        assert real_group is not None

        # Reshape the event as a pre-cutover partial-state event awaiting
        # resolution: its mapping lives only in SQL, under a placeholder group.
        placeholder = real_group + 100000
        delete_event_to_state_group_batch(self.engine, self.namespace, [event_id])
        decrement_state_group_refcounts_batch(self.engine, self.namespace, [real_group])
        self._insert_sql_mapping(event_id, placeholder)

        # The rewrite deletes the event's `partial_state_events` row, which
        # references a partial-state room.
        self.get_success(
            self.store.store_partial_state_room(
                room_id=room_id,
                servers={"remote.example.org"},
                device_lists_stream_id=0,
                joined_via="remote.example.org",
            )
        )
        self.get_success(
            self.store.db_pool.simple_insert(
                table="partial_state_events",
                values={"room_id": room_id, "event_id": event_id},
                desc="test_partial_state_rewrite",
            )
        )

        event = self.get_success(self.store.get_event(event_id))
        context = EventContext.with_state(
            storage=self.hs.get_storage_controllers(),
            state_group=real_group,
            state_group_before_event=real_group,
            state_delta_due_to_event=None,
            partial_state=False,
            state_group_deltas={},
        )
        refcount_before = self._refcount(real_group)
        self.get_success(
            self.store.update_state_for_partial_state_event(event, context)
        )

        # The SQL row is gone before mtxdb is touched, so a migration batch
        # can neither hold its row lock across the rewrite nor select the row
        # afterwards.
        self.assertIsNone(self._sql_mapping(event_id))
        self.assertEqual(self._mtxdb_mapping(event_id), real_group)
        self.assertEqual(self._refcount(real_group), refcount_before + 1)
        self.assertEqual(self._refcount(placeholder), 0)

        # Now the migration batch whose probe ran *before* the rewrite (forced
        # here by a probe that always misses) must not resurrect the stale
        # placeholder group over the resolved mapping: with the SQL row
        # already deleted it has no row to copy.
        original_probe = events_bg_updates.get_state_group_for_events_batch

        def probe_before_rewrite(*args: Any, **kwargs: Any) -> Any:
            if kwargs.get("purpose") == "migration_probe":
                return {}
            return original_probe(*args, **kwargs)

        with mock.patch.object(
            events_bg_updates,
            "get_state_group_for_events_batch",
            side_effect=probe_before_rewrite,
        ):
            self._migrate()

        self.assertEqual(self._mtxdb_mapping(event_id), real_group)
        self.assertEqual(self._refcount(placeholder), 0)
        self.assertEqual(
            self.get_success(self.store._get_state_group_for_events([event_id])),
            {event_id: real_group},
        )
