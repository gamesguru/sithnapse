#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2018-2021 The Matrix.org Foundation C.I.C.
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

import json
import logging
from typing import cast
from unittest.mock import patch

from immutabledict import immutabledict

from twisted.internet.testing import MemoryReactor

from synapse.api.constants import EventTypes, Membership
from synapse.api.room_versions import RoomVersions
from synapse.events import EventBase
from synapse.events.snapshot import UnpersistedEventContext
from synapse.server import HomeServer
from synapse.types import JsonDict, RoomID, StateMap, UserID, create_requester
from synapse.types.state import StateFilter
from synapse.util.clock import Clock
from synapse.util.stringutils import random_string

from tests.unittest import HomeserverTestCase

logger = logging.getLogger(__name__)


class StateStoreTestCase(HomeserverTestCase):
    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main
        self.storage = hs.get_storage_controllers()
        self.state_datastore = self.storage.state.stores.state
        self.event_builder_factory = hs.get_event_builder_factory()
        self.event_creation_handler = hs.get_event_creation_handler()

        self.u_alice = UserID.from_string("@alice:test")
        self.u_bob = UserID.from_string("@bob:test")

        self.room = RoomID.from_string("!abc123:test")

        self.get_success(
            self.store.store_room(
                self.room.to_string(),
                room_creator_user_id=self.u_alice.to_string(),
                is_public=True,
                room_version=RoomVersions.V1,
            )
        )

    def _force_sql_only_hamt(self) -> None:
        """Some tests assert pure-SQL HAMT behaviour specifically and must
        stay deterministic regardless of SYNAPSE_TEST_EMBEDDED_HAMT_ENGINE
        (the trial-mdbx CI job runs the *whole* suite through the embedded
        engine by default -- see tests/utils.py's default_config -- so a
        test that specifically wants SQL must force it off locally rather
        than assume it's already off).
        """
        self.state_datastore.embedded_hamt_engine = None

    def inject_state_event(
        self, room: RoomID, sender: UserID, typ: str, state_key: str, content: JsonDict
    ) -> EventBase:
        builder = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": typ,
                "sender": sender.to_string(),
                "state_key": state_key,
                "room_id": room.to_string(),
                "content": content,
            },
        )

        event, unpersisted_context = self.get_success(
            self.event_creation_handler.create_new_client_event(builder)
        )

        context = self.get_success(unpersisted_context.persist(event))

        assert self.storage.persistence is not None
        self.get_success(self.storage.persistence.persist_event(event, context))

        return event

    def assertStateMapEqual(
        self, s1: StateMap[EventBase], s2: StateMap[EventBase]
    ) -> None:
        for t in s1:
            # just compare event IDs for simplicity
            self.assertEqual(s1[t].event_id, s2[t].event_id)
        self.assertEqual(len(s1), len(s2))

    def test_get_state_groups_ids(self) -> None:
        e1 = self.inject_state_event(self.room, self.u_alice, EventTypes.Create, "", {})
        e2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )

        state_group_map = self.get_success(
            self.storage.state.get_state_groups_ids(
                self.room.to_string(), [e2.event_id]
            )
        )
        self.assertEqual(len(state_group_map), 1)
        state_map = list(state_group_map.values())[0]
        self.assertDictEqual(
            state_map,
            {(EventTypes.Create, ""): e1.event_id, (EventTypes.Name, ""): e2.event_id},
        )

    def test_state_group_reads_use_hamt_by_default(self) -> None:
        e1 = self.inject_state_event(self.room, self.u_alice, EventTypes.Create, "", {})
        e2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )

        state_group = self.get_success(
            self.store._get_state_group_for_event(e2.event_id)
        )
        assert state_group is not None

        self.get_success(
            self.store.db_pool.simple_delete(
                table="state_groups_state",
                keyvalues={"state_group": state_group},
                desc="test_state_group_reads_use_hamt_by_default",
            )
        )

        # The active refactor path materializes state from HAMT first. Removing
        # the legacy SQL snapshot rows should not affect state-group reads.
        state_group_map = self.get_success(
            self.storage.state.stores.state._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            )
        )

        self.assertDictEqual(
            state_group_map[state_group],
            {(EventTypes.Create, ""): e1.event_id, (EventTypes.Name, ""): e2.event_id},
        )

    def test_state_group_reads_via_embedded_mdbx_engine(self) -> None:
        """With `embedded_hamt_engine` configured before these events are
        persisted, `_store_state_hamt_nodes_txn` writes exclusively to mdbx
        (not SQL -- see `_persist_state_hamt_txn`), and reads resolve
        entirely through `_materialize_state_hamts_from_embedded_txn` /
        `_lookup_state_hamts_from_embedded_txn` against a real mdbx
        database.
        """
        import shutil
        import tempfile

        from synapse.synapse_rust import mdbx_engine

        tmpdir = tempfile.mkdtemp(prefix="test-embedded-mdbx-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        mdbx_engine.open_client(tmpdir)
        self.state_datastore.embedded_hamt_engine = "mdbx"
        self.state_datastore.embedded_hamt_path = tmpdir

        e1 = self.inject_state_event(self.room, self.u_alice, EventTypes.Create, "", {})
        e2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )
        e3 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Topic, "", {"topic": "test topic"}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(e3.event_id)
        )
        assert state_group is not None

        # Full materialize via the embedded engine.
        full_state = self.get_success(
            self.state_datastore._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            )
        )
        self.assertDictEqual(
            full_state[state_group],
            {
                (EventTypes.Create, ""): e1.event_id,
                (EventTypes.Name, ""): e2.event_id,
                (EventTypes.Topic, ""): e3.event_id,
            },
        )

        # Selective (exact-keys) lookup via the embedded engine.
        selective_state = self.get_success(
            self.state_datastore._get_state_groups_from_groups(
                [state_group],
                StateFilter.from_types([(EventTypes.Name, "")]),
            )
        )
        self.assertDictEqual(
            selective_state[state_group],
            {(EventTypes.Name, ""): e2.event_id},
        )

    def test_embedded_engine_writes_are_exclusive_not_dual(self) -> None:
        """Once `embedded_hamt_engine` is configured, new state groups are
        written to mdbx ONLY -- `state_hamt_roots`/`state_hamt_nodes` SQL
        rows are not also inserted (see `_persist_state_hamt_txn`).
        """
        import shutil
        import tempfile

        from synapse.synapse_rust import mdbx_engine

        tmpdir = tempfile.mkdtemp(prefix="test-exclusive-write-mdbx-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        mdbx_engine.open_client(tmpdir)
        self.state_datastore.embedded_hamt_engine = "mdbx"
        self.state_datastore.embedded_hamt_path = tmpdir

        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None

        sql_root = self.get_success(
            self.store.db_pool.simple_select_one(
                table="state_hamt_roots",
                keyvalues={"state_group": state_group},
                retcols=("state_group",),
                allow_none=True,
                desc="test_embedded_engine_writes_are_exclusive_not_dual",
            )
        )
        self.assertIsNone(
            sql_root,
            "state_hamt_roots got a row for a group persisted with the "
            "embedded engine configured -- writes should be exclusive, not "
            "dual",
        )

        # But it really is in mdbx -- not just "nowhere".
        full_state = self.get_success(
            self.state_datastore._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            )
        )
        self.assertDictEqual(
            full_state[state_group], {(EventTypes.Create, ""): event.event_id}
        )

    def test_embedded_hamt_migration_copies_existing_sql_data(self) -> None:
        """A state group written before `embedded_hamt_engine` was turned on
        stays SQL-only until `_background_migrate_state_hamt_to_embedded`
        runs; after it completes, the group is readable via mdbx with SQL
        deleted out from under it -- proving the data actually moved, not
        just that the SQL fallback happened to still work.
        """
        import shutil
        import tempfile

        from synapse.synapse_rust import mdbx_engine

        # Persist with no embedded engine configured -- goes to SQL only.
        self._force_sql_only_hamt()
        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None

        sql_root_before = self.get_success(
            self.store.db_pool.simple_select_one_onecol(
                table="state_hamt_roots",
                keyvalues={"state_group": state_group},
                retcol="state_group",
                desc="test_embedded_hamt_migration.check_sql_before",
            )
        )
        self.assertEqual(sql_root_before, state_group)

        # Now turn on the embedded engine and run the migration. In real
        # deployments `embedded_hamt_engine` is set before the store is
        # constructed, so __init__ registers the handler for
        # do_next_background_update to dispatch to; this test flips the
        # config after construction (same pattern the other embedded-engine
        # tests here use), so no handler was ever registered for this store
        # instance. Exercise _enqueue_embedded_hamt_migration_if_needed
        # (real production code, still worth covering), but do not start its
        # poller: it would race this test's direct handler invocation and
        # repeatedly fail to dispatch the unregistered handler. Drive the
        # handler directly instead.
        tmpdir = tempfile.mkdtemp(prefix="test-hamt-migration-mdbx-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        mdbx_engine.open_client(tmpdir)
        self.state_datastore.embedded_hamt_engine = "mdbx"
        self.state_datastore.embedded_hamt_path = tmpdir

        with patch.object(
            self.store.db_pool.updates, "start_doing_background_updates"
        ) as start_background_updates:
            self.get_success(
                self.state_datastore._enqueue_embedded_hamt_migration_if_needed()
            )
        start_background_updates.assert_called_once_with()

        # `_background_migrate_state_hamt_to_embedded` ends the update after
        # its final batch, which normally follows the poller selecting it.
        # Model that selection explicitly rather than relying on a concurrently
        # running poller to set this private dispatcher state.
        self.store.db_pool.updates._current_background_update = (
            self.state_datastore.EMBEDDED_HAMT_MIGRATION_UPDATE_NAME
        )
        progress: dict = {}
        while True:
            moved = self.get_success(
                self.state_datastore._background_migrate_state_hamt_to_embedded(
                    progress, batch_size=500
                )
            )
            if moved == 0:
                break
            progress = {"last_state_group": state_group}

        # Delete the SQL rows entirely -- if the read below still works,
        # the data really moved into mdbx rather than the read just still
        # falling back to SQL.
        self.get_success(
            self.store.db_pool.simple_delete(
                table="state_hamt_roots",
                keyvalues={"state_group": state_group},
                desc="test_embedded_hamt_migration.delete_sql_root",
            )
        )

        full_state = self.get_success(
            self.state_datastore._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            )
        )
        self.assertDictEqual(
            full_state[state_group], {(EventTypes.Create, ""): event.event_id}
        )

    def test_embedded_engine_root_lookup_does_not_need_sql(self) -> None:
        """`_store_state_hamt_root_embedded_txn` mirrors the HAMT root
        record into the embedded engine itself (under the `hamt:root:...`
        key), not just SQL's `state_hamt_roots`. Deleting that SQL row
        entirely and still reading correctly proves
        `_fetch_hamt_roots_for_embedded_txn` actually took the embedded-
        engine fast path rather than silently falling back to SQL.
        """
        import shutil
        import tempfile

        from synapse.synapse_rust import mdbx_engine

        tmpdir = tempfile.mkdtemp(prefix="test-embedded-root-mdbx-")
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        mdbx_engine.open_client(tmpdir)
        self.state_datastore.embedded_hamt_engine = "mdbx"
        self.state_datastore.embedded_hamt_path = tmpdir

        e1 = self.inject_state_event(self.room, self.u_alice, EventTypes.Create, "", {})
        e2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(e2.event_id)
        )
        assert state_group is not None

        self.get_success(
            self.store.db_pool.simple_delete(
                table="state_hamt_roots",
                keyvalues={"state_group": state_group},
                desc="test_embedded_engine_root_lookup_does_not_need_sql",
            )
        )

        state_group_map = self.get_success(
            self.state_datastore._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            )
        )
        self.assertDictEqual(
            state_group_map[state_group],
            {(EventTypes.Create, ""): e1.event_id, (EventTypes.Name, ""): e2.event_id},
        )

    def test_exact_state_filter_uses_selective_hamt_lookup(self) -> None:
        create = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        name = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )
        topic = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Topic, "", {"topic": "test topic"}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(topic.event_id)
        )
        assert state_group is not None

        with patch.object(
            self.state_datastore,
            "_materialize_state_hamt_from_postgres_txn",
            side_effect=AssertionError("exact filters must not materialize full state"),
        ):
            result = self.get_success(
                self.store.db_pool.runInteraction(
                    "test_exact_state_filter_uses_selective_hamt_lookup",
                    self.state_datastore._get_state_groups_from_groups_txn,
                    [state_group],
                    StateFilter.from_types(
                        [(EventTypes.Name, ""), (EventTypes.Topic, "")]
                    ),
                )
            )

        self.assertDictEqual(
            result[state_group],
            {
                (EventTypes.Name, ""): name.event_id,
                (EventTypes.Topic, ""): topic.event_id,
            },
        )

        # An empty key set excludes that type while ``include_others`` still
        # selects every non-enumerated type, alongside explicitly requested
        # entries. This must use full materialization rather than accidentally
        # treating the empty set as an exact lookup.
        result = self.get_success(
            self.store.db_pool.runInteraction(
                "test_exact_state_filter_uses_selective_hamt_lookup_include_others",
                self.state_datastore._get_state_groups_from_groups_txn,
                [state_group],
                StateFilter.freeze(
                    {EventTypes.Name: set(), EventTypes.Create: {""}},
                    include_others=True,
                ),
            )
        )
        self.assertDictEqual(
            result[state_group],
            {
                (EventTypes.Create, ""): create.event_id,
                (EventTypes.Topic, ""): topic.event_id,
            },
        )

    def test_state_group_hamt_corruption_does_not_fallback_to_sql(self) -> None:
        self._force_sql_only_hamt()
        event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_group = self.get_success(
            self.store._get_state_group_for_event(event.event_id)
        )
        assert state_group is not None

        # Insert a corrupt replacement node, then repoint the root at it. This
        # simulates corrupt node content rather than a missing node row.
        garbage_structural_hash = random_string(32).encode("ascii")
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_hamt_nodes",
                values={
                    # Postgres rejects raw `bytes` (c.f. matrix-org/synapse#6186);
                    # wrap in `bytearray`, matching the rest of this codebase.
                    "structural_hash": bytearray(garbage_structural_hash),
                    "node_bytes": bytearray(b"not a valid persisted HAMT node"),
                },
                desc="test_state_group_hamt_corruption.insert_garbage_node",
            )
        )
        self.get_success(
            self.store.db_pool.simple_update_one(
                table="state_hamt_roots",
                keyvalues={"state_group": state_group},
                updatevalues={
                    "root_structural_hash": bytearray(garbage_structural_hash)
                },
                desc="test_state_group_hamt_corruption.repoint_root",
            )
        )

        # If a HAMT root exists, missing/corrupt nodes are data corruption.
        # Do not hide that by falling back to the legacy SQL snapshot.
        failure = self.get_failure(
            self.storage.state.stores.state._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            ),
            RuntimeError,
        )
        self.assertIn(
            "Failed to decode persisted HAMT node",
            str(failure.value),
        )

    def test_multi_group_exact_filter_under_pure_sql_shares_node_fetches(self) -> None:
        """A selective (exact_keys) lookup across several state groups must
        go through _lookup_state_hamt_from_postgres_many_txn, not the
        singular per-group SQL loop, and must return correct per-group
        results without mocking anything -- this exercises the real SQL HAMT
        node-sharing path end to end."""
        self._force_sql_only_hamt()
        event1 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        sg1 = self.get_success(self.store._get_state_group_for_event(event1.event_id))
        event2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "room_name"}
        )
        sg2 = self.get_success(self.store._get_state_group_for_event(event2.event_id))
        assert sg1 is not None and sg2 is not None

        state_filter = StateFilter.from_types([(EventTypes.Name, "")])

        with (
            patch.object(
                self.state_datastore,
                "_lookup_state_hamt_from_postgres_many_txn",
                wraps=self.state_datastore._lookup_state_hamt_from_postgres_many_txn,
            ) as mock_many,
            patch.object(
                self.state_datastore,
                "_lookup_state_hamt_from_postgres_txn",
                wraps=self.state_datastore._lookup_state_hamt_from_postgres_txn,
            ) as mock_singular,
        ):
            res = self.get_success(
                self.storage.state.stores.state._get_state_groups_from_groups(
                    [sg1, sg2], state_filter
                )
            )

        # The batched path was used exactly once for both groups together;
        # the per-group singular path was never reached.
        mock_many.assert_called_once()
        mock_singular.assert_not_called()

        self.assertEqual(
            res,
            {
                sg1: {},
                sg2: {(EventTypes.Name, ""): event2.event_id},
            },
        )

    def test_multi_room_exact_filter_under_pure_sql_does_not_reject_batch(self) -> None:
        """Regression test: `_lookup_state_hamt_from_postgres_many_txn` fetches
        one shared node pool for every group in the batch and hands the whole
        pool to `state_hamt.lookup_state_entries` for each group individually
        -- so a batch spanning two different *rooms* legitimately contains
        nodes that don't belong to the room currently being resolved.
        Real-world symptom (reproduced on CI): `RuntimeError: Failed to decode
        persisted HAMT node: persisted node contents do not match expected
        structural hash`, surfacing as a 500 on federation invites into an
        empty room and on search across an upgraded room + its predecessor --
        both cases resolve state for two rooms in the same batched call.
        """
        self._force_sql_only_hamt()
        event1 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        sg1 = self.get_success(self.store._get_state_group_for_event(event1.event_id))

        room2 = RoomID.from_string("!other-room:test")
        self.get_success(
            self.store.store_room(
                room2.to_string(),
                room_creator_user_id=self.u_alice.to_string(),
                is_public=True,
                room_version=RoomVersions.V1,
            )
        )
        event2 = self.inject_state_event(room2, self.u_alice, EventTypes.Create, "", {})
        sg2 = self.get_success(self.store._get_state_group_for_event(event2.event_id))
        assert sg1 is not None and sg2 is not None

        state_filter = StateFilter.from_types([(EventTypes.Create, "")])

        res = self.get_success(
            self.storage.state.stores.state._get_state_groups_from_groups(
                [sg1, sg2], state_filter
            )
        )

        self.assertEqual(
            res,
            {
                sg1: {(EventTypes.Create, ""): event1.event_id},
                sg2: {(EventTypes.Create, ""): event2.event_id},
            },
        )

    def test_existing_unresolved_group_raises_in_sql_mode(self) -> None:
        """Verify that an existing state group with no HAMT root raises
        RuntimeError via the SQL/legacy-HAMT path, instead of silently
        falling through to the legacy `state_groups_state` walk and
        returning an empty state map."""
        state_group = 999997
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_groups",
                values={
                    "id": state_group,
                    "room_id": self.room.to_string(),
                    "event_id": "$fake-sql:test",
                },
                desc="test_unresolved_sql.insert_sg",
            )
        )
        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_group_edges",
                values={"state_group": state_group, "prev_state_group": 1},
                desc="test_unresolved_sql.insert_edge",
            )
        )

        self.get_failure(
            self.state_datastore._get_state_groups_from_groups(
                [state_group], StateFilter.all()
            ),
            RuntimeError,
        )

    def test_nonexistent_group_returns_empty_dict(self) -> None:
        """Verify that a nonexistent state group (not in SQL) returns {} without raising."""
        nonexistent_group = 9999991

        res = self.get_success(
            self.state_datastore._get_state_groups_from_groups(
                [nonexistent_group], StateFilter.all()
            )
        )
        self.assertEqual(res[nonexistent_group], {})

    def test_get_state_groups(self) -> None:
        e1 = self.inject_state_event(self.room, self.u_alice, EventTypes.Create, "", {})
        e2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )

        state_group_map = self.get_success(
            self.storage.state.get_state_groups(self.room.to_string(), [e2.event_id])
        )
        self.assertEqual(len(state_group_map), 1)
        state_list = list(state_group_map.values())[0]

        self.assertEqual({ev.event_id for ev in state_list}, {e1.event_id, e2.event_id})

    def test_get_state_for_event(self) -> None:
        # this defaults to a linear DAG as each new injection defaults to whatever
        # forward extremities are currently in the DB for this room.
        e1 = self.inject_state_event(self.room, self.u_alice, EventTypes.Create, "", {})
        e2 = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Name, "", {"name": "test room"}
        )
        e3 = self.inject_state_event(
            self.room,
            self.u_alice,
            EventTypes.Member,
            self.u_alice.to_string(),
            {"membership": Membership.JOIN},
        )
        e4 = self.inject_state_event(
            self.room,
            self.u_bob,
            EventTypes.Member,
            self.u_bob.to_string(),
            {"membership": Membership.JOIN},
        )
        e5 = self.inject_state_event(
            self.room,
            self.u_bob,
            EventTypes.Member,
            self.u_bob.to_string(),
            {"membership": Membership.LEAVE},
        )

        # check we get the full state as of the final event
        state = self.get_success(self.storage.state.get_state_for_event(e5.event_id))

        self.assertIsNotNone(e4)

        self.assertStateMapEqual(
            {
                (e1.type, e1.state_key): e1,
                (e2.type, e2.state_key): e2,
                (e3.type, e3.state_key): e3,
                # e4 is overwritten by e5
                (e5.type, e5.state_key): e5,
            },
            state,
        )

        # check we can filter to the m.room.name event (with a '' state key)
        state = self.get_success(
            self.storage.state.get_state_for_event(
                e5.event_id, StateFilter.from_types([(EventTypes.Name, "")])
            )
        )

        self.assertStateMapEqual({(e2.type, e2.state_key): e2}, state)

        # check we can filter to the m.room.name event (with a wildcard None state key)
        state = self.get_success(
            self.storage.state.get_state_for_event(
                e5.event_id, StateFilter.from_types([(EventTypes.Name, None)])
            )
        )

        self.assertStateMapEqual({(e2.type, e2.state_key): e2}, state)

        # check we can grab the m.room.member events (with a wildcard None state key)
        state = self.get_success(
            self.storage.state.get_state_for_event(
                e5.event_id, StateFilter.from_types([(EventTypes.Member, None)])
            )
        )

        self.assertStateMapEqual(
            {(e3.type, e3.state_key): e3, (e5.type, e5.state_key): e5}, state
        )

        # check we can grab a specific room member without filtering out the
        # other event types
        state = self.get_success(
            self.storage.state.get_state_for_event(
                e5.event_id,
                state_filter=StateFilter(
                    types=immutabledict(
                        {EventTypes.Member: frozenset({self.u_alice.to_string()})}
                    ),
                    include_others=True,
                ),
            )
        )

        self.assertStateMapEqual(
            {
                (e1.type, e1.state_key): e1,
                (e2.type, e2.state_key): e2,
                (e3.type, e3.state_key): e3,
            },
            state,
        )

        # check that we can grab everything except members
        state = self.get_success(
            self.storage.state.get_state_for_event(
                e5.event_id,
                state_filter=StateFilter(
                    types=immutabledict({EventTypes.Member: frozenset()}),
                    include_others=True,
                ),
            )
        )

        self.assertStateMapEqual(
            {(e1.type, e1.state_key): e1, (e2.type, e2.state_key): e2}, state
        )

        #######################################################
        # _get_state_for_group_using_cache tests against a full cache
        #######################################################

        room_id = self.room.to_string()
        group_ids = self.get_success(
            self.storage.state.get_state_groups_ids(room_id, [e5.event_id])
        )
        group = list(group_ids.keys())[0]

        # test _get_state_for_group_using_cache correctly filters out members
        # with types=[]
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset()}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual(
            {
                (e1.type, e1.state_key): e1.event_id,
                (e2.type, e2.state_key): e2.event_id,
            },
            state_dict,
        )

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset()}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({}, state_dict)

        # test _get_state_for_group_using_cache correctly filters in members
        # with wildcard types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: None}), include_others=True
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual(
            {
                (e1.type, e1.state_key): e1.event_id,
                (e2.type, e2.state_key): e2.event_id,
            },
            state_dict,
        )

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: None}), include_others=True
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual(
            {
                (e3.type, e3.state_key): e3.event_id,
                # e4 is overwritten by e5
                (e5.type, e5.state_key): e5.event_id,
            },
            state_dict,
        )

        # test _get_state_for_group_using_cache correctly filters in members
        # with specific types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual(
            {
                (e1.type, e1.state_key): e1.event_id,
                (e2.type, e2.state_key): e2.event_id,
            },
            state_dict,
        )

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({(e5.type, e5.state_key): e5.event_id}, state_dict)

        # test _get_state_for_group_using_cache correctly filters in members
        # with specific types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=False,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({(e5.type, e5.state_key): e5.event_id}, state_dict)

        #######################################################
        # deliberately remove e2 (room name) from the _state_group_cache

        cache_entry = self.state_datastore._state_group_cache.get(group)
        state_dict_ids = cache_entry.value

        self.assertEqual(cache_entry.full, True)
        self.assertEqual(cache_entry.known_absent, set())
        self.assertDictEqual(
            state_dict_ids,
            {
                (e1.type, e1.state_key): e1.event_id,
                (e2.type, e2.state_key): e2.event_id,
            },
        )

        state_dict_ids.pop((e2.type, e2.state_key))
        self.state_datastore._state_group_cache.invalidate(group)
        self.state_datastore._state_group_cache.update(
            sequence=self.state_datastore._state_group_cache.sequence,
            key=group,
            value=state_dict_ids,
            # list fetched keys so it knows it's partial
            fetched_keys=((e1.type, e1.state_key),),
        )

        cache_entry = self.state_datastore._state_group_cache.get(group)
        state_dict_ids = cache_entry.value

        self.assertEqual(cache_entry.full, False)
        self.assertEqual(cache_entry.known_absent, set())
        self.assertDictEqual(state_dict_ids, {})

        ############################################
        # test that things work with a partial cache

        # test _get_state_for_group_using_cache correctly filters out members
        # with types=[]
        room_id = self.room.to_string()
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset()}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, False)
        self.assertDictEqual({}, state_dict)

        room_id = self.room.to_string()
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset()}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({}, state_dict)

        # test _get_state_for_group_using_cache correctly filters in members
        # wildcard types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: None}), include_others=True
            ),
        )

        self.assertEqual(is_all, False)
        self.assertDictEqual({}, state_dict)

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: None}), include_others=True
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual(
            {
                (e3.type, e3.state_key): e3.event_id,
                (e5.type, e5.state_key): e5.event_id,
            },
            state_dict,
        )

        # test _get_state_for_group_using_cache correctly filters in members
        # with specific types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, False)
        self.assertDictEqual({}, state_dict)

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=True,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({(e5.type, e5.state_key): e5.event_id}, state_dict)

        # test _get_state_for_group_using_cache correctly filters in members
        # with specific types
        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=False,
            ),
        )

        self.assertEqual(is_all, False)
        self.assertDictEqual({}, state_dict)

        state_dict, is_all = self.state_datastore._get_state_for_group_using_cache(
            self.state_datastore._state_group_members_cache,
            group,
            state_filter=StateFilter(
                types=immutabledict({EventTypes.Member: frozenset({e5.state_key})}),
                include_others=False,
            ),
        )

        self.assertEqual(is_all, True)
        self.assertDictEqual({(e5.type, e5.state_key): e5.event_id}, state_dict)

    def test_batched_state_group_storing(self) -> None:
        creation_event = self.inject_state_event(
            self.room, self.u_alice, EventTypes.Create, "", {}
        )
        state_to_event = self.get_success(
            self.storage.state.get_state_groups(
                self.room.to_string(), [creation_event.event_id]
            )
        )
        current_state_group = list(state_to_event.keys())[0]
        state_map = dict(
            self.get_success(
                self.storage.state.get_state_ids_for_group(current_state_group)
            )
        )
        prev_event_id = creation_event.event_id

        # create some unpersisted events and event contexts to store against room
        events_and_context: list[tuple[EventBase, UnpersistedEventContext]] = []
        builder = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": EventTypes.Name,
                "sender": self.u_alice.to_string(),
                "state_key": "",
                "room_id": self.room.to_string(),
                "content": {"name": "first rename of room"},
            },
        )

        event1, unpersisted_context1 = self.get_success(
            self.event_creation_handler.create_new_client_event_for_batch(
                builder,
                requester=create_requester(self.u_alice),
                prev_event_ids=[prev_event_id],
                state_map=dict(state_map),
                current_state_group=current_state_group,
            )
        )
        events_and_context.append((event1, unpersisted_context1))
        prev_event_id = event1.event_id
        if event1.is_state():
            state_map[(event1.type, event1.state_key)] = event1.event_id

        builder2 = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": EventTypes.JoinRules,
                "sender": self.u_alice.to_string(),
                "state_key": "",
                "room_id": self.room.to_string(),
                "content": {"join_rule": "private"},
            },
        )

        event2, unpersisted_context2 = self.get_success(
            self.event_creation_handler.create_new_client_event_for_batch(
                builder2,
                requester=create_requester(self.u_alice),
                prev_event_ids=[prev_event_id],
                state_map=dict(state_map),
                current_state_group=current_state_group,
            )
        )
        events_and_context.append((event2, unpersisted_context2))
        prev_event_id = event2.event_id
        if event2.is_state():
            state_map[(event2.type, event2.state_key)] = event2.event_id

        builder3 = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": EventTypes.Message,
                "sender": self.u_alice.to_string(),
                "room_id": self.room.to_string(),
                "content": {"body": "hello from event 3", "msgtype": "m.text"},
            },
        )

        event3, unpersisted_context3 = self.get_success(
            self.event_creation_handler.create_new_client_event_for_batch(
                builder3,
                requester=create_requester(self.u_alice),
                prev_event_ids=[prev_event_id],
                state_map=dict(state_map),
                current_state_group=current_state_group,
            )
        )
        events_and_context.append((event3, unpersisted_context3))
        prev_event_id = event3.event_id

        builder4 = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": EventTypes.JoinRules,
                "sender": self.u_alice.to_string(),
                "state_key": "",
                "room_id": self.room.to_string(),
                "content": {"join_rule": "public"},
            },
        )

        event4, unpersisted_context4 = self.get_success(
            self.event_creation_handler.create_new_client_event_for_batch(
                builder4,
                requester=create_requester(self.u_alice),
                prev_event_ids=[prev_event_id],
                state_map=dict(state_map),
                current_state_group=current_state_group,
            )
        )
        events_and_context.append((event4, unpersisted_context4))

        processed_events_and_context = self.get_success(
            self.hs.get_datastores().state.store_state_deltas_for_batched(
                events_and_context, self.room.to_string(), current_state_group
            )
        )

        # check that only state events are in state_groups, and all state events are in state_groups
        res = cast(
            list[tuple[str]],
            self.get_success(
                self.store.db_pool.simple_select_list(
                    table="state_groups",
                    keyvalues=None,
                    retcols=("event_id",),
                )
            ),
        )

        events = []
        for result in res:
            self.assertNotIn(event3.event_id, result)  # XXX
            events.append(result[0])

        for event, _ in processed_events_and_context:
            if event.is_state():
                self.assertIn(event.event_id, events)

        # The HAMT path is now the source of truth for live state snapshots.
        # `state_groups_state` should not receive rows for freshly written state
        # groups anymore.
        for event, context in processed_events_and_context:
            if event.is_state():
                state = cast(
                    list[tuple[str, str, str]],
                    self.get_success(
                        self.store.db_pool.simple_select_list(
                            table="state_groups_state",
                            keyvalues={"state_group": context.state_group_after_event},
                            retcols=("type", "state_key", "event_id"),
                        )
                    ),
                )
                self.assertEqual(state, [])

                groups = cast(
                    list[tuple[str]],
                    self.get_success(
                        self.store.db_pool.simple_select_list(
                            table="state_group_edges",
                            keyvalues={"state_group": context.state_group_after_event},
                            retcols=("prev_state_group",),
                        )
                    ),
                )
                self.assertEqual(len(groups), 1)
                self.assertEqual(context.state_group_before_event, groups[0][0])

        final_sg = processed_events_and_context[-1][1].state_group_after_event
        assert final_sg is not None
        final_state = self.get_success(
            self.state_datastore._get_state_groups_from_groups(
                [final_sg], StateFilter.all()
            )
        )
        self.assertEqual(
            final_state[final_sg][(EventTypes.Create, "")], creation_event.event_id
        )
        self.assertEqual(final_state[final_sg][(EventTypes.Name, "")], event1.event_id)
        self.assertEqual(
            final_state[final_sg][(EventTypes.JoinRules, "")], event4.event_id
        )

    def test_purge_room_state_concurrent_insertion_no_orphans(self) -> None:
        """Verify PostgreSQL READ COMMITTED concurrent insertion race during purge."""
        import threading
        from typing import Any
        from unittest.mock import patch

        from synapse.storage.database import LoggingTransaction

        from tests.utils import USE_POSTGRES_FOR_TESTS

        if not USE_POSTGRES_FOR_TESTS:
            self.skipTest("Requires PostgreSQL")

        room_id_str = "!purge_race:test"
        room_id = RoomID.from_string(room_id_str)
        self.get_success(
            self.store.store_room(
                room_id_str,
                room_creator_user_id=self.u_alice.to_string(),
                is_public=True,
                room_version=RoomVersions.V1,
            )
        )

        event1 = self.inject_state_event(
            room_id, self.u_alice, EventTypes.Create, "", {}
        )
        sg1 = self.get_success(self.store._get_state_group_for_event(event1.event_id))
        assert sg1 is not None

        self.get_success(
            self.store.db_pool.simple_insert(
                table="state_groups_pending_deletion",
                values={"state_group": sg1, "insertion_ts": 123456789},
            )
        )

        from synapse.storage.engines._base import IsolationLevel

        original_runInteraction = self.store.db_pool.runInteraction

        async def mock_runInteraction(
            desc: str, func: Any, *args: Any, **kwargs: Any
        ) -> Any:
            if desc == "purge_room_state":
                kwargs["isolation_level"] = IsolationLevel.READ_COMMITTED
            return await original_runInteraction(desc, func, *args, **kwargs)

        with patch.object(
            self.store.db_pool, "runInteraction", side_effect=mock_runInteraction
        ):
            resume_before_event = threading.Event()
            resume_after_event = threading.Event()
            bg_ready_to_insert_sg2 = threading.Event()
            bg_ready_to_insert_sg3 = threading.Event()

            original_execute = LoggingTransaction.execute

            db_config = self.hs.config.database.get_single_database()
            conn_args = dict(db_config.config.get("args", {}))
            conn_args.pop("cp_min", None)
            conn_args.pop("cp_max", None)

            # Use lists to pass out values from the background thread
            generated_ids = []
            bg_thread_error = []

            def background_worker() -> None:
                import psycopg2

                try:
                    conn = psycopg2.connect(**conn_args)
                    conn.autocommit = True
                    cursor = conn.cursor()

                    # --- BRANCH 1: Commit BEFORE parent DELETE ---
                    if bg_ready_to_insert_sg2.wait(timeout=10.0):
                        try:
                            # Allocate sg2 via PostgreSQL sequence
                            cursor.execute("SELECT nextval('state_group_id_seq')")
                            row = cursor.fetchone()
                            assert row is not None
                            sg2 = row[0]
                            generated_ids.append(sg2)

                            cursor.execute(
                                "INSERT INTO state_groups (id, room_id, event_id) VALUES (%s, %s, %s)",
                                (sg2, room_id_str, "$fake_event2:test"),
                            )
                            # Actual outgoing edge for sg2
                            cursor.execute(
                                "INSERT INTO state_group_edges (state_group, prev_state_group) VALUES (%s, %s)",
                                (sg2, sg1),
                            )
                            cursor.execute(
                                "INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES (%s, %s, %s, %s, %s)",
                                (sg2, room_id_str, "m.room.name", "", "$name2:test"),
                            )
                            cursor.execute(
                                "INSERT INTO state_groups_pending_deletion (state_group, insertion_ts) VALUES (%s, %s)",
                                (sg2, 123456789),
                            )
                            # Dummy HAMT root fixture
                            cursor.execute(
                                "INSERT INTO state_hamt_roots (state_group, room_prefix, root_structural_hash) VALUES (%s, %s, %s)",
                                (
                                    sg2,
                                    psycopg2.Binary(b"prefix12"),
                                    psycopg2.Binary(b"0123456789abcdef"),
                                ),
                            )
                        finally:
                            resume_before_event.set()

                    # --- BRANCH 2: Commit AFTER parent DELETE ---
                    if bg_ready_to_insert_sg3.wait(timeout=10.0):
                        try:
                            cursor.execute("SELECT nextval('state_group_id_seq')")
                            row = cursor.fetchone()
                            assert row is not None
                            sg3 = row[0]
                            generated_ids.append(sg3)

                            cursor.execute(
                                "INSERT INTO state_groups (id, room_id, event_id) VALUES (%s, %s, %s)",
                                (sg3, room_id_str, "$fake_event3:test"),
                            )
                            cursor.execute(
                                "INSERT INTO state_groups_state (state_group, room_id, type, state_key, event_id) VALUES (%s, %s, %s, %s, %s)",
                                (sg3, room_id_str, "m.room.name", "", "$name3:test"),
                            )
                        finally:
                            resume_after_event.set()

                    cursor.close()
                    conn.close()
                except Exception as e:
                    bg_thread_error.append(e)
                    resume_before_event.set()
                    resume_after_event.set()

            bg_thread = threading.Thread(target=background_worker)
            bg_thread.start()

            try:

                def mock_execute(
                    txn: LoggingTransaction, sql: str, parameters: Any = None
                ) -> object:
                    if "DELETE FROM state_groups WHERE room_id =" in sql:
                        bg_ready_to_insert_sg2.set()
                        if not resume_before_event.wait(timeout=10.0):
                            raise Exception("Timeout waiting for resume_before_event")

                        if bg_thread_error:
                            raise Exception(
                                f"Background thread error: {bg_thread_error[0]}"
                            )

                        res = original_execute(txn, sql, parameters)

                        bg_ready_to_insert_sg3.set()
                        if not resume_after_event.wait(timeout=10.0):
                            raise Exception("Timeout waiting for resume_after_event")
                        return res
                    return original_execute(txn, sql, parameters)

                with patch(
                    "synapse.storage.database.LoggingTransaction.execute",
                    side_effect=mock_execute,
                    autospec=True,
                ):
                    self.get_success(self.state_datastore.purge_room_state(room_id_str))

            finally:
                # Ensure the background thread never leaks if an assertion fails
                bg_ready_to_insert_sg2.set()
                bg_ready_to_insert_sg3.set()
                bg_thread.join(timeout=5.0)

            self.assertFalse(bg_thread.is_alive(), "Background thread failed to exit")
            if bg_thread_error:
                raise bg_thread_error[0]

            self.assertEqual(
                len(generated_ids), 2, "Background worker did not generate sg2 and sg3"
            )
            sg2, sg3 = generated_ids

            # 1. Assert sg1 and sg2 parents are deleted. sg3 is not.
            def get_parents(txn: LoggingTransaction) -> list[int]:
                txn.execute(
                    f"SELECT id FROM state_groups WHERE id IN ({sg1}, {sg2}, {sg3})"
                )
                return [row[0] for row in txn.fetchall()]

            parents = self.get_success(
                self.store.db_pool.runInteraction("get_parents", get_parents)
            )
            self.assertNotIn(sg1, parents, "sg1 parent survived")
            self.assertNotIn(sg2, parents, "sg2 parent survived")
            self.assertIn(sg3, parents, "sg3 parent did not survive")

            # 2. Assert NO orphans exist for sg1 or sg2 in any child table
            def check_orphans(txn: LoggingTransaction) -> dict[str, int]:
                res = {}
                child_tables = [
                    "state_groups_state",
                    "state_group_edges",
                    "state_hamt_roots",
                    "state_groups_pending_deletion",
                ]
                for table in child_tables:
                    txn.execute(
                        f"""
                        SELECT count(*) FROM {table}
                        WHERE state_group IN ({sg1}, {sg2})
                          AND NOT EXISTS (
                              SELECT 1 FROM state_groups
                              WHERE state_groups.id = {table}.state_group
                          )
                        """
                    )
                    fetch_res = txn.fetchone()
                    assert fetch_res is not None
                    res[table] = fetch_res[0]
                return res

            orphans = self.get_success(
                self.store.db_pool.runInteraction("check_orphans", check_orphans)
            )
            for table, count in orphans.items():
                self.assertEqual(
                    count, 0, f"Found {count} orphans for sg1/sg2 in {table}"
                )

            # 3. Explicitly verify sg3's single expected child row
            def check_sg3_children(txn: LoggingTransaction) -> dict[str, list[int]]:
                res = {}
                for table in [
                    "state_groups_state",
                    "state_group_edges",
                    "state_hamt_roots",
                    "state_groups_pending_deletion",
                ]:
                    txn.execute(
                        f"SELECT state_group FROM {table} WHERE state_group = {sg3}"
                    )
                    res[table] = [row[0] for row in txn.fetchall()]
                return res

            sg3_children = self.get_success(
                self.store.db_pool.runInteraction(
                    "check_sg3_children", check_sg3_children
                )
            )
            self.assertIn(
                sg3,
                sg3_children["state_groups_state"],
                "sg3 child missing from state_groups_state",
            )
            self.assertNotIn(
                sg3, sg3_children["state_group_edges"], "sg3 has unexpected edge"
            )


class CurrentStateDeltaStreamTestCase(HomeserverTestCase):
    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        super().prepare(reactor, clock, hs)
        self.store = hs.get_datastores().main
        self.storage = hs.get_storage_controllers()
        self.state_datastore = self.storage.state.stores.state
        self.event_creation_handler = hs.get_event_creation_handler()
        self.event_builder_factory = hs.get_event_builder_factory()

        # Create a made-up room and a user.
        self.alice_user_id = UserID.from_string("@alice:test")
        self.room = RoomID.from_string("!abc1234:test")

        self.get_success(
            self.store.store_room(
                self.room.to_string(),
                room_creator_user_id="@creator:text",
                is_public=True,
                room_version=RoomVersions.V1,
            )
        )

    def inject_state_event(
        self, room: RoomID, sender: UserID, typ: str, state_key: str, content: JsonDict
    ) -> EventBase:
        builder = self.event_builder_factory.for_room_version(
            RoomVersions.V1,
            {
                "type": typ,
                "sender": sender.to_string(),
                "state_key": state_key,
                "room_id": room.to_string(),
                "content": content,
            },
        )

        event, unpersisted_context = self.get_success(
            self.event_creation_handler.create_new_client_event(builder)
        )

        context = self.get_success(unpersisted_context.persist(event))

        assert self.storage.persistence is not None
        self.get_success(self.storage.persistence.persist_event(event, context))

        return event

    def test_get_partial_current_state_deltas_limit(self) -> None:
        """
        Tests that `get_partial_current_state_deltas` actually returns `limit` rows.

        Regression test for https://github.com/element-hq/synapse/pull/18960.
        """
        # Inject a create event which other events can auth with.
        self.inject_state_event(
            self.room, self.alice_user_id, EventTypes.Create, "", {}
        )

        limit = 2

        # Make N*2 state changes in the room, resulting in 2N+1 total state
        # events (including the create event) in the room.
        for i in range(limit * 2):
            self.inject_state_event(
                self.room,
                self.alice_user_id,
                EventTypes.Name,
                "",
                {"name": f"rename #{i}"},
            )

        # Call the function under test. This must return <= `limit` rows.
        max_stream_id = self.store.get_room_max_stream_ordering()
        clipped_stream_id, deltas = self.get_success(
            self.store.get_partial_current_state_deltas(
                prev_stream_id=0,
                max_stream_id=max_stream_id,
                limit=limit,
            )
        )

        self.assertLessEqual(
            len(deltas), limit, f"Returned {len(deltas)} rows, expected at most {limit}"
        )

        # Advancing from the clipped point should eventually drain the remainder.
        # Make sure we make progress and don’t get stuck.
        if deltas:
            next_prev = clipped_stream_id
            next_clipped, next_deltas = self.get_success(
                self.store.get_partial_current_state_deltas(
                    prev_stream_id=next_prev, max_stream_id=max_stream_id, limit=limit
                )
            )
            self.assertNotEqual(
                next_clipped, clipped_stream_id, "Did not advance clipped_stream_id"
            )
            # Still should respect the limit.
            self.assertLessEqual(len(next_deltas), limit)

    def test_non_unique_stream_ids_in_current_state_delta_stream(self) -> None:
        """
        Tests that `get_partial_current_state_deltas` always returns entire
        groups of state deltas (grouped by `stream_id`), and never part of one.

        We check by passing a `limit` that to the function that, if followed
        blindly, would split a group of state deltas that share a `stream_id`.
        The test passes if that group is not returned at all (because doing so
        would overshoot the limit of returned state deltas).

        Regression test for https://github.com/element-hq/synapse/pull/18960.
        """
        # Inject a create event to start with.
        self.inject_state_event(
            self.room, self.alice_user_id, EventTypes.Create, "", {}
        )

        # Then inject one "real" m.room.name event. This will give us a stream_id that
        # we can create some more (fake) events with.
        self.inject_state_event(
            self.room,
            self.alice_user_id,
            EventTypes.Name,
            "",
            {"name": "rename #1"},
        )

        # Get the stream_id of the last-inserted event.
        max_stream_id = self.store.get_room_max_stream_ordering()

        # Make 3 more state changes in the room, resulting in 5 total state
        # events (including the create event, and the first name update) in
        # the room.
        #
        # All of these state deltas have the same `stream_id` as the original name event.
        # Do so by editing the table directly as that's the simplest way to have
        # all share the same `stream_id`.
        self.get_success(
            self.store.db_pool.simple_insert_many(
                "current_state_delta_stream",
                keys=(
                    "stream_id",
                    "room_id",
                    "type",
                    "state_key",
                    "event_id",
                    "prev_event_id",
                    "instance_name",
                ),
                values=[
                    (
                        max_stream_id,
                        self.room.to_string(),
                        EventTypes.Name,
                        "",
                        f"${random_string(5)}:test",
                        json.dumps({"name": f"rename #{i}"}),
                        "master",
                    )
                    for i in range(3)
                ],
                desc="inject_room_name_state_events",
            )
        )

        # Call the function under test with a limit of 4. Without the limit, we
        # would return 5 state deltas:
        #
        # C N N N N
        # 1 2 3 4 5
        #
        # C = m.room.create
        # N = m.room.name
        #
        # With the limit, we should return only the create event, as returning 4
        # state deltas would result in splitting a group:
        #
        # 2 3 3 3 3 - state IDs/groups
        # C N N N N
        # 1 2 3 4 X

        clipped_stream_id, deltas = self.get_success(
            self.store.get_partial_current_state_deltas(
                prev_stream_id=0,
                max_stream_id=max_stream_id,
                limit=4,
            )
        )

        # 2 is the stream ID of the m.room.create event.
        self.assertEqual(clipped_stream_id, 2)
        self.assertEqual(
            len(deltas),
            1,
            f"Returned {len(deltas)} rows, expected only one (the create event): {deltas}",
        )

        # Advance once more with our limit of 4. We should now get all 4
        # `m.room.name` state deltas as they can fit under the limit.
        clipped_stream_id, next_deltas = self.get_success(
            self.store.get_partial_current_state_deltas(
                prev_stream_id=clipped_stream_id, max_stream_id=max_stream_id, limit=4
            )
        )
        self.assertEqual(
            clipped_stream_id, 3
        )  # The stream ID of the 4 m.room.name events.

        self.assertEqual(
            len(next_deltas),
            4,
            f"Returned {len(next_deltas)} rows, expected all 4 m.room.name events: {next_deltas}",
        )

    def test_get_partial_current_state_deltas_does_not_enter_infinite_loop(
        self,
    ) -> None:
        """
        Tests that `get_partial_current_state_deltas` does not repeatedly return
        zero entries due to the passed `limit` parameter being less than the
        size of the next group of state deltas from the given `prev_stream_id`.
        """
        # Inject a create event to start with.
        self.inject_state_event(
            self.room, self.alice_user_id, EventTypes.Create, "", {}
        )

        # Then inject one "real" m.room.name event. This will give us a stream_id that
        # we can create some more (fake) events with.
        self.inject_state_event(
            self.room,
            self.alice_user_id,
            EventTypes.Name,
            "",
            {"name": "rename #1"},
        )

        # Get the stream_id of the last-inserted event.
        max_stream_id = self.store.get_room_max_stream_ordering()

        # Make 3 more state changes in the room, resulting in 5 total state
        # events (including the create event, and the first name update) in
        # the room.
        #
        # All of these state deltas have the same `stream_id` as the original name event.
        # Do so by editing the table directly as that's the simplest way to have
        # all share the same `stream_id`.
        self.get_success(
            self.store.db_pool.simple_insert_many(
                "current_state_delta_stream",
                keys=(
                    "stream_id",
                    "room_id",
                    "type",
                    "state_key",
                    "event_id",
                    "prev_event_id",
                    "instance_name",
                ),
                values=[
                    (
                        max_stream_id,
                        self.room.to_string(),
                        EventTypes.Name,
                        "",
                        f"${random_string(5)}:test",
                        json.dumps({"name": f"rename #{i}"}),
                        "master",
                    )
                    for i in range(3)
                ],
                desc="inject_room_name_state_events",
            )
        )

        # Call the function under test with a limit of 4. Without the limit, we would return
        # 5 state deltas:
        #
        # C N N N N
        # 1 2 3 4 5
        #
        # C = m.room.create
        # N = m.room.name
        #
        # With the limit, we should return only the create event, as returning 4
        # state deltas would result in splitting a group:
        #
        # 2 3 3 3 3 - state IDs/groups
        # C N N N N
        # 1 2 3 4 X

        clipped_stream_id, deltas = self.get_success(
            self.store.get_partial_current_state_deltas(
                prev_stream_id=2,  # Start after the create event (which has stream_id 2).
                max_stream_id=max_stream_id,
                limit=2,  # Less than the size of the next group (which is 4).
            )
        )

        self.assertEqual(
            clipped_stream_id, 3
        )  # The stream ID of the 4 m.room.name events.

        # We should get all 4 `m.room.name` state deltas, instead of 0, which
        # would result in the caller entering an infinite loop.
        self.assertEqual(
            len(deltas),
            4,
            f"Returned {len(deltas)} rows, expected 4 even though it broke our limit: {deltas}",
        )


class HAMTStructuralKeyRegressionTest(HomeserverTestCase):
    """HAMT structural hashes must not depend on the macaroon secret."""

    def test_hamt_roots_are_deterministic(self) -> None:
        from synapse.synapse_rust import state_hamt

        room_id = "!test:example.com"
        entries = [
            ("m.room.create", "", "ev1"),
            ("m.room.name", "", "ev2"),
            ("m.room.topic", "", "ev3"),
            ("m.room.member", "@alice:example.com", "ev4"),
        ]
        hash_a, sg_a, lattice_a, nodes_a = state_hamt.build_root_handle_with_lattice(
            room_id, entries
        )
        hash_b, sg_b, lattice_b, nodes_b = state_hamt.build_root_handle_with_lattice(
            room_id, entries
        )

        self.assertEqual(hash_a, hash_b)
        self.assertEqual(sg_a, sg_b)
        self.assertEqual(lattice_a, lattice_b)
        self.assertEqual(nodes_a, nodes_b)

        # Golden-value assertions: if these change, the structural hash is no
        # longer derived from the room ID alone (or the encoding changed).
        # This catches regressions where the macaroon secret or another
        # ambient key leaks into the structural hash -- a mere length check
        # would pass regardless of what fed the hash, so pin the exact bytes.
        self.assertEqual(
            hash_a.hex(),
            "8dd913b7c06b71b0922167cc5468e40b83617ebbf3789483c05afb312343c32a",
        )
        self.assertEqual(
            sg_a.hex(),
            "cbd967fa5a267868fd32c3701ef0e9c7afb78b0ace049003701393ee59f903a8",
        )

    def test_room_structural_key_is_sha256_of_room_id(self) -> None:
        import hashlib

        from synapse.synapse_rust import state_hamt

        room_id = "!test:example.com"
        self.assertEqual(
            state_hamt.room_structural_key(room_id),
            hashlib.sha256(room_id.encode()).digest(),
        )

    def test_hamt_root_depends_on_room_id(self) -> None:
        """Verify that the structural hash changes when the room_id changes,
        confirming it is derived from the room ID (not ambient config)."""
        from synapse.synapse_rust import state_hamt

        entries = [
            ("m.room.create", "", "ev1"),
            ("m.room.name", "", "ev2"),
        ]
        hash_a, _, _, _ = state_hamt.build_root_handle_with_lattice(
            "!room1:example.com", entries
        )
        hash_b, _, _, _ = state_hamt.build_root_handle_with_lattice(
            "!room2:example.com", entries
        )
        self.assertNotEqual(hash_a, hash_b)
