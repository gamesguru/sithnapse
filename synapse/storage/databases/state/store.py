#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2014-2016 OpenMarket Ltd
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
import time
from typing import (
    TYPE_CHECKING,
    Iterable,
    Mapping,
    cast,
)

from synapse.api.constants import EventTypes
from synapse.api.room_versions import RoomVersion
from synapse.events import EventBase
from synapse.events.snapshot import (
    UnpersistedEventContext,
)
from synapse.logging.context import defer_to_thread
from synapse.logging.opentracing import tag_args, trace
from synapse.metrics.background_process_metrics import wrap_as_background_process
from synapse.storage._base import SQLBaseStore
from synapse.storage.database import (
    DatabasePool,
    LoggingDatabaseConnection,
    LoggingTransaction,
)
from synapse.storage.databases.state.bg_updates import (
    StateBackgroundUpdateStore,
    _encode_state_hamt_root,
    _state_hamt_node_key,
    _state_hamt_root_key,
)
from synapse.storage.engines import PostgresEngine
from synapse.storage.types import Cursor
from synapse.storage.util.sequence import build_sequence_generator
from synapse.types import MutableStateMap, StateKey, StateMap
from synapse.types.state import StateFilter
from synapse.util.caches.dictionary_cache import DictionaryCache
from synapse.util.cancellation import cancellable
from synapse.util.duration import Duration

if TYPE_CHECKING:
    from synapse.server import HomeServer
    from synapse.storage.databases.state.deletion import StateDeletionDataStore

logger = logging.getLogger(__name__)


class StateGroupDataStore(StateBackgroundUpdateStore, SQLBaseStore):
    """A data store for fetching/storing state groups."""

    EMBEDDED_HAMT_MIGRATION_UPDATE_NAME = "state_hamt_embedded_migration"

    def __init__(
        self,
        database: DatabasePool,
        db_conn: LoggingDatabaseConnection,
        hs: "HomeServer",
        state_deletion_store: "StateDeletionDataStore",
    ):
        super().__init__(database, db_conn, hs)
        self._state_deletion_store = state_deletion_store
        self.server_name = hs.hostname

        # Originally the state store used a single DictionaryCache to cache the
        # event IDs for the state types in a given state group to avoid hammering
        # on the state_group* tables.
        #
        # The point of using a DictionaryCache is that it can cache a subset
        # of the state events for a given state group (i.e. a subset of the keys for a
        # given dict which is an entry in the cache for a given state group ID).
        #
        # However, this poses problems when performing complicated queries
        # on the store - for instance: "give me all the state for this group, but
        # limit members to this subset of users", as DictionaryCache's API isn't
        # rich enough to say "please cache any of these fields, apart from this subset".
        # This is problematic when lazy loading members, which requires this behaviour,
        # as without it the cache has no choice but to speculatively load all
        # state events for the group, which negates the efficiency being sought.
        #
        # Rather than overcomplicating DictionaryCache's API, we instead split the
        # state_group_cache into two halves - one for tracking non-member events,
        # and the other for tracking member_events.  This means that lazy loading
        # queries can be made in a cache-friendly manner by querying both caches
        # separately and then merging the result.  So for the example above, you
        # would query the members cache for a specific subset of state keys
        # (which DictionaryCache will handle efficiently and fine) and the non-members
        # cache for all state (which DictionaryCache will similarly handle fine)
        # and then just merge the results together.
        #
        # We size the non-members cache to be smaller than the members cache as the
        # vast majority of state in Matrix (today) is member events.

        self._state_group_cache: DictionaryCache[int, StateKey, str] = DictionaryCache(
            name="*stateGroupCache*",
            clock=hs.get_clock(),
            server_name=self.server_name,
            # TODO: this hasn't been tuned yet
            max_entries=50000,
        )
        self._state_group_members_cache: DictionaryCache[int, StateKey, str] = (
            DictionaryCache(
                name="*stateGroupMembersCache*",
                clock=hs.get_clock(),
                server_name=self.server_name,
                max_entries=500000,
            )
        )

        def get_max_state_group_txn(txn: Cursor) -> int:
            txn.execute("SELECT COALESCE(max(id), 0) FROM state_groups")
            return txn.fetchone()[0]  # type: ignore

        self._state_group_seq_gen = build_sequence_generator(
            db_conn,
            self.database_engine,
            get_max_state_group_txn,
            "state_group_id_seq",
            table="state_groups",
            id_column="id",
        )

        self.embedded_hamt_engine = hs.config.database.embedded_hamt_engine
        self.embedded_hamt_path = hs.config.database.embedded_hamt_path
        if self.embedded_hamt_engine and self.embedded_hamt_path:
            # mdbx is the only embedded engine: it beat fjall on every real
            # benchmark (point reads, batch reads) and needs no worker-
            # process bridge (native multi-process mmap access), so fjall
            # was dropped rather than kept as a second maintained option.
            if self.embedded_hamt_engine != "mdbx":
                raise RuntimeError(
                    f"Unknown embedded_hamt_engine: {self.embedded_hamt_engine!r} "
                    "(only 'mdbx' is supported)"
                )
            try:
                from synapse.synapse_rust import mdbx_engine

                mdbx_engine.open_client(self.embedded_hamt_path)
                logger.info(
                    "Opened embedded mdbx engine at %s for state HAMT offload",
                    self.embedded_hamt_path,
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to open embedded mdbx engine at {self.embedded_hamt_path}"
                ) from e

            if hs.config.database.embedded_hamt_namespace:
                self.hamt_namespace = hs.config.database.embedded_hamt_namespace

            if hs.config.worker.run_background_tasks:
                hs.get_clock().looping_call(
                    self._drain_embedded_state_hamt_root_deletion_queue,
                    Duration(minutes=5),
                )

            self.db_pool.updates.register_background_update_handler(
                self.EMBEDDED_HAMT_MIGRATION_UPDATE_NAME,
                self._background_migrate_state_hamt_to_embedded,
            )
            if hs.config.worker.run_background_tasks:
                hs.run_as_background_process(
                    "enqueue_state_hamt_embedded_migration",
                    self._enqueue_embedded_hamt_migration_if_needed,
                )

    async def _enqueue_embedded_hamt_migration_if_needed(self) -> None:
        """Turning on the embedded engine doesn't retroactively move
        existing `state_hamt_nodes`/`state_hamt_roots` SQL rows into it --
        new writes go exclusively to whichever engine is configured (see
        `_persist_state_hamt_txn`), but old data written before the switch
        stays SQL-only until this migration copies it over.

        `has_completed_background_update` isn't a reliable "already
        enqueued?" check here: it short-circuits to `True` once the
        updater's own poll loop has run dry and set `_all_done` (which will
        already be true on almost every real startup, since this row is
        inserted at runtime rather than being a schema-declared update the
        loop knew about from the start) -- that would make this a no-op
        forever, never actually inserting the row. `simple_upsert` on the
        primary key is what actually makes this idempotent (a second call
        updates the existing row's non-key columns to the same values
        rather than erroring or duplicating); `start_doing_background_updates`
        unconditionally resets `_all_done` and restarts the poll loop so a
        freshly-inserted row is picked up even if the loop had already gone
        idle.
        """
        await self.db_pool.simple_upsert(
            table="background_updates",
            keyvalues={"update_name": self.EMBEDDED_HAMT_MIGRATION_UPDATE_NAME},
            values={},
            insertion_values={"progress_json": "{}"},
            desc="enqueue_state_hamt_embedded_migration",
        )
        self.db_pool.updates.start_doing_background_updates()

    async def _background_migrate_state_hamt_to_embedded(
        self, progress: dict, batch_size: int
    ) -> int:
        """Copy existing SQL `state_hamt_roots` rows (and, per root, every
        node reachable from it) into the embedded engine, for data written
        before `embedded_hamt_engine` was turned on. New writes never need
        this -- they already go straight to the configured engine
        exclusively -- this only backfills history.

        Existing SQL rows are left in place rather than deleted: harmless
        once migrated (nothing reads them anymore -- see
        `_fetch_hamt_roots_for_embedded_txn`), and keeping them means
        turning the embedded engine back off doesn't lose data.
        """
        last_state_group = progress.get("last_state_group", 0)

        def get_batch_txn(
            txn: LoggingTransaction,
        ) -> list[tuple[int, bytes, bytes, bytes, str]]:
            txn.execute(
                """
                SELECT hr.state_group, hr.room_prefix, hr.root_structural_hash,
                       hr.root_lattice, sg.room_id
                FROM state_hamt_roots hr
                INNER JOIN state_groups sg ON sg.id = hr.state_group
                WHERE hr.state_group > ?
                ORDER BY hr.state_group
                LIMIT ?
                """,
                (last_state_group, batch_size),
            )
            return cast(
                list[tuple[int, bytes, bytes, bytes, str]],
                [
                    (
                        state_group,
                        bytes(room_prefix),
                        bytes(root_hash),
                        bytes(lattice) if lattice is not None else b"",
                        room_id,
                    )
                    for state_group, room_prefix, root_hash, lattice, room_id in txn
                ],
            )

        rows = await self.db_pool.runInteraction(
            f"{self.EMBEDDED_HAMT_MIGRATION_UPDATE_NAME}_select", get_batch_txn
        )

        if not rows:
            await self.db_pool.updates._end_background_update(
                self.EMBEDDED_HAMT_MIGRATION_UPDATE_NAME
            )
            return 0

        from synapse.synapse_rust import mdbx_engine, state_hamt

        def migrate_one_txn(
            txn: LoggingTransaction,
            state_group: int,
            room_prefix: bytes,
            root_hash: bytes,
            lattice: bytes,
            room_id: str,
        ) -> None:
            nodes: dict[bytes, bytes] = {}
            frontier = [root_hash]
            while frontier:
                fetched = self.db_pool.simple_select_many_txn(
                    txn,
                    table="state_hamt_nodes",
                    column="structural_hash",
                    iterable=[bytearray(h) for h in frontier if h not in nodes],
                    keyvalues={},
                    retcols=("structural_hash", "node_bytes"),
                )
                frontier = []
                for node_hash, node_bytes in fetched:
                    node_hash = bytes(node_hash)
                    node_bytes = bytes(node_bytes)
                    if node_hash in nodes:
                        continue
                    nodes[node_hash] = node_bytes
                    frontier.extend(
                        bytes(child)
                        for child in state_hamt.node_child_hashes(node_bytes)
                    )
            mdbx_engine.put_state_hamt_nodes(
                self.hamt_namespace, room_prefix, list(nodes.items())
            )
            if lattice:
                self._store_state_hamt_root_embedded_txn(
                    state_group, room_prefix, root_hash, lattice, room_id
                )

        def migrate_batch_txn(txn: LoggingTransaction) -> None:
            for state_group, room_prefix, root_hash, lattice, room_id in rows:
                migrate_one_txn(
                    txn, state_group, room_prefix, root_hash, lattice, room_id
                )
            self.db_pool.updates._background_update_progress_txn(
                txn,
                self.EMBEDDED_HAMT_MIGRATION_UPDATE_NAME,
                {"last_state_group": rows[-1][0]},
            )

        await self.db_pool.runInteraction(
            self.EMBEDDED_HAMT_MIGRATION_UPDATE_NAME, migrate_batch_txn
        )

        return len(rows)

    @trace
    @tag_args
    @cancellable
    async def _get_state_groups_from_groups(
        self, groups: list[int], state_filter: StateFilter
    ) -> dict[int, StateMap[str]]:
        """Returns the state groups for a given set of groups from the
        database, filtering on types of state events.

        Args:
            groups: list of state group IDs to query
            state_filter: The state filter used to fetch state
                from the database.
        Returns:
            Dict of state group to state map.
        """
        # _get_state_groups_from_groups_txn (bg_updates.py) already
        # disambiguates a missing HAMT root from a legitimately empty/purged
        # state group *inside* the transaction, and raises RuntimeError
        # immediately on corruption (an existing `state_groups` row with no
        # HAMT root). HAMT roots are published atomically with the
        # `state_groups` row that references them, so there is no
        # cross-connection visibility race to poll for here: a caller-side
        # retry loop could never observe a corrupt group without the inner
        # txn having already raised on the very first attempt.
        chunks = [groups[i : i + 100] for i in range(0, len(groups), 100)]
        _gg_sql_start = time.monotonic()
        results: dict[int, StateMap[str]] = {}
        for chunk in chunks:
            res = await self.db_pool.runInteraction(
                "_get_state_groups_from_groups",
                self._get_state_groups_from_groups_txn,
                chunk,
                state_filter,
            )
            results.update(res)

        logger.debug(
            "[gg-state-timing] _get_state_groups_from_groups sql_dispatch "
            "groups=%d elapsed_ms=%.1f",
            len(groups),
            (time.monotonic() - _gg_sql_start) * 1000,
        )
        return results

    @trace
    @tag_args
    def _get_state_for_group_using_cache(
        self,
        cache: DictionaryCache[int, StateKey, str],
        group: int,
        state_filter: StateFilter,
    ) -> tuple[MutableStateMap[str], bool]:
        """Checks if group is in cache. See `get_state_for_groups`

        Args:
            cache: the state group cache to use
            group: The state group to lookup
            state_filter: The state filter used to fetch state from the database.

        Returns:
             2-tuple (`state_dict`, `got_all`).
                `got_all` is a bool indicating if we successfully retrieved all
                requests state from the cache, if False we need to query the DB for the
                missing state.
        """
        # If we are asked explicitly for a subset of keys, we only ask for those
        # from the cache. This ensures that the `DictionaryCache` can make
        # better decisions about what to cache and what to expire.
        dict_keys = None
        if not state_filter.has_wildcards():
            dict_keys = state_filter.concrete_types()

        cache_entry = cache.get(group, dict_keys=dict_keys)
        state_dict_ids = cache_entry.value

        if cache_entry.full or state_filter.is_full():
            # Either we have everything or want everything, either way
            # `is_all` tells us whether we've gotten everything.
            return state_filter.filter_state(state_dict_ids), cache_entry.full

        # tracks whether any of our requested types are missing from the cache
        missing_types = False

        if state_filter.has_wildcards():
            # We don't know if we fetched all the state keys for the types in
            # the filter that are wildcards, so we have to assume that we may
            # have missed some.
            missing_types = True
        else:
            # There aren't any wild cards, so `concrete_types()` returns the
            # complete list of event types we're wanting.
            for key in state_filter.concrete_types():
                if key not in state_dict_ids and key not in cache_entry.known_absent:
                    missing_types = True
                    break

        return state_filter.filter_state(state_dict_ids), not missing_types

    @trace
    @tag_args
    @cancellable
    async def _get_state_for_groups(
        self, groups: Iterable[int], state_filter: StateFilter | None = None
    ) -> dict[int, MutableStateMap[str]]:
        """Gets the state at each of a list of state groups, optionally
        filtering by type/state_key

        Args:
            groups: list of state groups for which we want
                to get the state.
            state_filter: The state filter used to fetch state
                from the database.
        Returns:
            Dict of state group to state map.
        """
        if state_filter is None:
            state_filter = StateFilter.all()

        member_filter, non_member_filter = state_filter.get_member_split()

        # Now we look them up in the member and non-member caches
        non_member_state, incomplete_groups_nm = self._get_state_for_groups_using_cache(
            groups, self._state_group_cache, state_filter=non_member_filter
        )

        member_state, incomplete_groups_m = self._get_state_for_groups_using_cache(
            groups, self._state_group_members_cache, state_filter=member_filter
        )

        state = dict(non_member_state)
        for group in groups:
            state[group].update(member_state[group])

        # Now fetch any missing groups from the database

        incomplete_groups = incomplete_groups_m | incomplete_groups_nm

        if not incomplete_groups:
            return state

        cache_sequence_nm = self._state_group_cache.sequence
        cache_sequence_m = self._state_group_members_cache.sequence

        # Help the cache hit ratio by expanding the filter a bit
        db_state_filter = state_filter.return_expanded()

        group_to_state_dict = await self._get_state_groups_from_groups(
            list(incomplete_groups), state_filter=db_state_filter
        )

        # Now lets update the caches
        self._insert_into_cache(
            group_to_state_dict,
            db_state_filter,
            cache_seq_num_members=cache_sequence_m,
            cache_seq_num_non_members=cache_sequence_nm,
        )

        # And finally update the result dict, by filtering out any extra
        # stuff we pulled out of the database.
        for group, group_state_dict in group_to_state_dict.items():
            # We just replace any existing entries, as we will have loaded
            # everything we need from the database anyway.
            state[group] = state_filter.filter_state(group_state_dict)

        return state

    @trace
    @tag_args
    def _get_state_for_groups_using_cache(
        self,
        groups: Iterable[int],
        cache: DictionaryCache[int, StateKey, str],
        state_filter: StateFilter,
    ) -> tuple[dict[int, MutableStateMap[str]], set[int]]:
        """Gets the state at each of a list of state groups, optionally
        filtering by type/state_key, querying from a specific cache.

        Args:
            groups: list of state groups for which we want to get the state.
            cache: the cache of group ids to state dicts which
                we will pass through - either the normal state cache or the
                specific members state cache.
            state_filter: The state filter used to fetch state from the
                database.

        Returns:
            Tuple of dict of state_group_id to state map of entries in the
            cache, and the state group ids either missing from the cache or
            incomplete.
        """
        results = {}
        incomplete_groups = set()
        for group in set(groups):
            state_dict_ids, got_all = self._get_state_for_group_using_cache(
                cache, group, state_filter
            )
            results[group] = state_dict_ids

            if not got_all:
                incomplete_groups.add(group)

        return results, incomplete_groups

    def _insert_into_cache(
        self,
        group_to_state_dict: dict[int, StateMap[str]],
        state_filter: StateFilter,
        cache_seq_num_members: int,
        cache_seq_num_non_members: int,
    ) -> None:
        """Inserts results from querying the database into the relevant cache.

        Args:
            group_to_state_dict: The new entries pulled from database.
                Map from state group to state dict
            state_filter: The state filter used to fetch state
                from the database.
            cache_seq_num_members: Sequence number of member cache since
                last lookup in cache
            cache_seq_num_non_members: Sequence number of member cache since
                last lookup in cache
        """

        # We need to work out which types we've fetched from the DB for the
        # member vs non-member caches. This should be as accurate as possible,
        # but can be an underestimate (e.g. when we have wild cards)

        member_filter, non_member_filter = state_filter.get_member_split()
        if member_filter.is_full():
            # We fetched all member events
            member_types = None
        else:
            # `concrete_types()` will only return a subset when there are wild
            # cards in the filter, but that's fine.
            member_types = member_filter.concrete_types()

        if non_member_filter.is_full():
            # We fetched all non member events
            non_member_types = None
        else:
            non_member_types = non_member_filter.concrete_types()

        for group, group_state_dict in group_to_state_dict.items():
            state_dict_members = {}
            state_dict_non_members = {}

            for k, v in group_state_dict.items():
                if k[0] == EventTypes.Member:
                    state_dict_members[k] = v
                else:
                    state_dict_non_members[k] = v

            self._state_group_members_cache.update(
                cache_seq_num_members,
                key=group,
                value=state_dict_members,
                fetched_keys=member_types,
            )

            self._state_group_cache.update(
                cache_seq_num_non_members,
                key=group,
                value=state_dict_non_members,
                fetched_keys=non_member_types,
            )

    def _build_state_hamt_entries(
        self, current_state_ids: StateMap[str]
    ) -> list[tuple[str, str, str]]:
        return [
            (state_key[0], state_key[1], event_id)
            for state_key, event_id in current_state_ids.items()
        ]

    def _persist_state_hamt_txn(
        self,
        txn: LoggingTransaction,
        state_group: int,
        room_id: str,
        room_prefix: bytes,
        current_state_ids: StateMap[str] | None,
        prev_state_group: int | None = None,
        updates: list[tuple[str, str, str]] | None = None,
        local_nodes: dict[bytes, bytes] | None = None,
        local_roots: dict[int, tuple[bytes, bytes]] | None = None,
    ) -> tuple[bytes, bytes, list[tuple[bytes, bytes]]]:
        """Persist a new state_group's HAMT root and nodes.

        If `prev_state_group` has a usable stored root+lattice and
        `updates` names the `(event_type, state_key, event_id)` changes
        that produce `current_state_ids` from `prev_state_group`'s state
        (a delta of any size -- one key for a plain state event, several
        for a state-resolution/merge result whose delta the caller already
        computed), this applies them via O(K log S) path-copying
        (`apply_flat_state_updates`) instead of rebuilding the whole tree
        from `current_state_ids` -- for either backend (SQL or the embedded
        engine). Otherwise (no prev root/lattice -- e.g. a room's first
        state group -- or no delta given, i.e. the caller only has a full
        current_state_ids map with no known relationship to prev_group)
        this falls back to a full rebuild, exactly as before.

        `local_nodes`: an optional cache of hash->node-bytes the caller
        already holds in memory, consulted before hitting SQL/the embedded
        engine. This matters for a caller (`store_state_deltas_for_batched`)
        that persists a *chain* of state groups within one transaction:
        both backends' writes are synchronous and in-transaction, but a
        fresh read isn't guaranteed to see a row written earlier in the
        *same* uncommitted transaction (true for SQL always, and for the
        embedded engine depending on its own transaction semantics) -- so
        state group N+1's incremental update, which needs to read state
        group N's just-written root node back, uses this in-memory cache
        rather than relying on that.
        """
        incremental = None
        if prev_state_group is not None and updates is not None:
            incremental = self._persist_state_hamt_incremental_txn(
                txn,
                state_group,
                room_id,
                room_prefix,
                prev_state_group,
                updates,
                local_nodes=local_nodes,
                local_roots=local_roots,
            )
        if incremental is not None:
            return incremental

        _gg_reb_start = time.monotonic()
        if current_state_ids is None:
            if prev_state_group is None:
                raise RuntimeError("A state map is required for an initial state group")
            current_state_ids = dict(
                self._get_state_groups_from_groups_txn(txn, [prev_state_group])[
                    prev_state_group
                ]
            )
            if updates:
                current_state_ids.update(
                    {
                        (event_type, state_key): event_id
                        for event_type, state_key, event_id in updates
                    }
                )

        from synapse.synapse_rust import state_hamt

        root_structural_hash, _state_group_id, root_lattice, nodes = (
            state_hamt.build_root_handle_with_lattice(
                room_id,
                self._build_state_hamt_entries(current_state_ids),
            )
        )

        # Exclusive by configured engine, not a dual-write: SQL owns this
        # data unless the embedded engine is configured, in which case it
        # owns it instead. _store_state_hamt_nodes_txn already makes this
        # same choice for nodes.
        self._store_state_hamt_nodes_txn(txn, room_prefix, nodes)
        if self.embedded_hamt_engine == "mdbx":
            self._store_state_hamt_root_embedded_txn(
                state_group, room_prefix, root_structural_hash, root_lattice, room_id
            )
        else:
            self.db_pool.simple_insert_txn(
                txn,
                table="state_hamt_roots",
                values={
                    "state_group": state_group,
                    "room_prefix": bytearray(room_prefix),
                    "root_structural_hash": bytearray(root_structural_hash),
                    "root_lattice": bytearray(root_lattice),
                },
            )

        logger.debug(
            "[gg-state-timing] _persist_state_hamt_txn mode=rebuild "
            "group=%d entries=%d elapsed_ms=%.1f",
            state_group,
            len(current_state_ids),
            (time.monotonic() - _gg_reb_start) * 1000,
        )
        return root_structural_hash, root_lattice, nodes

    def _persist_state_hamt_incremental_txn(
        self,
        txn: LoggingTransaction,
        state_group: int,
        room_id: str,
        room_prefix: bytes,
        prev_state_group: int,
        updates: list[tuple[str, str, str]],
        local_nodes: dict[bytes, bytes] | None = None,
        local_roots: dict[int, tuple[bytes, bytes]] | None = None,
    ) -> tuple[bytes, bytes, list[tuple[bytes, bytes]]] | None:
        """Apply `updates` -- a delta of any size, from a single state event
        to a whole state-resolution/merge result the caller already computed
        as a delta -- against `prev_state_group`'s HAMT root via O(K log S)
        path-copying, instead of materializing and rebuilding the whole
        state map. This is the fix for the O(S)-per-update tax described in
        docs/development-gg/persistent-typed-hamt-architecture.md: cost is
        proportional to len(updates), not to the room's total state size.

        Returns None -- signalling the caller to fall back to a full
        rebuild -- if `prev_state_group` has no stored root+lattice to
        update from: a pre-existing root written before this column
        existed, or a room's very first state group.

        """
        from synapse.synapse_rust import state_hamt

        _gg_inc_start = time.monotonic()
        local_roots = local_roots or {}
        if prev_state_group in local_roots:
            prev_root_hash, prev_lattice = local_roots[prev_state_group]
        else:
            prev_root_hash = None
            prev_lattice = None
            mdbx_active = self.embedded_hamt_engine == "mdbx"
            if mdbx_active:
                embedded_root = self._get_embedded_hamt_root(prev_state_group)
                if embedded_root is not None:
                    prev_root_hash, prev_lattice = embedded_root
            if prev_root_hash is None and (
                not mdbx_active or self._embedded_hamt_migration_pending_txn(txn)
            ):
                prev_root = self.db_pool.simple_select_one_txn(
                    txn,
                    table="state_hamt_roots",
                    keyvalues={"state_group": prev_state_group},
                    retcols=("root_structural_hash", "root_lattice"),
                    allow_none=True,
                )
                if prev_root is not None and prev_root[1] is not None:
                    prev_root_hash, prev_lattice = (
                        bytes(prev_root[0]),
                        bytes(prev_root[1]),
                    )
            if prev_root_hash is None or prev_lattice is None:
                return None

        assert prev_root_hash is not None
        assert prev_lattice is not None
        local_nodes = local_nodes or {}
        root_node_bytes = local_nodes.get(prev_root_hash)
        if root_node_bytes is None:
            root_node_bytes = self._get_embedded_hamt_node(room_prefix, prev_root_hash)
        if root_node_bytes is None and (
            self.embedded_hamt_engine != "mdbx"
            or self._embedded_hamt_migration_pending_txn(txn)
        ):
            root_node_bytes = self.db_pool.simple_select_one_onecol_txn(
                txn,
                table="state_hamt_nodes",
                keyvalues={"structural_hash": bytearray(prev_root_hash)},
                retcol="node_bytes",
                allow_none=True,
            )
        if root_node_bytes is None:
            raise RuntimeError(
                "Missing HAMT root node for state group "
                f"{prev_state_group}: {prev_root_hash.hex()}"
            )
        root_bytes = bytes(root_node_bytes)
        # Start with only the root node from the local cache (not all accumulated nodes).
        # The retry loop below will fetch any missing child nodes via SQL/mdbx.
        nodes: dict[bytes, bytes] = {prev_root_hash: root_bytes}
        # Add any root nodes from the local cache (needed for the retry loop)
        for node_hash, node_bytes in local_nodes.items():
            if node_hash not in nodes:
                nodes[node_hash] = node_bytes

        # Mirrors _lookup_state_hamt_from_postgres_txn's retry loop: each
        # round trip surfaces one more tree level's worth of missing
        # hashes, rather than fetching the whole reachable tree up front.
        while True:
            applied, missing = state_hamt.apply_flat_state_updates(
                room_id,
                root_bytes,
                list(nodes.items()),
                prev_lattice,
                updates,
            )
            if applied is not None:
                break
            missing = [
                bytes(node_hash)
                for node_hash in missing
                if bytes(node_hash) not in nodes
            ]
            if not missing:
                raise RuntimeError(
                    "apply_flat_state_updates reported no progress for state group "
                    f"{prev_state_group}"
                )
            found = self._get_embedded_hamt_nodes_batch(room_prefix, missing)
            still_missing = [
                node_hash for node_hash in missing if node_hash not in found
            ]
            if still_missing:
                rows = self.db_pool.simple_select_many_txn(
                    txn,
                    table="state_hamt_nodes",
                    column="structural_hash",
                    iterable=[bytearray(node_hash) for node_hash in still_missing],
                    keyvalues={},
                    retcols=("structural_hash", "node_bytes"),
                )
                found.update(
                    {
                        bytes(node_hash): bytes(node_bytes)
                        for node_hash, node_bytes in rows
                    }
                )
            nodes.update(found)
            unresolved = set(missing) - found.keys()
            if unresolved:
                raise RuntimeError(
                    "Missing HAMT child nodes for state group "
                    f"{prev_state_group}: {[node_hash.hex() for node_hash in unresolved]}"
                )

        new_root_hash, _new_state_group_id, new_lattice, new_nodes = applied

        self._store_state_hamt_nodes_txn(txn, room_prefix, new_nodes)
        if self.embedded_hamt_engine == "mdbx":
            self._store_state_hamt_root_embedded_txn(
                state_group, room_prefix, new_root_hash, new_lattice, room_id
            )
        else:
            self.db_pool.simple_insert_txn(
                txn,
                table="state_hamt_roots",
                values={
                    "state_group": state_group,
                    "room_prefix": bytearray(room_prefix),
                    "root_structural_hash": bytearray(new_root_hash),
                    "root_lattice": bytearray(new_lattice),
                },
            )
        logger.debug(
            "[gg-state-timing] _persist_state_hamt_incremental_txn "
            "group=%d prev=%d updates=%d nodes=%d elapsed_ms=%.1f",
            state_group,
            prev_state_group,
            len(updates),
            len(new_nodes),
            (time.monotonic() - _gg_inc_start) * 1000,
        )
        return bytes(new_root_hash), new_lattice, new_nodes

    def _store_state_hamt_nodes_txn(
        self,
        txn: LoggingTransaction,
        room_prefix: bytes,
        nodes: list[tuple[bytes, bytes]],
    ) -> None:
        # Exclusive by configured engine -- not a dual-write. Written under
        # the namespaced, room-prefixed key (`database::core::node_key`) the
        # embedded engine's materialize/lookup BFS walk actually looks up --
        # a plain `batch_put` keyed by the raw structural_hash would be
        # invisible to it.
        if self.embedded_hamt_engine == "mdbx":
            from synapse.synapse_rust import mdbx_engine

            mdbx_engine.put_state_hamt_nodes(self.hamt_namespace, room_prefix, nodes)
            return

        txn.executemany(
            """
            INSERT INTO state_hamt_nodes (structural_hash, node_bytes)
            VALUES (?, ?)
            ON CONFLICT (structural_hash) DO NOTHING
            """,
            [
                (bytearray(structural_hash), bytearray(node_bytes))
                for structural_hash, node_bytes in nodes
            ],
        )

    def _embedded_hamt_migration_pending_txn(self, txn: LoggingTransaction) -> bool:
        """Whether `EMBEDDED_HAMT_MIGRATION_UPDATE_NAME` is still queued or
        running -- the one bounded, explicit window where reading SQL
        alongside the embedded engine is legitimate rather than silent
        self-healing. See `_background_migrate_state_hamt_to_embedded`.
        """
        txn.execute(
            "SELECT 1 FROM background_updates WHERE update_name = ?",
            (self.EMBEDDED_HAMT_MIGRATION_UPDATE_NAME,),
        )
        return txn.fetchone() is not None

    def _get_embedded_hamt_root(self, state_group: int) -> tuple[bytes, bytes] | None:
        """Point lookup of a single HAMT root's `(root_hash, lattice)` in
        the embedded engine, if configured. Returns `None` on a miss.
        """
        if self.embedded_hamt_engine != "mdbx":
            return None
        from synapse.synapse_rust import mdbx_engine

        (record,) = mdbx_engine.batch_get_state_hamt_roots(
            self.hamt_namespace, [state_group]
        )
        if record is None:
            return None
        _group, _room_prefix, root_hash, _room_id, lattice = record
        if not lattice:
            # A root written before the lattice column existed -- no usable
            # base for an incremental update, same as SQL's NULL root_lattice.
            return None
        return bytes(root_hash), bytes(lattice)

    def _get_embedded_hamt_node(
        self, room_prefix: bytes, node_hash: bytes
    ) -> bytes | None:
        """Point lookup of a single HAMT node in the embedded engine, if
        configured -- consulted before falling back to `state_hamt_nodes`
        SQL. Returns `None` on a miss (caller falls back), never raises for
        a missing key.
        """
        if self.embedded_hamt_engine != "mdbx":
            return None
        from synapse.synapse_rust import mdbx_engine

        key = _state_hamt_node_key(self.hamt_namespace, room_prefix, node_hash)
        value = mdbx_engine.get(key)
        return bytes(value) if value is not None else None

    def _get_embedded_hamt_nodes_batch(
        self, room_prefix: bytes, node_hashes: list[bytes]
    ) -> dict[bytes, bytes]:
        """Batched version of `_get_embedded_hamt_node`. Returns only the
        hashes actually found; missing ones are simply absent from the
        result, same self-healing shape as `embedded_event_json`'s
        `get_event_json_batch`.
        """
        if self.embedded_hamt_engine != "mdbx" or not node_hashes:
            return {}
        from synapse.synapse_rust import mdbx_engine

        key_to_hash = {
            _state_hamt_node_key(self.hamt_namespace, room_prefix, node_hash): node_hash
            for node_hash in node_hashes
        }
        found = mdbx_engine.batch_get(list(key_to_hash))
        return {key_to_hash[bytes(key)]: bytes(value) for key, value in found}

    def _store_state_hamt_root_embedded_txn(
        self,
        state_group: int,
        room_prefix: bytes,
        root_hash: bytes,
        lattice: bytes,
        room_id: str,
    ) -> None:
        """Mirror a HAMT root record into the configured embedded engine,
        under the `hamt:root:<namespace_hash><state_group>` key
        (`_state_hamt_root_key`). Called synchronously in the persisting
        transaction: an embedded engine is a local call with no network
        round-trip to defer past commit, so there's no reason to route it
        through an async post-commit publish path.
        `_fetch_hamt_roots_for_embedded_txn` (bg_updates.py) reads this and
        falls back to `state_hamt_roots` SQL only inside the bounded
        `EMBEDDED_HAMT_MIGRATION_UPDATE_NAME` window -- see that function's
        docstring.
        """
        if not self.embedded_hamt_engine:
            return
        root_key = _state_hamt_root_key(self.hamt_namespace, state_group)
        root_value = _encode_state_hamt_root(
            room_prefix, root_hash, lattice, room_id=room_id
        )
        if self.embedded_hamt_engine == "mdbx":
            from synapse.synapse_rust import mdbx_engine

            mdbx_engine.put(root_key, root_value)

    async def _background_backfill_state_hamt_roots(
        self, progress: dict, batch_size: int
    ) -> int:
        """Give every state group that predates schema v95 (the HAMT
        tables) a `state_hamt_roots` row, by reconstructing its state via
        the legacy `state_group_edges`/`state_groups_state` walk
        (`_get_legacy_state_for_groups_txn`) and building+persisting a root
        for it exactly as `_persist_state_hamt_txn` does for a
        newly-created group.

        Written via `_persist_state_hamt_txn`, so it goes to whichever
        backend is exclusively configured (SQL's `state_hamt_roots`, or the
        embedded engine) -- see that function.

        State groups are immutable once created -- nothing else ever
        writes to an *existing* group's root -- so backfilling one here
        can't race with a concurrent write to the same group.
        """
        last_state_group = progress.get("last_state_group", 0)

        def get_batch_txn(txn: LoggingTransaction) -> list[tuple[int, str]]:
            txn.execute(
                """
                SELECT sg.id, sg.room_id
                FROM state_groups sg
                LEFT JOIN state_hamt_roots hr ON hr.state_group = sg.id
                WHERE sg.id > ? AND hr.state_group IS NULL
                ORDER BY sg.id
                LIMIT ?
                """,
                (last_state_group, batch_size),
            )
            return cast(list[tuple[int, str]], txn.fetchall())

        rows = await self.db_pool.runInteraction(
            f"{self.STATE_HAMT_BACKFILL_ROOTS_UPDATE_NAME}_select", get_batch_txn
        )

        if not rows:
            await self.db_pool.updates._end_background_update(
                self.STATE_HAMT_BACKFILL_ROOTS_UPDATE_NAME
            )
            return 0

        from synapse.api.errors import NotFoundError, UnsupportedRoomVersionError
        from synapse.synapse_rust import state_hamt

        main_store = self.hs.get_datastores().main
        room_prefixes: dict[str, bytes | None] = {}
        for _state_group, room_id in rows:
            if room_id in room_prefixes:
                continue
            try:
                room_version = await main_store.get_room_version(room_id)
            except (NotFoundError, UnsupportedRoomVersionError):
                # Either no `rooms` row for this room, or one whose
                # room_version is unknown/no longer supported -- either way
                # we can't compute the HAMT room prefix (it needs
                # msc4291_room_ids_as_hashes off a resolved RoomVersion).
                # Leave it unbackfilled; `None` still lets the batch make
                # progress (below) rather than looping forever on it, or
                # -- for UnsupportedRoomVersionError -- failing the whole
                # background update outright.
                room_prefixes[room_id] = None
                continue
            room_prefixes[room_id] = state_hamt.room_hamt_prefix(
                room_id, room_version.msc4291_room_ids_as_hashes
            )

        # Once the embedded engine is exclusive, a group it already has a
        # root for never gets a state_hamt_roots row -- so the SELECT above
        # (which only checks SQL) would otherwise keep re-matching every
        # already-migrated group forever as the id cursor passes it, and
        # _get_legacy_state_for_groups_txn returns {} for any post-v95 group
        # (state_groups_state isn't written for those), silently overwriting
        # a good root with an empty one. Skip anything the embedded engine
        # already has.
        already_embedded: set[int] = set()
        if self.embedded_hamt_engine == "mdbx":
            from synapse.synapse_rust import mdbx_engine

            state_groups = [state_group for state_group, _ in rows]
            already_embedded = {
                state_group
                for state_group, record in zip(
                    state_groups,
                    mdbx_engine.batch_get_state_hamt_roots(
                        self.hamt_namespace, state_groups
                    ),
                )
                if record is not None
            }

        def backfill_txn(txn: LoggingTransaction) -> None:
            for state_group, room_id in rows:
                if state_group in already_embedded:
                    continue
                room_prefix = room_prefixes[room_id]
                if room_prefix is None:
                    continue
                current_state_ids = self._get_legacy_state_for_groups_txn(
                    txn, [state_group], StateFilter.all()
                )[state_group]
                # Both SQL and (if configured) the embedded engine are
                # written synchronously here -- see _persist_state_hamt_txn.
                self._persist_state_hamt_txn(
                    txn, state_group, room_id, room_prefix, current_state_ids
                )

            self.db_pool.updates._background_update_progress_txn(
                txn,
                self.STATE_HAMT_BACKFILL_ROOTS_UPDATE_NAME,
                {"last_state_group": rows[-1][0]},
            )

        await self.db_pool.runInteraction(
            self.STATE_HAMT_BACKFILL_ROOTS_UPDATE_NAME, backfill_txn
        )

        return len(rows)

    def _persist_state_group_snapshot_txn(
        self,
        txn: LoggingTransaction,
        state_group: int,
        room_id: str,
        room_prefix: bytes,
        event_id: str,
        current_state_ids: StateMap[str] | None,
        prev_group: int | None = None,
        updates: list[tuple[str, str, str]] | None = None,
        local_nodes: dict[bytes, bytes] | None = None,
        local_roots: dict[int, tuple[bytes, bytes]] | None = None,
    ) -> tuple[bytes, bytes, list[tuple[bytes, bytes]]]:
        self.db_pool.simple_insert_txn(
            txn,
            table="state_groups",
            values={"id": state_group, "room_id": room_id, "event_id": event_id},
        )

        if prev_group is not None:
            # `state_group_edges` is lifecycle ancestry metadata for purge and
            # deletion safety. The live state payload now lives in the HAMT
            # (SQL or the embedded engine), so this edge is only ancestry
            # metadata.
            self.db_pool.simple_insert_txn(
                txn,
                table="state_group_edges",
                values={
                    "state_group": state_group,
                    "prev_state_group": prev_group,
                },
            )

        if current_state_ids is not None:
            current_member_state_ids = {
                s: ev
                for (s, ev) in current_state_ids.items()
                if s[0] == EventTypes.Member
            }
            txn.call_after(
                self._state_group_members_cache.update,
                self._state_group_members_cache.sequence,
                key=state_group,
                value=current_member_state_ids,
            )

            current_non_member_state_ids = {
                s: ev
                for (s, ev) in current_state_ids.items()
                if s[0] != EventTypes.Member
            }
            txn.call_after(
                self._state_group_cache.update,
                self._state_group_cache.sequence,
                key=state_group,
                value=current_non_member_state_ids,
            )

        return self._persist_state_hamt_txn(
            txn,
            state_group,
            room_id,
            room_prefix,
            current_state_ids,
            prev_state_group=prev_group,
            updates=updates,
            local_nodes=local_nodes,
            local_roots=local_roots,
        )

    @trace
    @tag_args
    async def store_state_deltas_for_batched(
        self,
        events_and_context: list[tuple[EventBase, UnpersistedEventContext]],
        room_id: str,
        prev_group: int,
    ) -> list[tuple[EventBase, UnpersistedEventContext]]:
        """Generate and store state groups for a batch of events.

        Note that all the events must be in a linear chain (ie a <- b <- c).

        Args:
            events_and_context: the events to generate and store a state groups for
            and their associated contexts
            room_id: the id of the room the events were created for
            prev_group: the state group of the last event persisted before the batched events
            were created
        """

        # All events in the batch are in the same room (and hence share the
        # same, immutable room_version) -- see the linear-chain requirement
        # above. Read it off the first event rather than looking it up by
        # room_id, for the same reason as store_state_group: no DB read, no
        # race against a `rooms` row that may not be visible on this
        # connection yet.
        room_version = events_and_context[0][0].room_version

        from synapse.synapse_rust import state_hamt

        room_prefix = state_hamt.room_hamt_prefix(
            room_id,
            room_version.msc4291_room_ids_as_hashes,
        )

        # Each group's incremental update looks up its predecessor via
        # _get_embedded_hamt_node (mdbx, if configured) then SQL -- no
        # prefetch needed before the transaction starts; both are local
        # reads, unlike a real network round-trip would require.
        initial_nodes: dict[bytes, bytes] = {}
        initial_roots: dict[int, tuple[bytes, bytes]] = {}

        def insert_deltas_group_txn(
            txn: LoggingTransaction,
            events_and_context: list[tuple[EventBase, UnpersistedEventContext]],
            prev_group: int,
        ) -> tuple[
            list[tuple[EventBase, UnpersistedEventContext]],
            list[tuple[int, bytes, bytes, list[tuple[bytes, bytes]]]],
        ]:
            """Generate and store state groups for the provided events and contexts.

            Requires that we have the state as a delta from the last persisted state group.

            Returns:
                A list of state groups
            """

            # We need to check that the prev group isn't about to be deleted
            is_missing = (
                self._state_deletion_store._check_state_groups_and_bump_deletion_txn(
                    txn,
                    {prev_group},
                )
            )
            if is_missing:
                raise Exception(
                    "Trying to persist state with unpersisted prev_group: %r"
                    % (prev_group,)
                )

            num_state_groups = sum(
                1 for event, _ in events_and_context if event.is_state()
            )

            state_groups = self._state_group_seq_gen.get_next_mult_txn(
                txn, num_state_groups
            )

            sg_before = prev_group
            state_group_iter = iter(state_groups)
            hamt_writes: list[tuple[int, bytes, bytes, list[tuple[bytes, bytes]]]] = []
            # Nodes for state groups persisted earlier *in this same batch*
            # aren't visible to a fresh SQL/mdbx lookup mid-transaction (SQL
            # rows aren't visible to other reads in the same txn until
            # commit, and mdbx isn't written until _store_state_hamt_nodes_txn
            # runs for that row either) -- so a later group's incremental
            # update needs this in-memory cache to find its predecessor's
            # root.
            local_nodes = dict(initial_nodes)
            local_roots = dict(initial_roots)

            for event, context in events_and_context:
                if not event.is_state():
                    context.state_group_after_event = sg_before
                    context.state_group_before_event = sg_before
                    continue

                sg_after = next(state_group_iter)
                context.state_group_after_event = sg_after
                context.state_group_before_event = sg_before
                context.state_delta_due_to_event = {
                    (event.type, event.state_key): event.event_id
                }
                root_hash, lattice, nodes = self._persist_state_group_snapshot_txn(
                    txn,
                    sg_after,
                    room_id,
                    room_prefix,
                    event.event_id,
                    None,
                    prev_group=sg_before,
                    # A linear batch changes exactly one (type, state_key)
                    # per state event -- this is the delta
                    # _persist_state_hamt_txn needs to try an O(log S)
                    # incremental update against sg_before's HAMT root
                    # instead of rebuilding from all of current_state_ids.
                    updates=[(event.type, event.state_key, event.event_id)],
                    local_nodes=local_nodes,
                    local_roots=local_roots,
                )
                hamt_writes.append((sg_after, root_hash, lattice, nodes))
                # Only keep the root node in the local cache for the next iteration.
                # Child nodes are fetched via SQL/mdbx in the retry loop if needed.
                local_nodes.clear()
                # nodes is a list of (hash, bytes) tuples; find the root node entry
                for node_hash, node_bytes in nodes:
                    if node_hash == root_hash:
                        local_nodes[root_hash] = node_bytes
                        break
                local_roots[sg_after] = (root_hash, lattice)
                sg_before = sg_after

            return events_and_context, hamt_writes

        # Both SQL and (if configured) the embedded engine were already
        # written synchronously for every group in insert_deltas_group_txn
        # (via _persist_state_group_snapshot_txn -> _persist_state_hamt_txn /
        # _persist_state_hamt_incremental_txn), so there's nothing left to
        # publish post-commit here.
        events_and_context, _hamt_writes = await self.db_pool.runInteraction(
            "store_state_deltas_for_batched.insert_deltas_group",
            insert_deltas_group_txn,
            events_and_context,
            prev_group,
        )

        return events_and_context

    @trace
    @tag_args
    async def store_state_group(
        self,
        event_id: str,
        room_id: str,
        room_version: RoomVersion,
        prev_group: int | None,
        delta_ids: StateMap[str] | None,
        current_state_ids: StateMap[str] | None,
    ) -> int:
        """Store a new state snapshot, returning a newly assigned state group.

        At least one of `current_state_ids` and `prev_group` must be provided.

        Args:
            event_id: The event ID for which the state was calculated
            room_id
            room_version: The version of the room `room_id` is in. Passed
                explicitly rather than looked up, to avoid a lookup that can
                race the `rooms` row for a room not yet visible here -- see
                _put_state_hamt_objects_after_txn.
            prev_group: A previous state group for the room.
            delta_ids: The delta between state at `prev_group` and
                `current_state_ids`, if `prev_group` was given. Same format as
                `current_state_ids`.
            current_state_ids: The state to store. Map of (type, state_key)
                to event_id.

        Returns:
            The state group ID
        """
        _gg_store_start = time.monotonic()

        if prev_group is None and current_state_ids is None:
            raise Exception("current_state_ids and prev_group can't both be None")

        # `updates` is the delta from prev_group's state to current_state_ids,
        # for _persist_state_hamt_txn to apply via O(K log S) path-copying
        # instead of rebuilding the whole tree. It falls back to a full
        # rebuild inside _persist_state_hamt_txn regardless if prev_group has
        # no usable stored root+lattice (e.g. the room's first state group).
        updates: list[tuple[str, str, str]] | None = None

        if current_state_ids is None:
            assert prev_group is not None
            assert delta_ids is not None
            groups = await self._get_state_for_groups([prev_group])
            current_state_ids = dict(groups[prev_group])
            current_state_ids.update(delta_ids)
            # delta_ids already *is* the delta here -- no need to diff.
            updates = [
                (event_type, state_key, event_id)
                for (event_type, state_key), event_id in delta_ids.items()
            ]
        elif prev_group is not None:
            if delta_ids is None:
                raise ValueError(
                    "A state-group delta is required when prev_group is provided"
                )
            updates = [
                (event_type, state_key, event_id)
                for (event_type, state_key), event_id in delta_ids.items()
            ]

        from synapse.synapse_rust import state_hamt

        room_prefix = state_hamt.room_hamt_prefix(
            room_id,
            room_version.msc4291_room_ids_as_hashes,
        )

        initial_nodes: dict[bytes, bytes] = {}
        initial_roots: dict[int, tuple[bytes, bytes]] = {}

        def insert_full_state_txn(
            txn: LoggingTransaction, current_state_ids: StateMap[str]
        ) -> tuple[int, bytes, bytes, list[tuple[bytes, bytes]]]:
            if prev_group is not None:
                is_missing = self._state_deletion_store._check_state_groups_and_bump_deletion_txn(
                    txn,
                    {prev_group},
                )
                if is_missing:
                    raise Exception(
                        "Trying to persist state with unpersisted prev_group: %r"
                        % (prev_group,)
                    )

            state_group = self._state_group_seq_gen.get_next_id_txn(txn)
            root_structural_hash, lattice, nodes = (
                self._persist_state_group_snapshot_txn(
                    txn,
                    state_group,
                    room_id,
                    room_prefix,
                    event_id,
                    current_state_ids,
                    prev_group=prev_group,
                    updates=updates,
                    local_nodes=initial_nodes,
                    local_roots=initial_roots,
                )
            )

            return state_group, root_structural_hash, lattice, nodes

        # Both SQL and (if configured) the embedded engine were already
        # written synchronously in insert_full_state_txn -- nothing left to
        # publish post-commit.
        state_group, _root_hash, _lattice, _nodes = await self.db_pool.runInteraction(
            "store_state_group.insert_full_state",
            insert_full_state_txn,
            current_state_ids,
        )

        logger.debug(
            "[gg-state-timing] store_state_group group=%d elapsed_ms=%.1f",
            state_group,
            (time.monotonic() - _gg_store_start) * 1000,
        )
        return state_group

    async def purge_unreferenced_state_groups(
        self,
        room_id: str,
        state_groups_to_sequence_numbers: Mapping[int, int],
    ) -> bool:
        """Deletes no longer referenced state groups and de-deltas any state
        groups that reference them.

        Args:
            room_id: The room the state groups belong to (must all be in the
                same room).
            state_groups_to_delete: Set of all state groups to delete.

        Returns:
            Whether any state groups were actually deleted.
        """

        deleted, state_groups = await self.db_pool.runInteraction(
            "purge_unreferenced_state_groups",
            self._purge_unreferenced_state_groups,
            room_id,
            state_groups_to_sequence_numbers,
        )
        if self.embedded_hamt_engine == "mdbx" and state_groups:
            from synapse.synapse_rust import mdbx_engine

            await defer_to_thread(
                self.hs.get_reactor(),
                mdbx_engine.batch_delete,
                [
                    _state_hamt_root_key(self.hamt_namespace, int(state_group))
                    for state_group in state_groups
                ],
            )
        return deleted

    def _purge_unreferenced_state_groups(
        self,
        txn: LoggingTransaction,
        room_id: str,
        state_groups_to_sequence_numbers: Mapping[int, int],
    ) -> tuple[bool, set[int]]:
        state_groups_to_delete = self._state_deletion_store.get_state_groups_ready_for_potential_deletion_txn(
            txn, state_groups_to_sequence_numbers
        )

        if not state_groups_to_delete:
            return False, set()

        logger.info(
            "[purge] found %i state groups to delete", len(state_groups_to_delete)
        )

        rows = cast(
            list[tuple[int]],
            self.db_pool.simple_select_many_txn(
                txn,
                table="state_group_edges",
                column="prev_state_group",
                iterable=state_groups_to_delete,
                keyvalues={},
                retcols=("state_group",),
            ),
        )

        remaining_state_groups = {
            state_group
            for (state_group,) in rows
            if state_group not in state_groups_to_delete
        }

        logger.info(
            "[purge] de-delta-ing %i remaining state groups",
            len(remaining_state_groups),
        )

        # Now we turn the state groups that reference to-be-deleted state
        # groups to non delta versions.
        for sg in remaining_state_groups:
            logger.info("[purge] de-delta-ing remaining state group %s", sg)
            curr_state_by_group = self._get_state_groups_from_groups_txn(txn, [sg])
            curr_state = curr_state_by_group[sg]

            self.db_pool.simple_delete_txn(
                txn, table="state_groups_state", keyvalues={"state_group": sg}
            )

            self.db_pool.simple_delete_txn(
                txn, table="state_group_edges", keyvalues={"state_group": sg}
            )

            self.db_pool.simple_insert_many_txn(
                txn,
                table="state_groups_state",
                keys=("state_group", "room_id", "type", "state_key", "event_id"),
                values=[
                    (sg, room_id, key[0], key[1], state_id)
                    for key, state_id in curr_state.items()
                ],
            )

        logger.info("[purge] removing redundant state groups")
        txn.execute_batch(
            "DELETE FROM state_groups_state WHERE state_group = ?",
            [(sg,) for sg in state_groups_to_delete],
        )
        txn.execute_batch(
            "DELETE FROM state_group_edges WHERE state_group = ?",
            [(sg,) for sg in state_groups_to_delete],
        )
        txn.execute_batch(
            "DELETE FROM state_groups WHERE id = ?",
            [(sg,) for sg in state_groups_to_delete],
        )
        txn.execute_batch(
            "DELETE FROM state_groups_pending_deletion WHERE state_group = ?",
            [(sg,) for sg in state_groups_to_delete],
        )

        # Delete the SQL root pointer -- a no-op when the embedded engine is
        # exclusively configured (state_hamt_roots was never written for
        # this group; see _persist_state_hamt_txn), harmless either way.
        # The `state_hamt_nodes`/embedded-engine node objects themselves are
        # content-addressed and may be shared by other, still-live roots, so
        # they are intentionally retained rather than reference-counted/
        # GC'd here. This trades some unreachable node storage for avoiding
        # an unsafe delete of a node another root still points to.
        txn.execute_batch(
            "DELETE FROM state_hamt_roots WHERE state_group = ?",
            [(sg,) for sg in state_groups_to_delete],
        )

        return True, set(state_groups_to_delete)

    @trace
    @tag_args
    async def get_previous_state_groups(
        self, state_groups: Iterable[int]
    ) -> dict[int, int]:
        """Fetch the previous groups of the given state groups.

        Args:
            state_groups

        Returns:
            A mapping from state group to previous state group.
        """

        rows = cast(
            list[tuple[int, int]],
            await self.db_pool.simple_select_many_batch(
                table="state_group_edges",
                column="state_group",
                iterable=state_groups,
                keyvalues={},
                retcols=("state_group", "prev_state_group"),
                desc="get_previous_state_groups",
            ),
        )

        return dict(rows)

    @trace
    @tag_args
    async def get_next_state_groups(
        self, state_groups: Iterable[int]
    ) -> dict[int, int]:
        """Fetch the groups that have the given state groups as their previous
        state groups.

        Args:
            state_groups

        Returns:
            A mapping from state group to previous state group.
        """

        rows = cast(
            list[tuple[int, int]],
            await self.db_pool.simple_select_many_batch(
                table="state_group_edges",
                column="prev_state_group",
                iterable=state_groups,
                keyvalues={},
                retcols=("state_group", "prev_state_group"),
                desc="get_next_state_groups",
            ),
        )

        return dict(rows)

    async def purge_room_state(self, room_id: str) -> None:
        await self.db_pool.runInteraction(
            "purge_room_state",
            self._purge_room_state_txn,
            room_id,
        )
        if self.embedded_hamt_engine == "mdbx":
            await self._drain_embedded_state_hamt_root_deletion_queue()

    @wrap_as_background_process("drain_embedded_state_hamt_root_deletion_queue")
    async def _drain_embedded_state_hamt_root_deletion_queue(self) -> None:
        """Delete queued embedded-engine roots after their SQL state groups
        are purged."""

        if self.embedded_hamt_engine != "mdbx":
            return

        from synapse.synapse_rust import mdbx_engine

        while True:

            def get_batch_txn(txn: LoggingTransaction) -> list[int]:
                txn.execute(
                    """
                    SELECT state_group
                    FROM state_hamt_root_deletion_queue
                    LIMIT 500
                    """
                )
                return [int(row[0]) for row in txn]

            state_groups = await self.db_pool.runInteraction(
                "get_embedded_state_hamt_root_deletion_queue_batch", get_batch_txn
            )
            if not state_groups:
                return

            try:
                await defer_to_thread(
                    self.hs.get_reactor(),
                    mdbx_engine.batch_delete,
                    [
                        _state_hamt_root_key(self.hamt_namespace, state_group)
                        for state_group in state_groups
                    ],
                )
                await self.db_pool.simple_delete_many(
                    table="state_hamt_root_deletion_queue",
                    column="state_group",
                    iterable=state_groups,
                    keyvalues={},
                    desc="remove_embedded_state_hamt_root_deletion_queue_batch",
                )
            except Exception:
                # The IDs remain durably queued for the periodic retry, so a
                # transient embedded-engine failure cannot leave
                # directly-readable stale roots forever.
                logger.warning(
                    "Failed to clean up %d queued embedded HAMT roots; will retry",
                    len(state_groups),
                    exc_info=True,
                )
                return

    def _purge_room_state_txn(
        self,
        txn: LoggingTransaction,
        room_id: str,
    ) -> list[int]:
        if isinstance(self.database_engine, PostgresEngine):
            # Disable statement timeouts for this transaction; purging rooms can
            # take a while!
            txn.execute("SET LOCAL statement_timeout = 0")

        # 1. Delete state_groups first and capture the exact IDs deleted.
        # Using RETURNING here (rather than a prior SELECT or a separate room_id
        # predicate later) ensures atomicity: under PostgreSQL READ COMMITTED,
        # a concurrent insertion that commits before this statement runs will be
        # included, while one that commits after will not be touched.
        logger.info("[purge] removing %s from state_groups", room_id)
        txn.execute(
            "DELETE FROM state_groups WHERE room_id = ? RETURNING id",
            (room_id,),
        )
        deleted_state_groups: list[int] = [row[0] for row in txn.fetchall()]

        if not deleted_state_groups:
            return []

        if self.embedded_hamt_engine == "mdbx":
            # Persist a retry record in the same transaction as the SQL purge.
            # If the embedded engine write fails after commit, its root keys
            # can still be removed on a later retry instead of being lost
            # with these IDs.
            txn.execute_batch(
                """
                INSERT INTO state_hamt_root_deletion_queue (state_group)
                VALUES (?) ON CONFLICT (state_group) DO NOTHING
                """,
                [(state_group,) for state_group in deleted_state_groups],
            )

        # 2. Delete all dependent child rows using strictly the returned IDs.
        logger.info("[purge] removing %s from state_group_edges", room_id)
        self.db_pool.simple_delete_many_txn(
            txn,
            table="state_group_edges",
            column="state_group",
            values=deleted_state_groups,
            keyvalues={},
        )

        logger.info("[purge] removing %s from state_groups_state", room_id)
        self.db_pool.simple_delete_many_txn(
            txn,
            table="state_groups_state",
            column="state_group",
            values=deleted_state_groups,
            keyvalues={},
        )

        # Delete SQL HAMT root pointers for exactly the state groups removed
        # above. In SQL mode, state_hamt_roots holds the authoritative root
        # structural hash for each group and must be cleaned up. When the
        # embedded engine is exclusively configured the table is empty for
        # these groups, so this is a no-op but is kept for correctness on
        # mode transitions. Driving the delete from the RETURNING set (not
        # an independent room_id subquery) ensures it covers the same
        # groups as the embedded-engine batch_delete above.
        logger.info("[purge] removing %s from state_hamt_roots", room_id)
        self.db_pool.simple_delete_many_txn(
            txn,
            table="state_hamt_roots",
            column="state_group",
            values=deleted_state_groups,
            keyvalues={},
        )

        logger.info("[purge] removing %s from state_groups_pending_deletion", room_id)
        self.db_pool.simple_delete_many_txn(
            txn,
            table="state_groups_pending_deletion",
            column="state_group",
            values=deleted_state_groups,
            keyvalues={},
        )

        return deleted_state_groups
