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
import datetime
import itertools
import logging
import time
from queue import Empty, PriorityQueue
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Iterable,
    Sequence,
    TypeVar,
    cast,
)

import attr
from prometheus_client import Counter, Gauge

from synapse.api.constants import MAX_DEPTH
from synapse.api.errors import StoreError
from synapse.api.room_versions import EventFormatVersions, RoomVersion
from synapse.events import EventBase, make_event_from_dict
from synapse.events.py_protocol import MSC4242Event, supports_msc4242_state_dag
from synapse.logging.opentracing import tag_args, trace
from synapse.metrics import SERVER_NAME_LABEL
from synapse.metrics.background_process_metrics import wrap_as_background_process
from synapse.storage._base import db_to_json, make_in_list_sql_clause
from synapse.storage.background_updates import ForeignKeyConstraint
from synapse.storage.database import (
    DatabasePool,
    LoggingDatabaseConnection,
    LoggingTransaction,
)
from synapse.storage.databases.main.cache import CacheInvalidationWorkerStore
from synapse.storage.databases.main.embedded_common import ffi_count, ffi_timing
from synapse.storage.databases.main.events_worker import EventsWorkerStore
from synapse.storage.databases.main.signatures import SignatureWorkerStore
from synapse.storage.engines import PostgresEngine, Sqlite3Engine
from synapse.types import JsonDict, StrCollection
from synapse.util.caches.descriptors import cached
from synapse.util.caches.lrucache import LruCache
from synapse.util.cancellation import cancellable
from synapse.util.duration import Duration
from synapse.util.iterutils import batch_iter
from synapse.util.json import json_encoder

if TYPE_CHECKING:
    from synapse.server import HomeServer

oldest_pdu_in_federation_staging = Gauge(
    "synapse_federation_server_oldest_inbound_pdu_in_staging",
    "The age in seconds since we received the oldest pdu in the federation staging area",
    labelnames=[SERVER_NAME_LABEL],
)

number_pdus_in_federation_queue = Gauge(
    "synapse_federation_server_number_inbound_pdu_in_staging",
    "The total number of events in the inbound federation staging",
    labelnames=[SERVER_NAME_LABEL],
)

pdus_pruned_from_federation_queue = Counter(
    "synapse_federation_server_number_inbound_pdu_pruned",
    "The number of events in the inbound federation staging that have been "
    "pruned due to the queue getting too long",
    labelnames=[SERVER_NAME_LABEL],
)

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# Parameters controlling exponential backoff between backfill failures.
# After the first failure to backfill, we wait 2 hours before trying again. If the
# second attempt fails, we wait 4 hours before trying again. If the third attempt fails,
# we wait 8 hours before trying again, ... and so on.
#
# Each successive backoff period is twice as long as the last. However we cap this
# period at a maximum of 2^8 = 256 hours: a little over 10 days. (This is the smallest
# power of 2 which yields a maximum backoff period of at least 7 days---which was the
# original maximum backoff period.) Even when we hit this cap, we will continue to
# make backfill attempts once every 10 days.
BACKFILL_EVENT_EXPONENTIAL_BACKOFF_MAXIMUM_DOUBLING_STEPS = 8
BACKFILL_EVENT_EXPONENTIAL_BACKOFF_STEP_MILLISECONDS = int(
    datetime.timedelta(hours=1).total_seconds() * 1000
)

# We need a cap on the power of 2 or else the backoff period
#   2^N * (milliseconds per hour)
# will overflow when calcuated within the database. We ensure overflow does not occur
# by checking that the largest backoff period fits in a 32-bit signed integer.
_LONGEST_BACKOFF_PERIOD_MILLISECONDS = (
    2**BACKFILL_EVENT_EXPONENTIAL_BACKOFF_MAXIMUM_DOUBLING_STEPS
) * BACKFILL_EVENT_EXPONENTIAL_BACKOFF_STEP_MILLISECONDS
assert 0 < _LONGEST_BACKOFF_PERIOD_MILLISECONDS <= ((2**31) - 1)


# All the info we need while iterating the DAG while backfilling
@attr.s(frozen=True, slots=True, auto_attribs=True)
class BackfillQueueNavigationItem:
    depth: int
    stream_ordering: int
    event_id: str
    type: str


@attr.s(frozen=True, slots=True, auto_attribs=True)
class StateDifference:
    # The event IDs in the auth difference.
    auth_difference: set[str]
    # The event IDs in the conflicted state subgraph. Used in v2.1 only.
    conflicted_subgraph: set[str] | None


def _auth_coverage(
    operation: str, outcome: str, seeds: int, auth_events: int | None = None
) -> None:
    """Record opt-in auth-chain path coverage for the diagnostics report."""
    ffi_count(f"auth_chain_{operation}_{outcome}_calls", 1)
    ffi_count(f"auth_chain_{operation}_{outcome}_seeds", seeds)
    if auth_events is not None:
        ffi_count(f"auth_chain_{operation}_{outcome}_auth_events", auth_events)


class EventFederationWorkerStore(
    SignatureWorkerStore, EventsWorkerStore, CacheInvalidationWorkerStore
):
    # TODO: this attribute comes from EventPushActionWorkerStore. Should we inherit from
    # that store so that mypy can deduce this for itself?
    stream_ordering_month_ago: int | None

    def __init__(
        self,
        database: DatabasePool,
        db_conn: LoggingDatabaseConnection,
        hs: "HomeServer",
    ):
        super().__init__(database, db_conn, hs)

        self.hs = hs

        if hs.config.worker.run_background_tasks:
            hs.get_clock().looping_call(
                self._delete_old_forward_extrem_cache, Duration(hours=1)
            )

        # Cache of event ID to list of auth event IDs and their depths.
        self._event_auth_cache: LruCache[str, list[tuple[str, int]]] = LruCache(
            max_size=500000,
            clock=self.hs.get_clock(),
            server_name=self.server_name,
            cache_name="_event_auth_cache",
            size_callback=len,
        )

        # RAM-only ancestor-closure cache for the embedded (mtxdb) auth-chain
        # path -- see embedded_event_auth_chains.py. Lives here (not module
        # level) so each store/worker gets its own bounded LRU.
        from synapse.storage.databases.main.embedded_event_auth_chains import (
            ClosureCache,
        )

        self._auth_chain_closure_cache = ClosureCache()

        self.clock.looping_call(
            self._get_stats_for_federation_staging, Duration(seconds=30)
        )

        if isinstance(self.database_engine, PostgresEngine):
            self.db_pool.updates.register_background_validate_constraint_and_delete_rows(
                update_name="event_forward_extremities_event_id_foreign_key_constraint_update",
                table="event_forward_extremities",
                constraint_name="event_forward_extremities_event_id",
                constraint=ForeignKeyConstraint(
                    "events", [("event_id", "event_id")], deferred=True
                ),
                unique_columns=("event_id", "room_id"),
            )

    async def get_auth_chain(
        self, room_id: str, event_ids: Collection[str], include_given: bool = False
    ) -> list[EventBase]:
        """Get auth events for given event_ids. The events *must* be state events.

        Args:
            room_id: The room the event is in.
            event_ids: state events
            include_given: include the given events in result

        Returns:
            list of events
        """
        event_ids = await self.get_auth_chain_ids(
            room_id, event_ids, include_given=include_given
        )
        return await self.get_events_as_list(event_ids)

    @trace
    @tag_args
    async def get_auth_chain_ids(
        self,
        room_id: str,
        event_ids: Collection[str],
        include_given: bool = False,
    ) -> set[str]:
        """Get auth events for given event_ids. The events *must* be state events.

        Args:
            room_id: The room the event is in.
            event_ids: state events
            include_given: include the given events in result

        Returns:
            set of event_ids
        """

        from synapse.storage.databases.main.embedded_event_auth_chains import (
            IncompleteAuthGraph,
            resolve_namespace,
        )

        embedded_db_namespace = resolve_namespace(self)
        if embedded_db_namespace is not None:
            assert self._embedded_db_engine is not None
            _auth_coverage("get_auth_chain_ids", "embedded_attempt", len(event_ids))
            try:
                result = await self._run_embedded_off_reactor(
                    "get_auth_chain_ids_embedded",
                    self._get_auth_chain_ids_using_embedded_closures,
                    embedded_db_namespace,
                    room_id,
                    event_ids,
                    include_given,
                )
            except IncompleteAuthGraph:
                raise RuntimeError(
                    "Auth-chain graph is incomplete after the mtxdb cutover"
                ) from None
            _auth_coverage(
                "get_auth_chain_ids",
                "embedded_complete",
                len(event_ids),
                len(result),
            )
            return result

        _auth_coverage("get_auth_chain_ids", "sql_only", len(event_ids))
        return await self.db_pool.runInteraction(
            "get_auth_chain_ids",
            self._get_auth_chain_ids_txn,
            event_ids,
            include_given,
        )

    async def _run_embedded_off_reactor(
        self, desc: str, func: Callable[..., _T], *args: Any
    ) -> _T:
        """Run a blocking mtxdb read on a database thread, not the reactor.

        `ClosureCache` serializes its own access, so concurrent calls are safe.
        """

        def _run(_txn: LoggingTransaction) -> _T:
            return func(*args)

        return await self.db_pool.runInteraction(desc, _run)

    def _get_auth_chain_ids_using_embedded_closures(
        self,
        embedded_db_namespace: str,
        room_id: str,
        event_ids: Collection[str],
        include_given: bool,
    ) -> set[str]:
        """Embedded-store equivalent of
        the SQL recursive traversal: union the requested events' ancestor
        closures (short-id bitmaps), resolve back to event
        ids, and apply `include_given` the same way the legacy method does
        (its `chains[chain_id] = max(seq_no - 1, ...)` excludes each given
        event's own chain position, then unions `initial_events` back in
        only when `include_given` -- i.e. the closure is ancestors-only, and
        `include_given` alone controls whether the starting set is added
        back). Raises `IncompleteAuthGraph` on a genuine data gap; the
        caller translates that to `RuntimeError`.
        """
        from synapse.storage.databases.main.embedded_event_auth_chains import (
            IncompleteAuthGraph,
            resolve_event_ids_to_short_ids,
            resolve_short_ids_to_event_ids,
        )

        initial_events = list(dict.fromkeys(event_ids))
        if not initial_events:
            return set()

        logger.debug(
            "get_auth_chain_ids: using EMBEDDED CLOSURES path for room=%s "
            "namespace=%s initial_events=%s include_given=%s",
            room_id,
            embedded_db_namespace,
            initial_events,
            include_given,
        )

        engine_name = self._embedded_db_engine
        resolved_short_ids = resolve_event_ids_to_short_ids(
            engine_name, embedded_db_namespace, room_id, initial_events
        )
        if any(short_id is None for short_id in resolved_short_ids):
            raise IncompleteAuthGraph(initial_events[0])
        short_ids = [
            short_id for short_id in resolved_short_ids if short_id is not None
        ]

        closures = self._auth_chain_closure_cache.get_closures_batch(
            engine_name,
            embedded_db_namespace,
            room_id,
            short_ids,
        )

        result_short_ids: set[int] = set()
        for short_id in short_ids:
            result_short_ids.update(closures[short_id])

        if include_given:
            result_short_ids.update(short_ids)

        if not result_short_ids:
            return set()

        resolved = resolve_short_ids_to_event_ids(
            engine_name,
            embedded_db_namespace,
            room_id,
            list(result_short_ids),
        )
        return {event_id for event_id in resolved if event_id is not None}

    def _get_auth_chain_ids_txn(
        self, txn: LoggingTransaction, event_ids: Collection[str], include_given: bool
    ) -> set[str]:
        """Calculates auth-chain reachability by traversing SQL `event_auth`."""
        logger.debug(
            "get_auth_chain_ids: using SQL auth-graph traversal "
            "for event_ids=%s include_given=%s",
            list(event_ids),
            include_given,
        )
        if include_given:
            results = set(event_ids)
        else:
            results = set()

        # We pull out the depth simply so that we can populate the
        # `_event_auth_cache` cache.
        base_sql = """
            SELECT a.event_id, auth_id, depth
            FROM event_auth AS a
            INNER JOIN events AS e ON (e.event_id = a.auth_id)
            WHERE
        """

        front = set(event_ids)
        while front:
            new_front: set[str] = set()
            for chunk in batch_iter(front, 100):
                # Pull the auth events either from the cache or DB.
                to_fetch: list[str] = []  # Event IDs to fetch from DB
                for event_id in chunk:
                    res = self._event_auth_cache.get(event_id)
                    if res is None:
                        to_fetch.append(event_id)
                    else:
                        new_front.update(auth_id for auth_id, depth in res)

                if to_fetch:
                    clause, args = make_in_list_sql_clause(
                        txn.database_engine, "a.event_id", to_fetch
                    )
                    txn.execute(base_sql + clause, args)

                    # Note we need to batch up the results by event ID before
                    # adding to the cache.
                    to_cache: dict[str, list[tuple[str, int]]] = {}
                    for event_id, auth_event_id, auth_event_depth in txn:
                        to_cache.setdefault(event_id, []).append(
                            (auth_event_id, auth_event_depth)
                        )
                        new_front.add(auth_event_id)

                    for event_id, auth_events in to_cache.items():
                        self._event_auth_cache.set(event_id, auth_events)

            new_front -= results

            front = new_front
            results.update(front)

        return results

    async def get_auth_chain_difference(
        self,
        room_id: str,
        state_sets: list[set[str]],
    ) -> set[str]:
        state_diff = await self.get_auth_chain_difference_extended(
            room_id, state_sets, None, None
        )
        return state_diff.auth_difference

    async def get_auth_chain_difference_extended(
        self,
        room_id: str,
        state_sets: list[set[str]],
        conflicted_set: set[str] | None,
        additional_backwards_reachable_conflicted_events: set[str] | None,
    ) -> StateDifference:
        """ "Given sets of state events figure out the auth chain difference (as
        per state res v2 algorithm).

        This is equivalent to fetching the full auth chain for each set of state
        and returning the events that don't appear in each and every auth
        chain.

        If conflicted_set is not None, calculate and return the conflicted sub-graph as per
        state res v2.1. The event IDs in the conflicted set MUST be a subset of the event IDs in
        state_sets.

        If additional_backwards_reachable_conflicted_events is set, the provided events are included
        when calculating the conflicted subgraph. This is primarily useful for calculating the
        subgraph across a combination of persisted and unpersisted events. The event IDs in this set
        MUST be a subset of the event IDs in state_sets.

        Returns:
            information on the auth chain difference, and also the conflicted subgraph if
            conflicted_set is not None
        """

        from synapse.storage.databases.main.embedded_event_auth_chains import (
            IncompleteAuthGraph,
            resolve_namespace,
        )

        embedded_db_namespace = resolve_namespace(self)
        if embedded_db_namespace is not None:
            assert self._embedded_db_engine is not None
            _auth_coverage(
                "get_auth_chain_difference",
                "embedded_attempt",
                sum(len(state_set) for state_set in state_sets),
            )
            try:
                result = await self._run_embedded_off_reactor(
                    "get_auth_chain_difference_embedded",
                    self._get_auth_chain_difference_using_embedded_closures,
                    embedded_db_namespace,
                    room_id,
                    state_sets,
                    conflicted_set,
                    additional_backwards_reachable_conflicted_events,
                )
            except IncompleteAuthGraph:
                raise RuntimeError(
                    "Auth-chain graph is incomplete after the mtxdb cutover"
                ) from None
            _auth_coverage(
                "get_auth_chain_difference",
                "embedded_complete",
                sum(len(state_set) for state_set in state_sets),
                len(result.auth_difference),
            )
            return result

        # The SQL backend has no chain cover index any more. State res v2.1
        # computes the conflicted subgraph directly from `event_auth` instead.
        if conflicted_set is not None:
            return await self.db_pool.runInteraction(
                "get_auth_chain_difference_sql_v21",
                self._get_auth_chain_difference_sql_v21_txn,
                state_sets,
                conflicted_set,
                additional_backwards_reachable_conflicted_events,
            )

        _auth_coverage(
            "get_auth_chain_difference",
            "sql_only",
            sum(len(state_set) for state_set in state_sets),
        )
        auth_diff = await self.db_pool.runInteraction(
            "get_auth_chain_difference",
            self._get_auth_chain_difference_txn,
            state_sets,
        )
        return StateDifference(auth_difference=auth_diff, conflicted_subgraph=None)

    def _get_auth_chain_difference_using_embedded_closures(
        self,
        embedded_db_namespace: str,
        room_id: str,
        state_sets: list[set[str]],
        conflicted_set: set[str] | None = None,
        additional_backwards_reachable_conflicted_events: set[str] | None = None,
    ) -> StateDifference:
        """Embedded-store auth difference, computed from the ancestor-closure
        bitmaps in
        `embedded_event_auth_chains.py` instead of the chain-id/sequence-number
        cover-index reduction.

        `auth_difference` follows the spec definition directly -- the union of
        each state set's combined auth chain, minus the events common to all of
        them, where a combined auth chain is *inclusive* of the set's own
        members (matching the cover-index path and the legacy BFS, both of
        which seed the given events into the result when their ownership
        differs across sets). Each state set's combined closure is the union of
        its members' ancestor closures plus the members themselves.

        `conflicted_subgraph` (v2.1 only) is `backwards ∩ forwards`, with both
        sides *inclusive* of the conflicted events. Backwards is the union of the
        conflicted and additional events' inclusive ancestor closures; forwards
        is the seed-inclusive forward BFS (`get_forward_reachable_short_ids`)
        *pruned to the backwards set* -- a descendant outside the backwards
        universe provably can't be in the intersection, which is what bounds
        the walk (it may not touch data the conflicted-set computation doesn't
        need). Raises `IncompleteAuthGraph` on a genuine data gap; the caller
        translates that to `RuntimeError`.
        """
        from synapse.storage.databases.main.embedded_event_auth_chains import (
            IncompleteAuthGraph,
            get_forward_reachable_short_ids,
            resolve_event_ids_to_short_ids,
            resolve_short_ids_to_event_ids,
        )

        engine_name = self._embedded_db_engine

        is_state_res_v21 = conflicted_set is not None
        initial_events = set(state_sets[0]).union(*state_sets[1:])

        if is_state_res_v21:
            # Sanity check v2.1 fields -- mirroring the cover-index method.
            assert conflicted_set is not None
            assert conflicted_set.issubset(initial_events)
            if additional_backwards_reachable_conflicted_events:
                assert additional_backwards_reachable_conflicted_events.issubset(
                    initial_events
                )

        if not initial_events:
            conflicted_subgraph: set[str] = set()
            if is_state_res_v21:
                assert conflicted_set is not None
                conflicted_subgraph = conflicted_set
            return StateDifference(
                auth_difference=set(), conflicted_subgraph=conflicted_subgraph
            )

        event_ids = list(initial_events)
        resolved_short_ids = resolve_event_ids_to_short_ids(
            engine_name, embedded_db_namespace, room_id, event_ids
        )
        if any(short_id is None for short_id in resolved_short_ids):
            missing = event_ids[resolved_short_ids.index(None)]
            raise IncompleteAuthGraph(missing)
        short_ids = [
            short_id for short_id in resolved_short_ids if short_id is not None
        ]
        short_id_of = dict(zip(event_ids, short_ids))

        # State-set closures: union of each set's members' ancestor closures,
        # *plus the members themselves* -- auth-chain-difference semantics are
        # inclusive on the given events, exactly like the cover-index path
        # (a chain reachable at seq n by one set but un-reachable from another
        # defaults that set's max to 0, so the gap spans `(0, n]` and pulls the
        # member's own position into the result) and the legacy BFS (initial
        # events are seeded into `event_to_missing_sets`).
        set_closures: list[set[int]] = []
        for state_set in state_sets:
            member_short_ids = [short_id_of[event_id] for event_id in state_set]
            closures = self._auth_chain_closure_cache.get_closures_batch(
                engine_name,
                embedded_db_namespace,
                room_id,
                member_short_ids,
            )
            union: set[int] = set(member_short_ids)
            for closure in closures.values():
                union.update(closure)
            set_closures.append(union)

        all_closures = set().union(*set_closures) if set_closures else set()
        common_closures = set.intersection(*set_closures) if set_closures else set()
        auth_difference_ids = all_closures - common_closures

        conflicted_subgraph_ids: set[int] = set()
        if is_state_res_v21:
            assert conflicted_set is not None
            conflicted_short_ids = [
                short_id_of[event_id] for event_id in conflicted_set
            ]

            # Inclusive backwards-reachable universe: the conflicted events
            # themselves, their ancestor closures, plus the additional
            # backwards-reachable events and theirs.
            backwards_ids: set[int] = set(conflicted_short_ids)
            for closure in self._auth_chain_closure_cache.get_closures_batch(
                engine_name,
                embedded_db_namespace,
                room_id,
                conflicted_short_ids,
            ).values():
                backwards_ids.update(closure)
            if additional_backwards_reachable_conflicted_events:
                additional_short_ids = [
                    short_id_of[event_id]
                    for event_id in additional_backwards_reachable_conflicted_events
                ]
                backwards_ids.update(additional_short_ids)
                for closure in self._auth_chain_closure_cache.get_closures_batch(
                    engine_name,
                    embedded_db_namespace,
                    room_id,
                    additional_short_ids,
                ).values():
                    backwards_ids.update(closure)

            # Inclusive forwards (seed-inclusive BFS), pruned to the backwards
            # universe so the walk cost is bounded by |backwards|, not |room|.
            forwards_ids = get_forward_reachable_short_ids(
                engine_name,
                embedded_db_namespace,
                room_id,
                conflicted_short_ids,
                candidate_short_ids=backwards_ids,
            )
            conflicted_subgraph_ids = backwards_ids & forwards_ids

        def resolve(short_id_iter: "Collection[int]") -> set[str]:
            if not short_id_iter:
                return set()
            resolved = resolve_short_ids_to_event_ids(
                engine_name,
                embedded_db_namespace,
                room_id,
                list(short_id_iter),
            )
            return {event_id for event_id in resolved if event_id is not None}

        if is_state_res_v21:
            return StateDifference(
                auth_difference=resolve(auth_difference_ids),
                conflicted_subgraph=resolve(conflicted_subgraph_ids),
            )
        return StateDifference(
            auth_difference=resolve(auth_difference_ids),
            conflicted_subgraph=None,
        )

    def _get_auth_chain_difference_sql_v21_txn(
        self,
        txn: LoggingTransaction,
        state_sets: list[set[str]],
        conflicted_set: set[str],
        additional_backwards_reachable_conflicted_events: set[str] | None,
    ) -> StateDifference:
        """SQL v2.1 auth difference without the retired chain cover.

        Backwards is the inclusive ancestor closure of the conflicted and
        additional events; forwards is an ephemeral BFS over `event_auth`
        restricted to that closure. The conflicted subgraph is their
        intersection, matching the mtxdb closure path.
        """
        auth_difference = self._get_auth_chain_difference_txn(txn, state_sets)

        backwards = self._get_auth_chain_ids_txn(
            txn, conflicted_set, include_given=True
        )
        if additional_backwards_reachable_conflicted_events:
            backwards |= self._get_auth_chain_ids_txn(
                txn,
                additional_backwards_reachable_conflicted_events,
                include_given=True,
            )

        forwards: set[str] = set(conflicted_set)
        front = set(conflicted_set)
        while front:
            new_front: set[str] = set()
            for chunk in batch_iter(front, 100):
                clause, args = make_in_list_sql_clause(
                    txn.database_engine, "auth_id", chunk
                )
                txn.execute(
                    "SELECT event_id FROM event_auth WHERE " + clause,
                    args,
                )
                new_front.update(
                    event_id for (event_id,) in txn if event_id in backwards
                )
            new_front -= forwards
            forwards.update(new_front)
            front = new_front

        return StateDifference(
            auth_difference=auth_difference,
            conflicted_subgraph=backwards & forwards,
        )

    def _get_auth_chain_difference_txn(
        self, txn: LoggingTransaction, state_sets: list[set[str]]
    ) -> set[str]:
        """Calculates auth-chain difference using SQL graph traversals."""

        # Algorithm Description
        # ~~~~~~~~~~~~~~~~~~~~~
        #
        # The idea here is to basically walk the auth graph of each state set in
        # tandem, keeping track of which auth events are reachable by each state
        # set. If we reach an auth event we've already visited (via a different
        # state set) then we mark that auth event and all ancestors as reachable
        # by the state set. This requires that we keep track of the auth chains
        # in memory.
        #
        # Doing it in a such a way means that we can stop early if all auth
        # events we're currently walking are reachable by all state sets.
        #
        # *Note*: We can't stop walking an event's auth chain if it is reachable
        # by all state sets. This is because other auth chains we're walking
        # might be reachable only via the original auth chain. For example,
        # given the following auth chain:
        #
        #       A -> C -> D -> E
        #           /         /
        #       B -´---------´
        #
        # and state sets {A} and {B} then walking the auth chains of A and B
        # would immediately show that C is reachable by both. However, if we
        # stopped at C then we'd only reach E via the auth chain of B and so E
        # would erroneously get included in the returned difference.
        #
        # The other thing that we do is limit the number of auth chains we walk
        # at once, due to practical limits (i.e. we can only query the database
        # with a limited set of parameters). We pick the auth chains we walk
        # each iteration based on their depth, in the hope that events with a
        # lower depth are likely reachable by those with higher depths.
        #
        # We could use any ordering that we believe would give a rough
        # topological ordering, e.g. origin server timestamp. If the ordering
        # chosen is not topological then the algorithm still produces the right
        # result, but perhaps a bit more inefficiently. This is why it is safe
        # to use "depth" here.

        initial_events = set(state_sets[0]).union(*state_sets[1:])

        # Dict from events in auth chains to which sets *cannot* reach them.
        # I.e. if the set is empty then all sets can reach the event.
        event_to_missing_sets = {
            event_id: {i for i, a in enumerate(state_sets) if event_id not in a}
            for event_id in initial_events
        }

        # The sorted list of events whose auth chains we should walk.
        search: list[tuple[int, str]] = []

        # We need to get the depth of the initial events for sorting purposes.
        sql = """
            SELECT depth, event_id FROM events
            WHERE %s
        """
        # the list can be huge, so let's avoid looking them all up in one massive
        # query.
        for batch in batch_iter(initial_events, 1000):
            clause, args = make_in_list_sql_clause(
                txn.database_engine, "event_id", batch
            )
            txn.execute(sql % (clause,), args)

            # I think building a temporary list with fetchall is more efficient than
            # just `search.extend(txn)`, but this is unconfirmed
            search.extend(cast(list[tuple[int, str]], txn.fetchall()))

        # sort by depth
        search.sort()

        # Map from event to its auth events
        event_to_auth_events: dict[str, set[str]] = {}

        base_sql = """
            SELECT a.event_id, auth_id, depth
            FROM event_auth AS a
            INNER JOIN events AS e ON (e.event_id = a.auth_id)
            WHERE
        """

        while search:
            # Check whether all our current walks are reachable by all state
            # sets. If so we can bail.
            if all(not event_to_missing_sets[eid] for _, eid in search):
                break

            # Fetch the auth events and their depths of the N last events we're
            # currently walking, either from cache or DB.
            search, chunk = search[:-100], search[-100:]

            found: list[tuple[str, str, int]] = []  # Results found
            to_fetch: list[str] = []  # Event IDs to fetch from DB
            for _, event_id in chunk:
                res = self._event_auth_cache.get(event_id)
                if res is None:
                    to_fetch.append(event_id)
                else:
                    found.extend((event_id, auth_id, depth) for auth_id, depth in res)

            if to_fetch:
                clause, args = make_in_list_sql_clause(
                    txn.database_engine, "a.event_id", to_fetch
                )
                txn.execute(base_sql + clause, args)

                # We parse the results and add the to the `found` set and the
                # cache (note we need to batch up the results by event ID before
                # adding to the cache).
                to_cache: dict[str, list[tuple[str, int]]] = {}
                for event_id, auth_event_id, auth_event_depth in txn:
                    to_cache.setdefault(event_id, []).append(
                        (auth_event_id, auth_event_depth)
                    )
                    found.append((event_id, auth_event_id, auth_event_depth))

                for event_id, auth_events in to_cache.items():
                    self._event_auth_cache.set(event_id, auth_events)

            for event_id, auth_event_id, auth_event_depth in found:
                event_to_auth_events.setdefault(event_id, set()).add(auth_event_id)

                sets = event_to_missing_sets.get(auth_event_id)
                if sets is None:
                    # First time we're seeing this event, so we add it to the
                    # queue of things to fetch.
                    search.append((auth_event_depth, auth_event_id))

                    # Assume that this event is unreachable from any of the
                    # state sets until proven otherwise
                    sets = event_to_missing_sets[auth_event_id] = set(
                        range(len(state_sets))
                    )
                else:
                    # We've previously seen this event, so look up its auth
                    # events and recursively mark all ancestors as reachable
                    # by the current event's state set.
                    a_ids = event_to_auth_events.get(auth_event_id)
                    while a_ids:
                        new_aids = set()
                        for a_id in a_ids:
                            event_to_missing_sets[a_id].intersection_update(
                                event_to_missing_sets[event_id]
                            )

                            b = event_to_auth_events.get(a_id)
                            if b:
                                new_aids.update(b)

                        a_ids = new_aids

                # Mark that the auth event is reachable by the appropriate sets.
                sets.intersection_update(event_to_missing_sets[event_id])

            search.sort()

        # Return all events where not all sets can reach them.
        return {eid for eid, n in event_to_missing_sets.items() if n}

    async def get_state_dag(
        self, room_id: str, forward_extrems: set[str]
    ) -> dict[str, MSC4242Event]:
        """Get the current state DAG for the given room.

        This function is called when calculating a /send_join response.
        This does not check that the room is an state DAG room, so check this
        before calling this function!

        This functions guarantees that the returned state DAG is connected.

        Args:
            room_id: The room to get the state dag for
            forward_extrems: latest event IDs in the room. The state DAG is all events reachable from these events.
        Returns:
            A map of event_id => event
        """

        def _get_state_events_txn(txn: LoggingTransaction, room_id: str) -> list[str]:
            sql = """
                SELECT event_id FROM msc4242_state_dag_edges WHERE room_id = ?
            """
            txn.execute(sql, (room_id,))
            event_ids = [ev_id for (ev_id,) in txn]
            return event_ids

        # Pull out all state events for this room using the events_by_room_and_type index.
        event_ids = await self.db_pool.runInteraction(
            "_get_state_events_txn",
            _get_state_events_txn,
            room_id,
        )
        event_map = await self.get_events(event_ids)
        # Filter the returned state events to only include ones on the paths back from the forward
        # extremities.
        result: dict[str, MSC4242Event] = {}
        next_ids = forward_extrems
        seen: set[str] = set()
        while len(next_ids) > 0:
            # Pull the event and add the prev_state_events.
            # We must have the event.
            event_id = next_ids.pop()
            if event_id in seen:
                continue
            seen.add(event_id)
            ev = event_map[event_id]
            # `prev_state_events` only exists on MSC4242 event formats, and this is only
            # called for state DAG rooms.
            assert supports_msc4242_state_dag(ev)
            result[event_id] = ev
            for prev_state_event_id in ev.prev_state_events:
                next_ids.add(prev_state_event_id)

        assert len(result) > 0  # we always return the forward extremities
        # Assert that the create event was returned. Pick the first event (any will do) to verify
        # that this room version supports room IDs as hashes.
        first_event: MSC4242Event = next(iter(result.values()))
        if first_event.room_version.msc4291_room_ids_as_hashes:
            create_event_id = f"${room_id[1:]}"
            assert create_event_id in result

        return result

    async def get_missing_events_state_dag(
        self,
        *,
        room_id: str,
        earliest_event_ids: list[str],
        latest_event_ids: list[str],
        limit: int,
    ) -> list[EventBase]:
        """Get parts of the state DAG in response to a /get_missing_events query.

        Args:
            room_id: The state DAG to look at
            earliest_event_ids: Which events the caller has seen. These events will not be returned.
            latest_event_ids: Which events to start walking back from via prev_state_events.
            limit: The max number of events to return.
        Returns:
            A list of events, deterministically ordered according to MSC4242.
        """
        ids = await self.db_pool.runInteraction(
            "get_missing_events_state_dag",
            self._get_missing_events_state_dag_txn,
            room_id,
            earliest_event_ids,
            latest_event_ids,
            limit,
        )
        return await self.get_events_as_list(ids)

    def _get_missing_events_state_dag_txn(
        self,
        txn: LoggingTransaction,
        room_id: str,
        earliest_event_ids: list[str],
        latest_event_ids: list[str],
        limit: int,
    ) -> list[str]:
        """Walk the state DAG backward from `latest_event_ids`, stopping at
        `limit` results or the beginning of the DAG, whichever comes first.

        Earliest events are treated as already-visited: they are not emitted,
        and their predecessors are not traversed via them.

        Results are deterministic and ordered by breadth-first search (BFS), with lexicographic
        tie-breaking among siblings at the same hops away.

        Executes as a single recursive CTE in both SQLite and Postgres.
        """
        earliest_set = set(earliest_event_ids)
        # Sort the seeds lexicographically by event ID: seeds are all 0 hops away from
        # themselves, so the event ID is the only tie-breaker available for them and we need
        # the walk to start from a deterministic order.
        # See https://github.com/matrix-org/matrix-spec-proposals/blob/kegan/placeholder-1/proposals/4242-state-dags.md#get_missing_events
        seed_ids = sorted(set(latest_event_ids) - earliest_set)
        if not seed_ids or limit <= 0:
            return []

        seed_clause, seed_args = make_in_list_sql_clause(
            self.database_engine, "e.event_id", seed_ids
        )

        # With no earliest events this is a no-op clause that is TRUE for every row.
        earliest_clause, earliest_args = make_in_list_sql_clause(
            self.database_engine,
            "e.prev_state_event_id",
            earliest_event_ids,
            negative=True,
        )

        query = f"""
            WITH RECURSIVE walk(event_id, hops) AS (
                SELECT
                    e.prev_state_event_id,
                    1
                FROM msc4242_state_dag_edges e
                WHERE e.room_id = ?
                AND {seed_clause}
                -- The create event has no edges, so it is stored as a single row with a NULL
                -- `prev_state_event_id`. Skipping NULLs therefore only skips that sentinel
                -- row: the create event is still returned by the walk, because it appears as
                -- the `prev_state_event_id` of the events that reference it.
                AND e.prev_state_event_id IS NOT NULL
                AND {earliest_clause}

                -- `UNION` rather than `UNION ALL`: the same (event_id, hops) pair can be
                -- reached via multiple children, and de-duplicating here stops us expanding
                -- the same row over and over.
                UNION

                SELECT
                    e.prev_state_event_id,
                    w.hops + 1
                FROM walk w
                JOIN msc4242_state_dag_edges e
                ON e.room_id = ?
                AND e.event_id = w.event_id
                WHERE e.prev_state_event_id IS NOT NULL
                AND {earliest_clause}
                -- Bound the recursion by `limit`: every extra hop adds at least one event to
                -- the result, so events more than `limit` hops away can never make it into a
                -- `limit`-sized response. This is also what makes the query safe against a
                -- cyclic edge set: a cycle cannot be created via the normal write path, but
                -- if one did exist the walk would still stop after `limit` hops rather than
                -- spinning forever.
                AND w.hops < ?
            )
            -- An event can be reached by several paths with different hop counts. MSC4242
            -- orders by distance from the seed events, which is the *shortest* such path, so
            -- collapse each event to its minimum hop count before sorting. `hops` is only
            -- selected because we need it as the primary sort key.
            SELECT event_id, MIN(hops) AS hops
            FROM walk
            GROUP BY event_id
            ORDER BY hops, event_id
            LIMIT ?
        """

        params: list = [room_id]
        params.extend(seed_args)
        params.extend(earliest_args)
        params.append(room_id)
        params.extend(earliest_args)
        params.append(limit)
        params.append(limit)

        txn.execute(query, params)
        return [row[0] for row in txn]

    def _get_backfill_points_mtxdb(
        self,
        txn: LoggingTransaction,
        room_id: str,
        nearby_depth: int,
        limit: int,
    ) -> list[tuple[str, int]]:
        """mtxdb-backed equivalent of ``get_backfill_points_in_room_txn``."""
        from synapse.storage.databases.main.embedded_event_edges import (
            get_event_edges_forward_batch,
        )

        txn.execute(
            "SELECT event_id FROM event_backward_extremities WHERE room_id = ?",
            (room_id,),
        )
        extremity_ids = [event_id for (event_id,) in txn]
        if not extremity_ids:
            return []

        forward = get_event_edges_forward_batch(
            self._embedded_db_namespace, extremity_ids
        )
        candidates = [
            (child_id, extremity_id)
            for extremity_id in extremity_ids
            for child_id in (forward.get(extremity_id) or ())
        ]
        if not candidates:
            return []

        # Chunk the IN lists to stay under SQLite's bind-parameter limit.
        child_ids = list({c for c, _ in candidates})
        depth_by_id: dict[str, int] = {}
        for start in range(0, len(child_ids), 500):
            child_clause, child_args = make_in_list_sql_clause(
                self.database_engine, "event_id", child_ids[start : start + 500]
            )
            txn.execute(
                f"SELECT event_id, depth FROM events WHERE {child_clause} AND depth <= ?",
                (*child_args, nearby_depth),
            )
            depth_by_id.update(dict(txn))

        failed_attempts: dict[str, tuple[int, int]] = {}
        for start in range(0, len(extremity_ids), 500):
            extremity_clause, extremity_args = make_in_list_sql_clause(
                self.database_engine, "event_id", extremity_ids[start : start + 500]
            )
            txn.execute(
                "SELECT event_id, num_attempts, last_attempt_ts "
                "FROM event_failed_pull_attempts "
                f"WHERE room_id = ? AND {extremity_clause}",
                (room_id, *extremity_args),
            )
            for event_id, num_attempts, last_attempt_ts in txn:
                failed_attempts[event_id] = (num_attempts, last_attempt_ts)

        now = self.clock.time_msec()
        results: list[tuple[str, int]] = []
        for child_id, extremity_id in candidates:
            depth = depth_by_id.get(child_id)
            if depth is None:
                continue
            failed = failed_attempts.get(extremity_id)
            if failed is not None:
                num_attempts, last_attempt_ts = failed
                backoff = (
                    1
                    << min(
                        num_attempts,
                        BACKFILL_EVENT_EXPONENTIAL_BACKOFF_MAXIMUM_DOUBLING_STEPS,
                    )
                ) * BACKFILL_EVENT_EXPONENTIAL_BACKOFF_STEP_MILLISECONDS
                if now < last_attempt_ts + backoff:
                    continue
            results.append((extremity_id, depth))

        results.sort(key=lambda row: (row[1], row[0]), reverse=True)
        return results[:limit]

    @trace
    @tag_args
    async def get_backfill_points_in_room(
        self,
        room_id: str,
        nearby_depth: int,
        limit: int,
    ) -> list[tuple[str, int]]:
        """
        Get the backward extremities to backfill from in the room along with the
        approximate depth.

        Only returns events that are at a depth lower than or equal to the `nearby_depth`.
        Sorted by depth, highest to lowest (descending) so the closest events to the
        `nearby_depth` are first in the list.

        ### Why `nearby_depth`?

        We find backfill points from the backward extremities in the DAG. Backward
        extremities are the oldest events we know of in the room but we only know of
        them because some other event referenced them by prev_event and aren't persisted
        in our database yet (meaning we don't know their depth specifically). So we can
        only do approximate depth comparisons (use the depth of the known events they're
        connected to). And we don't know if those backward extremities point to a long
        chain/fork of history that could stretch back far enough to be visible.

        This means a naive homeserver implementation that looks for backward extremities <=
        depth of the `/messages?dir=b&from=xxx` token may overlook a backfill point that could
        reveal more history in the window the user is currently paginating in.

        We consider "nearby" as anything within range of the number of events you plan
        to backfill from the given backfill point. This is a good heuristic as since we
        plan to backfill N events, the chain of events from a backfill point could
        extend back into the visible window.

        Example:

         - Your pagination token represents a scroll position at a depth of `100`.
         - We have a backfill point at an approximate depth of `125`
         - You plan to backfill `50` events from that backfill point.

        When we pad the token `depth` with the number of messages we plan to backfill,
        `100` + `50` = `150`, we find the backfill point at `125` (because <= `150`, our
        `nearby_depth`), backfill `50` events to a depth of `75` in the timeline
        (exposing new events that we can return `100` -> `75`).

        When we don't pad our token `depth`, `100` is lower than any of the backfill
        points so we don't pick any and miss out on backfilling any events. Without
        something like MSC3871 to indicate gaps in the timeline, clients will most
        likely never know they are missing any events and never try to paginate again.

        Generally though, we ignore extremities that are newer than the user's current
        scroll position (ie, those with depth greater than `nearby_depth`) as:
            1. we don't really care about getting events that have happened after our
               current position; and
            2. by the nature of paginating and scrolling back, we have likely previously
               tried and failed to backfill from that extremity, so to avoid getting
               "stuck" requesting the same backfill repeatedly we drop those
               extremities. Although we also have `event_failed_pull_attempts` nowadays
               to backoff as well.

        Args:
            room_id: Room where we want to find the oldest events
            nearby_depth: Typically, this is depth at the user's current scrollback
                position + the number of events you plan to backfill from these backfill
                points.
            limit: The max number of backfill points to return

        Returns:
            List of (event_id, depth) tuples. Sorted by depth, highest to lowest
            (descending) so the closest events to the `nearby_depth` are first
            in the list.
        """

        def get_backfill_points_in_room_txn(
            txn: LoggingTransaction, room_id: str
        ) -> list[tuple[str, int]]:
            if getattr(self, "_embedded_event_edges_enabled", False):
                return self._get_backfill_points_mtxdb(
                    txn, room_id, nearby_depth, limit
                )

            # Assemble a tuple lookup of event_id -> depth for the oldest events
            # we know of in the room. Backwards extremities are the oldest
            # events we know of in the room but we only know of them because
            # some other event referenced them by prev_event and aren't
            # persisted in our database yet (meaning we don't know their depth
            # specifically). So we need to look for the approximate depth from
            # the events connected to the current backwards extremities.

            if isinstance(self.database_engine, PostgresEngine):
                least_function = "LEAST"
            elif isinstance(self.database_engine, Sqlite3Engine):
                least_function = "MIN"
            else:
                raise RuntimeError("Unknown database engine")

            sql = f"""
                SELECT backward_extrem.event_id, event.depth FROM events AS event
                /**
                 * Get the edge connections from the event_edges table
                 * so we can see whether this event's prev_events points
                 * to a backward extremity in the next join.
                 */
                INNER JOIN event_edges AS edge
                ON edge.event_id = event.event_id
                /**
                 * We find the "oldest" events in the room by looking for
                 * events connected to backwards extremities (oldest events
                 * in the room that we know of so far).
                 */
                INNER JOIN event_backward_extremities AS backward_extrem
                ON edge.prev_event_id = backward_extrem.event_id
                /**
                 * We use this info to make sure we don't retry to use a backfill point
                 * if we've already attempted to backfill from it recently.
                 */
                LEFT JOIN event_failed_pull_attempts AS failed_backfill_attempt_info
                ON
                    failed_backfill_attempt_info.room_id = backward_extrem.room_id
                    AND failed_backfill_attempt_info.event_id = backward_extrem.event_id
                WHERE
                    backward_extrem.room_id = ?
                    /**
                     * We only want backwards extremities that are older than or at
                     * the same position of the given `nearby_depth` (where older
                     * means less than the given depth) because we're looking backwards
                     * from the `nearby_depth` when backfilling.
                     *
                     * Keep in mind that `event.depth` is an approximate depth of the
                     * backward extremity itself.
                     *
                     *                         nearby_depth (ignore events that come after this, ignore 2-4)
                     *                         |
                     *                         ▼
                     * <oldest-in-time> [0]<--[1]<--[2]<--[3]<--[4] <newest-in-time>
                     */
                    AND event.depth <= ? /* nearby_depth */
                    /**
                     * Exponential back-off (up to the upper bound) so we don't retry the
                     * same backfill point over and over. ex. 2hr, 4hr, 8hr, 16hr, etc.
                     *
                     * We use `1 << n` as a power of 2 equivalent for compatibility
                     * with older SQLites. The left shift equivalent only works with
                     * powers of 2 because left shift is a binary operation (base-2).
                     * Otherwise, we would use `power(2, n)` or the power operator, `2^n`.
                     */
                    AND (
                        failed_backfill_attempt_info.event_id IS NULL
                        OR ? /* current_time */ >= failed_backfill_attempt_info.last_attempt_ts + (
                            (1 << {least_function}(failed_backfill_attempt_info.num_attempts, ? /* max doubling steps */))
                            * ? /* step */
                        )
                    )
                /**
                 * Sort from highest (closest to the `nearby_depth`) to the lowest depth
                 * because the closest are most relevant to backfill from first.
                 * Then tie-break on alphabetical order of the event_ids so we get a
                 * consistent ordering which is nice when asserting things in tests.
                 */
                ORDER BY event.depth DESC, backward_extrem.event_id DESC
                LIMIT ?
            """

            txn.execute(
                sql,
                (
                    room_id,
                    nearby_depth,
                    self.clock.time_msec(),
                    BACKFILL_EVENT_EXPONENTIAL_BACKOFF_MAXIMUM_DOUBLING_STEPS,
                    BACKFILL_EVENT_EXPONENTIAL_BACKOFF_STEP_MILLISECONDS,
                    limit,
                ),
            )

            return cast(list[tuple[str, int]], txn.fetchall())

        return await self.db_pool.runInteraction(
            "get_backfill_points_in_room",
            get_backfill_points_in_room_txn,
            room_id,
        )

    async def get_max_depth_of(
        self, event_ids: Collection[str]
    ) -> tuple[str | None, int]:
        """Returns the event ID and depth for the event that has the max depth from a set of event IDs

        Args:
            event_ids: The event IDs to calculate the max depth of.
        """
        rows = cast(
            list[tuple[str, int]],
            await self.db_pool.simple_select_many_batch(
                table="events",
                column="event_id",
                iterable=event_ids,
                retcols=(
                    "event_id",
                    "depth",
                ),
                desc="get_max_depth_of",
            ),
        )

        if not rows:
            return None, 0
        else:
            max_depth_event_id = ""
            current_max_depth = 0
            for event_id, depth in rows:
                if depth > current_max_depth:
                    max_depth_event_id = event_id
                    current_max_depth = depth

            return max_depth_event_id, current_max_depth

    async def get_min_depth_of(self, event_ids: list[str]) -> tuple[str | None, int]:
        """Returns the event ID and depth for the event that has the min depth from a set of event IDs

        Args:
            event_ids: The event IDs to calculate the max depth of.
        """
        rows = cast(
            list[tuple[str, int]],
            await self.db_pool.simple_select_many_batch(
                table="events",
                column="event_id",
                iterable=event_ids,
                retcols=(
                    "event_id",
                    "depth",
                ),
                desc="get_min_depth_of",
            ),
        )

        if not rows:
            return None, 0
        else:
            min_depth_event_id = ""
            current_min_depth = MAX_DEPTH
            for event_id, depth in rows:
                if depth < current_min_depth:
                    min_depth_event_id = event_id
                    current_min_depth = depth

            return min_depth_event_id, current_min_depth

    async def get_prev_events_for_room(self, room_id: str) -> list[str]:
        """
        Gets a subset of the current forward extremities in the given room.

        Limits the result to 10 extremities, so that we can avoid creating
        events which refer to hundreds of prev_events.

        Args:
            room_id: room_id

        Returns:
            The event ids of the forward extremities.

        """

        return await self.db_pool.runInteraction(
            "get_prev_events_for_room", self._get_prev_events_for_room_txn, room_id
        )

    def _get_prev_events_for_room_txn(
        self, txn: LoggingTransaction, room_id: str
    ) -> list[str]:
        # we just use the 10 newest events. Older events will become
        # prev_events of future events.

        sql = """
            SELECT e.event_id FROM event_forward_extremities AS f
            INNER JOIN events AS e USING (event_id)
            WHERE f.room_id = ?
            ORDER BY e.depth DESC
            LIMIT 10
        """

        txn.execute(sql, (room_id,))

        return [row[0] for row in txn]

    async def get_rooms_with_many_extremities(
        self, min_count: int, limit: int, room_id_filter: Iterable[str]
    ) -> list[str]:
        """Get the top rooms with at least N extremities.

        Args:
            min_count: The minimum number of extremities
            limit: The maximum number of rooms to return.
            room_id_filter: room_ids to exclude from the results

        Returns:
            At most `limit` room IDs that have at least `min_count` extremities,
            sorted by extremity count.
        """

        def _get_rooms_with_many_extremities_txn(txn: LoggingTransaction) -> list[str]:
            where_clause = "1=1"
            if room_id_filter:
                where_clause = "room_id NOT IN (%s)" % (
                    ",".join("?" for _ in room_id_filter),
                )

            sql = """
                SELECT room_id FROM event_forward_extremities
                WHERE %s
                GROUP BY room_id
                HAVING count(*) > ?
                ORDER BY count(*) DESC
                LIMIT ?
            """ % (where_clause,)

            query_args = list(itertools.chain(room_id_filter, [min_count, limit]))
            txn.execute(sql, query_args)
            return [room_id for (room_id,) in txn]

        return await self.db_pool.runInteraction(
            "get_rooms_with_many_extremities", _get_rooms_with_many_extremities_txn
        )

    @cached(max_entries=5000, iterable=True)
    async def get_latest_event_ids_in_room(self, room_id: str) -> frozenset[str]:
        event_ids = await self.db_pool.simple_select_onecol(
            table="event_forward_extremities",
            keyvalues={"room_id": room_id},
            retcol="event_id",
            desc="get_latest_event_ids_in_room",
        )
        return frozenset(event_ids)

    async def get_state_dag_extremities(self, room_id: str) -> frozenset[str]:
        event_ids = await self.db_pool.simple_select_onecol(
            table="msc4242_state_dag_forward_extremities",
            keyvalues={"room_id": room_id},
            retcol="event_id",
            desc="get_state_dag_extremities",
        )
        return frozenset(event_ids)

    async def get_min_depth(self, room_id: str) -> int | None:
        """For the given room, get the minimum depth we have seen for it."""
        return await self.db_pool.runInteraction(
            "get_min_depth", self._get_min_depth_interaction, room_id
        )

    def _get_min_depth_interaction(
        self, txn: LoggingTransaction, room_id: str
    ) -> int | None:
        min_depth = self.db_pool.simple_select_one_onecol_txn(
            txn,
            table="room_depth",
            keyvalues={"room_id": room_id},
            retcol="min_depth",
            allow_none=True,
        )

        return int(min_depth) if min_depth is not None else None

    async def have_room_forward_extremities_changed_since(
        self,
        room_id: str,
        stream_ordering: int,
    ) -> bool:
        """Check if the forward extremities in a room have changed since the
        given stream ordering

        Throws a StoreError if we have since purged the index for
        stream_orderings from that point.
        """
        assert self.stream_ordering_month_ago is not None
        if stream_ordering <= self.stream_ordering_month_ago:
            raise StoreError(400, f"stream_ordering too old {stream_ordering}")

        sql = """
            SELECT 1 FROM stream_ordering_to_exterm
            WHERE stream_ordering > ? AND room_id = ?
            LIMIT 1
        """

        def have_room_forward_extremities_changed_since_txn(
            txn: LoggingTransaction,
        ) -> bool:
            txn.execute(sql, (stream_ordering, room_id))
            return txn.fetchone() is not None

        return await self.db_pool.runInteraction(
            "have_room_forward_extremities_changed_since",
            have_room_forward_extremities_changed_since_txn,
        )

    @cancellable
    async def get_forward_extremities_for_room_at_stream_ordering(
        self, room_id: str, stream_ordering: int
    ) -> Sequence[str]:
        """For a given room_id and stream_ordering, return the forward
        extremeties of the room at that point in "time".

        Throws a StoreError if we have since purged the index for
        stream_orderings from that point.

        Args:
            room_id:
            stream_ordering:

        Returns:
            A list of event_ids
        """
        # We want to make the cache more effective, so we clamp to the last
        # change before the given ordering.
        last_change = self._events_stream_cache.get_max_pos_of_last_change(room_id)  # type: ignore[attr-defined]
        if last_change is None:
            # If the room isn't in the cache we know that the last change was
            # somewhere before the earliest known position of the cache, so we
            # can clamp to that.
            last_change = self._events_stream_cache.get_earliest_known_position()  # type: ignore[attr-defined]

        # We don't always have a full stream_to_exterm_id table, e.g. after
        # the upgrade that introduced it, so we make sure we never ask for a
        # stream_ordering from before a restart
        last_change = max(self._stream_order_on_start, last_change)  # type: ignore[attr-defined]

        # provided the last_change is recent enough, we now clamp the requested
        # stream_ordering to it.
        assert self.stream_ordering_month_ago is not None
        if last_change > self.stream_ordering_month_ago:
            stream_ordering = min(last_change, stream_ordering)

        return await self._get_forward_extremeties_for_room(room_id, stream_ordering)

    @cached(max_entries=5000, num_args=2)
    async def _get_forward_extremeties_for_room(
        self, room_id: str, stream_ordering: int
    ) -> Sequence[str]:
        """For a given room_id and stream_ordering, return the forward
        extremeties of the room at that point in "time".

        Throws a StoreError if we have since purged the index for
        stream_orderings from that point.
        """
        assert self.stream_ordering_month_ago is not None
        if stream_ordering <= self.stream_ordering_month_ago:
            raise StoreError(400, "stream_ordering too old %s" % (stream_ordering,))

        sql = """
                SELECT event_id FROM stream_ordering_to_exterm
                INNER JOIN (
                    SELECT room_id, MAX(stream_ordering) AS stream_ordering
                    FROM stream_ordering_to_exterm
                    WHERE stream_ordering <= ? GROUP BY room_id
                ) AS rms USING (room_id, stream_ordering)
                WHERE room_id = ?
        """

        def get_forward_extremeties_for_room_txn(txn: LoggingTransaction) -> list[str]:
            txn.execute(sql, (stream_ordering, room_id))
            return [event_id for (event_id,) in txn]

        event_ids = await self.db_pool.runInteraction(
            "get_forward_extremeties_for_room", get_forward_extremeties_for_room_txn
        )

        # If we didn't find any IDs, then we must have cleared out the
        # associated `stream_ordering_to_exterm`.
        if not event_ids:
            raise StoreError(400, "stream_ordering too old %s" % (stream_ordering,))

        return event_ids

    def _get_connected_prev_event_backfill_results_txn(
        self, txn: LoggingTransaction, event_id: str, limit: int
    ) -> list[BackfillQueueNavigationItem]:
        """
        Find any events connected by prev_event the specified event_id.

        Args:
            txn: The database transaction to use
            event_id: The event ID to navigate from
            limit: Max number of event ID's to query for and return

        Returns:
            List of prev events that the backfill queue can process
        """
        if getattr(self, "_embedded_event_edges_enabled", False):
            from synapse.storage.databases.main.embedded_event_edges import (
                get_event_edges_backward_batch,
            )

            backward = get_event_edges_backward_batch(
                self._embedded_db_namespace, [event_id]
            )
            prev_ids = [
                prev_event_id
                for prev_event_id, _is_state in (backward.get(event_id) or ())
            ]
            if not prev_ids:
                return []
            clause, args = make_in_list_sql_clause(
                self.database_engine, "event_id", prev_ids
            )
            txn.execute(
                f"""
                SELECT depth, stream_ordering, event_id, type FROM events
                WHERE NOT outlier AND {clause}
                ORDER BY depth DESC, stream_ordering DESC
                LIMIT ?
                """,
                (*args, limit),
            )
            return [
                BackfillQueueNavigationItem(
                    depth=row[0],
                    stream_ordering=row[1],
                    event_id=row[2],
                    type=row[3],
                )
                for row in txn
            ]

        # Look for the prev_event_id connected to the given event_id
        connected_prev_event_query = """
            SELECT depth, stream_ordering, prev_event_id, events.type FROM event_edges
            /* Get the depth and stream_ordering of the prev_event_id from the events table */
            INNER JOIN events
            ON prev_event_id = events.event_id

            /* exclude outliers from the results (we don't have the state, so cannot
             * verify if the requesting server can see them).
             */
            WHERE NOT events.outlier

            /* Look for an edge which matches the given event_id */
            AND event_edges.event_id = ?

            /* Because we can have many events at the same depth,
            * we want to also tie-break and sort on stream_ordering */
            ORDER BY depth DESC, stream_ordering DESC
            LIMIT ?
        """

        txn.execute(
            connected_prev_event_query,
            (event_id, limit),
        )
        return [
            BackfillQueueNavigationItem(
                depth=row[0],
                stream_ordering=row[1],
                event_id=row[2],
                type=row[3],
            )
            for row in txn
        ]

    async def get_backfill_events(
        self, room_id: str, seed_event_id_list: list[str], limit: int
    ) -> list[EventBase]:
        """Get a list of Events for a given topic that occurred before (and
        including) the events in seed_event_id_list. Return a list of max size `limit`

        Args:
            room_id
            seed_event_id_list
            limit
        """
        event_ids = await self.db_pool.runInteraction(
            "get_backfill_events",
            self._get_backfill_events,
            room_id,
            seed_event_id_list,
            limit,
        )
        events = await self.get_events_as_list(event_ids)
        return sorted(
            # type-ignore: mypy doesn't like negating the int | None stream_ordering.
            # But it's never None, because these events were previously persisted to the DB.
            events,
            key=lambda e: (-e.depth, -e.internal_metadata.stream_ordering),  # type: ignore[operator]
        )

    def _get_backfill_events(
        self,
        txn: LoggingTransaction,
        room_id: str,
        seed_event_id_list: list[str],
        limit: int,
    ) -> set[str]:
        """
        We want to make sure that we do a breadth-first, "depth" ordered search.
        We also handle navigating historical branches of history connected by
        insertion and batch events.
        """
        logger.debug(
            "_get_backfill_events(room_id=%s): seeding backfill with seed_event_id_list=%s limit=%s",
            room_id,
            seed_event_id_list,
            limit,
        )

        event_id_results: set[str] = set()
        queued_event_ids: set[str] = set()
        # Outlier seeds are walked back from but never returned themselves: we
        # don't have the state at an outlier, so can't check whether the
        # requesting server may see it (`filter_events_for_server` assumes
        # outliers are open). Their non-outlier predecessors can be checked.
        outlier_seed_ids: set[str] = set()

        # In a PriorityQueue, the lowest valued entries are retrieved first.
        # We're using depth as the priority in the queue and tie-break based on
        # stream_ordering. Depth is lowest at the oldest-in-time message and
        # highest and newest-in-time message. We add events to the queue with a
        # negative depth so that we process the newest-in-time messages first
        # going backwards in time. stream_ordering follows the same pattern.
        queue: "PriorityQueue[tuple[int, int, str, str]]" = PriorityQueue()

        for seed_event_id in seed_event_id_list:
            event_lookup_result = self.db_pool.simple_select_one_txn(
                txn,
                table="events",
                keyvalues={"event_id": seed_event_id, "room_id": room_id},
                retcols=(
                    "type",
                    "depth",
                    "stream_ordering",
                    "outlier",
                ),
                allow_none=True,
            )

            if event_lookup_result is not None:
                event_type, depth, stream_ordering, outlier = event_lookup_result
                logger.debug(
                    "_get_backfill_events(room_id=%s): seed_event_id=%s depth=%s stream_ordering=%s type=%s",
                    room_id,
                    seed_event_id,
                    depth,
                    stream_ordering,
                    event_type,
                )

                if depth and seed_event_id not in queued_event_ids:
                    queued_event_ids.add(seed_event_id)
                    if outlier:
                        outlier_seed_ids.add(seed_event_id)
                    queue.put((-depth, -stream_ordering, seed_event_id, event_type))

        while not queue.empty() and len(event_id_results) < limit:
            try:
                _, _, event_id, event_type = queue.get_nowait()
            except Empty:
                break

            if event_id in event_id_results:
                continue

            if event_id not in outlier_seed_ids:
                event_id_results.add(event_id)

            # Now we just look up the DAG by prev_events as normal
            connected_prev_event_backfill_results = (
                self._get_connected_prev_event_backfill_results_txn(
                    txn, event_id, limit - len(event_id_results)
                )
            )
            logger.debug(
                "_get_backfill_events(room_id=%s): connected_prev_event_backfill_results=%s",
                room_id,
                connected_prev_event_backfill_results,
            )
            for (
                connected_prev_event_backfill_item
            ) in connected_prev_event_backfill_results:
                if (
                    connected_prev_event_backfill_item.event_id not in event_id_results
                    and connected_prev_event_backfill_item.event_id
                    not in queued_event_ids
                ):
                    queued_event_ids.add(connected_prev_event_backfill_item.event_id)
                    queue.put(
                        (
                            -connected_prev_event_backfill_item.depth,
                            -connected_prev_event_backfill_item.stream_ordering,
                            connected_prev_event_backfill_item.event_id,
                            connected_prev_event_backfill_item.type,
                        )
                    )

        return event_id_results

    @trace
    async def record_event_failed_pull_attempt(
        self, room_id: str, event_id: str, cause: str
    ) -> None:
        """
        Record when we fail to pull an event over federation.

        This information allows us to be more intelligent when we decide to
        retry (we don't need to fail over and over) and we can process that
        event in the background so we don't block on it each time.

        Args:
            room_id: The room where the event failed to pull from
            event_id: The event that failed to be fetched or processed
            cause: The error message or reason that we failed to pull the event
        """
        logger.debug(
            "record_event_failed_pull_attempt room_id=%s, event_id=%s, cause=%s",
            room_id,
            event_id,
            cause,
        )
        await self.db_pool.runInteraction(
            "record_event_failed_pull_attempt",
            self._record_event_failed_pull_attempt_upsert_txn,
            room_id,
            event_id,
            cause,
            db_autocommit=True,  # Safe as it's a single upsert
        )

    def _record_event_failed_pull_attempt_upsert_txn(
        self,
        txn: LoggingTransaction,
        room_id: str,
        event_id: str,
        cause: str,
    ) -> None:
        sql = """
            INSERT INTO event_failed_pull_attempts (
                room_id, event_id, num_attempts, last_attempt_ts, last_cause
            )
                VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (room_id, event_id) DO UPDATE SET
                num_attempts=event_failed_pull_attempts.num_attempts + 1,
                last_attempt_ts=EXCLUDED.last_attempt_ts,
                last_cause=EXCLUDED.last_cause;
        """

        txn.execute(sql, (room_id, event_id, 1, self.clock.time_msec(), cause))

    @trace
    async def get_event_ids_with_failed_pull_attempts(
        self, event_ids: StrCollection
    ) -> set[str]:
        """
        Filter the given list of `event_ids` and return events which have any failed
        pull attempts.

        Args:
            event_ids: A list of events to filter down.

        Returns:
            A filtered down list of `event_ids` that have previous failed pull attempts.
        """

        rows = cast(
            list[tuple[str]],
            await self.db_pool.simple_select_many_batch(
                table="event_failed_pull_attempts",
                column="event_id",
                iterable=event_ids,
                keyvalues={},
                retcols=("event_id",),
                desc="get_event_ids_with_failed_pull_attempts",
            ),
        )
        return {row[0] for row in rows}

    @trace
    async def get_event_ids_to_not_pull_from_backoff(
        self,
        room_id: str,
        event_ids: Collection[str],
    ) -> dict[str, int]:
        """
        Filter down the events to ones that we've failed to pull before recently. Uses
        exponential backoff.

        Args:
            room_id: The room that the events belong to
            event_ids: A list of events to filter down

        Returns:
            A dictionary of event_ids that should not be attempted to be pulled and the
            next timestamp at which we may try pulling them again.
        """
        event_failed_pull_attempts = cast(
            list[tuple[str, int, int]],
            await self.db_pool.simple_select_many_batch(
                table="event_failed_pull_attempts",
                column="event_id",
                iterable=event_ids,
                keyvalues={},
                retcols=(
                    "event_id",
                    "last_attempt_ts",
                    "num_attempts",
                ),
                desc="get_event_ids_to_not_pull_from_backoff",
            ),
        )

        current_time = self.clock.time_msec()

        event_ids_with_backoff = {}
        for event_id, last_attempt_ts, num_attempts in event_failed_pull_attempts:
            # Exponential back-off (up to the upper bound) so we don't try to
            # pull the same event over and over. ex. 2hr, 4hr, 8hr, 16hr, etc.
            backoff_end_time = (
                last_attempt_ts
                + (
                    2
                    ** min(
                        num_attempts,
                        BACKFILL_EVENT_EXPONENTIAL_BACKOFF_MAXIMUM_DOUBLING_STEPS,
                    )
                )
                * BACKFILL_EVENT_EXPONENTIAL_BACKOFF_STEP_MILLISECONDS
            )

            if current_time < backoff_end_time:  # `backoff_end_time` is exclusive
                event_ids_with_backoff[event_id] = backoff_end_time

        return event_ids_with_backoff

    async def get_missing_events(
        self,
        room_id: str,
        earliest_events: list[str],
        latest_events: list[str],
        limit: int,
    ) -> list[EventBase]:
        """
        Walk backwards in the DAG of events,
        starting at `latest_events` and stopping at `earliest_events` (or when having reached `limit` events).

        This function will check that `latest_events` and `earliest_events` are in the correct
        room (`room_id`), appropriately ignoring any that aren't.
        """
        ids = await self.db_pool.runInteraction(
            "get_missing_events",
            self._get_missing_events,
            room_id,
            earliest_events,
            latest_events,
            limit,
        )
        return await self.get_events_as_list(ids)

    def _get_missing_events(
        self,
        txn: LoggingTransaction,
        room_id: str,
        earliest_events: list[str],
        latest_events: list[str],
        limit: int,
    ) -> list[str]:
        if getattr(self, "_embedded_event_edges_enabled", False):
            from synapse.storage.databases.main.embedded_event_edges import (
                get_event_edges_backward_batch,
            )

            seen_events = set(earliest_events)
            latest_clause, latest_args = make_in_list_sql_clause(
                self.database_engine,
                "event_id",
                set(latest_events) - seen_events,
            )
            txn.execute(
                f"SELECT event_id FROM events WHERE {latest_clause} AND room_id = ?",
                (*latest_args, room_id),
            )
            front = {event_id for (event_id,) in txn}
            mtxdb_results: list[str] = []

            while front and len(mtxdb_results) < limit:
                backward = get_event_edges_backward_batch(
                    self._embedded_db_namespace, list(front)
                )
                candidates = {
                    prev_event_id
                    for edges in backward.values()
                    if edges
                    for prev_event_id, _is_state in edges
                } - seen_events
                if not candidates:
                    break

                candidate_clause, candidate_args = make_in_list_sql_clause(
                    self.database_engine, "event_id", list(candidates)
                )
                txn.execute(
                    f"SELECT event_id FROM events WHERE {candidate_clause} "
                    "AND room_id = ?",
                    (*candidate_args, room_id),
                )
                new_results = {event_id for (event_id,) in txn} - seen_events
                if not new_results:
                    break

                limited_results = list(
                    itertools.islice(new_results, limit - len(mtxdb_results))
                )
                new_front = set(limited_results)
                seen_events |= new_front
                mtxdb_results.extend(limited_results)
                front = new_front

            mtxdb_results.reverse()
            return mtxdb_results

        # It's OK that this has not been filtered by correct-room,
        # because we will only compare based on event ID from the events
        # we happen to run into.
        seen_events = set(earliest_events)

        # Pre-filter the `latest_events` to only include those
        # that are in this room (and that we know about)
        # This makes events in the wrong room get treated the same as unknown events.
        events_clause, events_args = make_in_list_sql_clause(
            self.database_engine,
            "event_id",
            # Don't waste time looking at events that the requester told us
            # they already know about.
            # (They probably shouldn't send this in the first place)
            set(latest_events) - seen_events,
        )
        txn.execute(
            f"""
            SELECT event_id
            FROM events
            WHERE {events_clause} AND room_id = ?
            """,
            (*events_args, room_id),
        )
        # Start walking back from the legitimate and known `latest_events`
        front = {latest_event_id for (latest_event_id,) in txn}

        event_results: list[str] = []

        while front and len(event_results) < limit:
            new_front = set()
            for event_id in front:
                txn.execute(
                    """
                    SELECT ee.prev_event_id FROM event_edges AS ee
                    JOIN events ON events.event_id = ee.prev_event_id
                    WHERE ee.event_id = ?
                    AND events.room_id = ?
                    LIMIT ?
                    """,
                    (event_id, room_id, limit - len(event_results)),
                )
                new_results = {t[0] for t in txn} - seen_events

                new_front |= new_results
                seen_events |= new_results
                event_results.extend(new_results)

            front = new_front

        # we built the list working backwards from latest_events; we now need to
        # reverse it so that the events are approximately chronological.
        event_results.reverse()
        return event_results

    @trace
    @tag_args
    async def get_successor_events(self, event_id: str) -> list[str]:
        """Fetch all events that have the given event as a prev event

        Args:
            event_id: The event to search for as a prev_event.
        """
        started = time.monotonic()

        from synapse.storage.databases.main.embedded_event_edges import (
            get_event_edges_forward_batch,
            record_event_edge_read,
        )

        if getattr(self, "_embedded_event_edges_enabled", False):
            embedded_started = time.monotonic()
            forward_map = get_event_edges_forward_batch(
                self._embedded_db_namespace, [event_id]
            )
            ffi_timing(
                "event_edges_successor_embedded_read",
                time.monotonic() - embedded_started,
            )
            successors = forward_map.get(event_id)
            if successors is not None:
                # The forward index is written before the SQL commit, so it can
                # name children without a committed row.
                if successors:
                    committed = await self.db_pool.simple_select_many_batch(
                        table="events",
                        column="event_id",
                        iterable=successors,
                        retcols=["event_id"],
                        desc="get_successor_events_committed",
                    )
                    committed_ids = {row[0] for row in committed}
                    successors = [e for e in successors if e in committed_ids]
                ffi_count("event_edges_successor_hits", 1)
                record_event_edge_read(self.server_name, "successor", "hit")
                ffi_timing("event_edges_successor_total", time.monotonic() - started)
                return successors

            ffi_count("event_edges_successor_misses", 1)
            record_event_edge_read(self.server_name, "successor", "miss")
            ffi_timing("event_edges_successor_total", time.monotonic() - started)
            return []

        record_event_edge_read(self.server_name, "successor", "sql_only")
        sql_started = time.monotonic()
        sql_res = await self.db_pool.simple_select_onecol(
            table="event_edges",
            keyvalues={"prev_event_id": event_id},
            retcol="event_id",
            desc="get_successor_events",
        )
        ffi_timing("event_edges_successor_sql_only", time.monotonic() - sql_started)
        ffi_timing("event_edges_successor_total", time.monotonic() - started)
        return sql_res

    @wrap_as_background_process("delete_old_forward_extrem_cache")
    async def _delete_old_forward_extrem_cache(self) -> None:
        def _delete_old_forward_extrem_cache_txn(txn: LoggingTransaction) -> None:
            sql = """
                DELETE FROM stream_ordering_to_exterm
                WHERE stream_ordering < ?
            """
            txn.execute(sql, (self.stream_ordering_month_ago,))

        await self.db_pool.runInteraction(
            "_delete_old_forward_extrem_cache",
            _delete_old_forward_extrem_cache_txn,
        )

    async def insert_received_event_to_staging(
        self, origin: str, event: EventBase
    ) -> None:
        """Insert a newly received event from federation into the staging area."""

        # We use an upsert here to handle the case where we see the same event
        # from the same server multiple times.
        await self.db_pool.simple_upsert(
            table="federation_inbound_events_staging",
            keyvalues={
                "origin": origin,
                "event_id": event.event_id,
            },
            values={},
            insertion_values={
                "room_id": event.room_id,
                "received_ts": self.clock.time_msec(),
                "event_json": json_encoder.encode(event.get_dict()),
                "internal_metadata": json_encoder.encode(
                    event.internal_metadata.get_dict()
                ),
            },
            desc="insert_received_event_to_staging",
        )

    async def remove_received_event_from_staging(
        self,
        origin: str,
        event_id: str,
    ) -> int | None:
        """Remove the given event from the staging area.

        Returns:
            The received_ts of the row that was deleted, if any.
        """

        def _remove_received_event_from_staging_txn(
            txn: LoggingTransaction,
        ) -> int | None:
            sql = """
                DELETE FROM federation_inbound_events_staging
                WHERE origin = ? AND event_id = ?
                RETURNING received_ts
            """

            txn.execute(sql, (origin, event_id))
            row = cast(tuple[int] | None, txn.fetchone())

            if row is None:
                return None

            return row[0]

        return await self.db_pool.runInteraction(
            "remove_received_event_from_staging",
            _remove_received_event_from_staging_txn,
            db_autocommit=True,
        )

    async def get_next_staged_event_id_for_room(
        self,
        room_id: str,
    ) -> tuple[str, str] | None:
        """
        Get the next event ID in the staging area for the given room.

        Returns:
            Tuple of the `origin` and `event_id`
        """

        def _get_next_staged_event_id_for_room_txn(
            txn: LoggingTransaction,
        ) -> tuple[str, str] | None:
            sql = """
                SELECT origin, event_id
                FROM federation_inbound_events_staging
                WHERE room_id = ?
                ORDER BY received_ts ASC
                LIMIT 1
            """

            txn.execute(sql, (room_id,))

            return cast(tuple[str, str] | None, txn.fetchone())

        return await self.db_pool.runInteraction(
            "get_next_staged_event_id_for_room", _get_next_staged_event_id_for_room_txn
        )

    async def get_next_staged_event_for_room(
        self,
        room_id: str,
        room_version: RoomVersion,
    ) -> tuple[str, EventBase] | None:
        """Get the next event in the staging area for the given room."""

        def _get_next_staged_event_for_room_txn(
            txn: LoggingTransaction,
        ) -> tuple[str, str, str] | None:
            sql = """
                SELECT event_json, internal_metadata, origin
                FROM federation_inbound_events_staging
                WHERE room_id = ?
                ORDER BY received_ts ASC
                LIMIT 1
            """
            txn.execute(sql, (room_id,))

            return cast(tuple[str, str, str] | None, txn.fetchone())

        row = await self.db_pool.runInteraction(
            "get_next_staged_event_for_room", _get_next_staged_event_for_room_txn
        )

        if not row:
            return None

        event_d = db_to_json(row[0])
        internal_metadata_d = db_to_json(row[1])
        origin = row[2]

        event = make_event_from_dict(
            event_dict=event_d,
            room_version=room_version,
            internal_metadata_dict=internal_metadata_d,
        )

        return origin, event

    async def get_staged_events_for_room(
        self,
        room_id: str,
        room_version: RoomVersion,
        limit: int,
    ) -> list[tuple[str, EventBase]]:
        """Get the oldest staged events for a room.

        The caller holds the per-room inbound-PDU lock, so the rows remain owned by
        it until they are removed from staging. This is deliberately a read rather
        than a claim: an event must stay staged until it has been persisted.
        """

        def _get_staged_events_for_room_txn(
            txn: LoggingTransaction,
        ) -> list[tuple[str, str, str]]:
            txn.execute(
                """
                SELECT event_json, internal_metadata, origin
                FROM federation_inbound_events_staging
                WHERE room_id = ?
                ORDER BY received_ts ASC
                LIMIT ?
                """,
                (room_id, limit),
            )
            return cast(list[tuple[str, str, str]], txn.fetchall())

        rows = await self.db_pool.runInteraction(
            "get_staged_events_for_room", _get_staged_events_for_room_txn
        )

        return [
            (
                row[2],
                make_event_from_dict(
                    event_dict=db_to_json(row[0]),
                    room_version=room_version,
                    internal_metadata_dict=db_to_json(row[1]),
                ),
            )
            for row in rows
        ]

    async def prune_staged_events_in_room(
        self,
        room_id: str,
        room_version: RoomVersion,
    ) -> bool:
        """Checks if there are lots of staged events for the room, and if so
        prune them down.

        Returns:
            Whether any events were pruned
        """

        # First check the size of the queue.
        count = await self.db_pool.simple_select_one_onecol(
            table="federation_inbound_events_staging",
            keyvalues={"room_id": room_id},
            retcol="COUNT(*)",
            desc="prune_staged_events_in_room_count",
        )

        if count < 100:
            return False

        # If the queue is too large, then we want clear the entire queue,
        # keeping only the forward extremities (i.e. the events not referenced
        # by other events in the queue). We do this so that we can always
        # backpaginate in all the events we have dropped.
        rows = cast(
            list[tuple[str, str]],
            await self.db_pool.simple_select_list(
                table="federation_inbound_events_staging",
                keyvalues={"room_id": room_id},
                retcols=("event_id", "event_json"),
                desc="prune_staged_events_in_room_fetch",
            ),
        )

        # Find the set of events referenced by those in the queue, as well as
        # collecting all the event IDs in the queue.
        referenced_events: set[str] = set()
        seen_events: set[str] = set()
        for event_id, event_json in rows:
            seen_events.add(event_id)
            event_d = db_to_json(event_json)

            # We don't bother parsing the dicts into full blown event objects,
            # as that is needlessly expensive.

            # We haven't checked that the `prev_events` have the right format
            # yet, so we check as we go.
            prev_events = event_d.get("prev_events", [])
            if not isinstance(prev_events, list):
                logger.info("Invalid prev_events for %s", event_id)
                continue

            if room_version.event_format == EventFormatVersions.ROOM_V1_V2:
                for prev_event_tuple in prev_events:
                    if (
                        not isinstance(prev_event_tuple, list)
                        or len(prev_event_tuple) != 2
                    ):
                        logger.info("Invalid prev_events for %s", event_id)
                        break

                    prev_event_id = prev_event_tuple[0]
                    if not isinstance(prev_event_id, str):
                        logger.info("Invalid prev_events for %s", event_id)
                        break

                    referenced_events.add(prev_event_id)
            else:
                for prev_event_id in prev_events:
                    if not isinstance(prev_event_id, str):
                        logger.info("Invalid prev_events for %s", event_id)
                        break

                    referenced_events.add(prev_event_id)

        to_delete = referenced_events & seen_events
        if not to_delete:
            return False

        pdus_pruned_from_federation_queue.labels(
            **{SERVER_NAME_LABEL: self.server_name}
        ).inc(len(to_delete))
        logger.info(
            "Pruning %d events in room %s from federation queue",
            len(to_delete),
            room_id,
        )

        await self.db_pool.simple_delete_many(
            table="federation_inbound_events_staging",
            keyvalues={"room_id": room_id},
            iterable=to_delete,
            column="event_id",
            desc="prune_staged_events_in_room_delete",
        )

        return True

    async def get_all_rooms_with_staged_incoming_events(self) -> list[str]:
        """Get the room IDs of all events currently staged."""
        return await self.db_pool.simple_select_onecol(
            table="federation_inbound_events_staging",
            keyvalues={},
            retcol="DISTINCT room_id",
            desc="get_all_rooms_with_staged_incoming_events",
        )

    @wrap_as_background_process("_get_stats_for_federation_staging")
    async def _get_stats_for_federation_staging(self) -> None:
        """Update the prometheus metrics for the inbound federation staging area."""

        def _get_stats_for_federation_staging_txn(
            txn: LoggingTransaction,
        ) -> tuple[int, int]:
            txn.execute("SELECT count(*) FROM federation_inbound_events_staging")
            (count,) = cast(tuple[int], txn.fetchone())

            txn.execute(
                "SELECT min(received_ts) FROM federation_inbound_events_staging"
            )

            (received_ts,) = cast(tuple[int | None], txn.fetchone())

            # If there is nothing in the staging area default it to 0.
            age = 0
            if received_ts is not None:
                age = self.clock.time_msec() - received_ts

            return count, age

        count, age = await self.db_pool.runInteraction(
            "_get_stats_for_federation_staging", _get_stats_for_federation_staging_txn
        )

        number_pdus_in_federation_queue.labels(
            **{SERVER_NAME_LABEL: self.server_name}
        ).set(count)
        oldest_pdu_in_federation_staging.labels(
            **{SERVER_NAME_LABEL: self.server_name}
        ).set(age)

    async def clean_room_for_join(self, room_id: str) -> None:
        await self.db_pool.runInteraction(
            "clean_room_for_join", self._clean_room_for_join_txn, room_id
        )

    def _clean_room_for_join_txn(self, txn: LoggingTransaction, room_id: str) -> None:
        query = "DELETE FROM event_forward_extremities WHERE room_id = ?"

        txn.execute(query, (room_id,))
        self._invalidate_cache_and_stream(
            txn, self.get_latest_event_ids_in_room, (room_id,)
        )


class EventFederationStore(EventFederationWorkerStore):
    """Responsible for storing and serving up the various graphs associated
    with an event. Including the main event graph and the auth chains for an
    event.

    Also has methods for getting the front (latest) and back (oldest) edges
    of the event graphs. These are used to generate the parents for new events
    and backfilling from another server respectively.
    """

    EVENT_AUTH_STATE_ONLY = "event_auth_state_only"

    def __init__(
        self,
        database: DatabasePool,
        db_conn: LoggingDatabaseConnection,
        hs: "HomeServer",
    ):
        super().__init__(database, db_conn, hs)

        self.db_pool.updates.register_background_update_handler(
            self.EVENT_AUTH_STATE_ONLY, self._background_delete_non_state_event_auth
        )

    async def _background_delete_non_state_event_auth(
        self, progress: JsonDict, batch_size: int
    ) -> int:
        if self._embedded_db_engine is not None:
            # The table is no longer part of the live graph after the mtxdb
            # cutover. Retire any queued legacy cleanup without scanning SQL.
            await self.db_pool.updates._end_background_update(
                self.EVENT_AUTH_STATE_ONLY
            )
            return 0

        def delete_event_auth(txn: LoggingTransaction) -> bool:
            target_min_stream_id = progress.get("target_min_stream_id_inclusive")
            max_stream_id = progress.get("max_stream_id_exclusive")

            if not target_min_stream_id or not max_stream_id:
                txn.execute("SELECT COALESCE(MIN(stream_ordering), 0) FROM events")
                rows = txn.fetchall()
                target_min_stream_id = rows[0][0]

                txn.execute("SELECT COALESCE(MAX(stream_ordering), 0) FROM events")
                rows = txn.fetchall()
                max_stream_id = rows[0][0]

            min_stream_id = max_stream_id - batch_size

            sql = """
                DELETE FROM event_auth
                WHERE event_id IN (
                    SELECT event_id FROM events
                    LEFT JOIN state_events AS se USING (room_id, event_id)
                    WHERE ? <= stream_ordering AND stream_ordering < ?
                        AND se.state_key IS null
                )
            """

            txn.execute(sql, (min_stream_id, max_stream_id))

            new_progress = {
                "target_min_stream_id_inclusive": target_min_stream_id,
                "max_stream_id_exclusive": min_stream_id,
            }

            self.db_pool.updates._background_update_progress_txn(
                txn, self.EVENT_AUTH_STATE_ONLY, new_progress
            )

            return min_stream_id >= target_min_stream_id

        result = await self.db_pool.runInteraction(
            self.EVENT_AUTH_STATE_ONLY, delete_event_auth
        )

        if not result:
            await self.db_pool.updates._end_background_update(
                self.EVENT_AUTH_STATE_ONLY
            )

        return batch_size
