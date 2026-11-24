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

import logging
from typing import Any, Callable, cast

from synapse.api.errors import SynapseError
from synapse.logging.context import defer_to_thread
from synapse.storage.database import (
    LoggingTransaction,
    make_in_list_sql_clause,
    user_is_local_like_pattern,
)
from synapse.storage.databases.main import CacheInvalidationWorkerStore
from synapse.storage.databases.main.embedded_common import (
    Pool,
    retry_on_contention,
    sync_now,
)
from synapse.storage.databases.main.embedded_event_edges import (
    get_event_edges_forward_batch,
    purge_event_edges_room,
)
from synapse.storage.databases.main.embedded_event_json import delete_event_json_batch
from synapse.storage.databases.main.embedded_event_to_state_group import (
    decrement_state_group_refcounts_batch,
    delete_event_to_state_group_batch,
    get_state_group_for_events_batch,
)
from synapse.storage.databases.main.embedded_redactions import delete_redactions_batch
from synapse.storage.databases.main.state import StateGroupWorkerStore
from synapse.storage.engines import PostgresEngine
from synapse.storage.engines._base import IsolationLevel
from synapse.types import RoomStreamToken

logger = logging.getLogger(__name__)


def _delete_purge_event_mirrors(
    engine_name: str | None,
    redacted_event_ids: list[str],
    deleted_event_ids: list[str],
) -> None:
    """Delete purge mirrors after SQL commit, retrying transient mtxdb contention.

    Both deletes are idempotent, so retrying the pair is safe if the first
    mirror was deleted before contention interrupted the second one.
    """

    def delete() -> None:
        delete_redactions_batch(engine_name, redacted_event_ids)
        if redacted_event_ids:
            # Redaction tombstones live in the State pool, which the event_json
            # delete below does not sync. Make their removal durable first, so
            # a crash cannot bring back a redaction mirror for events whose
            # event_json is already gone.
            sync_now(pools=[Pool.STATE])
        delete_event_json_batch(engine_name, deleted_event_ids)

    retry_on_contention(delete, label="purge_mirrors")


def _run_purge_cleanups(steps: list[tuple[str, Callable[[], None]]]) -> None:
    """Run each post-commit purge cleanup even if an earlier one fails.

    Run on a worker thread (see `_run_post_commit`): the steps delete mtxdb
    entries and flush to disk, which must not block the reactor. A step that
    raised would otherwise stop every later one. Each step is
    isolated here and every failure is logged; the SQL purge has already
    committed, so there is nothing to roll back.
    """
    for label, step in steps:
        try:
            step()
        except Exception:
            logger.exception(
                "[purge] post-commit cleanup of %s failed; the SQL purge is "
                "committed but this mirror may still hold purged data",
                label,
            )


purge_room_tables_with_event_id_index = (
    "event_json",
    "event_push_actions_staging",
    "event_relations",
    "event_to_state_groups",
    "redactions",
    "rejections",
    "state_events",
)
"""
Tables which lack an index on `room_id` but have one on `event_id`
"""

purge_room_tables_with_room_id_column = (
    "current_state_events",
    "destination_rooms",
    "destination_rooms_sticky_events_backlog",
    "event_backward_extremities",
    "event_forward_extremities",
    "event_push_actions",
    "event_search",
    "event_failed_pull_attempts",
    # Note: the partial state tables have foreign keys between each other, and to
    # `events` and `rooms`. We need to delete from them in the right order.
    "partial_state_events",
    "partial_state_rooms_servers",
    "partial_state_rooms",
    # Note: the _membership(s) tables have foreign keys to the `events` table
    # so must be deleted first.
    "local_current_membership",
    "room_memberships",
    # Note: the sliding_sync_ tables have foreign keys to the `events` table
    # so must be deleted first.
    "sliding_sync_joined_rooms",
    "sliding_sync_membership_snapshots",
    # Note: msc4242_state_dag_forward_extremities/edges have a foreign key to the `events` table
    # so must be deleted first.
    "msc4242_state_dag_forward_extremities",
    "msc4242_state_dag_edges",
    # Note: `sticky_events` has foreign key to `events` so must be deleted first.
    "sticky_events",
    "events",
    "federation_inbound_events_staging",
    "receipts_graph",
    "receipts_linearized",
    "room_aliases",
    "room_depth",
    "room_stats_state",
    "room_stats_current",
    "room_stats_earliest_token",
    "stream_ordering_to_exterm",
    "users_in_public_rooms",
    "users_who_share_private_rooms",
    # no useful index, but let's clear them anyway
    "appservice_room_list",
    "e2e_room_keys",
    "event_push_summary",
    "pusher_throttle",
    "room_account_data",
    "room_tags",
    # "rooms" happens last, to keep the foreign keys in the other tables
    # happy
    "rooms",
)
"""
The tables with a `room_id` column regardless of whether they have a useful index on
`room_id`.
"""


class PurgeEventsStore(StateGroupWorkerStore, CacheInvalidationWorkerStore):
    async def purge_history(
        self, room_id: str, token: str, delete_local_events: bool
    ) -> set[int]:
        """Deletes room history before a certain point.

        Note that only a single purge can occur at once, this is guaranteed via
        a higher level (in the PaginationHandler).

        Args:
            room_id:
            token: A topological token to delete events before
            delete_local_events:
                if True, we will delete local events as well as remote ones
                (instead of just marking them as outliers and deleting their
                state groups).

        Returns:
            The set of state groups that are referenced by deleted events.
        """

        parsed_token = await RoomStreamToken.parse(self, token)

        post_commit: list[tuple[str, Callable[[], None]]] = []
        referenced_state_groups = await self.db_pool.runInteraction(
            "purge_history",
            self._purge_history_txn,
            room_id,
            parsed_token,
            delete_local_events,
            post_commit,
        )
        await self._run_post_commit(post_commit)
        return referenced_state_groups

    async def _run_post_commit(
        self, steps: list[tuple[str, Callable[[], None]]]
    ) -> None:
        """Run the mtxdb cleanups collected by a committed purge transaction on
        a worker thread, so the blocking deletes and disk syncs do not stall the
        reactor."""
        if steps:
            await defer_to_thread(self.hs.get_reactor(), _run_purge_cleanups, steps)

    def _purge_history_txn(
        self,
        txn: LoggingTransaction,
        room_id: str,
        token: RoomStreamToken,
        delete_local_events: bool,
        post_commit: list[tuple[str, Callable[[], None]]],
    ) -> set[int]:
        # The transaction may be retried, so drop steps queued by a failed attempt.
        post_commit.clear()
        # Tables that should be pruned:
        #     event_auth
        #     event_backward_extremities
        #     event_edges
        #     event_forward_extremities
        #     event_json
        #     event_push_actions
        #     event_relations
        #     event_search
        #     event_to_state_groups
        #     events
        #     rejections
        #     room_depth
        #     state_groups
        #     state_groups_state
        #     destination_rooms

        # we will build a temporary table listing the events so that we don't
        # have to keep shovelling the list back and forth across the
        # connection. Annoyingly the python sqlite driver commits the
        # transaction on CREATE, so let's do this first.
        #
        # furthermore, we might already have the table from a previous (failed)
        # purge attempt, so let's drop the table first.

        if isinstance(self.database_engine, PostgresEngine):
            # Disable statement timeouts for this transaction; purging rooms can
            # take a while!
            txn.execute("SET LOCAL statement_timeout = 0")

        txn.execute("DROP TABLE IF EXISTS events_to_purge")

        txn.execute(
            "CREATE TEMPORARY TABLE events_to_purge ("
            "    event_id TEXT NOT NULL,"
            "    should_delete BOOLEAN NOT NULL"
            ")"
        )

        # First ensure that we're not about to delete all the forward extremeties
        txn.execute(
            "SELECT e.event_id, e.depth FROM events as e "
            "INNER JOIN event_forward_extremities as f "
            "ON e.event_id = f.event_id "
            "AND e.room_id = f.room_id "
            "WHERE f.room_id = ?",
            (room_id,),
        )
        rows = txn.fetchall()
        # if we already have no forwards extremities (for example because they were
        # cleared out by the `delete_old_current_state_events` background database
        # update), then we may as well carry on.
        if rows:
            max_depth = max(row[1] for row in rows)

            if max_depth < token.topological:
                # We need to ensure we don't delete all the events from the database
                # otherwise we wouldn't be able to send any events (due to not
                # having any backwards extremities)
                raise SynapseError(
                    400, "topological_ordering is greater than forward extremities"
                )

        logger.info("[purge] looking for events to delete")

        should_delete_expr = "state_events.state_key IS NULL"
        should_delete_params: tuple[Any, ...] = ()
        if not delete_local_events:
            should_delete_expr += " AND sender NOT LIKE ?"

            # We include the parameter twice since we use the expression twice
            should_delete_params += (
                user_is_local_like_pattern(self.hs),
                user_is_local_like_pattern(self.hs),
            )

        should_delete_params += (room_id, token.topological)

        # Note that we insert events that are outliers and aren't going to be
        # deleted, as nothing will happen to them.
        txn.execute(
            "INSERT INTO events_to_purge"
            " SELECT event_id, %s"
            " FROM events AS e LEFT JOIN state_events USING (event_id)"
            " WHERE (NOT outlier OR (%s)) AND e.room_id = ? AND topological_ordering < ?"
            % (should_delete_expr, should_delete_expr),
            should_delete_params,
        )

        # We create the indices *after* insertion as that's a lot faster.

        # create an index on should_delete because later we'll be looking for
        # the should_delete / shouldn't_delete subsets
        txn.execute(
            "CREATE INDEX events_to_purge_should_delete"
            " ON events_to_purge(should_delete)"
        )

        # We do joins against events_to_purge for e.g. calculating state
        # groups to purge, etc., so lets make an index.
        txn.execute("CREATE INDEX events_to_purge_id ON events_to_purge(event_id)")

        txn.execute("SELECT event_id, should_delete FROM events_to_purge")
        event_rows = txn.fetchall()

        if len(event_rows) == 0:
            logger.info("[purge] no events found to purge")

            # For the sake of cleanliness: drop the temp table.
            # This will commit the txn in sqlite, so make sure to keep this actually last.
            txn.execute("DROP TABLE events_to_purge")
            # no referenced state groups
            return set()

        logger.info(
            "[purge] found %i events before cutoff, of which %i can be deleted",
            len(event_rows),
            sum(1 for e in event_rows if e[1]),
        )

        # Take the summary watermark now: the purge is about to start its
        # destructive work, and the row lock is held until this transaction
        # commits. Rotation and receipt processing therefore cannot summarize
        # rows we are about to delete. Doing this before the deletes matters on
        # Postgres: the transaction snapshot is fixed at its first data
        # statement, so a rotation that commits during a long purge would
        # otherwise make a late write fail with a serialization error and redo
        # every delete (the pool only retries a handful of times). The cost is
        # that rotation and receipt summary updates wait for the purge to
        # finish.
        #
        # Placing it after `events_to_purge` is populated skips the lock
        # entirely for purges that find nothing to delete, and still catches
        # serialization conflicts before any expensive work.
        #
        # This must be a write, not just `SELECT ... FOR UPDATE`: both sides run
        # at REPEATABLE READ, so a lock that leaves the row unmodified would not
        # make an older snapshot retry. SQLite commits on the temporary-table
        # DDL above, so the write starts a fresh transaction here.
        txn.execute(
            "UPDATE event_push_summary_stream_ordering"
            " SET stream_ordering = stream_ordering"
        )
        txn.execute("SELECT stream_ordering FROM event_push_summary_stream_ordering")
        rotation_row = txn.fetchone()
        if rotation_row is None:
            raise RuntimeError("event push summary rotation watermark is missing")
        rotated_upto = int(rotation_row[0])

        logger.info("[purge] Finding new backward extremities")

        # We calculate the new entries for the backward extremities by finding
        # events to be purged that are pointed to by events we're not going to
        # purge.
        purge_ids = {event_id for event_id, _ in event_rows}
        forward = get_event_edges_forward_batch(list(purge_ids))
        # The forward index can name children whose SQL rows are not
        # committed (or were deleted); only a child that is outside the
        # purge set and still has an events row keeps its parent.
        candidate_children = {
            child_id
            for event_id in purge_ids
            for child_id in (forward.get(event_id) or ())
            if child_id not in purge_ids
        }
        existing_children: set[str] = set()
        candidate_list = list(candidate_children)
        for start in range(0, len(candidate_list), 500):
            clause, args = make_in_list_sql_clause(
                self.database_engine,
                "event_id",
                candidate_list[start : start + 500],
            )
            txn.execute(f"SELECT event_id FROM events WHERE {clause}", args)
            existing_children.update(event_id for (event_id,) in txn)
        new_backwards_extrems = [
            (event_id,)
            for event_id in purge_ids
            if any(
                child_id in existing_children
                for child_id in (forward.get(event_id) or ())
            )
        ]

        logger.info("[purge] replacing backward extremities: %r", new_backwards_extrems)

        txn.execute(
            "DELETE FROM event_backward_extremities WHERE room_id = ?", (room_id,)
        )

        # Update backward extremeties
        txn.execute_batch(
            "INSERT INTO event_backward_extremities (room_id, event_id) VALUES (?, ?)",
            [(room_id, event_id) for (event_id,) in new_backwards_extrems],
        )

        logger.info("[purge] finding state groups referenced by purge-set events")

        if getattr(self, "_embedded_event_json_enabled", False):
            # Exclusive by configured engine, not a dual-write -- see
            # embedded_event_to_state_group.py. This is the *forward*
            # lookup (event_id -> state_group) for exactly the event_ids in
            # this purge batch's temp table, which mtxdb answers directly;
            # it is not the reverse "is state_group X still referenced
            # elsewhere" question (that's get_referenced_state_groups,
            # backed by the separate refcount).
            purge_set_event_ids = [event_id for event_id, _should_delete in event_rows]

            # Rows not yet migrated from SQL (the migration deletes each row once
            # copied) may reference state groups too. Delete them *before*
            # consulting mtxdb: the delete
            # takes the row locks the migration batch also takes, so the
            # migration either finishes first (and the lookup below sees what
            # it wrote) or can no longer select these rows. Otherwise it could
            # re-insert a stale mapping for a purged event.
            txn.execute(
                """
                SELECT DISTINCT state_group FROM events_to_purge
                INNER JOIN event_to_state_groups USING (event_id)
            """
            )
            referenced_state_groups = {sg for (sg,) in txn}
            logger.info("[purge] removing events from event_to_state_groups")
            # State events retained as outliers must also stop referencing their
            # state groups, even though their event rows are kept.
            txn.execute(
                "DELETE FROM event_to_state_groups "
                "WHERE event_id IN (SELECT event_id from events_to_purge)"
            )

            # Only mtxdb entries are refcounted, so only those are decremented.
            event_id_to_state_group = get_state_group_for_events_batch(
                self._embedded_db_engine,
                purge_set_event_ids,
                purpose="purge_traversal",
            )
            referenced_state_groups.update(event_id_to_state_group.values())
            logger.info(
                "[purge] found %i referenced state groups", len(referenced_state_groups)
            )

            state_group_keys = list(event_id_to_state_group.keys())
            state_group_values = list(event_id_to_state_group.values())
            post_commit.append(
                (
                    "event_to_state_group delete",
                    lambda: delete_event_to_state_group_batch(
                        self._embedded_db_engine, state_group_keys
                    ),
                )
            )
            post_commit.append(
                (
                    "state group refcounts",
                    lambda: decrement_state_group_refcounts_batch(
                        self._embedded_db_engine, state_group_values
                    ),
                )
            )
            post_commit.append(("state sync", lambda: sync_now(pools=[Pool.STATE])))
        else:
            # Get all state groups that are referenced by events that are to be
            # deleted.
            txn.execute(
                """
                SELECT DISTINCT state_group FROM events_to_purge
                INNER JOIN event_to_state_groups USING (event_id)
            """
            )

            referenced_state_groups = {sg for (sg,) in txn}
            logger.info(
                "[purge] found %i referenced state groups", len(referenced_state_groups)
            )

            logger.info("[purge] removing events from event_to_state_groups")
            txn.execute(
                "DELETE FROM event_to_state_groups "
                "WHERE event_id IN (SELECT event_id from events_to_purge)"
            )

        # Capture redaction targets before deleting the redactions rows by their
        # own event IDs. The embedded redaction mirror is keyed by `redacts`.
        redacted_event_ids: list[str] = []
        if getattr(self, "_embedded_event_json_enabled", False):
            txn.execute(
                "SELECT redacts FROM redactions WHERE event_id IN ("
                "    SELECT event_id FROM events_to_purge WHERE should_delete"
                ")"
            )
            redacted_event_ids = [redacts for (redacts,) in txn if redacts is not None]

        # Delete all remote non-state events
        for table in (
            "events",
            "event_json",
            "event_auth",
            "event_forward_extremities",
            "event_relations",
            "event_search",
            "rejections",
            "redactions",
        ):
            logger.info("[purge] removing events from %s", table)

            txn.execute(
                "DELETE FROM %s WHERE event_id IN ("
                "    SELECT event_id FROM events_to_purge WHERE should_delete"
                ")" % (table,)
            )

        # Remove the corresponding flat-KV entries, keyed by the redaction's
        # target (`redacts`). Like the edge mirror below, this runs only after
        # the SQL transaction commits: it is not rolled back with the
        # transaction, so doing it here could lose the mirror for events whose
        # SQL rows survive a failed or retried purge.
        # The `event_json` DELETE above only removed the SQL rows; the
        # embedded mirror (if any) has its own copy under `event_json:<id>`
        # keys that must be removed too, or purged events keep serving their
        # pre-purge content forever from mtxdb -- see embedded_event_json.py.
        # Deferred until after commit for the same reason as above.
        deleted_ids = [
            event_id for event_id, should_delete in event_rows if should_delete
        ]
        cleanups: list[tuple[str, Callable[[], None]]] = []
        if getattr(self, "_embedded_event_json_enabled", False):
            cleanups.append(
                (
                    "event_json/redaction mirrors",
                    lambda: _delete_purge_event_mirrors(
                        self._embedded_db_engine,
                        redacted_event_ids,
                        deleted_ids,
                    ),
                )
            )
        post_commit.extend(cleanups)

        # Some of the `event_push_actions` we're about to delete may have already
        # been rotated into the aggregate `event_push_summary` counts. Deleting
        # the rows without adjusting those counts would leave the summary
        # over-counting, inflating users' notification counts. So first work out
        # how much of each summary is attributable to the events being deleted and
        # decrement it.
        #
        # We only count rows that have already been rotated into the summary:
        # those at or before the rotated-up-to position
        # (`event_push_summary_stream_ordering`) and after the receipt used to
        # compute the summary. Rows beyond that position aren't in the summary
        # yet (they're still counted live from `event_push_actions`), so deleting
        # them needs no adjustment here. This mirrors how rotation counts them in
        # `_rotate_notifs_before_txn`.
        #
        # `rotated_upto` was read while holding the watermark write lock before
        # the deletes started, so rotation and receipt processing have not been
        # able to add these rows to the summary since.
        logger.info("[purge] adjusting event_push_summary for deleted events")

        txn.execute(
            """
            SELECT epa.user_id, epa.thread_id,
                COUNT(CASE WHEN epa.notif = 1 THEN 1 END),
                COUNT(CASE WHEN epa.unread = 1 THEN 1 END)
            FROM event_push_actions AS epa
            INNER JOIN event_push_summary AS eps USING (user_id, room_id, thread_id)
            WHERE epa.room_id = ?
                AND epa.event_id IN (
                    SELECT event_id FROM events_to_purge WHERE should_delete
                )
                AND epa.stream_ordering <= ?
                AND (
                    eps.last_receipt_stream_ordering IS NULL
                    OR epa.stream_ordering > eps.last_receipt_stream_ordering
                )
            GROUP BY epa.user_id, epa.thread_id
            """,
            (room_id, rotated_upto),
        )
        summary_decrements = cast(list[tuple[str, str, int, int]], txn.fetchall())

        # `unread_count` is nullable, so `COALESCE` it before subtracting (else
        # the result would be NULL). Clamp both counts at 0 via `GREATEST`/`MAX`
        # to guard against ever driving a count negative if the summary is
        # somehow out of sync with `event_push_actions`.
        greatest_func = (
            "GREATEST" if isinstance(self.database_engine, PostgresEngine) else "MAX"
        )
        txn.execute_batch(
            f"""
            UPDATE event_push_summary
            SET notif_count = {greatest_func}(notif_count - ?, 0),
                unread_count = {greatest_func}(COALESCE(unread_count, 0) - ?, 0)
            WHERE room_id = ? AND user_id = ? AND thread_id = ?
            """,
            [
                (notif_count, unread_count, room_id, user_id, thread_id)
                for user_id, thread_id, notif_count, unread_count in summary_decrements
            ],
        )

        # event_push_actions lacks an index on event_id, and has one on
        # (room_id, event_id) instead.
        for table in ("event_push_actions",):
            logger.info("[purge] removing events from %s", table)

            txn.execute(
                "DELETE FROM %s WHERE room_id = ? AND event_id IN ("
                "    SELECT event_id FROM events_to_purge WHERE should_delete"
                ")" % (table,),
                (room_id,),
            )

        # Mark all state and own events as outliers
        logger.info("[purge] marking remaining events as outliers")
        txn.execute(
            "UPDATE events SET outlier = TRUE"
            " WHERE event_id IN ("
            "   SELECT event_id FROM events_to_purge "
            "   WHERE NOT should_delete"
            ")"
        )

        # synapse tries to take out an exclusive lock on room_depth whenever it
        # persists events (because upsert), and once we run this update, we
        # will block that for the rest of our transaction.
        #
        # So, let's stick it at the end so that we don't block event
        # persistence.
        #
        # We do this by calculating the minimum depth of the backwards
        # extremities. However, the events in event_backward_extremities
        # are ones we don't have yet so we need to look at the events that
        # point to them.
        txn.execute(
            "SELECT event_id FROM event_backward_extremities WHERE room_id = ?",
            (room_id,),
        )
        extremity_ids = [event_id for (event_id,) in txn]
        forward = get_event_edges_forward_batch(extremity_ids)
        child_ids = {
            child_id for children in forward.values() for child_id in (children or ())
        }
        # Batch the IN list like the extremity lookup above. `MIN` is NULL
        # for a batch with no matching rows; those are ignored, and the
        # depth is 0 only if no batch matched anything.
        child_list = list(child_ids)
        batch_depths: list[int] = []
        for start in range(0, len(child_list), 500):
            clause, args = make_in_list_sql_clause(
                self.database_engine,
                "event_id",
                child_list[start : start + 500],
            )
            txn.execute(f"SELECT MIN(depth) FROM events WHERE {clause}", args)
            (batch_min,) = cast(tuple[int | None], txn.fetchone())
            if batch_min is not None:
                batch_depths.append(batch_min)
        min_depth = min(batch_depths) if batch_depths else 0

        logger.info("[purge] updating room_depth to %d", min_depth)

        txn.execute(
            "UPDATE room_depth SET min_depth = ? WHERE room_id = ?",
            (min_depth, room_id),
        )

        # finally, drop the temp table. this will commit the txn in sqlite,
        # so make sure to keep this actually last.
        txn.execute("DROP TABLE events_to_purge")

        self._invalidate_cache_and_stream_bulk(
            txn,
            self._get_state_group_for_event_sql,
            [(event_id,) for event_id, _ in event_rows],
        )

        # XXX: This is racy, since have_seen_events could be called between the
        #    transaction completing and the invalidation running. On the other hand,
        #    that's no different to calling `have_seen_events` just before the
        #    event is deleted from the database.
        self._invalidate_cache_and_stream_bulk(
            txn,
            self.have_seen_event,
            [
                (room_id, event_id)
                for event_id, should_delete in event_rows
                if should_delete
            ],
        )

        for event_id, should_delete in event_rows:
            if should_delete:
                self.invalidate_get_event_cache_after_txn(txn, event_id)

        logger.info("[purge] done")

        self._invalidate_caches_for_room_events_and_stream(txn, room_id)

        return referenced_state_groups

    async def purge_room(self, room_id: str) -> None:
        """Deletes all record of a room

        Args:
            room_id

        Returns:
            The list of state groups to delete.
        """

        # This first runs the purge transaction with READ_COMMITTED isolation level,
        # meaning any new rows in the tables will not trigger a serialization error.
        # We then run the same purge a second time without this isolation level to
        # purge any of those rows which were added during the first.

        logger.info("[purge] Starting initial main purge of [1/2]")
        post_commit: list[tuple[str, Callable[[], None]]] = []
        await self.db_pool.runInteraction(
            "purge_room",
            self._purge_room_txn,
            room_id=room_id,
            post_commit=post_commit,
            isolation_level=IsolationLevel.READ_COMMITTED,
        )
        await self._run_post_commit(post_commit)

        logger.info("[purge] Starting secondary main purge of [2/2]")
        post_commit = []
        await self.db_pool.runInteraction(
            "purge_room",
            self._purge_room_txn,
            room_id=room_id,
            post_commit=post_commit,
        )
        await self._run_post_commit(post_commit)

        logger.info("[purge] Done with main purge")

    def _purge_room_txn(
        self,
        txn: LoggingTransaction,
        room_id: str,
        post_commit: list[tuple[str, Callable[[], None]]],
    ) -> None:
        post_commit.clear()
        # This collides with event persistence so we cannot write new events and metadata into
        # a room while deleting it or this transaction will fail.
        if isinstance(self.database_engine, PostgresEngine):
            txn.execute(
                "SELECT room_version FROM rooms WHERE room_id = ? FOR UPDATE",
                (room_id,),
            )

        if isinstance(self.database_engine, PostgresEngine):
            # Disable statement timeouts for this transaction; purging rooms can
            # take a while!
            txn.execute("SET LOCAL statement_timeout = 0")

        # The auth-chain collections are keyed by room and have no foreign key
        # to the SQL rows, so purge them alongside the room. Bump the in-process
        # generation first so cached closures are discarded; the durable delete
        # itself runs after the SQL commit, like the other mtxdb purge work.
        room_cleanups: list[tuple[str, Callable[[], None]]] = []
        if getattr(self, "_embedded_db_engine", None) is not None:
            from synapse.storage.databases.main.embedded_event_auth_chains import (
                bump_room_generation,
                purge_room as purge_auth_chain_room,
            )

            bump_room_generation(room_id)

            def purge_then_bump(engine_name: str | None, purged_room_id: str) -> None:
                try:
                    purge_auth_chain_room(engine_name, purged_room_id)
                finally:
                    # A closure computed between the bump above and this
                    # durable delete was built from data that no longer
                    # exists (the SQL purge has committed), and was cached
                    # under the current generation. Advance it again so
                    # such entries cannot be served, even when the delete
                    # fails partway.
                    bump_room_generation(purged_room_id)

            # Isolated like the other mirrors: a failed auth-chain delete
            # must not stop the event/edge/state-group cleanups below.
            room_cleanups.append(
                (
                    "auth-chain index",
                    lambda: purge_then_bump(self._embedded_db_engine, room_id),
                )
            )

        # event_auth is still retained as an offline migration snapshot, so
        # it may contain rows whose event rows are already absent (for
        # example, after an earlier partial purge). Delete by its own room_id
        # before removing events, rather than relying on an events subquery.
        txn.execute("DELETE FROM event_auth WHERE room_id = ?", (room_id,))

        # Keyed by (old_room_id, new_room_id, user_id) rather than a single
        # room_id column, so it is not covered by the generic room_id sweep.
        txn.execute(
            "DELETE FROM room_upgrade_user_transfers "
            "WHERE old_room_id = ? OR new_room_id = ?",
            (room_id, room_id),
        )

        room_event_ids: list[str] = []
        room_redacted_event_ids: list[str] = []
        if getattr(self, "_embedded_event_json_enabled", False):
            txn.execute("SELECT event_id FROM events WHERE room_id=?", (room_id,))
            room_event_ids = [event_id for (event_id,) in txn]
            txn.execute(
                "SELECT redactions.redacts FROM redactions "
                "INNER JOIN events ON redactions.event_id = events.event_id "
                "WHERE events.room_id = ?",
                (room_id,),
            )
            room_redacted_event_ids = [
                redacts for (redacts,) in txn if redacts is not None
            ]

        # Now we delete tables which lack an index on `room_id` but have one on `event_id`
        for table in purge_room_tables_with_event_id_index:
            logger.info("[purge] removing from %s", table)

            txn.execute(
                """
                DELETE FROM %s WHERE event_id IN (
                  SELECT event_id FROM events WHERE room_id=?
                )
                """
                % (table,),
                (room_id,),
            )

        # next, the tables with a `room_id` column regardless of whether they have a
        # useful index on `room_id`
        for table in purge_room_tables_with_room_id_column:
            logger.info("[purge] removing from %s", table)
            txn.execute("DELETE FROM %s WHERE room_id=?" % (table,), (room_id,))

        # As in _purge_history_txn, the mirrors are deleted only after the SQL
        # purge commits. The callbacks retry transient mtxdb contention.
        if room_event_ids and getattr(self, "_embedded_event_json_enabled", False):
            room_cleanups.append(
                (
                    "event_json/redaction mirrors",
                    lambda: _delete_purge_event_mirrors(
                        self._embedded_db_engine,
                        room_redacted_event_ids,
                        room_event_ids,
                    ),
                )
            )
        if room_event_ids and getattr(self, "_embedded_event_edges_writable", False):
            room_cleanups.append(
                (
                    "event edges",
                    lambda: purge_event_edges_room(room_id),
                )
            )
        post_commit.extend(room_cleanups)
        if room_event_ids:
            # Same for event_to_state_groups: fetch state_groups before
            # deleting so the refcount can be rebalanced.
            event_id_to_state_group = get_state_group_for_events_batch(
                self._embedded_db_engine,
                room_event_ids,
                purpose="purge_traversal",
            )
            room_state_group_keys = list(event_id_to_state_group.keys())
            room_state_group_values = list(event_id_to_state_group.values())
            post_commit.append(
                (
                    "event_to_state_group delete",
                    lambda: delete_event_to_state_group_batch(
                        self._embedded_db_engine, room_state_group_keys
                    ),
                )
            )
            post_commit.append(
                (
                    "state group refcounts",
                    lambda: decrement_state_group_refcounts_batch(
                        self._embedded_db_engine, room_state_group_values
                    ),
                )
            )
            # One sync for the whole purge batch (event_json delete +
            # Immediate sync + clear dirty flags for event_dag and state.
            post_commit.append(
                ("state sync", lambda: sync_now(pools=[Pool.EVENT_DAG, Pool.STATE]))
            )

        # Other tables we do NOT need to clear out:
        #
        #  - blocked_rooms
        #    This is important, to make sure that we don't accidentally rejoin a blocked
        #    room after it was purged
        #
        #  - user_directory
        #    This has a room_id column, but it is unused
        #

        # Other tables that we might want to consider clearing out include:
        #
        #  - event_reports
        #       Given that these are intended for abuse management my initial
        #       inclination is to leave them in place.
        #
        #  - current_state_delta_stream
        #  - ex_outlier_stream
        #  - room_tags_revisions
        #       The problem with these is that they are largeish and there is no room_id
        #       index on them. In any case we should be clearing out 'stream' tables
        #       periodically anyway (https://github.com/matrix-org/synapse/issues/5888)

        self._invalidate_caches_for_room_and_stream(txn, room_id)
