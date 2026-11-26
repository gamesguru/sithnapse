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

"""Rebuild mtxdb CSR event adjacency from SQL edges or event JSON.

Run with every Synapse process stopped, after the embedded-store key rekey.
The event-JSON source requires the current unsalted event locator to be
readable. The tool validates the full source before changing any CSR room.
"""

import argparse
import json
import logging
from collections.abc import Sequence
from typing import Any, TypedDict, cast

import yaml

from twisted.internet import defer, reactor as reactor_

from synapse.config.homeserver import HomeServerConfig
from synapse.server import HomeServer
from synapse.storage import DataStore
from synapse.storage.database import LoggingTransaction, make_in_list_sql_clause
from synapse.storage.databases.main.embedded_common import Pool, sync_now
from synapse.storage.databases.main.embedded_event_edges import (
    EdgeRow,
    get_event_edges_backward_batch,
    get_event_edges_forward_batch,
    purge_event_edges_room,
    put_event_edges_batch,
)
from synapse.storage.databases.main.embedded_event_json import get_event_json_batch
from synapse.storage.engines import PostgresEngine
from synapse.types import ISynapseReactor

reactor = cast(ISynapseReactor, reactor_)
logger = logging.getLogger("synapse_mtxdb_edges_rebuild")


class RebuildStats(TypedDict):
    source: str
    rooms: int
    events_scanned: int
    events_needing_rebuild: int
    events_without_body: int
    rooms_not_rebuilt: int
    written: int
    mismatches_after_write: int


class RebuildHomeserver(HomeServer):
    DATASTORE_CLASS = DataStore
    # This offline tool must never run database schema migrations.
    DATABASE_SCHEMA_READ_ONLY = True

    def __init__(self, config: HomeServerConfig):
        super().__init__(
            hostname=config.server.server_name,
            config=config,
            reactor=reactor,
        )


def _table_exists(txn: LoggingTransaction, postgres: bool) -> bool:
    if postgres:
        txn.execute("SELECT to_regclass('edge_backend')")
    else:
        txn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'edge_backend'"
        )
    row = txn.fetchone()
    # Postgres's to_regclass returns a NULL row for a missing relation, so a
    # present row alone is not enough.
    return row is not None and row[0] is not None


def _select_source(txn: LoggingTransaction, postgres: bool, requested: str) -> str:
    if requested != "auto":
        return requested
    if not _table_exists(txn, postgres):
        # The current schema delta retires this marker. Event JSON is the
        # authoritative fallback once the salted event records are rekeyed.
        return "event-json"
    txn.execute("SELECT backend FROM edge_backend WHERE lock = 'X'")
    row = txn.fetchone()
    backend = str(row[0]) if row else "sql"
    if backend == "sql":
        return "sql-edges"
    if backend == "mtxdb":
        return "event-json"
    raise RuntimeError(f"unrecognized edge_backend value: {backend!r}")


def _room_ids_txn(txn: LoggingTransaction, requested: Sequence[str]) -> list[str]:
    if requested:
        clause, args = make_in_list_sql_clause(
            # `room_id` is text on both supported engines.
            txn.database_engine,
            "room_id",
            requested,
        )
        txn.execute(f"SELECT room_id FROM rooms WHERE {clause} ORDER BY room_id", args)
    else:
        txn.execute("SELECT room_id FROM rooms ORDER BY room_id")
    return [str(row[0]) for row in txn.fetchall()]


def _event_ids_txn(
    txn: LoggingTransaction, room_id: str, after: str, limit: int
) -> list[str]:
    # Runtime persistence deliberately skips adjacency writes for rejected
    # events; keep the rebuild's source set consistent with that contract.
    txn.execute(
        "SELECT event_id FROM events e WHERE room_id = ? AND event_id > ? "
        "AND NOT EXISTS (SELECT 1 FROM rejections r WHERE r.event_id = e.event_id) "
        "ORDER BY event_id LIMIT ?",
        (room_id, after, limit),
    )
    return [str(row[0]) for row in txn.fetchall()]


def _sql_edge_rows_txn(
    txn: LoggingTransaction, event_ids: Sequence[str]
) -> dict[str, list[str]]:
    if not event_ids:
        return {}
    clause, args = make_in_list_sql_clause(txn.database_engine, "event_id", event_ids)
    txn.execute(
        f"SELECT event_id, prev_event_id FROM event_edges WHERE {clause} "
        "ORDER BY event_id, prev_event_id",
        args,
    )
    result: dict[str, list[str]] = {}
    for event_id, prev_event_id in txn.fetchall():
        result.setdefault(str(event_id), []).append(str(prev_event_id))
    return result


def _sql_event_json_txn(
    txn: LoggingTransaction, event_ids: Sequence[str]
) -> dict[str, str]:
    if not event_ids:
        return {}
    clause, args = make_in_list_sql_clause(txn.database_engine, "event_id", event_ids)
    txn.execute(
        f"SELECT event_id, json FROM event_json WHERE {clause}",
        args,
    )
    return {str(event_id): str(event_json) for event_id, event_json in txn.fetchall()}


def _normalise_prev_events(event_json: str, event_id: str) -> list[str]:
    try:
        body = json.loads(event_json)
    except (TypeError, ValueError) as e:
        raise RuntimeError(f"invalid event JSON for {event_id}: {e}") from e
    prev_events = body.get("prev_events")
    if not isinstance(prev_events, list):
        raise RuntimeError(f"event {event_id} has no prev_events list")
    result: list[str] = []
    for prev in prev_events:
        # Older room versions encode [event_id, hashes]; newer versions encode
        # just the event ID. Both forms are present in persisted event JSON.
        prev_id = prev[0] if isinstance(prev, list) and prev else prev
        if not isinstance(prev_id, str):
            raise RuntimeError(f"event {event_id} has malformed prev_events")
        result.append(prev_id)
    return result


async def _source_rows(
    store: Any,
    engine_name: str,
    source: str,
    room_id: str,
    event_ids: Sequence[str],
    allow_missing_bodies: bool,
) -> list[EdgeRow]:
    sql_prevs: dict[str, list[str]] = {}
    if source == "sql-edges":
        sql_prevs = await store.db_pool.runInteraction(
            "mtxdb_edges_rebuild_sql_edges",
            _sql_edge_rows_txn,
            event_ids,
        )

    embedded = get_event_json_batch(engine_name, list(event_ids))
    missing_ids = [event_id for event_id in event_ids if event_id not in embedded]
    sql_json = await store.db_pool.runInteraction(
        "mtxdb_edges_rebuild_sql_event_json",
        _sql_event_json_txn,
        missing_ids,
    )
    body_prevs: dict[str, list[str]] = {}
    missing: list[str] = []
    for event_id in event_ids:
        embedded_row = embedded.get(event_id)
        event_json = (
            embedded_row[1] if embedded_row is not None else sql_json.get(event_id)
        )
        if event_json is None:
            sql_prev_ids = sql_prevs.get(event_id)
            if source == "sql-edges" and sql_prev_ids is not None:
                body_prevs[event_id] = sql_prev_ids
            else:
                missing.append(event_id)
            continue
        body_prevs[event_id] = _normalise_prev_events(event_json, event_id)
    if missing:
        if source == "event-json" and allow_missing_bodies:
            logger.warning(
                "skipping %d events without readable bodies in room %s: %s%s",
                len(missing),
                room_id,
                ", ".join(missing[:10]),
                " ..." if len(missing) > 10 else "",
            )
        elif source == "event-json":
            detail = "event JSON source is incomplete"
        else:
            detail = "SQL edge rows are absent and event JSON fallback is unavailable"
        if not (source == "event-json" and allow_missing_bodies):
            raise RuntimeError(
                detail
                + "; missing bodies for "
                + ", ".join(missing[:10])
                + (" ..." if len(missing) > 10 else "")
            )

    if source == "sql-edges":
        # Event bodies are the completeness check and fallback: an absent SQL
        # row is never interpreted as a parentless event. A discrepant SQL
        # source is reported, while the body remains authoritative.
        for event_id in event_ids:
            sql_ids = sql_prevs.get(event_id)
            body_ids = body_prevs[event_id]
            if sql_ids is not None and set(sql_ids) != set(body_ids):
                logger.warning(
                    "SQL event_edges disagree with event body for %s; using body",
                    event_id,
                )

    rows: list[EdgeRow] = []
    for event_id in event_ids:
        prev_ids = body_prevs.get(event_id)
        if prev_ids is None:
            continue
        rows.extend((room_id, event_id, prev_id) for prev_id in prev_ids)
        if not prev_ids:
            rows.append((room_id, event_id, None))
    return rows


async def rebuild(
    hs: HomeServer,
    source_arg: str,
    rooms: Sequence[str],
    batch_size: int,
    dry_run: bool,
    allow_missing_bodies: bool = False,
) -> RebuildStats:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if (
        not hs.config.database.embedded_db_engine
        or not hs.config.database.embedded_db_path
    ):
        raise ValueError("configure embedded_db.engine and embedded_db.path")

    store = hs.get_datastores().main
    engine_name = hs.config.database.embedded_db_engine
    postgres = isinstance(store.database_engine, PostgresEngine)
    source = await store.db_pool.runInteraction(
        "mtxdb_edges_rebuild_source",
        _select_source,
        postgres,
        source_arg,
    )
    room_ids = await store.db_pool.runInteraction(
        "mtxdb_edges_rebuild_rooms", _room_ids_txn, rooms
    )
    missing_rooms = set(rooms) - set(room_ids)
    if missing_rooms:
        raise ValueError("unknown room IDs: " + ", ".join(sorted(missing_rooms)))
    stats: RebuildStats = {
        "source": source,
        "rooms": 0,
        "events_scanned": 0,
        "events_needing_rebuild": 0,
        "events_without_body": 0,
        "rooms_not_rebuilt": 0,
        "written": 0,
        "mismatches_after_write": 0,
    }
    rooms_to_rebuild: list[str] = []

    # Preflight the entire source before the first destructive room reset.
    for room_id in room_ids:
        after = ""
        room_mismatch_count = 0
        room_skipped = 0
        while True:
            event_ids = await store.db_pool.runInteraction(
                "mtxdb_edges_rebuild_event_ids",
                _event_ids_txn,
                room_id,
                after,
                batch_size,
            )
            if not event_ids:
                break
            rows = await _source_rows(
                store,
                engine_name,
                source,
                room_id,
                event_ids,
                allow_missing_bodies,
            )
            readable = {event_id for _, event_id, _ in rows}
            skipped = len(event_ids) - len(readable)
            stats["events_without_body"] += skipped
            room_skipped += skipped
            expected: dict[str, set[str]] = {event_id: set() for event_id in readable}
            for _, event_id, prev_id in rows:
                if prev_id is not None:
                    expected[event_id].add(prev_id)
            actual = get_event_edges_backward_batch(sorted(readable))
            # An unreadable event has no expected value, so it is never a
            # mismatch: it cannot be compared, and must not look parentless.
            mismatch_count = sum(
                set(actual.get(event_id, [])) != expected[event_id]
                for event_id in readable
            )
            stats["events_scanned"] += len(event_ids)
            room_mismatch_count += mismatch_count
            stats["events_needing_rebuild"] += mismatch_count
            after = event_ids[-1]
        if room_mismatch_count and room_skipped:
            # Rebuilding resets the room, which would drop the existing
            # adjacency of the events whose bodies cannot be read.
            logger.error(
                "Not rebuilding room %s: %d event(s) have no readable body",
                room_id,
                room_skipped,
            )
            stats["rooms_not_rebuilt"] += 1
        elif room_mismatch_count:
            rooms_to_rebuild.append(room_id)
        stats["rooms"] += 1

    if dry_run:
        return stats

    for room_id in rooms_to_rebuild:
        # A room is rebuilt as a unit because CSR rows are add-only. Source
        # validation completed for every room before this destructive reset.
        purge_event_edges_room(room_id)
        after = ""
        while True:
            event_ids = await store.db_pool.runInteraction(
                "mtxdb_edges_rebuild_event_ids",
                _event_ids_txn,
                room_id,
                after,
                batch_size,
            )
            if not event_ids:
                break
            rows = await _source_rows(
                store,
                engine_name,
                source,
                room_id,
                event_ids,
                allow_missing_bodies,
            )
            put_event_edges_batch(rows)
            stats["written"] += len(event_ids)
            after = event_ids[-1]
        sync_now(pools=[Pool.EVENT_DAG])
        after = ""
        while True:
            event_ids = await store.db_pool.runInteraction(
                "mtxdb_edges_rebuild_verify_ids",
                _event_ids_txn,
                room_id,
                after,
                batch_size,
            )
            if not event_ids:
                break
            expected_rows = await _source_rows(
                store,
                engine_name,
                source,
                room_id,
                event_ids,
                allow_missing_bodies,
            )
            expected_by_event: dict[str, set[str]] = {
                event_id: set() for event_id in event_ids
            }
            for _, event_id, prev_id in expected_rows:
                if prev_id is not None:
                    expected_by_event[event_id].add(prev_id)
            actual = get_event_edges_backward_batch(event_ids)
            mismatches = [
                event_id
                for event_id in event_ids
                if set(actual.get(event_id, [])) != expected_by_event[event_id]
            ]
            if mismatches:
                stats["mismatches_after_write"] += len(mismatches)
                raise RuntimeError(
                    f"CSR verification failed for room {room_id}; first mismatches: "
                    + ", ".join(mismatches[:10])
                )
            expected_children: dict[str, set[str]] = {}
            for event_id, prev_ids in expected_by_event.items():
                for prev_id in prev_ids:
                    expected_children.setdefault(prev_id, set()).add(event_id)
            actual_children = get_event_edges_forward_batch(list(expected_children))
            reverse_mismatches = [
                parent_id
                for parent_id, children in expected_children.items()
                if not children.issubset(set(actual_children.get(parent_id) or []))
            ]
            if reverse_mismatches:
                stats["mismatches_after_write"] += len(reverse_mismatches)
                raise RuntimeError(
                    f"CSR reverse verification failed for room {room_id}; first parents: "
                    + ", ".join(reverse_mismatches[:10])
                )
            after = event_ids[-1]
        logger.info("rebuilt CSR adjacency for room %s", room_id)

    return stats


async def rebuild_edges(
    hs: HomeServer,
    *,
    rooms: Sequence[str] = (),
    batch_size: int = 500,
    dry_run: bool = False,
    allow_missing_bodies: bool = False,
) -> RebuildStats:
    """Convenience entry point for tests and offline callers."""
    return await rebuild(hs, "auto", rooms, batch_size, dry_run, allow_missing_bodies)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild mtxdb CSR event adjacency offline; stop all Synapse processes first."
    )
    parser.add_argument("--config", type=argparse.FileType("r"), required=True)
    parser.add_argument(
        "--source",
        choices=["auto", "sql-edges", "event-json"],
        default="auto",
        help="auto uses edge_backend when present, otherwise event JSON (after rekey)",
    )
    parser.add_argument("--rooms", nargs="*", default=[])
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--allow-missing-bodies",
        action="store_true",
        help="explicitly permit incomplete event-json rebuilds (logs each affected page)",
    )
    parser.add_argument(
        "--rekey-complete",
        action="store_true",
        help="confirm the embedded store has already been rekeyed to unsalted IDs",
    )
    parser.add_argument(
        "--upgrade-schema",
        action="store_true",
        help="apply pending schema deltas first, as the new build would at its "
        "first start. Needed on a store whose deltas are not applied yet (a "
        "pre-migration prod snapshot). Delta 97/09 drops `edge_backend`, so "
        "this requires an explicit --source.",
    )
    parser.add_argument("-v", action="store_true")
    args = parser.parse_args()
    if args.upgrade_schema and args.source == "auto":
        parser.error(
            "--upgrade-schema drops the edge_backend marker that --source auto "
            "reads; read its value first and pass --source event-json or sql-edges"
        )
    logging.basicConfig(
        level=logging.DEBUG if args.v else logging.INFO,
        format="%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s",
    )

    config = HomeServerConfig()
    config.parse_config_dict(yaml.safe_load(args.config), "", "")
    if not args.rekey_complete:
        parser.error("run only after the embedded-store rekey; pass --rekey-complete")
    hs = RebuildHomeserver(config)
    if args.upgrade_schema:
        hs.DATABASE_SCHEMA_READ_ONLY = False
    hs.setup()
    hs.get_storage_controllers()
    exit_code = 0

    async def run() -> None:
        nonlocal exit_code
        try:
            result = await rebuild(
                hs,
                args.source,
                args.rooms,
                args.batch_size,
                args.dry_run,
                args.allow_missing_bodies,
            )
            print(json.dumps(result, indent=2, sort_keys=True))
            if result["rooms_not_rebuilt"]:
                # Some rooms were left as they were; the run is not complete.
                exit_code = 1
        except Exception:
            logger.exception("CSR edge rebuild failed")
            exit_code = 1
        finally:
            reactor.stop()

    hs.get_clock().call_when_running(lambda: defer.ensureDeferred(run()))
    reactor.run()
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
