#!/usr/bin/env python3
"""Shared helpers for the one-off event_json record rewrite scripts."""

from __future__ import annotations

import hashlib
import json
import os
import struct
from pathlib import Path
from typing import Any, Callable, Iterable

from synapse.config import find_config_files
from synapse.config._base import read_config_files


def embedded_database(config_path: str) -> tuple[str, Path, str]:
    """Resolve the mtxdb settings the same way Synapse resolves them."""
    config_files = find_config_files([config_path])
    if not config_files:
        raise RuntimeError(f"no config file found at {config_path}")
    config = read_config_files(config_files)
    embedded = config.get("embedded_db") or {}

    engine = os.environ.get("SYNAPSE_EMBEDDED_DB_ENGINE") or embedded.get("engine")
    path = os.environ.get("SYNAPSE_EMBEDDED_DB_PATH") or embedded.get("path")
    if os.environ.get("SYNAPSE_MTXDB"):
        engine = "mtxdb"
        path = os.environ.get("SYNAPSE_MTXDB_PATH") or path

    if engine != "mtxdb":
        raise RuntimeError(
            f"config resolves embedded_db.engine to {engine!r}, expected 'mtxdb'"
        )
    if not isinstance(path, str) or not path:
        raise RuntimeError("config does not resolve an embedded_db.path")

    server_name = config.get("server_name")
    namespace = embedded.get("namespace") or server_name
    if not isinstance(namespace, str) or not namespace:
        raise RuntimeError("could not resolve embedded_db.namespace or server_name")

    # Match the server's relative-path behavior: it resolves relative to its
    # working directory. Run these tools from the service's WorkingDirectory.
    return engine, Path(os.path.abspath(os.path.expanduser(path))), namespace


def event_node_id(namespace: str, event_id: str) -> bytes:
    digest = hashlib.sha256(
        b"event_json:event:"
        + namespace.encode("utf-8")
        + b"\0"
        + event_id.encode("utf-8")
    ).digest()
    return digest[:16]


def event_meta_node_id(namespace: str, event_id: str) -> bytes:
    digest = hashlib.sha256(
        b"event_json:meta:"
        + namespace.encode("utf-8")
        + b"\0"
        + event_id.encode("utf-8")
    ).digest()
    return digest[:16]


def decode_record_metadata(record: bytes) -> str:
    """Return the `internal_metadata` string from a combined event record."""
    if len(record) < 8:
        raise RuntimeError("event record is too short to carry metadata")
    _format_version, metadata_len = struct.unpack(">iI", record[:8])
    end = 8 + metadata_len
    if end > len(record):
        raise RuntimeError("event record metadata length runs past the record")
    return record[8:end].decode("utf-8")


def soft_failed_from_record(record: bytes) -> bool:
    metadata = json.loads(decode_record_metadata(record))
    if not isinstance(metadata, dict):
        raise RuntimeError("event internal_metadata is not a JSON object")
    return bool(metadata.get("soft_failed"))


def sql_event_statuses(
    sql: Any, sql_name: str, event_ids: Iterable[str]
) -> dict[str, tuple[bool, bool]]:
    """Read `(outlier, rejected)` for every id from the authoritative SQL rows.

    A missing SQL row is an error: writing a locator without its status would
    silently turn a rejected or outlier event into a normal one.
    """
    ids = list(event_ids)
    if not ids:
        return {}
    mark = "%s" if sql_name == "psycopg2" else "?"
    placeholders = ", ".join(mark for _ in ids)
    cursor = sql.cursor()
    cursor.execute(
        "SELECT e.event_id, e.outlier, "
        "(r.event_id IS NOT NULL OR e.rejection_reason IS NOT NULL) "
        "FROM events AS e LEFT JOIN rejections AS r USING (event_id) "
        f"WHERE e.event_id IN ({placeholders})",
        ids,
    )
    statuses = {
        str(event_id): (bool(outlier), bool(rejected))
        for event_id, outlier, rejected in cursor.fetchall()
    }
    missing = [event_id for event_id in ids if event_id not in statuses]
    if missing:
        raise RuntimeError(
            f"no SQL event row for {missing[0]}; cannot restore its status"
        )
    return statuses


def write_event_batch(
    engine: Any,
    sql: Any,
    sql_name: str,
    rows: Iterable[tuple[str, str, bytes]],
    *,
    soft_failed_for_record: Callable[[bytes], bool] = soft_failed_from_record,
) -> int:
    """Write bodies first, then locators carrying the SQL status.

    `event_json_put` is the initial-persist path and creates locators with
    every status flag cleared, which would drop outlier/rejected/soft-failed
    state on a restore. Bodies go first so a failure leaves the previous
    locators resolvable and a retry safe.
    """
    batch = list(rows)
    if not batch:
        return 0
    statuses = sql_event_statuses(sql, sql_name, (event_id for _, event_id, _ in batch))
    engine.event_json_body_put(
        [(room_id, event_id, record) for room_id, event_id, record in batch]
    )
    engine.event_locator_put(
        [
            (
                room_id,
                event_id,
                statuses[event_id][0],
                statuses[event_id][1],
                soft_failed_for_record(record),
            )
            for room_id, event_id, record in batch
        ]
    )
    return len(batch)


def mark_event_status_locators_complete(sql: Any, sql_name: str) -> None:
    """Record that every locator in this store carries its status flags."""
    mark = "%s" if sql_name == "psycopg2" else "?"
    cursor = sql.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS event_status_locator_backfill ("
        " lock TEXT NOT NULL,"
        " completed INTEGER NOT NULL DEFAULT 0,"
        " last_event_id TEXT NOT NULL DEFAULT '',"
        " PRIMARY KEY (lock))"
    )
    cursor.execute(
        "INSERT INTO event_status_locator_backfill"
        " (lock, completed, last_event_id)"
        f" VALUES ({mark}, 1, '')"
        " ON CONFLICT (lock) DO UPDATE SET completed = 1, last_event_id = ''",
        ("X",),
    )
    sql.commit()


def _main_database_config(config: dict[str, Any]) -> dict[str, Any]:
    """Resolve the connection that owns Synapse's main data store."""
    database = config.get("database")
    if database is None:
        databases = config.get("databases") or {}
        if not isinstance(databases, dict):
            raise RuntimeError("config 'databases' must be a mapping")
        main_databases = [
            database_config
            for database_config in databases.values()
            if isinstance(database_config, dict)
            and "main" in database_config.get("data_stores", ["main", "state"])
        ]
        if len(main_databases) != 1:
            raise RuntimeError(
                "config must identify exactly one database for the 'main' data store"
            )
        database = main_databases[0]
    if not isinstance(database, dict):
        raise RuntimeError("config 'database' must be a mapping")
    return database


def sql_connection(config_path: str, *, read_only: bool = True) -> tuple[str, Any]:
    """Open the main SQL database named in the same config.

    Reads the authoritative `events` (event_id, room_id) list; node ids are
    hashes and the stored PDU JSON has no event_id for room v3+, so ids cannot
    be recovered from mtxdb itself. Pass `read_only=False` only when the
    caller also writes the migration marker.
    """
    config_files = find_config_files([config_path])
    config = read_config_files(config_files)
    database = _main_database_config(config)
    name = database.get("name")
    args = {
        key: value
        for key, value in (database.get("args") or {}).items()
        if not key.startswith("cp_")
    }
    if name == "sqlite3":
        import sqlite3

        if read_only:
            return name, sqlite3.connect(f"file:{args['database']}?mode=ro", uri=True)
        return name, sqlite3.connect(str(args["database"]))
    if name == "psycopg2":
        import psycopg2

        return name, psycopg2.connect(**args)
    raise RuntimeError(
        f"unsupported database.name {name!r}; expected sqlite3 or psycopg2"
    )
