#!/usr/bin/env python3
"""Shared helpers for the one-off event_json record rewrite scripts."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

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


def sql_connection(config_path: str) -> tuple[str, Any]:
    """Open the main SQL database named in the same config, for reading the
    authoritative `events` (event_id, room_id) list. Node ids are hashes and
    the stored PDU JSON has no event_id for room v3+, so ids cannot be
    recovered from mtxdb itself."""
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

        return name, sqlite3.connect(f"file:{args['database']}?mode=ro", uri=True)
    if name == "psycopg2":
        import psycopg2

        return name, psycopg2.connect(**args)
    raise RuntimeError(
        f"unsupported database.name {name!r}; expected sqlite3 or psycopg2"
    )
