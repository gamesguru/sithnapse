#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

"""
Schema metadata mapping SQL tables to RocksDB key-value schemas.
This allows the translation layer to construct appropriate key prefixes and map
serialized row dictionaries back to columns.
"""

from typing import Any


class TableSchema:
    def __init__(self, primary_keys: tuple[str, ...], columns: list[str]):
        self.primary_keys = primary_keys
        self.columns = columns


# Explicitly map the core Synapse tables to their primary keys and columns.
# For any table not listed here, the translation layer can fall back to
# generic behavior (e.g., parsing the CREATE TABLE statements at runtime,
# or assuming the first column is the primary key).
ROCKSDB_TABLE_SCHEMAS: dict[str, TableSchema] = {
    "users": TableSchema(
        primary_keys=("name",),
        columns=[
            "name",
            "password_hash",
            "creation_ts",
            "admin",
            "upgrade_ts",
            "is_guest",
            "appservice_id",
            "consent_version",
            "consent_server_notice_sent",
            "user_type",
            "deactivated",
            "shadow_banned",
            "consent_ts",
        ],
    ),
    "profiles": TableSchema(
        primary_keys=("user_id",),
        columns=["user_id", "displayname", "avatar_url"],
    ),
    "rooms": TableSchema(
        primary_keys=("room_id",),
        columns=[
            "room_id",
            "is_public",
            "creator",
            "room_version",
            "has_auth_chain_index",
        ],
    ),
    "room_memberships": TableSchema(
        primary_keys=("event_id",),
        columns=[
            "event_id",
            "user_id",
            "sender",
            "room_id",
            "membership",
            "forgotten",
            "display_name",
            "avatar_url",
        ],
    ),
    "events": TableSchema(
        primary_keys=("event_id",),
        columns=[
            "stream_ordering",
            "topological_ordering",
            "event_id",
            "type",
            "room_id",
            "content",
            "unrecognized_keys",
            "processed",
            "outlier",
            "depth",
            "origin_server_ts",
            "received_ts",
            "sender",
            "contains_url",
            "instance_name",
            "state_key",
            "rejection_reason",
        ],
    ),
    "event_json": TableSchema(
        primary_keys=("event_id",),
        columns=["event_id", "room_id", "internal_metadata", "json", "format_version"],
    ),
    "current_state_events": TableSchema(
        primary_keys=("event_id",),
        columns=["event_id", "room_id", "type", "state_key", "membership"],
    ),
    "state_events": TableSchema(
        primary_keys=("event_id",),
        columns=["event_id", "room_id", "type", "state_key", "prev_state"],
    ),
    "account_data": TableSchema(
        primary_keys=("user_id", "account_data_type"),
        columns=[
            "user_id",
            "account_data_type",
            "stream_id",
            "content",
            "instance_name",
        ],
    ),
    "room_account_data": TableSchema(
        primary_keys=("user_id", "room_id", "account_data_type"),
        columns=[
            "user_id",
            "room_id",
            "account_data_type",
            "stream_id",
            "content",
            "instance_name",
        ],
    ),
    "devices": TableSchema(
        primary_keys=("user_id", "device_id"),
        columns=[
            "user_id",
            "device_id",
            "access_token",
            "device_display_name",
            "ts",
            "last_seen",
            "ip",
            "user_agent",
        ],
    ),
    "user_filters": TableSchema(
        primary_keys=("user_id", "filter_id"),
        columns=["user_id", "filter_id", "filter_json"],
    ),
}


def get_rocksdb_key(table_name: str, primary_key_values: tuple[Any, ...]) -> bytes:
    """
    Generates a RocksDB key for a given table and primary key value(s).
    Format: <table>:<pk_val_1>:<pk_val_2>:...
    """
    pk_str = ":".join(str(val) for val in primary_key_values)
    return f"{table_name}:{pk_str}".encode("utf-8")


def parse_rocksdb_key(key: bytes) -> tuple[str, list[str]]:
    """
    Parses a RocksDB key back into its table name and primary key values.
    """
    decoded = key.decode("utf-8")
    parts = decoded.split(":")
    table_name = parts[0]
    primary_key_values = parts[1:]
    return table_name, primary_key_values
