#!/usr/bin/env python3
"""Rewrite a split-layout event_json dump into the current combined layout.

Run only after the dump is complete, with the service stopped and a backup of
the mtxdb directory. Re-running the full dump is safe: each write replaces the
same event record at the same identity.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3  # noqa: F401
import struct
import sys
from pathlib import Path
from typing import Iterator

from _common import embedded_database

EXPORT_FORMAT = "sithnapse-event-json-rewrite/v1"


def read_dump(path: Path) -> Iterator[dict[str, object]]:
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                raise RuntimeError(f"blank JSONL line at {line_number}")
            try:
                row = json.loads(line)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"invalid JSON on line {line_number}: {e}") from e
            if not isinstance(row, dict):
                raise RuntimeError(f"line {line_number} is not a JSON object")
            yield row


def validate_row(row: dict[str, object], line_number: int) -> None:
    for field in ("event_id", "room_id", "internal_metadata", "json"):
        if not isinstance(row.get(field), str) or not row[field]:
            raise RuntimeError(
                f"line {line_number}: {field} must be a non-empty string"
            )
    format_version = row.get("format_version")
    if format_version is not None and (
        isinstance(format_version, bool)
        or not isinstance(format_version, int)
        or not -(2**31) <= format_version < 2**31
    ):
        raise RuntimeError(f"line {line_number}: invalid format_version")
    internal_metadata_json = row["internal_metadata"]
    event_json = row["json"]
    assert isinstance(internal_metadata_json, str)
    assert isinstance(event_json, str)
    try:
        metadata = json.loads(internal_metadata_json)
        event = json.loads(event_json)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"line {line_number}: invalid embedded JSON: {e}") from e
    if not isinstance(metadata, dict):
        raise RuntimeError(f"line {line_number}: internal_metadata is not an object")
    if not isinstance(event, dict):
        raise RuntimeError(f"line {line_number}: event JSON is not an object")
    # No event_id/room_id check against the PDU: room v3+ events do not carry
    # event_id in their stored JSON (it is a hash), and v12 create events
    # carry no room_id. The ids come from SQL `events` in the dump.


def encode_record(row: dict[str, object]) -> bytes:
    metadata = str(row["internal_metadata"]).encode("utf-8")
    event_json = str(row["json"]).encode("utf-8")
    if len(metadata) > 0xFFFFFFFF:
        raise RuntimeError(f"metadata is too large for event {row['event_id']}")
    format_version = row["format_version"]
    if format_version is None:
        encoded_format_version = -1
    elif isinstance(format_version, bool) or not isinstance(format_version, int):
        raise RuntimeError(f"invalid format_version for event {row['event_id']}")
    else:
        encoded_format_version = format_version
    return (
        struct.pack(">iI", encoded_format_version, len(metadata))
        + metadata
        + event_json
    )


def validate_dump(path: Path, namespace: str) -> int:
    rows = read_dump(path)
    try:
        header = next(rows)
    except StopIteration as e:
        raise RuntimeError("dump is empty") from e
    if header.get("type") != "header" or header.get("format") != EXPORT_FORMAT:
        raise RuntimeError("unsupported event_json dump format")
    if header.get("namespace") != namespace:
        raise RuntimeError(
            "dump namespace "
            f"{header.get('namespace')!r} does not match config namespace {namespace!r}"
        )
    expected_count = header.get("record_count")
    if (
        isinstance(expected_count, bool)
        or not isinstance(expected_count, int)
        or expected_count < 0
    ):
        raise RuntimeError("dump header has an invalid record_count")

    seen = sqlite3.connect(":memory:")
    seen.execute("CREATE TABLE ids (event_id TEXT PRIMARY KEY)")
    count = 0
    try:
        for line_number, row in enumerate(rows, 2):
            validate_row(row, line_number)
            try:
                seen.execute("INSERT INTO ids VALUES (?)", (row["event_id"],))
            except sqlite3.IntegrityError as e:
                raise RuntimeError(f"duplicate event_id on line {line_number}") from e
            count += 1
    finally:
        seen.close()
    if count != expected_count:
        raise RuntimeError(
            f"dump is incomplete: header says {expected_count}, found {count}"
        )
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", required=True, help="Synapse config used by the service"
    )
    parser.add_argument(
        "--input", required=True, type=Path, help="Dump JSONL from dump_event_json.py"
    )
    parser.add_argument(
        "--backup-dir",
        required=True,
        type=Path,
        help="New directory where the stopped mtxdb database will be copied before rewriting",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Events per write batch (default: 500)",
    )
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")

    _, db_path, namespace = embedded_database(args.config)
    count = validate_dump(args.input, namespace)
    if not db_path.is_dir():
        raise RuntimeError(f"mtxdb directory does not exist: {db_path}")
    backup_path = args.backup_dir.absolute()
    resolved_db_path = db_path.resolve()
    resolved_backup_path = backup_path.resolve()
    if (
        resolved_backup_path == resolved_db_path
        or resolved_db_path in resolved_backup_path.parents
        or resolved_backup_path in resolved_db_path.parents
    ):
        raise RuntimeError("backup directory must be separate from the mtxdb directory")
    if backup_path.exists():
        raise RuntimeError(f"backup destination already exists: {backup_path}")
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    print(
        f"Validated {count} events for namespace {namespace!r}; writing to {db_path}",
        file=sys.stderr,
    )
    print(
        "Ensure the service is stopped and the mtxdb directory is backed up. "
        "The old build cannot read the rewritten records.",
        file=sys.stderr,
    )
    print(f"Copying mtxdb backup to {backup_path}", file=sys.stderr)
    shutil.copytree(db_path, backup_path, symlinks=True)

    # Import after validating the complete dump, so a malformed or mismatched
    # input cannot partially rewrite the database.
    from synapse.synapse_rust import mtxdb_engine

    mtxdb_engine.open_client(str(db_path))
    batch: list[tuple[str, str, bytes]] = []
    written = 0
    rows = read_dump(args.input)
    next(rows)  # Revalidated header; the first pass checked the full dump.
    for row in rows:
        batch.append(
            (
                str(row["room_id"]),
                str(row["event_id"]),
                encode_record(row),
            )
        )
        if len(batch) >= args.batch_size:
            mtxdb_engine.event_json_put(namespace, batch)
            written += len(batch)
            print(f"Rewrote {written}/{count} events", file=sys.stderr)
            batch.clear()
    if batch:
        mtxdb_engine.event_json_put(namespace, batch)
        written += len(batch)
    if written != count:
        raise RuntimeError(
            f"restore coverage failed: wrote {written}, expected {count}"
        )

    mtxdb_engine.sync_event_dag()
    print(f"Rewrote and synced {written} event records in {db_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
