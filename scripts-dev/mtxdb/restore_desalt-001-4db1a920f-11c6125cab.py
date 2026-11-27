#!/usr/bin/env python3
# Migration 4db1a920f -> 11c6125cab: see the sibling dump script.
"""Write a de-salt dump into an mtxdb directory with the NEW build.

Event records are written with their bodies first and then status-bearing
EventDag locators. The dump predates the locator status header, so outlier and
rejected come from SQL (`events`, `rejections`) and soft-failed from each
record's own `internal_metadata`. Every locator is read back and compared with
SQL after writing.

Run with the build being deployed installed, the service stopped, and a
snapshot of the mtxdb directory taken. Every record is written under the
unsalted keys, then read back and compared with the dump; any mismatch exits
non-zero.

The old room-index shards are renamed to `room_index.pre-desalt` first (the new
build cannot read their record layout). Old salted records are otherwise left where they are: the new build cannot address them,
they only cost disk, and the snapshot is the rollback. Re-running is safe; each
write replaces the same record.

After this, rebuild CSR adjacency with `mtxdb_edges_rebuild.py`.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Iterator

from _common import (
    embedded_database,
    mark_event_status_locators_complete,
    soft_failed_from_record,
    sql_connection,
    sql_event_statuses,
    write_event_batch,
)
from _desalt import (
    EVENT,
    EVENT_TO_STATE_GROUP,
    RECORD_TYPES,
    REFCOUNT,
    ROOM_INDEX,
    ROOT,
    batched,
    count_records,
    new_event_to_state_group_key,
    new_state_group_refcount_key,
    read_dump,
    unb64,
)


def retire_old_room_index(db_path: Path) -> bool:
    """Move prod's old-format `room_index/` aside; return whether it moved.

    Prod's shards hold 36-byte records that carry a namespace digest. The new
    build's layout probe rejects sufficiently long old shards; a short shard
    can be misparsed. An in-place rollback would also misparse new records. A
    rename deletes nothing. If `room_index.pre-desalt` already exists, an
    earlier run retired the old files and the current `room_index/` is
    new-format: leave it alone so a re-run stays safe.
    """
    index = db_path / "room_index"
    aside = db_path / "room_index.pre-desalt"
    if aside.exists() or not index.exists():
        return False
    index.rename(aside)
    _fsync_directory(db_path)
    return True


def _fsync_directory(path: Path) -> None:
    """Persist directory-entry changes made by the migration."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def require_new_build(engine: Any) -> None:
    """The new FFI has no namespace argument."""
    try:
        engine.event_json_get([])
    except TypeError as error:
        raise RuntimeError(
            "this is the old build: event_json_get still takes a namespace. "
            "Run the restore from an environment built from the target commit."
        ) from error


def _rows(path: Path, record_type: str) -> Iterator[dict[str, Any]]:
    for row in read_dump(path):
        if row.get("type") == record_type:
            yield row


def _int(row: dict[str, Any], field: str) -> int:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"{row.get('type')}: {field} must be an integer")
    return value


def _text(row: dict[str, Any], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"{row.get('type')}: {field} must be a non-empty string")
    return value


def _counter(value: bytes, where: str) -> bytes:
    if len(value) != 8:
        raise RuntimeError(f"{where}: counter/mapping value must be 8 bytes")
    return value


def apply_dump(
    engine: Any, sql: Any, sql_name: str, path: Path, batch_size: int
) -> dict[str, int]:
    """Write every record under the new keys. Returns per-type written counts."""
    written = dict.fromkeys(RECORD_TYPES, 0)

    for batch in batched(_rows(path, EVENT), batch_size):
        written[EVENT] += write_event_batch(
            engine,
            sql,
            sql_name,
            [
                (_text(r, "room_id"), _text(r, "event_id"), unb64(r["value"], EVENT))
                for r in batch
            ],
        )

    for batch in batched(_rows(path, EVENT_TO_STATE_GROUP), batch_size):
        engine.batch_put(
            [
                (
                    new_event_to_state_group_key(_text(r, "event_id")),
                    _counter(
                        unb64(r["value"], EVENT_TO_STATE_GROUP), EVENT_TO_STATE_GROUP
                    ),
                )
                for r in batch
            ]
        )
        written[EVENT_TO_STATE_GROUP] += len(batch)

    for batch in batched(_rows(path, REFCOUNT), batch_size):
        engine.batch_put(
            [
                (
                    new_state_group_refcount_key(_int(r, "state_group")),
                    _counter(unb64(r["value"], REFCOUNT), REFCOUNT),
                )
                for r in batch
            ]
        )
        written[REFCOUNT] += len(batch)

    # The room index goes in before the roots, as the live writer does.
    for batch in batched(_rows(path, ROOM_INDEX), batch_size):
        engine.put_room_index(
            [
                (_int(r, "state_group"), unb64(r["room_prefix"], ROOM_INDEX))
                for r in batch
            ]
        )
        written[ROOM_INDEX] += len(batch)

    for batch in batched(_rows(path, ROOT), batch_size):
        by_prefix: dict[bytes, list[tuple[int, bytes]]] = {}
        for r in batch:
            prefix = unb64(r["room_prefix"], ROOT)
            by_prefix.setdefault(prefix, []).append(
                (_int(r, "state_group"), unb64(r["value"], ROOT))
            )
        for prefix, roots in by_prefix.items():
            engine.put_state_hamt_roots(prefix, roots)
        written[ROOT] += len(batch)

    return written


def verify_dump(
    engine: Any, sql: Any, sql_name: str, path: Path, batch_size: int
) -> list[str]:
    """Read every record back through the new build; return the mismatches."""
    bad: list[str] = []

    for batch in batched(_rows(path, EVENT), batch_size):
        expected = {_text(r, "event_id"): unb64(r["value"], EVENT) for r in batch}
        found_bodies = engine.event_json_get(list(expected))
        if len(found_bodies) != len(expected):
            bad.append(
                f"event lookup returned {len(found_bodies)} rows for {len(expected)} events"
            )
        for event_id, body in found_bodies:
            if body is None or bytes(body) != expected[event_id]:
                bad.append(f"event {event_id}")

        statuses = sql_event_statuses(sql, sql_name, expected)
        found_statuses = dict(engine.event_status_get(list(expected)))
        for event_id, record in expected.items():
            outlier, rejected = statuses[event_id]
            wanted = (outlier, rejected, soft_failed_from_record(record))
            got = found_statuses.get(event_id)
            if got is None or tuple(got) != wanted:
                bad.append(f"status {event_id}: wrote {wanted}, read {got}")

    for batch in batched(_rows(path, EVENT_TO_STATE_GROUP), batch_size):
        expected_kv = {
            new_event_to_state_group_key(_text(r, "event_id")): unb64(
                r["value"], EVENT_TO_STATE_GROUP
            )
            for r in batch
        }
        found = dict(engine.batch_get(list(expected_kv)))
        for key, value in expected_kv.items():
            if found.get(key) is None or bytes(found[key]) != value:
                bad.append(f"event_to_state_group {key!r}")

    for batch in batched(_rows(path, REFCOUNT), batch_size):
        expected_kv = {
            new_state_group_refcount_key(_int(r, "state_group")): unb64(
                r["value"], REFCOUNT
            )
            for r in batch
        }
        found = dict(engine.batch_get(list(expected_kv)))
        for key, value in expected_kv.items():
            if found.get(key) is None or bytes(found[key]) != value:
                bad.append(f"refcount {key!r}")

    for batch in batched(_rows(path, ROOM_INDEX), batch_size):
        groups = [_int(r, "state_group") for r in batch]
        found_prefixes = engine.get_room_index(groups)
        if len(found_prefixes) != len(batch):
            bad.append(
                f"room-index lookup returned {len(found_prefixes)} rows for {len(batch)} state groups"
            )
        for row, got in zip(batch, found_prefixes):
            if got is None or bytes(got) != unb64(row["room_prefix"], ROOM_INDEX):
                bad.append(f"room_index {row['state_group']}")

    for batch in batched(_rows(path, ROOT), batch_size):
        by_prefix: dict[bytes, list[dict[str, Any]]] = {}
        for r in batch:
            by_prefix.setdefault(unb64(r["room_prefix"], ROOT), []).append(r)
        for prefix, rows in by_prefix.items():
            groups = [_int(r, "state_group") for r in rows]
            found_roots = engine.get_state_hamt_roots_for_room(prefix, groups)
            if len(found_roots) != len(rows):
                bad.append(
                    f"HAMT-root lookup returned {len(found_roots)} rows for {len(rows)} state groups"
                )
            for row, got in zip(rows, found_roots):
                if got is None or bytes(got) != unb64(row["value"], ROOT):
                    bad.append(f"root {row['state_group']}")
    return bad


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Synapse config of the service")
    parser.add_argument("--input", required=True, type=Path, help="Dump JSONL path")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the dump's framing and counts; open and write nothing",
    )
    args = parser.parse_args()

    counts = count_records(read_dump(args.input))
    print(f"dump is well formed: {counts}", file=sys.stderr)
    if args.dry_run:
        return 0

    from synapse.synapse_rust import mtxdb_engine as engine

    _, db_path, _ = embedded_database(args.config)
    # Probe using a read-only handle first. This avoids opening the store as a
    # writer when the installed extension is the source build by mistake, and
    # it runs before the old index is renamed so a wrong-build invocation has
    # no side effects at all.
    engine.open_client_read_only(str(db_path))
    try:
        require_new_build(engine)
    finally:
        engine.close_client()

    if retire_old_room_index(db_path):
        print("Moved room_index/ to room_index.pre-desalt", file=sys.stderr)

    sql_name, sql = sql_connection(args.config, read_only=False)
    print(f"Writing mtxdb at {db_path}", file=sys.stderr)
    engine.open_client(str(db_path))
    try:
        written = apply_dump(engine, sql, sql_name, args.input, args.batch_size)
        engine.sync()
        # room_index is an append-only side file outside mtxdb's WAL. Its
        # sync() fsyncs shard contents; persist the directory entries too.
        room_index_dir = db_path / "room_index"
        if room_index_dir.exists():
            _fsync_directory(room_index_dir)
        _fsync_directory(db_path)
        if written != counts:
            raise RuntimeError(f"wrote {written}, dump holds {counts}")
        bad = verify_dump(engine, sql, sql_name, args.input, args.batch_size)
        if bad:
            for line in bad[:20]:
                print("  mismatch: " + line, file=sys.stderr)
            raise RuntimeError(f"{len(bad)} records did not read back equal")
        # Only now is every body and status header verified. A failed or
        # partial run must leave the marker incomplete so startup still
        # refuses and the restore can be repeated.
        mark_event_status_locators_complete(sql, sql_name)
    finally:
        sql.close()
        engine.close_client()
    print(f"restored and verified {written}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
