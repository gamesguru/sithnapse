#!/usr/bin/env python3
# Migration 4db1a920f -> 11c6125cab: see the sibling dump script.
"""Write a de-salt dump into an mtxdb directory with the NEW build.

Run with the build being deployed installed, the service stopped, and a
snapshot of the mtxdb directory taken. Every record is written under the
unsalted keys, then read back and compared with the dump; any mismatch exits
non-zero.

Old salted records are left where they are: the new build cannot address them,
they only cost disk, and the snapshot is the rollback. Re-running is safe; each
write replaces the same record.

After this, rebuild CSR adjacency with `mtxdb_edges_rebuild.py`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Iterator

from _common import embedded_database
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


def apply_dump(engine: Any, path: Path, batch_size: int) -> dict[str, int]:
    """Write every record under the new keys. Returns per-type written counts."""
    written = dict.fromkeys(RECORD_TYPES, 0)

    for batch in batched(_rows(path, EVENT), batch_size):
        # The FFI row order is (room_id, event_id, record), whatever the .pyi
        # parameter names say; `embedded_event_json.py` builds it the same way.
        engine.event_json_put(
            [
                (_text(r, "room_id"), _text(r, "event_id"), unb64(r["value"], EVENT))
                for r in batch
            ]
        )
        written[EVENT] += len(batch)

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


def verify_dump(engine: Any, path: Path, batch_size: int) -> list[str]:
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
    # writer when the installed extension is the source build by mistake.
    engine.open_client_read_only(str(db_path))
    try:
        require_new_build(engine)
    finally:
        engine.close_client()

    print(f"Writing mtxdb at {db_path}", file=sys.stderr)
    engine.open_client(str(db_path))
    try:
        written = apply_dump(engine, args.input, args.batch_size)
        engine.sync()
        if written != counts:
            raise RuntimeError(f"wrote {written}, dump holds {counts}")
        bad = verify_dump(engine, args.input, args.batch_size)
    finally:
        engine.close_client()
    if bad:
        for line in bad[:20]:
            print("  mismatch: " + line, file=sys.stderr)
        raise RuntimeError(f"{len(bad)} records did not read back equal")
    print(f"restored and verified {written}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
