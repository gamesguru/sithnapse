#!/usr/bin/env python3
# Migration 4db1a920f -> 11c6125cab: the embedded store drops the tenant
# salt (`embedded_db.namespace`) from event records, flat-KV keys, HAMT root
# pointers/refcounts and the room index.
"""Dump every salted mtxdb record family to a namespace-free JSONL file.

Run this with PROD'S OWN BUILD (`4db1a920f`) installed: the old FFI takes a
`namespace` argument and the new build cannot read these records. The service
must be stopped and a snapshot of the mtxdb directory taken first. The script
only reads the store.

Families dumped (keyed by logical id, so the restore needs no old-key logic):
  - event records (`event_json_get`)
  - `event_to_state_group:` and `state_group_refcount:` flat-KV values
  - room index entries (`get_room_index`)
  - HAMT root records (`get_state_hamt_roots_for_room`)

Not dumped, because their physical ids never contained the namespace: HAMT
nodes (room prefix + structural hash), auth-chain closure collections. CSR edge
adjacency is rebuilt from the restored event bodies by `mtxdb_edges_rebuild.py`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from _common import embedded_database, sql_connection
from _desalt import (
    EVENT,
    EVENT_TO_STATE_GROUP,
    EXPORT_FORMAT,
    RECORD_TYPES,
    REFCOUNT,
    ROOM_INDEX,
    ROOT,
    b64,
    batched,
    old_event_to_state_group_key,
    old_state_group_refcount_key,
)


def require_old_build(engine: Any) -> None:
    """The old FFI takes a namespace first; the new one does not."""
    try:
        engine.event_json_get("probe", [])
    except TypeError as error:
        raise RuntimeError(
            "this is not prod's build: event_json_get does not take a namespace. "
            "Run the dump from an environment built from 4db1a920f."
        ) from error


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Synapse config of the service")
    parser.add_argument("--output", required=True, type=Path, help="Output JSONL path")
    parser.add_argument(
        "--namespace",
        help="Old embedded_db.namespace (default: read from the config, "
        "falling back to server_name)",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--force", action="store_true", help="Replace the output")
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Write the dump even if some SQL events or state groups have no "
        "record in the store. The restore then reproduces that gap.",
    )
    args = parser.parse_args()
    if args.output.exists() and not args.force:
        parser.error(f"{args.output} already exists; pass --force to replace it")

    from synapse.synapse_rust import mtxdb_engine

    # Typed Any: the stub describes the new FFI, this runs against the old one.
    engine: Any = mtxdb_engine

    _, db_path, namespace = embedded_database(args.config)
    namespace = args.namespace or namespace
    print(f"Reading mtxdb at {db_path}", file=sys.stderr)
    engine.open_client_read_only(str(db_path))
    # The probe must run after opening: on the correct build the call reaches
    # Rust and needs an initialized client; only an argument-count mismatch
    # should be interpreted as the wrong build.
    try:
        require_old_build(engine)
    except BaseException:
        engine.close_client()
        raise

    _, sql = sql_connection(args.config)
    cursor = sql.cursor()
    # In the embedded-exclusive configuration, SQL no longer has the state
    # group mapping. It still has the outlier bit, which distinguishes the
    # events that may legitimately have no mapping from ordinary events whose
    # mapping is missing/corrupt in mtxdb.
    cursor.execute("SELECT room_id, event_id, outlier FROM events")
    events = [
        (room_id, event_id, bool(outlier)) for room_id, event_id, outlier in cursor
    ]
    cursor.execute("SELECT id FROM state_groups")
    sql_state_groups = {int(row[0]) for row in cursor}
    sql.close()
    print(
        f"SQL: {len(events)} events, {len(sql_state_groups)} state groups",
        file=sys.stderr,
    )

    counts = dict.fromkeys(RECORD_TYPES, 0)
    problems: list[str] = []
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{args.output.name}.", dir=args.output.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out:

            def emit(row: dict[str, Any]) -> None:
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
                counts[row["type"]] += 1

            out.write(
                json.dumps(
                    {
                        "type": "header",
                        "format": EXPORT_FORMAT,
                        "source_path": str(db_path),
                        "sql_events": len(events),
                    }
                )
                + "\n"
            )

            # 1. Event records.
            room_of = {event_id: room_id for room_id, event_id, _ in events}
            outlier_by = {event_id: outlier for _, event_id, outlier in events}
            for batch in batched(room_of, args.batch_size):
                body_rows = engine.event_json_get(namespace, batch)
                seen: set[str] = set()
                for event_id, body in body_rows:
                    seen.add(event_id)
                    if body is None:
                        problems.append(f"{event_id}: no event record")
                        continue
                    emit(
                        {
                            "type": EVENT,
                            "event_id": event_id,
                            "room_id": room_of[event_id],
                            "value": b64(bytes(body)),
                        }
                    )
                problems.extend(
                    f"{event_id}: event lookup returned no row"
                    for event_id in batch
                    if event_id not in seen
                )

            # 2. event_to_state_group. The state groups it references join the
            # SQL list: an unreferenced group still has a root worth keeping.
            state_groups = set(sql_state_groups)
            referenced: set[int] = set()
            unmapped_outliers = 0
            for batch in batched(room_of, args.batch_size):
                keys = {old_event_to_state_group_key(namespace, e): e for e in batch}
                found = dict(engine.batch_get(list(keys)))
                for key, event_id in keys.items():
                    value = found.get(key)
                    if value is None:
                        if outlier_by[event_id]:
                            # Outliers may legitimately have no state group.
                            unmapped_outliers += 1
                        else:
                            problems.append(
                                f"{event_id}: non-outlier has no event_to_state_group record"
                            )
                        continue
                    value = bytes(value)
                    if len(value) != 8:
                        problems.append(f"{event_id}: bad event_to_state_group value")
                        continue
                    mapped = int.from_bytes(value, "big", signed=True)
                    state_groups.add(mapped)
                    referenced.add(mapped)
                    emit(
                        {
                            "type": EVENT_TO_STATE_GROUP,
                            "event_id": event_id,
                            "value": b64(value),
                        }
                    )

            # 3. Refcounts, room index and roots, per state group.
            ordered = sorted(state_groups)
            for batch in batched(ordered, args.batch_size):
                keys = {old_state_group_refcount_key(namespace, sg): sg for sg in batch}
                found = dict(engine.batch_get(list(keys)))
                for key, state_group in keys.items():
                    value = found.get(key)
                    if value is None:
                        # A group no dumped event maps to has no counter; absence
                        # reads as zero. Only a referenced group must have one.
                        if state_group in referenced:
                            problems.append(
                                f"state group {state_group}: no reference-count record"
                            )
                        continue
                    value = bytes(value)
                    if len(value) != 8:
                        problems.append(
                            f"state group {state_group}: bad reference-count value"
                        )
                        continue
                    emit(
                        {
                            "type": REFCOUNT,
                            "state_group": state_group,
                            "value": b64(value),
                        }
                    )
                prefixes = engine.get_room_index(namespace, batch)
                if len(prefixes) != len(batch):
                    problems.append(
                        f"room-index lookup returned {len(prefixes)} rows for "
                        f"{len(batch)} state groups"
                    )
                    continue
                by_prefix: dict[bytes, list[int]] = {}
                for state_group, prefix in zip(batch, prefixes):
                    if prefix is None:
                        problems.append(f"state group {state_group}: no room index")
                        continue
                    prefix = bytes(prefix)
                    emit(
                        {
                            "type": ROOM_INDEX,
                            "state_group": state_group,
                            "room_prefix": b64(prefix),
                        }
                    )
                    by_prefix.setdefault(prefix, []).append(state_group)
                for prefix, groups in by_prefix.items():
                    roots = engine.get_state_hamt_roots_for_room(
                        namespace, prefix, groups
                    )
                    if len(roots) != len(groups):
                        problems.append(
                            f"HAMT-root lookup returned {len(roots)} rows for "
                            f"{len(groups)} state groups"
                        )
                        continue
                    for state_group, root in zip(groups, roots):
                        if root is None:
                            problems.append(f"state group {state_group}: no root")
                            continue
                        emit(
                            {
                                "type": ROOT,
                                "state_group": state_group,
                                "room_prefix": b64(prefix),
                                "value": b64(bytes(root)),
                            }
                        )

            print(
                f"dumped {counts}; {unmapped_outliers} outliers without a state group; "
                f"{len(problems)} problems",
                file=sys.stderr,
            )
            for line in problems[:20]:
                print("  " + line, file=sys.stderr)
            if problems and not args.allow_missing:
                raise RuntimeError(
                    "coverage check failed: records are missing from the store. "
                    "Investigate, or re-run with --allow-missing to dump the gap."
                )
            out.write(json.dumps({"type": "end", "counts": counts}) + "\n")
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, args.output)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    engine.close_client()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
