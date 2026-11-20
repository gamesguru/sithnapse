#!/usr/bin/env python3
"""Dump the old split event_json mirror into a rewrite JSONL file.

Run with the old Synapse package and a stopped server. This uses the generic
mtxdb CLI's inspection envelope to enumerate live room-collection records;
event ids and room ids come from SQL `events` (read-only, via the same
config), because node ids are hashes and room v3+ PDUs carry no event_id.
It does not need the old Python event_json API.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import struct
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterator

from _common import (
    embedded_database,
    event_meta_node_id,
    event_node_id,
    sql_connection,
)

EXPORT_FORMAT = "sithnapse-event-json-rewrite/v1"


def cli_command(cli: str, db_path: Path, *args: str) -> list[str]:
    return [
        cli,
        "--dir",
        str(db_path),
        "--shard-type",
        "events",
        *args,
    ]


def envelope_records(
    cli: str, db_path: Path, collection_id: str
) -> Iterator[tuple[bytes, bytes]]:
    command = cli_command(
        cli,
        db_path,
        "export",
        "--format",
        "envelope",
        collection_id,
    )
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    assert process.stdout is not None
    try:
        for line_number, line in enumerate(process.stdout, 1):
            try:
                envelope = json.loads(line)
                if envelope.get("schema") != "mtxdb.export/v1":
                    raise ValueError("unexpected envelope schema")
                node_text = envelope["node_id"]
                if not isinstance(node_text, str) or not node_text.startswith("0x"):
                    raise ValueError("missing node_id")
                node_id = bytes.fromhex(node_text[2:])
                payload = base64.b64decode(envelope["payload_base64"], validate=True)
                if len(node_id) != 16:
                    raise ValueError("node_id is not 16 bytes")
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
                raise RuntimeError(
                    f"invalid export envelope for {collection_id}, line {line_number}: {e}"
                ) from e
            yield node_id, payload
    finally:
        process.stdout.close()
    return_code = process.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, command)


def old_body_json(payload: bytes) -> tuple[int | None, str] | None:
    """Decode an old body record: `format_version(i32 BE) + json`."""
    if len(payload) < 4:
        return None
    (raw,) = struct.unpack(">i", payload[:4])
    if raw < -1:
        return None
    try:
        text = payload[4:].decode("utf-8")
        if not isinstance(json.loads(text), dict):
            return None
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return (None if raw == -1 else raw), text


def old_metadata(payload: bytes) -> str | None:
    try:
        text = payload.decode("utf-8")
        return text if isinstance(json.loads(text), dict) else None
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", required=True, help="Synapse config used by the service"
    )
    parser.add_argument("--output", required=True, type=Path, help="Output JSONL path")
    parser.add_argument(
        "--mtxdb-cli",
        default=os.environ.get("MTXDB_CLI", "mtxdb"),
        help="mtxdb CLI executable (default: mtxdb or MTXDB_CLI)",
    )
    parser.add_argument(
        "--force", action="store_true", help="Replace an existing output file"
    )
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="Write the dump even if some SQL events have no old-format mirror record",
    )
    args = parser.parse_args()

    if args.output.exists() and not args.force:
        parser.error(f"{args.output} already exists; pass --force to replace it")

    _, db_path, namespace = embedded_database(args.config)
    print(f"Reading mtxdb at {db_path} (namespace {namespace!r})", file=sys.stderr)

    # Authoritative (event_id, room_id) list.
    _, sql = sql_connection(args.config)
    cursor = sql.cursor()
    cursor.execute("SELECT room_id, event_id FROM events")
    events_by_room: dict[str, list[str]] = {}
    for room_id, event_id in cursor:
        events_by_room.setdefault(room_id, []).append(event_id)
    sql.close()
    sql_total = sum(len(ids) for ids in events_by_room.values())
    print(f"SQL events: {sql_total} in {len(events_by_room)} rooms", file=sys.stderr)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_output = tempfile.mkstemp(
        prefix=f".{args.output.name}.", dir=args.output.parent
    )
    written = 0
    problems: list[str] = []
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write("\n")  # header placeholder, rewritten below
            for room_id, event_ids in events_by_room.items():
                wanted: dict[bytes, tuple[str, str]] = {}
                for event_id in event_ids:
                    wanted[event_node_id(namespace, event_id)] = ("body", event_id)
                    wanted[event_meta_node_id(namespace, event_id)] = ("meta", event_id)
                bodies: dict[str, tuple[int | None, str]] = {}
                metas: dict[str, str] = {}
                # The CLI resolves a `!room:server` selector to the room's
                # collection itself; the canonical-id listing cannot be used
                # because Sithnapse's collections show as "[unregistered]".
                try:
                    records = list(envelope_records(args.mtxdb_cli, db_path, room_id))
                except subprocess.CalledProcessError:
                    problems.extend(
                        f"{e}: mtxdb export failed for {room_id}" for e in event_ids
                    )
                    continue
                for node_id, payload in records:
                    hit = wanted.get(node_id)
                    if hit is None:
                        continue
                    kind, event_id = hit
                    if kind == "body":
                        decoded = old_body_json(payload)
                        if decoded is not None:
                            bodies[event_id] = decoded
                    else:
                        metadata = old_metadata(payload)
                        if metadata is not None:
                            metas[event_id] = metadata
                for event_id in event_ids:
                    if event_id not in bodies or event_id not in metas:
                        which = "body" if event_id not in bodies else "metadata"
                        problems.append(f"{event_id}: no old-format {which} record")
                        continue
                    format_version, json_text = bodies[event_id]
                    output.write(
                        json.dumps(
                            {
                                "event_id": event_id,
                                "room_id": room_id,
                                "internal_metadata": metas[event_id],
                                "json": json_text,
                                "format_version": format_version,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    written += 1
                print(f"{room_id}: {len(event_ids)} events", file=sys.stderr)
            output.flush()
            os.fsync(output.fileno())

        print(
            f"dumped {written} of {sql_total} SQL events; {len(problems)} problems",
            file=sys.stderr,
        )
        for line in problems[:20]:
            print("  " + line, file=sys.stderr)
        if problems and not args.allow_missing:
            raise RuntimeError(
                "coverage check failed: events without an old-format record would "
                "raise under the new reader. Re-run with --allow-missing only if intended."
            )
        # Prepend the header now that the count is known.
        header = {
            "type": "header",
            "format": EXPORT_FORMAT,
            "namespace": namespace,
            "source_path": str(db_path),
            "record_count": written,
        }
        with open(temporary_output, encoding="utf-8") as body:
            body.readline()
            rest = body.read()
        with open(temporary_output, "w", encoding="utf-8") as final:
            final.write(json.dumps(header, ensure_ascii=False) + "\n" + rest)
            final.flush()
            os.fsync(final.fileno())
        os.replace(temporary_output, args.output)
    except BaseException:
        try:
            os.unlink(temporary_output)
        except FileNotFoundError:
            pass
        raise
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
