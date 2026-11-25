"""Shared helpers for the de-salt migration pair
(`dump_desalt-001-4db1a920f-11c6125cab.py` / `restore_desalt-001-4db1a920f-11c6125cab.py`).

The dump runs under prod's own build (`4db1a920f`, whose FFI still takes
`namespace`); the restore runs under the build being deployed. They exchange a
JSONL file keyed by *logical* ids, so neither script needs the other build's
key derivation. The old-key functions here are pure Python so they can be
tested without either build.
"""

from __future__ import annotations

import base64
import hashlib
import json
import struct
from pathlib import Path
from typing import Any, Iterable, Iterator, TypeVar

EXPORT_FORMAT = "sithnapse-desalt-dump/v1"

# Record types, in the order the dump writes them.
EVENT = "event"
EVENT_TO_STATE_GROUP = "event_to_state_group"
REFCOUNT = "state_group_refcount"
ROOM_INDEX = "room_index"
ROOT = "root"
RECORD_TYPES = (EVENT, EVENT_TO_STATE_GROUP, REFCOUNT, ROOM_INDEX, ROOT)

T = TypeVar("T")


def namespace_hash(namespace: str) -> bytes:
    """16-byte digest of the old namespace (prod's `embedded_common.namespace_hash`)."""
    return hashlib.sha256(namespace.encode("utf-8")).digest()[:16]


def old_event_to_state_group_key(namespace: str, event_id: str) -> bytes:
    return (
        b"event_to_state_group:"
        + namespace_hash(namespace).hex().encode("ascii")
        + b":"
        + event_id.encode("utf-8")
    )


def old_state_group_refcount_key(namespace: str, state_group: int) -> bytes:
    return (
        b"state_group_refcount:"
        + namespace_hash(namespace).hex().encode("ascii")
        + b":"
        + struct.pack(">q", state_group)
    )


def new_event_to_state_group_key(event_id: str) -> bytes:
    return b"event_to_state_group:" + event_id.encode("utf-8")


def new_state_group_refcount_key(state_group: int) -> bytes:
    return b"state_group_refcount:" + struct.pack(">q", state_group)


def b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def unb64(value: object, where: str) -> bytes:
    if not isinstance(value, str):
        raise RuntimeError(f"{where}: expected a base64 string")
    try:
        return base64.b64decode(value, validate=True)
    except ValueError as e:
        raise RuntimeError(f"{where}: invalid base64: {e}") from e


def batched(items: Iterable[T], size: int) -> Iterator[list[T]]:
    batch: list[T] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def read_dump(path: Path) -> Iterator[dict[str, Any]]:
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


def count_records(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Validate the header/footer framing and return per-type counts.

    Fails on a missing header, a missing footer (a truncated dump), an unknown
    record type, or a footer whose counts disagree with the records read.
    """
    iterator = iter(rows)
    header = next(iterator, None)
    if not header or header.get("type") != "header":
        raise RuntimeError("dump has no header record")
    if header.get("format") != EXPORT_FORMAT:
        raise RuntimeError(f"unsupported dump format {header.get('format')!r}")
    counts = dict.fromkeys(RECORD_TYPES, 0)
    footer: dict[str, Any] | None = None
    for row in iterator:
        if footer is not None:
            raise RuntimeError("records found after the footer")
        record_type = row.get("type")
        if record_type == "end":
            footer = row
        elif record_type in counts:
            counts[record_type] += 1
        else:
            raise RuntimeError(f"unknown record type {record_type!r}")
    if footer is None:
        raise RuntimeError("dump has no footer: it is truncated")
    if footer.get("counts") != counts:
        raise RuntimeError(
            f"footer counts {footer.get('counts')} disagree with records {counts}"
        )
    return counts
