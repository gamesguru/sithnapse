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

"""Per-event status flags in the EventDag event locator.

The EventDag locator is the authoritative room routing record and also carries
outlier, rejected and soft-failed bits. State-pool locators remain room-only.
"""

import time
from typing import TYPE_CHECKING, Any, NamedTuple

from synapse.storage.databases.main.embedded_common import (
    ffi_timing,
    mirror_timing,
    retry_on_contention,
)

if TYPE_CHECKING:
    from synapse.server import HomeServer
    from synapse.synapse_rust.mtxdb_engine import MtxdbTransaction


_BACKFILL_LOCK = "X"


class EventStatusBackfillIncompleteError(Exception):
    """Startup found stored events without status-bearing EventDag locators."""


class EventStatus(NamedTuple):
    outlier: bool
    rejected: bool
    soft_failed: bool


def get_event_statuses(event_ids: list[str]) -> dict[str, EventStatus]:
    """Return v1 status headers for IDs; absent locators are omitted."""
    if not event_ids:
        return {}
    from synapse.synapse_rust.mtxdb_engine import event_status_get

    with mirror_timing("event_status_get"):
        _et = time.monotonic()
        found = retry_on_contention(
            lambda: event_status_get(event_ids), label="event_status"
        )
        ffi_timing("ffi_event_status_get", time.monotonic() - _et)
    return {
        event_id: EventStatus(*value) for event_id, value in found if value is not None
    }


def patch_event_statuses(
    event_ids: list[str],
    *,
    outlier: bool | None = None,
    rejected: bool | None = None,
    soft_failed: bool | None = None,
    transaction: "MtxdbTransaction | None" = None,
) -> None:
    """Set selected status bits, preserving the other locator fields/bits.

    An event whose locator is already gone has no status to carry and is
    skipped, so a purge or sticky update racing a delete does not fail.
    """
    mask = 0
    values = 0
    for bit, value in ((1, outlier), (2, rejected), (4, soft_failed)):
        if value is not None:
            mask |= bit
            if value:
                values |= bit
    if not mask or not event_ids:
        return
    rows = [(event_id, mask, values) for event_id in event_ids]
    with mirror_timing("event_status_patch"):
        _et = time.monotonic()
        if transaction is not None:
            transaction.event_status_patch(rows)
        else:
            from synapse.synapse_rust.mtxdb_engine import event_status_patch

            retry_on_contention(lambda: event_status_patch(rows), label="event_status")
        ffi_timing("ffi_event_status_patch", time.monotonic() - _et)


def _read_backfill_state(txn: Any) -> tuple[bool, str]:
    txn.execute(
        "SELECT completed, last_event_id FROM event_status_locator_backfill"
        " WHERE lock = ?",
        (_BACKFILL_LOCK,),
    )
    row = txn.fetchone()
    if row is None:
        return False, ""
    return bool(row[0]), str(row[1])


def _write_backfill_state(txn: Any, last_event_id: str, completed: bool) -> None:
    txn.execute(
        "UPDATE event_status_locator_backfill"
        " SET completed = ?, last_event_id = ? WHERE lock = ?",
        (1 if completed else 0, last_event_id, _BACKFILL_LOCK),
    )
    if txn.rowcount == 0:
        txn.execute(
            "INSERT INTO event_status_locator_backfill"
            " (lock, completed, last_event_id) VALUES (?, ?, ?)",
            (_BACKFILL_LOCK, 1 if completed else 0, last_event_id),
        )


async def check_backfill(hs: "HomeServer") -> None:
    """Refuse startup when stored events lack a completed status migration.

    A database with no events adopts the marker itself: a fresh install never
    needs the offline migration. An upgraded database must run
    `synapse_event_status_migrate` first, because persist reads treat a
    missing locator as "event not stored".
    """
    if not hs.config.database.embedded_db_engine:
        return

    store = hs.get_datastores().main

    def _txn(txn: Any) -> bool:
        completed, _ = _read_backfill_state(txn)
        if completed:
            return True
        txn.execute("SELECT 1 FROM events LIMIT 1")
        if txn.fetchone() is None:
            _write_backfill_state(txn, "", True)
            return True
        return False

    if not await store.db_pool.runInteraction("check_event_status_backfill", _txn):
        raise EventStatusBackfillIncompleteError(
            "Event status is not recorded in the mtxdb EventDag locators for "
            "this database. Stop Synapse and run `synapse_event_status_migrate "
            "--config <config>` before starting this build."
        )


async def migrate_event_statuses(
    hs: "HomeServer",
    *,
    batch_size: int = 1000,
    dry_run: bool = False,
) -> dict[str, int]:
    """Rewrite every event's EventDag locator with its status flags.

    Stop every Synapse process first. Status comes from SQL (`events.outlier`,
    `rejections`, `events.rejection_reason`) and the body's `internal_metadata`
    (`soft_failed`); the body is read directly by `(room_id, event_id)` because
    a pre-status locator is not readable through the normal path. The scan is
    ordered by event id, resumes from the marker's `last_event_id`, and verifies
    each batch by reading it back. A dry run reports mismatches without writing
    or advancing the marker, including after a completed migration. There is no
    way to start past the marker: doing so could mark the store complete while
    earlier events were never rewritten.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if not hs.config.database.embedded_db_engine:
        raise ValueError("configure embedded_db.engine for the mtxdb locator store")

    from synapse.storage._base import db_to_json
    from synapse.storage.databases.main.embedded_event_json import (
        get_event_json_body_batch,
    )

    store = hs.get_datastores().main

    completed, last_event_id = await store.db_pool.runInteraction(
        "event_status_migrate_read_marker", _read_backfill_state
    )
    if completed and not dry_run:
        return {
            "already_migrated": 1,
            "events": 0,
            "written": 0,
            "unchanged": 0,
            "mismatched": 0,
        }

    stats = {"events": 0, "written": 0, "unchanged": 0, "mismatched": 0}

    def _read_batch(
        txn: Any, after: str, limit: int
    ) -> list[tuple[str, str, bool, str | None, str | None]]:
        txn.execute(
            """
            SELECT e.event_id, e.room_id, e.outlier,
                   COALESCE(r.reason, e.rejection_reason),
                   ej.internal_metadata
            FROM events AS e
                LEFT JOIN rejections AS r USING (event_id)
                LEFT JOIN event_json AS ej USING (event_id)
            WHERE e.event_id > ?
            ORDER BY e.event_id
            LIMIT ?
            """,
            (after, limit),
        )
        return txn.fetchall()

    while True:
        rows = await store.db_pool.runInteraction(
            "event_status_migrate_batch", _read_batch, last_event_id, batch_size
        )
        if not rows:
            break

        missing_meta = [
            (room_id, event_id)
            for event_id, room_id, _, _, metadata in rows
            if metadata is None
        ]
        embedded_meta = get_event_json_body_batch(missing_meta)

        try:
            existing = get_event_statuses([event_id for event_id, *_ in rows])
        except RuntimeError as error:
            # A pre-status locator is unreadable through the normal path; the
            # whole batch needs a rewrite and its soft-failed bit must come
            # from the body, which was the authoritative store before status
            # moved into the locator. Any other read failure must abort the
            # migration instead of being mistaken for an old-format batch.
            if "invalid or legacy EventDag event locator" not in str(error):
                raise
            existing = None

        expected: dict[str, tuple[str, EventStatus]] = {}
        for event_id, room_id, outlier, reason, metadata in rows:
            if existing is not None and event_id in existing:
                soft_failed = existing[event_id].soft_failed
            else:
                if metadata is None:
                    found = embedded_meta.get(event_id)
                    if found is None:
                        raise RuntimeError(
                            f"event {event_id} has no event JSON in mtxdb or SQL; "
                            "refusing to leave it without a status header"
                        )
                    metadata = found[0]
                soft_failed = bool(db_to_json(metadata).get("soft_failed"))
            expected[event_id] = (
                room_id,
                EventStatus(
                    outlier=bool(outlier),
                    rejected=reason is not None,
                    soft_failed=soft_failed,
                ),
            )

        existing = existing or {}

        if dry_run:
            stats["mismatched"] += sum(
                existing.get(event_id) != status
                for event_id, (_, status) in expected.items()
            )
        else:
            from synapse.synapse_rust.mtxdb_engine import event_locator_put

            event_locator_put(
                [
                    (
                        room_id,
                        event_id,
                        status.outlier,
                        status.rejected,
                        status.soft_failed,
                    )
                    for event_id, (room_id, status) in expected.items()
                ]
            )
            readback = get_event_statuses(list(expected))
            for event_id, (_, status) in expected.items():
                if readback.get(event_id) != status:
                    raise RuntimeError(
                        f"status verification failed for {event_id}: wrote "
                        f"{status}, read {readback.get(event_id)}"
                    )
            stats["written"] += len(expected)
            stats["unchanged"] += sum(
                existing.get(event_id) == status
                for event_id, (_, status) in expected.items()
            )

        stats["events"] += len(rows)
        last_event_id = rows[-1][0]
        if not dry_run:
            await store.db_pool.runInteraction(
                "event_status_migrate_marker",
                _write_backfill_state,
                last_event_id,
                False,
            )

    if not dry_run:
        await store.db_pool.runInteraction(
            "event_status_migrate_complete", _write_backfill_state, "", True
        )
    return stats
