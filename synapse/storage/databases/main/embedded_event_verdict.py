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

"""Per-event verdicts (outlier, rejected, soft-failed) in mtxdb.

Persistence writes one record for every event it stores, so an event with no
record is not stored. Every later change to one of the three facts has to be
written here too: de-outliering, `mark_event_rejected_txn`, un-soft-failing
sticky events, and purges (which delete records, or turn retained events back
into outliers).

A verdict stores a numeric rejection code, not the reason string.
"""

import time
from typing import TYPE_CHECKING, Any, Callable, NamedTuple

from synapse.storage.databases.main.embedded_common import (
    ffi_timing,
    mirror_timing,
    retry_on_contention,
)

if TYPE_CHECKING:
    from synapse.events import EventBase
    from synapse.server import HomeServer
    from synapse.synapse_rust.mtxdb_engine import MtxdbTransaction


_BACKFILL_LOCK = "X"


class EventVerdictBackfillIncompleteError(Exception):
    """The event verdicts have not been backfilled from SQL."""


class EventVerdict(NamedTuple):
    outlier: bool
    # `None` for an accepted event.
    rejection_code: int | None
    soft_failed: bool

    @property
    def rejected(self) -> bool:
        return self.rejection_code is not None


def rejection_code(reason: str | None) -> int | None:
    """The code to store for a rejection reason, or `None` for no reason.

    Any falsy reason means accepted, matching persistence's test for whether to
    write a `rejections` row.
    """
    if not reason:
        return None
    from synapse.synapse_rust.mtxdb_engine import event_rejection_code

    return event_rejection_code(reason)


def verdict_for_event(event: "EventBase", rejected_reason: str | None) -> EventVerdict:
    return EventVerdict(
        outlier=event.internal_metadata.is_outlier(),
        rejection_code=rejection_code(rejected_reason),
        soft_failed=event.internal_metadata.is_soft_failed(),
    )


def put_event_verdicts(
    verdicts: dict[str, EventVerdict],
    *,
    transaction: "MtxdbTransaction | None" = None,
) -> None:
    """Write whole verdicts, staged in `transaction` if one is given."""
    if not verdicts:
        return
    rows = [(event_id, *verdict) for event_id, verdict in verdicts.items()]
    with mirror_timing("event_verdict_put"):
        if transaction is not None:
            transaction.event_verdict_put(rows)
            return
        from synapse.synapse_rust.mtxdb_engine import event_verdict_put

        retry_on_contention(lambda: event_verdict_put(rows), label="event_verdict")


def get_event_verdicts(event_ids: list[str]) -> dict[str, EventVerdict]:
    """Verdicts for `event_ids`; an event with no record is absent."""
    if not event_ids:
        return {}
    from synapse.synapse_rust.mtxdb_engine import event_verdict_get

    with mirror_timing("event_verdict_get"):
        _et = time.monotonic()
        found = retry_on_contention(
            lambda: event_verdict_get(event_ids), label="event_verdict"
        )
        ffi_timing("ffi_event_verdict_get", time.monotonic() - _et)
    return {
        event_id: EventVerdict(*value) for event_id, value in found if value is not None
    }


def update_event_verdicts(
    event_ids: list[str],
    change: Callable[[EventVerdict], EventVerdict],
) -> None:
    """Apply `change` to existing verdicts; events with no record are skipped.

    Writes directly, so call it only for changes that are safe to repeat when
    the surrounding SQL transaction is retried.
    """
    put_event_verdicts(
        {
            event_id: change(verdict)
            for event_id, verdict in get_event_verdicts(event_ids).items()
        }
    )


def delete_event_verdicts(event_ids: list[str]) -> None:
    """Delete the verdicts of purged events. Not synced here."""
    if not event_ids:
        return
    from synapse.synapse_rust.mtxdb_engine import event_verdict_delete

    retry_on_contention(lambda: event_verdict_delete(event_ids), label="event_verdict")


def _read_backfill_state(txn: Any) -> tuple[bool, str]:
    """Read `(completed, last_event_id)` from the migration marker."""
    txn.execute(
        "SELECT completed, last_event_id FROM event_verdict_backfill WHERE lock = ?",
        (_BACKFILL_LOCK,),
    )
    row = txn.fetchone()
    if row is None:
        return False, ""
    return bool(row[0]), str(row[1])


def _write_backfill_state(txn: Any, last_event_id: str, completed: bool) -> None:
    """Advance or complete the migration marker."""
    txn.execute(
        "UPDATE event_verdict_backfill"
        " SET completed = ?, last_event_id = ? WHERE lock = ?",
        (1 if completed else 0, last_event_id, _BACKFILL_LOCK),
    )
    if txn.rowcount == 0:
        txn.execute(
            "INSERT INTO event_verdict_backfill (lock, completed, last_event_id)"
            " VALUES (?, ?, ?)",
            (_BACKFILL_LOCK, 1 if completed else 0, last_event_id),
        )


async def check_backfill(hs: "HomeServer") -> None:
    """Refuse startup if stored events have no completed verdict backfill.

    A database with no events adopts the marker itself: a fresh install never
    needs the offline migration. An upgraded database must run
    `synapse_event_verdicts_migrate` first, because persist reads treat a
    missing verdict as "event not stored".
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

    if not await store.db_pool.runInteraction("check_event_verdict_backfill", _txn):
        raise EventVerdictBackfillIncompleteError(
            "Event verdicts have not been backfilled for this database. "
            "Stop Synapse and run `synapse_event_verdicts_migrate --config "
            "<config>` before starting this build."
        )


async def migrate_event_verdicts(
    hs: "HomeServer",
    *,
    batch_size: int = 1000,
    dry_run: bool = False,
    start_after: str = "",
) -> dict[str, int]:
    """Rebuild every event's mtxdb verdict from the SQL event store.

    Stop every Synapse process first. The scan is ordered by event id, resumes
    from the marker's `last_event_id`, and verifies each batch by reading it
    back. The marker is completed only after the whole scan succeeds.

    `dry_run` reports missing or disagreeing verdicts without writing anything
    or advancing the marker.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    if not hs.config.database.embedded_db_engine:
        raise ValueError("configure embedded_db.engine for the mtxdb destination")

    from synapse.storage._base import db_to_json
    from synapse.storage.databases.main.embedded_event_json import (
        get_event_json_batch,
    )

    store = hs.get_datastores().main
    engine_name = hs.config.database.embedded_db_engine

    completed, last_event_id = await store.db_pool.runInteraction(
        "event_verdict_backfill_read_marker", _read_backfill_state
    )
    if completed:
        return {
            "already_migrated": 1,
            "events": 0,
            "written": 0,
            "unchanged": 0,
            "mismatched": 0,
        }
    if start_after:
        last_event_id = start_after

    stats = {
        "events": 0,
        "written": 0,
        "unchanged": 0,
        "mismatched": 0,
    }

    def _read_batch(
        txn: Any, after: str, limit: int
    ) -> list[tuple[str, bool, str | None, str | None]]:
        txn.execute(
            """
            SELECT e.event_id, e.outlier,
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

    def _advance(txn: Any, after: str, complete: bool) -> None:
        _write_backfill_state(txn, after, complete)

    while True:
        rows = await store.db_pool.runInteraction(
            "event_verdict_backfill_batch", _read_batch, last_event_id, batch_size
        )
        if not rows:
            break

        missing_meta_ids = [event_id for event_id, _, _, meta in rows if meta is None]
        embedded_meta = get_event_json_batch(engine_name, missing_meta_ids)

        expected: dict[str, EventVerdict] = {}
        for event_id, outlier, reason, metadata in rows:
            if metadata is None:
                found = embedded_meta.get(event_id)
                if found is None:
                    raise RuntimeError(
                        f"event {event_id} has no event JSON in mtxdb or SQL; "
                        "refusing to leave it without a verdict"
                    )
                metadata = found[0]
            expected[event_id] = EventVerdict(
                outlier=bool(outlier),
                rejection_code=rejection_code(reason),
                soft_failed=bool(db_to_json(metadata).get("soft_failed")),
            )

        existing = get_event_verdicts(list(expected))
        if dry_run:
            stats["mismatched"] += sum(
                existing.get(event_id) != verdict
                for event_id, verdict in expected.items()
            )
        else:
            stats["written"] += len(expected)
            put_event_verdicts(expected)
            readback = get_event_verdicts(list(expected))
            for event_id, verdict in expected.items():
                if readback.get(event_id) != verdict:
                    raise RuntimeError(
                        f"verdict verification failed for {event_id}: "
                        f"wrote {verdict}, read {readback.get(event_id)}"
                    )

        stats["events"] += len(rows)
        last_event_id = rows[-1][0]
        if not dry_run:
            await store.db_pool.runInteraction(
                "event_verdict_backfill_marker", _advance, last_event_id, False
            )

    if not dry_run:
        await store.db_pool.runInteraction(
            "event_verdict_backfill_complete", _advance, "", True
        )
    return stats
