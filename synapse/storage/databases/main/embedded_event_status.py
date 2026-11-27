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
from typing import TYPE_CHECKING, NamedTuple

from synapse.storage.databases.main.embedded_common import (
    ffi_timing,
    mirror_timing,
    retry_on_contention,
)

if TYPE_CHECKING:
    from synapse.synapse_rust.mtxdb_engine import MtxdbTransaction


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
