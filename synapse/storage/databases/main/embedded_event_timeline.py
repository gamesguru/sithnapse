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

"""Room timelines in mtxdb's ordered event indexes.

Persistence indexes every non-outlier event in two orders, matching the two
room paginations in `stream.py`:

- topological: `(topological_ordering, origin_server_ts, stream_ordering)`;
- stream: `stream_ordering`.

`SYNAPSE_MTXDB_TIMELINE` selects how much of this runs. Default `off`.

- `write`: index events as they are persisted. Reads stay on SQL.
- `compare`: also read each eligible page from the index, and log a warning
  when it differs from the SQL rows. SQL rows are still returned.
- `read`: return index rows for eligible pages, falling back to SQL when the
  index cannot answer.

`read` is only correct when every non-outlier event of every room was
indexed: a server whose events were all persisted with indexing on. Rooms with
earlier history have no entries for it, and nothing here rebuilds them yet.
Use `compare` to check a deployment before turning `read` on.

Eligible pages are unfiltered and use no multi-writer `instance_map`, since
the index stores neither filter columns nor the persisting instance. Other
pages always use SQL.

History purges (`purge_history`) and single-event deletions do not remove
index entries yet. A room purge does.
"""

import logging
import os
from typing import TYPE_CHECKING, Any, Iterable, Sequence

from synapse.storage.databases.main.embedded_common import retry_on_contention

if TYPE_CHECKING:
    from synapse.events import EventBase
    from synapse.synapse_rust.mtxdb_engine import MtxdbTransaction

logger = logging.getLogger(__name__)

_MODES = ("off", "write", "compare", "read")

# One page row as both the index and the pagination SQL return it:
# `(event_id, topological_ordering, origin_server_ts, stream_ordering)`.
TimelinePageRow = tuple[str, int, int, int]

# `(topological, origin_server_ts, stream)`, as `generate_pagination_bounds`
# returns it; the first two are `None` for a live token.
PaginationBound = tuple[int | None, int | None, int]


def timeline_mode() -> str:
    """The configured `SYNAPSE_MTXDB_TIMELINE` mode.

    An unknown value is an error rather than a silent `off`, so a typo cannot
    look like a working index.
    """
    value = os.environ.get("SYNAPSE_MTXDB_TIMELINE", "off").strip().lower() or "off"
    if value not in _MODES:
        raise ValueError(
            f"SYNAPSE_MTXDB_TIMELINE must be one of {', '.join(_MODES)}, got {value!r}"
        )
    return value


def timeline_writes_enabled() -> bool:
    return timeline_mode() != "off"


def timeline_reads_mode() -> str:
    """`off`, `compare` or `read` for the read path."""
    mode = timeline_mode()
    return mode if mode in ("compare", "read") else "off"


def put_timeline_rows(
    rows: Sequence[tuple[str, str, int, int, int]],
    transaction: "MtxdbTransaction | None",
) -> None:
    """Index `(room_id, event_id, topological, origin_server_ts, stream)` rows.

    With a transaction they are staged and published with it. Without one
    they are written directly, best effort, like the direct event JSON write.
    """
    if not rows:
        return
    rows = list(rows)
    if transaction is not None:
        transaction.event_timeline_put(rows)
        return
    from synapse.synapse_rust.mtxdb_engine import event_timeline_put

    event_timeline_put(rows)


def remove_timeline_rows(rows: Sequence[tuple[str, str, int, int, int]]) -> None:
    """Remove timeline keys and ID mappings for SQL-purged events."""
    if not rows:
        return
    from synapse.synapse_rust.mtxdb_engine import timeline_remove_rows

    retry_on_contention(
        lambda: timeline_remove_rows(list(rows)), label="timeline_remove"
    )


def timeline_rows_for_events(
    events: Iterable["EventBase"],
) -> list[tuple[str, str, int, int, int]]:
    """Index rows for the non-outlier events among `events`, with the values
    `_store_event_txn` writes to the `events` row."""
    rows = []
    for event in events:
        if event.internal_metadata.is_outlier():
            continue
        stream_ordering = event.internal_metadata.stream_ordering
        assert stream_ordering is not None, "persisted events have a stream ordering"
        rows.append(
            (
                event.room_id,
                event.event_id,
                int(event.depth),
                int(event.origin_server_ts or 0),
                int(stream_ordering),
            )
        )
    return rows


def purge_room_timeline(room_id: str) -> None:
    """Drop a purged room's index entries and event-id records."""
    from synapse.synapse_rust.mtxdb_engine import timeline_purge_room

    timeline_purge_room(room_id)


def page_topological(
    room_id: str,
    from_bound: PaginationBound | None,
    to_bound: PaginationBound | None,
    backwards: bool,
    limit: int,
) -> list[TimelinePageRow] | None:
    """Index rows for `_paginate_room_events_by_topological_ordering_txn`'s
    query, or `None` when the index cannot answer."""
    from synapse.synapse_rust.mtxdb_engine import timeline_page_topological

    return timeline_page_topological(room_id, from_bound, to_bound, backwards, limit)


def page_stream(
    room_id: str,
    from_stream: int | None,
    to_stream: int | None,
    backwards: bool,
    limit: int,
) -> list[TimelinePageRow] | None:
    """Index rows for `paginate_room_events_by_stream_ordering`'s query, or
    `None` when the index cannot answer."""
    from synapse.synapse_rust.mtxdb_engine import timeline_page_stream

    return timeline_page_stream(room_id, from_stream, to_stream, backwards, limit)


def report_mismatch(
    label: str,
    room_id: str,
    sql_rows: Sequence[tuple[Any, ...]],
    index_rows: Sequence[tuple[Any, ...]] | None,
) -> bool:
    """Log a warning when the index rows differ from the SQL rows; return
    whether they matched. Both are tuples starting with the event id, in the
    same column order. `None` (the index could not answer) is not a
    mismatch."""
    if index_rows is None:
        return True
    if list(sql_rows) == list(index_rows):
        return True
    sql_ids = [row[0] for row in sql_rows]
    index_ids = [row[0] for row in index_rows]
    logger.warning(
        "mtxdb timeline mismatch in %s for %s: sql=%d rows %s index=%d rows %s",
        label,
        room_id,
        len(sql_ids),
        sql_ids[:5],
        len(index_ids),
        index_ids[:5],
    )
    return False
