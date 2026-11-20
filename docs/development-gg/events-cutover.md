# Events cutover: replacing the SQL `events` index

Status: design note. Nothing here is implemented. This maps the current `events`
table by access pattern and mutation, then proposes the mtxdb records and
indexes those queries need. It is the full-events counterpart to the narrower
`event-status-mtxdb-metadata.md` note.

Related documents:

- `docs/development-gg/mtxdb-performance-master-plan.md`
- `docs/development-gg/event-status-mtxdb-metadata.md`
- `docs/development-gg/perf-track-b.md`
- `docs/development-gg/auth-chain-cutover.md`
- `res/docs/2026-09-20-events-table-deprecation-plan.md`
- `res/docs/2026-09-30-ordered-partitioned-append-stream.md`

## Why this is different from the auth-chain cutover

Auth chains were a keyed graph: exact-key reads plus a BFS. `events` is an
ordered relational index. Readers ask for "the newest N events in this room
after token T", "all events after global stream S", "events of these types",
"the event at this depth", and "metadata for these ids". The pinned mtxdb
provides a persistent ordered `TimelineIndex` (`TimelineCursor`,
`TimelineEntry`, and `TimelinePage`), but Synapse's Rust adapter does not
currently expose or use it. The gap is integration and proving its cursor/scope
semantics against Synapse's queries, not inventing an ordered-index primitive.

Moving rows into a key-value record without using the ordered index would
replace SQL's B-tree work with Python-side scans. The useful unit of work is
therefore the event record **and** the mtxdb timeline indexes together.

The SQL cost is real: `events` was the largest measured SQL table in the latest
run (56.9 s aggregate; 90 s in the 8:48 profile). Event bodies are already in
mtxdb; this cost is almost entirely index and metadata access.

## What `events` holds today

| Column                 | Nature                                           | Notes                                             |
| ---------------------- | ------------------------------------------------ | ------------------------------------------------- |
| `event_id`             | identity                                         | joins everywhere                                  |
| `room_id`              | immutable partition key                          | joins everywhere                                  |
| `stream_ordering`      | immutable-ish ordering key                       | multi-writer sequence; negative for backfill      |
| `topological_ordering` | immutable ordering key                           | currently set from `depth` on persist             |
| `depth`                | immutable                                        | backfill ordering                                 |
| `outlier`              | mutable flag                                     | set on persist/purge, cleared on de-outlier       |
| `type`                 | immutable                                        | filters and result columns                        |
| `sender`               | immutable                                        | result columns and filters                        |
| `state_key`            | immutable                                        | state lookups                                     |
| `origin_server_ts`     | immutable                                        | timestamp lookups and search                      |
| `received_ts`          | immutable                                        | push-action cutoff binary search                  |
| `instance_name`        | immutable                                        | replication position and `PersistedEventPosition` |
| `rejection_reason`     | mutable                                          | duplicated in `rejections`                        |
| `contains_url`         | immutable, derivable from JSON                   | one partial index                                 |
| `content`              | already in mtxdb `event_json`; NULL for new rows | dead in SQL                                       |
| `unrecognized_keys`    | never written                                    | dead                                              |
| `processed`            | always true                                      | dead                                              |

The five SQL indexes that the replacement must account for:

- `events_room_stream (room_id, stream_ordering)`
- `events_order_room (room_id, topological_ordering, stream_ordering)`
- `events_stream_ordering (stream_ordering)` unique
- `events_ts (origin_server_ts, stream_ordering)`
- `event_contains_url_index (room_id, topological_ordering, stream_ordering)`
  partial on `contains_url AND NOT outlier`

## Access-pattern inventory

Representative call sites, not an exhaustive list. The point is the shape of the
query, because that determines the index.

### A. Point reads by event id

| Call site                               | Shape                                                                                                                      |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `events_worker._fetch_event_rows`       | `SELECT stream_ordering, received_ts, instance_name, room_version, rejection, outlier FROM events WHERE event_id IN (...)` |
| `state.get_metadata_for_events`         | `event_id, room_id, type, state_key, rejection_reason WHERE event_id IN (...)`                                             |
| `censor_events`                         | `JOIN events` for original event metadata                                                                                  |
| `embedded_event_edges` migration checks | existence and `room_id` for ids                                                                                            |

Needs: one point record keyed by `(namespace, room_id, event_id)`, batch reads.
`room_version` stays in `rooms`; rejection should fold into the record so the
`rejections` join disappears from these paths.

### B. Per-room timeline by stream ordering

| Call site                                          | Shape                                                                                    |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `stream.get_room_events_stream_for_room`           | `WHERE room_id = ? AND NOT outlier` + stream bounds, `ORDER BY stream_ordering`, `LIMIT` |
| `stream.get_room_event_before_stream_ordering`     | `room_id, stream <= ?`, not outlier, not rejected, `ORDER BY stream DESC LIMIT 1`        |
| `stream.get_last_event_pos_in_room`                | same, optional `type IN (...)`, `ORDER BY stream DESC LIMIT 1`                           |
| `stream.get_topological_token_for_stream_position` | room, stream bound, not outlier, order by stream/topo                                    |
| `stream` sync/replication range                    | global stream or room stream window                                                      |
| `event_push_actions` cutoff                        | `MAX(stream_ordering)` and binary search by `received_ts`                                |

Needs: ordered `(room_id, stream_ordering) -> event_id` index, with a
non-outlier/non-rejected view. Type-filtered reads need either a
`(room_id, type, stream_ordering)` index or a bounded scan of the timeline.

### C. Global stream ordering

| Call site                                   | Shape                                              |
| ------------------------------------------- | -------------------------------------------------- |
| `events_worker` replication / event stream  | `stream_ordering > ?` and writer position          |
| `event_push_actions`, `room_stats`, metrics | `MAX/MIN(stream_ordering)`                         |
| `sticky_events`, `thread_subscriptions`     | global `MAX(stream_ordering/topological_ordering)` |
| `event_federation` stream bounds            | global `MIN/MAX(stream_ordering)`                  |

Needs: ordered global `stream_ordering -> event_id` index plus cheap min/max
counter reads. `stream_ordering` is allocated by a `MultiWriterIdGenerator` over
`events_stream_seq` and `events_backfill_stream_seq`; see the allocation
section.

### D. Per-room topological / depth ordering

| Call site                                                | Shape                                                     |
| -------------------------------------------------------- | --------------------------------------------------------- |
| `stream.get_current_topological_token`                   | `MIN(topological_ordering)` for a room/stream window      |
| `stream._get_max_topological_txn`                        | `MAX(topological_ordering)` per room                      |
| `event_federation` backfill queue                        | `ORDER BY depth DESC, stream_ordering DESC` over prev ids |
| `stream` pagination when a topological token is supplied | `(topological_ordering, stream_ordering)` bounds          |

Needs: `(room_id, topological_ordering, stream_ordering) -> event_id` and a
`(room_id, depth, stream_ordering)` view (or sort the candidate set in Rust
after batch-reading records).

### E. Metadata joins for ids selected elsewhere

| Call site                                    | Shape                                                                                                  |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `relations`                                  | relation rows `JOIN events` for `sender`, `type`, `topological_ordering`, `stream_ordering`, `room_id` |
| `receipts`                                   | `JOIN events` for room/event metadata                                                                  |
| `roommember`                                 | membership rows `JOIN events` for sender/stream/instance                                               |
| `event_push_actions`                         | push rows `JOIN events`                                                                                |
| `room`                                       | room/event listings `JOIN events`                                                                      |
| `state_deltas`, `events_forward_extremities` | joins for ordering/type                                                                                |
| `search`                                     | FTS rows `JOIN events` for ts/room/stream                                                              |

Two strategies, chosen per site:

1. run the owning-table query without the join, then batch-read event records
   from mtxdb and merge in Python; or
2. denormalize the few needed event fields into the owning table (for example
   relation rows already carry enough to render), avoiding the second read.

Do not put a general SQL join against mtxdb in the middle of a query. Pick one.

### F. Mutation and maintenance

| Path                                           | Mutation                                                                                 |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `events._store_event_txn`                      | insert the full row                                                                      |
| `events._update_outliers_txn` / de-outlier     | `UPDATE events SET outlier = FALSE`                                                      |
| `purge_events` retained events                 | `UPDATE events SET outlier = TRUE`                                                       |
| `events_bg_updates` legacy passes              | `sender`, `contains_url`, `origin_server_ts`, `topological_ordering`, `stream_ordering2` |
| `purge_events`                                 | delete by room / event                                                                   |
| `_store_rejections_txn`, accept/reject recheck | rejection state (also in `rejections`)                                                   |
| soft-fail writes                               | `internal_metadata` today; status record target                                          |
| room stats / metrics                           | counts, min/max aggregates                                                               |

The background updates mutate old rows only. They can stay SQL-only until the
one-shot import; new rows never take those paths.

## Proposed mtxdb records

### 1. `event_record` (point lookup)

Keyed by `(namespace, room_id, event_id)` so purge can delete a room's records
without a global scan.

```text
event_record = {
  stream_ordering: i64,
  instance_name: str,
  topological_ordering: i64,
  depth: i64,
  type: str,
  sender: str,
  state_key: str | None,
  origin_server_ts: i64,
  received_ts: i64,
  outlier: bool,
  rejected: bool,
  rejection_reason: str | None,
  soft_failed: bool,
}
```

Immutable fields plus the mutable status block. `content`, `processed`,
`unrecognized_keys`, `contains_url` do not belong here: the body is already in
`event_json`, and `contains_url` can be derived or kept in the body metadata.
`room_version` stays with `rooms`.

The mutable block can initially be the separate status record proposed in
`event-status-mtxdb-metadata.md`; either shape works as long as point reads get
status in one batch without a `rejections` join. A single record is simpler if
version/CAS semantics are available; a separate status record avoids rewriting
immutable fields on status changes.

### 2. Ordered indexes

These indexes should use mtxdb's persisted `TimelineIndex` where its scope and
cursor contract fit. The ordered-partitioned append-stream design in
`res/docs/2026-09-30-ordered-partitioned-append-stream.md` remains relevant to
stream allocation, replay, and consumer checkpoints; it should not be read as
evidence that mtxdb lacks an ordered timeline index. Do not implement these as
scans of `event_record`:

```text
global_stream   : (stream_ordering) -> event_id
room_stream     : (room_id, stream_ordering) -> event_id
room_topological: (room_id, topological_ordering, stream_ordering) -> event_id
room_type_stream: (room_id, type, stream_ordering) -> event_id      # optional
room_depth      : (room_id, depth, stream_ordering) -> event_id     # optional
event_ts        : (origin_server_ts, stream_ordering) -> event_id
```

- `global_stream` replaces `events_stream_ordering`, serves replication and
  global range scans, and makes min/max O(1) if the stream exposes ends.
- `room_stream` replaces `events_room_stream` and serves timeline pagination.
- `room_topological` replaces `events_order_room`.
- `room_type_stream` serves type-filtered "last event in room" reads. Start
  without it if a bounded backwards scan of `room_stream` is acceptable; add it
  if measurements show type filters dominating.
- `room_depth` serves backfill ordering; alternatively batch-read candidate
  records and sort in Rust, which is simpler and bounded by the candidate set.
- `event_ts` serves search and timestamp lookups; the push-action cutoff can
  instead binary-search `global_stream` by `received_ts`, as the SQL code does.

## Visibility and mutable flags

Timeline and backfill reads filter `NOT outlier` and exclude rejected events. An
ordered index cannot filter on a value stored in the record without reading
every candidate. Two workable designs:

1. **Eligible-only streams (recommended).** `room_stream`, `room_topological`,
   `global_stream`, and `event_ts` contain only events visible to ordinary
   timeline reads. De-outliering inserts the event into them; setting outlier or
   rejecting removes it; purge deletes it. These transitions are rare relative
   to inserts.
2. **All-events streams plus record filtering.** Simpler writes, but reads must
   over-fetch and may scan pathological runs of outliers/rejected events.

Point reads use `event_record` and see status regardless of index membership.
Backfill ordering must use an index that either excludes outliers or filters
after lookup; outliers are exactly the events backfill is trying to repair, so
this needs an explicit test.

Rejection is mutable after persist (accept/reject recheck). Treat it like
outlier: move the index membership on change, and keep the point record
authoritative. Do not infer rejection from absence in a timeline stream.

## Ordering, allocation, and multi-writer

`stream_ordering` is currently allocated by a `MultiWriterIdGenerator` over
`events.instance_name`, with a separate negative sequence for backfilled events.
The mtxdb timeline index does not by itself replace this Synapse allocation and
replication contract. Index writes must preserve:

- monotonic order per writer;
- the gap-tolerant global order replication and sync tokens rely on;
- negative backfill stream orderings, which sort before live events.

This is the same shared-writer problem as state groups. Two options:

1. keep SQL allocation (`events_stream_seq`, `stream_positions`) and only move
   the rows and indexes; or
2. make the event writer the only mtxdb allocator and have other workers request
   positions from it.

Option 1 is the low-risk first cutover and still removes `events` reads/writes,
though it leaves the sequence in SQL. Option 2 is required before SQL can be
retired completely, and lands with the ordered-stream primitive's cursor
contract.

## Migration and cutover

Follow the auth-chain shape: one authoritative backend selected by
`embedded_db.engine`, an offline one-shot import, a durable marker, and a
startup refusal while the import is incomplete.

1. Add `event_record` and the ordered streams, plus `events_backend` marker.
2. Offline `synapse_events_migrate` reads SQL `events` (and `event_json` where
   needed), writes records and index entries, verifies:
   - row count and per-room counts;
   - per-room stream order and global stream order;
   - point-record equality for a sample of every column;
   - min/max stream/topological values per room; then flips the marker.
3. Cut over reads in this order, each behind its own tests:
   - point reads (`_fetch_event_rows`, `get_metadata_for_events`);
   - timeline and pagination (`stream.py`);
   - replication/global stream and push cutoffs;
   - relations/receipts/membership metadata joins;
   - backfill ordering;
   - purge and stats.
4. Cut over writes: insert with the record, de-outlier/reject index moves, purge
   deletes, room stats counters. SQL `events` becomes the import snapshot only.
5. Drop the SQL reads and inserts, then the table and its five indexes, and
   update `synapse_port_db.py`.

Do not run a dual-write phase. The marker and startup check are the safety
mechanism, as with auth chains.

## Open decisions

- **Single vs separate status record.** Affects CAS/version requirements and how
  often immutable fields are rewritten.
- **Eligible-only vs all-events ordered streams.** Recommended eligible-only,
  but purge and de-outlier need explicit move tests.
- **Type/depth secondary indexes.** Start without them; add only if measurements
  show the bounded scan is not good enough.
- **Metadata joins.** Per call site, choose batch-read-and-merge or denormalize.
- **Stream allocation.** Keep SQL allocation for the first cutover, or move it
  with the ordered-stream primitive.
- **Room stats.** Move min/max/counts to mtxdb counters (needs CAS) or recompute
  from streams on demand.
- **Search.** FTS stays SQL; only its `events` join needs replacing.

## Non-goals

- `current_state_events`, `current_state_delta_stream`, and state groups are
  separate work.
- `event_edges`, `event_json`, and auth chains keep their own cutovers.
- Per-user sync tokens, push queues, and replication positions stay SQL until
  the ordered-stream contract exists.
