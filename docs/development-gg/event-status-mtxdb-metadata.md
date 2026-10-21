# Event status for mtxdb edge reads

Status: design note; nothing implemented. This describes the mutable event state
needed before the family-4 persist queries can read from mtxdb without SQL. See
`perf-track-b-family4-edges-map.md` and
`res/docs/2026-09-28-event-edges-sql-removal-plan.md`.

## Goal

Replace the persist-path SQL joins with Rust reads over mtxdb's forward edges
and the status of their child events. The required status is small:

- `outlier` from `events.outlier`;
- whether a rejection row exists;
- `soft_failed` from event internal metadata.

The first two are mutable SQL state and are not yet completely represented in
mtxdb. The rejection mirror is also not authoritative today.

## Pool placement and consistency

Edges are read from the Edges pool (`auth_chain_db()` in the Synapse binding).
Event JSON and its `event_meta_node_id` record are in the separate EventDag pool
(`event_dag_db()`). A `PackfileStorage::read_snapshot()` pins one pool; it
cannot give a consistent read across those two engines.

Put the new compact status record in the **Edges pool**, in its own stable
room-scoped collection (for example `STAT`, derived from the room's group
digest), keyed by event ID. Keep it out of the generation-scoped `FWD`
collection and out of `PREV`: a forward-generation rebuild or reset must not
delete or rewrite status. Do not copy mutable status into parent forward lists:
de-outliering or a rejection change would otherwise require rewriting every
parent that references the event.

Then a single Edges-pool snapshot can read the forward lists and the status
records from one boundary. The Rust binding should:

1. Capture one read snapshot.
2. Batch-read candidate forward lists in the room collection.
3. If children exist, batch-read their status records in the same snapshot.
4. Return qualifying parent IDs and diagnostic counts.

A snapshot's overlay covers committed journal groups only. A status record
staged in another, uncommitted transaction is deliberately invisible; a record
staged in the reader's own transaction is visible to that transaction. The read
path must not depend on seeing another writer's uncommitted status.

For `_get_prevs_before_rejected`, traverse levels in Rust with a visited set;
read each frontier and its statuses on the same snapshot. Do not call Python
once per recursion level. If publication advances or a writer restarts and a
snapshot returns `WouldBlock`, retry a bounded number of times inside Rust.
After bounded retries, fail closed to the existing SQL path during migration; do
not interpret an incomplete snapshot as “no edges”.

The first query's saved workload had only 12 SQL result rows for 20,579
candidates, so most calls should stop after the forward lookup. That makes a
separate status record reasonable: it is fetched only for actual children. The
rejected-predecessor traversal can have more children, so measure its status
batch separately.

## Atomic writes: what mtxdb already supports

The required body-plus-edges write does **not** require moving edge data into
EventDag or adding a cross-pool transaction feature. The mtxdb `Database`
documents atomic cross-pool transactions; `DatabaseTransaction::put` accepts a
`ShardType`, and Synapse's transaction stages writes into a shared journal
group. The existing event JSON staging targets EventDag, while edge staging
targets Edges. Use one mtxdb transaction to commit the body and edges together.

This guarantee is within mtxdb. SQL and mtxdb still cannot commit atomically
with each other. During migration, SQL remains authoritative and both stores are
updated. The cutover needs an explicit recovery/reconciliation policy for the
crash window between the mtxdb commit and SQL commit; it must not claim that
cross-pool atomicity closes that cross-database gap.

## Status record format and authority

Use a versioned, fixed-size Synapse-owned payload, written through the existing
Rust binding layer rather than a new mtxdb frame TLV. A minimal first version
needs bits for `outlier`, `rejected`, and `soft_failed`; omit unset/normal
records after the backfill completion gate. Absence means normal only after that
gate has completed.

Keep the API layout-agnostic:

```text
event_status_get_batch(room_id, event_ids) -> event_id -> status or missing
event_status_put_batch(room_id, rows, transaction=...) -> ()
event_status_delete_batch(room_id, event_ids, transaction=...) -> ()
```

The API should permit staging in the same cross-pool mtxdb transaction as body,
edge, or rejection-reason writes. Rust owns the encoding and the Edge-pool
collection/key mapping. Python callers should not know the record layout.

The status record carries a rejection bit, not the rejection reason. The current
reason is free text, and this read path only needs to know whether the event is
rejected. Keep full-reason reads and writes on their current SQL path until they
receive a separate migration plan. A category code is unnecessary for these
filters.

## Required mutation paths

Keep SQL authoritative and dual-write until backfill and verification finish.
Every status transition must update the Edge-pool record:

| Path                                     | Status change                                                           |
| ---------------------------------------- | ----------------------------------------------------------------------- |
| Event persistence                        | Initial outlier/rejected/soft-failed state                              |
| `_update_outliers_txn` in `events.py`    | Clear outlier on de-outlier                                             |
| Purge's retained-event outlier update    | Set outlier                                                             |
| Event purge (`event_edges_delete`)       | Delete the status record; purge only tombstones the backward edge today |
| `_store_rejections_txn` in `events.py`   | Set rejected                                                            |
| `events_worker.py` accept/reject recheck | Clear or set rejected; currently not mirrored                           |
| Both rejection purge paths               | Clear or delete rejected status                                         |
| Soft-fail state writes                   | Set/clear soft-failed; audit exact mutation paths before implementation |

The existing rejection mirror does not satisfy this contract: its writes are
documented as cache-tier, `get_rejections_batch` has no production caller, and
`events_worker.py` has direct SQL upsert/delete paths that do not mirror. The
status record must be updated on those paths too. This does not make the
existing full rejection-reason record authoritative.

`event_edges_delete` currently writes an empty backward record and deliberately
does not touch parents' forward lists or delete collections, so it will not
remove a `STAT` record implicitly. Add the status delete to that path (or
tombstone it in the same batch). A stale status record is not read for a
tombstoned child, because the forward read filters the child first, but it
violates the absence contract and must not survive indefinitely.

The body and edges should be staged in one mtxdb transaction. Status writes that
accompany initial persistence should join that transaction. Later
outlier/rejection transitions can be small status-only transactions. All updates
still need the SQL↔mtxdb failure/reconciliation policy described above.

## Migration and cutover

1. Add the Edge-pool status record and dual-write every mutation path above;
   keep reads on SQL.
2. Backfill abnormal events from SQL (`outlier`, rejected rows, and
   `soft_failed`). Make the background update resumable. Do not treat a missing
   record as normal until the update is complete.
3. Verify status against SQL for sampled records and specifically exercise
   persist, de-outlier, purge's outlier flip, reject, re-reject, accept, and
   purge deletion. Check transaction overlays and a read-only worker's published
   view.
4. After the verification gate, switch the two persist queries to the Rust
   snapshot API. Keep the current SQL/version fallback until the edge cutover
   makes a forward miss authoritative.
5. Stage body and edges in one cross-pool mtxdb transaction. Then remove the
   persist SQL joins and, only after all other edge readers are migrated, retire
   the SQL edge mirror, outbox, and version gate.

The SQL and mtxdb commits remain separate systems throughout. Before declaring
mtxdb authoritative, define how a crash between those commits is detected and
repaired; a manual reconciliation tool may be sufficient, but the failure mode
must be explicit.

## Why not a frame TLV now?

A TLV on `event_meta_node_id` would avoid one small sibling record, but that
record is in EventDag, while the edge snapshot is in Edges. It would therefore
reintroduce the cross-pool read-consistency problem. A Synapse-owned status
record in Edges gives the needed same-snapshot read without changing mtxdb's
frame format or exposing frame metadata through its storage API.

Revisit a generic mtxdb frame-metadata API only if another consumer needs typed
per-record metadata or measurements show the extra status records are material.
The current read design gets no snapshot advantage from a TLV on the EventDag
metadata record.

## Measurements

Instrument the Rust call with duration, candidate count, forward hits, status
records read, recursion levels, and `WouldBlock` retries. Compare these with
`persist_prevs_total` and `persist_rejected_total`; the desired result is no SQL
execution or `runInteraction` on the successful post-cutover path. Keep the SQL
fallback visible during migration and count its use explicitly.
