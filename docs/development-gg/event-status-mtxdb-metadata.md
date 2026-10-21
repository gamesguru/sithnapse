# Event status in the backward-edge record

Status: design note; nothing implemented. This describes the mutable event state
needed before the family-4 persist queries can read from mtxdb without SQL. See
`perf-track-b-family4-edges-map.md` and
`res/docs/2026-09-28-event-edges-sql-removal-plan.md`.

## Goal

Replace the persist-path SQL joins with Rust reads over mtxdb. The filter needs
three bits of per-event state:

- `outlier` from `events.outlier`;
- whether a rejection row exists;
- `soft_failed` from event internal metadata.

The first two are mutable SQL state and are not represented in mtxdb. The
rejection mirror is not authoritative today.

## Decision: a status byte in the backward-edge payload

Store the status as one optional trailing byte in each event's **backward-edge
record** (`encode_backward_edges`, `rust/src/database/embedded_edges.rs`). No
mtxdb change, no frame TLV, no sibling record.

Why this record:

- It is already in the Edges pool, so one `PackfileStorage::read_snapshot()` on
  that pool covers the forward lists and the status. Snapshots are per pool; the
  EventDag metadata record cannot share one with the edges.
- Both persist queries already need it. `_get_prevs_before_rejected` walks
  backwards: each level needs the current event's prev list and its own status,
  which is one backward record. `_get_events_which_are_prevs` reads forward
  children and must read each child's backward record to drop tombstoned
  children, so the status arrives in a record that read pays for.
- A purge tombstone (`event_edges_delete` writes an empty backward record)
  clears the status by construction. There is no separate delete.
- It is Synapse-owned structured data, not `internal_metadata` JSON.

Caveat on the second point: the ungated `event_edges_get_forward` does the child
backward read (`embedded_edges.rs` ~1068-1115). The gated getter that production
uses (`event_edges_get_forward_gated`) reads only the generation-scoped forward
list. The new Rust persist read must do the child backward batch itself, in the
same snapshot. It is one batched read per room collection, not free.

### Format

Today: `[version=2][self identity][u16 count]{[is_state u8][prev identity]}*`.
The decoder reads exactly `count` edges and never checks that the payload is
fully consumed, so an old reader ignores a trailing byte.

Keep version 2 and append an optional status byte:

- no trailing byte: legacy record, status unknown (see migration);
- one trailing byte: bit 0 `outlier`, bit 1 `rejected`, bit 2 `soft_failed`,
  remaining bits reserved and must be zero.

New readers accept both. Do not bump to version 3: that would hard-fail old
readers during a rolling deploy for no benefit. Centralise encode and decode so
the three backward-record writers (`write_edges`, `event_edges_delete`, the new
status update) cannot diverge.

The status carries a rejection bit, not the reason. The reason is free text and
this read path only needs to know whether the event is rejected. Full-reason
reads and writes stay on their current SQL path until they have their own
migration.

### Absence is not "normal" yet

A version-2 record written before this change has no status byte. It cannot mean
"normal": an abnormal legacy event would be read as normal. Treat a missing byte
as unknown and fall back to SQL until the backfill completion gate passes. After
the gate, missing means normal and backfill only needs to rewrite legacy
abnormal events, not every record.

Events with no prev events (room creators) have no backward record. If one is
ever abnormal, the status writer creates a zero-edge backward record to carry
the byte. Normal creators need nothing.

## Read path

The Rust binding does one call per persist batch on one Edges snapshot:

1. Capture one read snapshot.
2. `_get_events_which_are_prevs`: batch-read candidate forward lists in the room
   collection; for any children, batch-read their backward records and filter on
   status. Return qualifying parent IDs and diagnostic counts. Most candidates
   have no forward record (12 SQL rows for 20,579 candidates in the saved run),
   so most calls stop after the forward read.
3. `_get_prevs_before_rejected`: walk backwards in Rust with a visited set. Read
   each frontier's backward records once, use the prev list and status together.
   Do not call Python once per level.
4. On `WouldBlock` (publication advanced or the writer restarted), retry a
   bounded number of times inside Rust. After the bound, fail closed to the
   existing SQL path during migration. Never read an incomplete snapshot as "no
   edges".

A snapshot covers committed journal groups only. A record staged in another,
uncommitted transaction is invisible by design; one staged in the reader's own
transaction is visible to it. The read must not depend on another writer's
uncommitted status.

A forward-list miss is authoritative only after the edge cutover (body and edges
committed together). Until then the version gate stays; see "Migration".

## Writes

Initial persistence passes the known state into `event_edges_put` as an optional
status byte per event, in the same put as the backward record.

Later changes (de-outlier, reject, accept, soft-fail) go through
`event_status_put_batch`, a version-bound read-modify-write of the backward
record: read, replace the trailing byte, re-encode the same prev list, write
with `expect_record_version`. The prev list is immutable, so this is safe, and
the transitions are rare. Updates must serialise with edge writes and with purge
so a status write cannot overwrite a newer edge write or resurrect a purged
record. A version conflict retries.

Keep the API layout-agnostic so Python never knows the record layout:

```text
event_status_get_batch(room_id, event_ids) -> event_id -> status or unknown
event_status_put_batch(room_id, rows, transaction=...) -> ()
```

Body and edges are written in one mtxdb transaction. mtxdb's `Database` provides
atomic cross-pool transactions (`engine/database.rs`), and Synapse's transaction
already stages event JSON into EventDag and edges into Edges. Moving body writes
and edge writes into one transaction is a call-site change, not a pool change.
This is atomic within mtxdb only; SQL and mtxdb still cannot commit together.
The cutover needs an explicit policy for the window between the two commits (a
manual reconciliation tool may suffice), and must not claim cross-pool atomicity
closes it.

## Mutation paths

SQL stays authoritative and every path dual-writes until the verification gate.

| Path                                     | Status change                                               |
| ---------------------------------------- | ----------------------------------------------------------- |
| Event persistence                        | Initial outlier/rejected/soft-failed state, in the edge put |
| `_update_outliers_txn` in `events.py`    | Clear outlier on de-outlier                                 |
| Purge's retained-event outlier update    | Set outlier                                                 |
| Event purge (`event_edges_delete`)       | Tombstone clears the byte; nothing extra                    |
| `_store_rejections_txn` in `events.py`   | Set rejected                                                |
| `events_worker.py` accept/reject recheck | Clear or set rejected; currently not mirrored at all        |
| Both rejection purge paths               | Clear rejected                                              |
| Soft-fail state writes                   | Set/clear soft-failed; audit exact mutation paths first     |
| Repair and backfill of legacy records    | Pass the SQL-derived status when rewriting a legacy record  |

The existing rejection mirror does not satisfy this contract: its writes are
documented as cache-tier, `get_rejections_batch` has no production caller, and
`events_worker.py` upserts and deletes SQL rows without mirroring. The
unmirrored accept/reject path is an existing bug to fix first, independent of
this work.

The risk is a backward-record writer that forgets the byte, silently clearing
status. The surface is small (`write_edges`, `event_edges_delete`, the status
update); audit all three and test each.

## Migration and cutover

1. Fix the unmirrored accept/reject path. SQL stays authoritative.
2. Add the status byte to the backward record and dual-write every path above.
   Reads stay on SQL.
3. Backfill abnormal legacy events from SQL (`outlier`, rejected rows,
   `soft_failed`) with a resumable background update. A missing byte means
   unknown until the update completes.
4. Verify status against SQL for sampled records and for each transition:
   persist, de-outlier, purge's outlier flip, reject, re-reject, accept, purge
   deletion. Check transaction overlays and a read-only worker's published view.
5. Switch the two persist queries to the Rust snapshot read. Keep the SQL and
   version fallback until step 6 makes a forward miss authoritative.
6. Stage body and edges in one mtxdb transaction. Then remove the persist SQL
   joins and, only after the other edge readers are migrated, the SQL edge
   mirror, outbox and version gate.

Rollback: old writers omit the byte, so a record rewritten by an old writer
loses it. SQL remains authoritative during migration and backfill can repair it.
After cutover, require the new writer.

## Alternatives considered

- **Sibling STAT record in the Edges pool.** Same snapshot, but it adds a read
  to both queries (the backward record is already read), a collection to keep
  out of generation rebuilds, and an explicit delete on purge. Rejected because
  the payload byte gets the same consistency with no extra read.
- **Frame TLV on `event_meta_node_id`.** That record is in EventDag, so it
  cannot share an Edges snapshot. It would need the cross-pool snapshot
  capability, which mtxdb does not have (snapshots are scoped to one
  `PackfileStorage`).
- **Frame TLV on the backward-edge frame.** Shares the pool, but needs mtxdb
  changes (metadata on put, metadata on read, overlay, repack preservation) and
  does not remove a read, because the prev list must be decoded anyway. The
  payload byte is equivalent for these queries. Revisit only if a second
  consumer needs generic typed per-record metadata.
- **Copying status into parent forward lists.** De-outlier or rejection would
  rewrite every parent. Rejected.

## Measurements

This is a layout choice, not a measured speedup. Instrument the Rust call with
duration, candidate count, forward hits, backward records read, recursion levels
and `WouldBlock` retries. Compare with `persist_prevs_total` and
`persist_rejected_total` from the 470 s run (18.8 s and 11.9 s summed). The goal
on the post-cutover path is no SQL execution and no `runInteraction`. Count SQL
fallback use explicitly during migration.

## Open questions

- Every site that writes `soft_failed` into internal metadata.
- Which writer serialises status updates against edge writes and purge (the
  record-version CAS should be enough; confirm under concurrent persist).
- The crash window between the mtxdb and SQL commits: detection and repair.
- Whether any other reader decodes backward records and would choke on, or
  mishandle, the trailing byte.
- Which mtxdb revision Synapse is pinned to for this work. Cargo.lock is at
  `ce55563`, not `f41d02a`, and `dev` HEAD carries unfinished server-key
  commits.
