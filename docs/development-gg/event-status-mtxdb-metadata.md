# Event status for mtxdb edge reads

Status: design note. The v1 edge format reset is committed; the status byte,
rebuild and Rust persist reads are not implemented. This describes the mutable
event state needed before the family-4 persist queries can read from mtxdb
without SQL. See `perf-track-b-family4-edges-map.md` and
`res/docs/2026-09-28-event-edges-sql-removal-plan.md`.

## Goal

Replace the persist-path SQL joins with Rust reads over mtxdb's edge records and
the status of the events they reference. The required status is small:

- `outlier` from `events.outlier`;
- whether a rejection row exists;
- `soft_failed` from event internal metadata.

The first two are mutable SQL state and are not yet completely represented in
mtxdb. The rejection mirror is also not authoritative today.

## Storage: a status byte in the backward-edge payload

The backward-edge record is already the right home. It is Synapse-owned data,
stored in the Edges pool's stable room-scoped `PREV` collection, keyed by the
event's own ID. Its format is:

```text
[format_version: u8][self_identity][edge_count: u16]
{[is_state: u8][parent_identity]}*[status: u8]
```

Reset the backward and forward edge payload formats to version 1. The backward
record's status byte is mandatory. There are no releases, so there is no
compatibility contract to preserve; pre-reset records are unsupported rather
than decoded as “status unknown”.

Existing local mtxdb edge data is rebuilt, not migrated in place: the v1 decoder
accepts only v1, and the persist read is not enabled until the one-shot rebuild
completes.

Status bits:

- `0x01` outlier
- `0x02` rejected
- `0x04` soft-failed

Reserve the remaining bits. The status byte is a Synapse schema detail; it is
not the full rejection reason, which stays in the rejection mirror.

This location fits both queries without an extra lookup:

- For `_get_events_which_are_prevs`, the forward read returns children, and the
  read must fetch every child's backward record anyway to drop purged
  tombstones. `event_edges_get_forward` already performs exactly that batched
  read (`embedded_edges.rs:1073-1110`). The status arrives in a record already
  paid for.
- For `_get_prevs_before_rejected`, the traversal walks backwards. Each level
  needs the current event's prev list and its own status, which are in the same
  backward record. One record per visited event, not a record plus a second
  status lookup.

Do not copy mutable status into parent forward lists: de-outliering would
rewrite every parent. Do not add a separate `STAT` record now: it would add one
read on both paths for no snapshot or atomicity gain. Do not use a frame
header/TLV: it requires a new mtxdb write, overlay and header-read surface, and
it does not remove a read because the payload is already decompressed for the
prev list.

## Version and rebuild

New writers always write format 1, including a `0` status byte for normal. There
is no compatibility path for pre-reset records: the decoder accepts only v1.

The rebuild is a one-shot pass that runs while the persist queries still read
from SQL. A pre-reset record, a missing status byte, or any version/decode error
is a rebuild failure; do not interpret it as normal or unknown, and do not add a
runtime SQL fallback. The rebuild rewrites every forward and backward edge
record from SQL (or from event bodies if the SQL mirror is gone). When it
completes, every edge record is v1 and the persist read is enabled.

An old writer would emit the pre-reset layout, which the new decoder rejects.
After cutover, require the new writer.

## Read path

1. Capture one `read_snapshot()` on the Edges pool.
2. Batch-read the candidates' forward lists in the room collection.
3. For the returned children, batch-read their backward records in the same
   snapshot and decode each status. This read is required for tombstone
   filtering regardless of status.
4. Drop children that are outliers, rejected, or soft-failed, and return the
   qualifying parent IDs with diagnostic counts.

The existing gated forward getter does not do the backward filter; the new Rust
persist read must include it. Do not add a second status collection or a second
round trip.

For `_get_prevs_before_rejected`, traverse levels in Rust with a visited set.
Read each frontier event's backward record once; use its status and prev list
together. Do not call Python per level. If publication advances or a writer
restarts and a snapshot returns `WouldBlock`, retry a bounded number of times
inside Rust. After bounded retries, fail the read; never interpret an incomplete
snapshot as "no edges", and do not fall back to SQL.

The first query's saved workload had only 12 SQL result rows for 20,579
candidates; a later storage slice returned 8 rows for 5,102 candidates. Almost
every call ends after the forward lookup. The rejected traversal can visit more
records, and its backward records carry status for free; measure it separately.

Keep the binding API layout-agnostic:

```text
event_status_get_batch(room_id, event_ids) -> event_id -> status
event_status_put_batch(room_id, rows, transaction=...) -> ()
event_status_delete_batch(room_id, event_ids, transaction=...) -> ()
```

A missing row means there is no backward record, not that the status is unknown.
That is only valid for a normal event with no prevs; an abnormal event with no
prevs gets a zero-edge record. Python callers must not depend on the byte's
position.

## Mutation rules

Status changes rewrite the backward record. That is safe because the prev list
is immutable, but it must not clobber concurrent edge or purge writes.

| Path                                     | Change                                                             |
| ---------------------------------------- | ------------------------------------------------------------------ |
| Event persistence                        | Write the initial status byte in the same put as the backward edge |
| `_update_outliers_txn`                   | Set or clear outlier                                               |
| Purge's retained-event outlier update    | Set outlier                                                        |
| `_store_rejections_txn`                  | Set rejected                                                       |
| `events_worker.py` accept/reject recheck | Set or clear rejected; mirrored after commit                       |
| Both rejection purge paths               | Clear or delete rejected status                                    |
| Soft-fail state writes                   | Set/clear soft-failed; audit every mutation path                   |
| Event purge (`event_edges_delete`)       | Tombstone the backward record; the empty payload clears status     |

Serialization:

- A status update is a version-bound read-modify-write of the backward record:
  read it with its write version, replace the trailing status byte, re-encode
  the same prev list, stage `expect_record_version`, and write in one
  transaction. Retry on `StaleRead`.
- `event_edges_put` writes status with the initial backward put. A rebuild or
  repair rewrite of an existing backward record must carry the derived status
  and bind to the record version, so it cannot erase a newer status.
- Purge wins over a racing status update. If the record is empty (tombstoned), a
  status update must no-op rather than append a byte to the tombstone.
- An event with no prevs has no backward record. If such an event is abnormal,
  create a zero-edge backward record to carry its status; a normal creator needs
  none.
- The status byte covers only the three filters. Full rejection reasons stay in
  the rejection mirror and are not part of this read path.

## Atomic writes: what mtxdb already supports

The body-plus-edges write does **not** require moving edge data into EventDag or
adding a cross-pool transaction feature. The mtxdb `Database` documents atomic
cross-pool transactions; `DatabaseTransaction::put` accepts a `ShardType`, and
Synapse's transaction stages writes into a shared journal group. Event JSON
stages into EventDag, edges into Edges. Use one transaction to commit body and
edges together. The status byte rides in the backward-edge put, so it commits
with the edges.

This is atomicity within mtxdb. SQL and mtxdb still cannot commit atomically
with each other. During migration SQL remains authoritative and both stores are
updated. The cutover needs an explicit recovery/reconciliation policy for the
crash window between the mtxdb commit and the SQL commit; cross-pool atomicity
does not close that cross-database gap.

## Migration and cutover

1. Add the status byte and dual-write every mutation path above; keep reads on
   SQL.
2. Rebuild all forward and backward edge records from SQL (`event_edges`, or
   `prev_events` from event bodies if the SQL mirror is already gone), writing
   v1 records with the derived status. Make the rebuild resumable and mark
   completion. A pre-reset record, a missing status byte, or any decode error is
   a rebuild failure, not a SQL fallback.
3. Verify against SQL for sampled records and exercise persist, de-outlier,
   purge's outlier flip, reject, re-reject, accept, soft-fail/un-soft-fail, and
   purge deletion. Check transaction overlays, the read-only worker's published
   view, and a status update racing an edge rewrite and a purge.
4. Once the rebuild is complete and the edge cutover makes a forward miss
   authoritative, switch the two persist queries to the Rust snapshot read in
   one step. Do not add a per-read SQL fallback.
5. Stage body and edges in one cross-pool transaction. Then remove the persist
   SQL joins and, only after all other edge readers are migrated, retire the SQL
   edge mirror, outbox, and version gate.

The SQL and mtxdb commits remain separate systems throughout. Before declaring
mtxdb authoritative, define how a crash between those commits is detected and
repaired; a manual reconciliation tool may be sufficient, but the failure mode
must be explicit.

## Open checks before implementation

- Audit every backward-record writer: `write_edges`, `event_edges_delete`, and
  the new status read-modify-write. No other code path may rewrite the payload.
- Audit every status mutation call site, especially soft-fail/un-soft-fail and
  the accept/reject recheck.
- Confirm no consumer outside `embedded_edges.rs` parses the backward payload
  strictly; the decoder accepts only v1, and pre-reset records are rebuild
  failures.
- Confirm the body-plus-edges call path can share one `MtxdbTransaction`; the
  current edge write is post-SQL-commit.
- Confirm the tombstone/status race resolves to purge-wins.
- Define the rebuild source and completion marker: SQL `event_edges` while it is
  authoritative, else `prev_events` from event bodies. The rebuild must be
  resumable, rewrite both forward and backward records as v1, and enable the
  read only after a full pass.

## Measurements

Instrument the Rust call with duration, candidate count, forward hits, backward
and status records read, recursion levels, and `WouldBlock` retries. Compare
these with `persist_prevs_total` and `persist_rejected_total`; the desired
result is no SQL execution or `runInteraction` on the successful post-cutover
path. Count decode failures and missing records; there is no fallback path to
count.

The layout removes a separate status lookup, but it is not yet a measured
speedup. The persist slice is about 1.4-1.6% of wall time even if the reads were
free; the larger targets remain elsewhere.
