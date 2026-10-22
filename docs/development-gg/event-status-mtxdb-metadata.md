# Event status for mtxdb persist reads

Status: design note. The v1 edge-format reset is committed; per-event status
records, rebuild, and Rust persist reads are not implemented. See
`perf-track-b-family4-edges-map.md` and
`res/docs/2026-09-28-event-edges-sql-removal-plan.md`.

## Goal and ownership

The persist-path SQL reads need to filter events by outlier, rejection, and
soft-failed status. Those are properties of an event, not of an edge. Store them
in a separate per-event record in the Edges pool, keyed by event ID:

```text
status[event_id] = { status_bits, rejection_reason }
```

The exact byte/string encoding is an implementation detail to settle with the
binding. The record carries the three filter flags and the full rejection
reason. The backward-edge record remains pure adjacency; do not add status or
zero-edge records to it. This is a targeted status index, not a general event
metadata bag. Immutable provenance such as device ID stays in
`event_json.internal_metadata`.

The SQL sources remain authoritative during migration: `events.outlier`, the
presence and reason in `rejections`, and `event_json.internal_metadata` for
`soft_failed`. The mtxdb status record is a maintained read-optimized copy.

## Why status is separate from adjacency

The status record exists whether or not the event has a published backward edge.
This covers persist-time rejected events, no-prev events, and events whose
adjacency is later removed. It also avoids making a rejected or no-prev event
look like it has a normal backward-edge record.

The two persist reads already fetch adjacency. Fetch status records for the same
IDs in the same snapshot and batch operation. This is an additional key read,
but not a per-event SQL join or a separate network round trip. Keeping status
separate also means status updates do not rewrite immutable adjacency. Putting
the flags in event JSON is a valid alternative, but requires loading and
decoding event bodies on these paths and coordinating a second record with the
edge list.

## Relevant SQL behavior

`_get_events_which_are_prevs` starts from `event_edges` and filters its child
events by outlier, rejection, and soft-failed status.
`_get_prevs_before_rejected` also starts from `event_edges`; it returns prev IDs
of rejected or soft-failed rows and recurses through those rows.

Ordinary persist-time rejected events are removed by
`_store_rejected_events_txn` before `_update_metadata_tables_txn` calls
`_handle_mult_prev_events`, so they do not get `event_edges` rows. In
`_get_prevs_before_rejected`, they therefore cannot be selected as a current
event and their prev list is not traversed. In `_get_events_which_are_prevs`,
the status filter applies to the child that references a candidate; a
persist-time rejected event can still be returned as a candidate prev ID if an
eligible child references it. The mtxdb read must preserve these distinct SQL
semantics: a status record alone does not create adjacency or make an event's
own prev list traversable. Events that gained edges earlier and were rejected
later can still appear in the backward traversal, as can de-outliered events.

The de-outlier path was checked in `events.py`: it removes those events before
`_store_event_txn`, then explicitly calls `_handle_mult_prev_events` for them at
the de-outlier call site. Thus de-outliered events do get adjacency. This check
covered the relevant call sites in `events.py`; it was not a repository- wide
audit of every edge writer.

## Read path

Use one Edges-pool `read_snapshot()` for each operation.

1. `_get_events_which_are_prevs`: batch-read forward children for the
   candidates. Preserve the existing tombstone/existence filtering by checking
   their backward records, then batch-read status records for the surviving
   children in the same snapshot. Drop outlier, rejected, and soft-failed
   children as the SQL query does.
2. `_get_prevs_before_rejected`: for each traversal frontier, batch-read
   backward adjacency and status records for the same event IDs. Only events
   with published adjacency participate, matching the SQL starting table. Use
   the flags to decide whether to add their prev IDs and recurse. Preserve the
   current visited/deduplication behavior.

Do not infer status from a missing edge record. Do not synthesize an edge for a
status-only event. Once the one-shot rebuild is complete, a missing status
record for an event with SQL-backed status is corruption/incomplete migration,
not normal status; fail the read and do not fall back to SQL.

Suggested binding surface (layout-agnostic):

```text
event_status_get_batch(room_id, event_ids) -> event_id -> status
event_status_put_batch(room_id, rows, transaction=...) -> ()
event_status_delete_batch(room_id, event_ids, transaction=...) -> ()
```

The batch reader should read adjacency and status under one snapshot. Whether
that is one FFI call or coordinated batched reads is an implementation choice;
avoid a per-ID call from Python.

## Mutation and rebuild requirements

Every event persisted to the event store gets a status record, including
persist-time rejected events that do not get adjacency. Initial accepted-event
status is written with its edge changes; rejected-event status is still written
without fabricating an edge. De-outliering updates status and writes adjacency
through its explicit `_handle_mult_prev_events` path.

| Path                                     | Status mutation                                  |
| ---------------------------------------- | ------------------------------------------------ |
| Event persistence                        | Write initial flags and rejection reason         |
| `_update_outliers_txn` / de-outliering   | Set or clear outlier                             |
| Purge's retained-event outlier update    | Set outlier                                      |
| `_store_rejections_txn`                  | Set rejection and full reason                    |
| `events_worker.py` accept/reject recheck | Set or clear rejection and reason                |
| Rejection purge paths                    | Clear rejection and reason                       |
| Soft-fail state writes                   | Set/clear soft-failed; audit every mutation path |
| Event purge                              | Delete status with the event's records           |

Status updates use a version-bound read-modify-write of the status record, not
the backward-edge payload. Bind the write to the version read and retry on
`StaleRead`. Purge must win over a racing status update: a deleted status key
must not be recreated by a stale update. Where possible, stage status changes
with event body and edge changes in one mtxdb transaction.

The one-shot rebuild runs while persist reads still use SQL. Rebuild status for
all events from the SQL status sources, including events with no `event_edges`
row; rebuild adjacency only from the existing edge source. Make it resumable and
record completion. A missing status value, invalid encoding, or mismatch fails
the rebuild. Do not interpret missing status as normal and do not add a runtime
SQL fallback. Verify sampled rebuilt records against SQL before enabling the
read.

## Atomicity and cutover

mtxdb supports transactions across its pools, so an event body in EventDag,
adjacency and status in Edges can be committed together. SQL and mtxdb still
cannot commit atomically with each other. During migration, SQL remains
authoritative and both stores are updated. Before mtxdb becomes authoritative,
define how a crash between those commits is detected and repaired; a manual
reconciliation tool may be sufficient, but the failure mode must be explicit.

1. Add per-event status records and dual-write every status mutation; keep reads
   on SQL.
2. Rebuild status for every event and adjacency from the authoritative edge
   source. Verify, then mark completion.
3. Exercise persist-time rejection, later reject/accept, de-outlier, purge,
   soft-fail/un-soft-fail, and a status update racing purge. Verify that
   persist-time rejected events have status but no adjacency.
4. Switch both persist reads to the Rust snapshot path only after rebuild
   completion and the edge cutover gate. Do not add a runtime SQL fallback.
5. After all other edge readers have moved, retire the SQL edge mirror, outbox,
   and version gate.

## Open checks before implementation

- Audit every status mutation, especially soft-fail/un-soft-fail and the
  accept/reject recheck.
- Audit all edge writers and purge paths to ensure status-only events never
  acquire synthetic adjacency and purge deletes both records.
- Confirm the event body, adjacency, and status writes can share one
  `MtxdbTransaction` at each call site.
- Define the rebuild source and completion marker. The rebuild must include
  status-only events and must not infer status from adjacency presence.
- Define how full rejection reasons are represented and kept synchronized.
- Define recovery for the SQL/mtxdb crash window before cutover.

## Measurement

Instrument each Rust persist read with duration, candidate count, adjacency
hits, status records read, recursion levels, and snapshot retries. Compare with
`persist_prevs_total` and `persist_rejected_total`; the desired post-cutover
path has no SQL execution or `runInteraction`. Measure the extra batched status
read separately from adjacency work.

The status index is a design hypothesis, not a measured speedup. In the
storage-only slice, the two persist queries were about 1.4% of run wall time;
the larger test-time targets remain elsewhere.
