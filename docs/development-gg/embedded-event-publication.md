# Embedded event publication ordering

## Current behavior

When embedded event JSON is enabled, `event_json` is written exclusively to
mtxdb. Synapse does not populate SQL `event_json`, so SQL cannot provide a
fallback if a worker reads the event before the embedded record is visible.

The current short-term fix is in `events.py`: the event JSON batch calls
`put_event_json_batch(..., sync=True)`. This uses a narrow EVENT_DAG barrier
which does not drain the independently coalesced edge queue. It makes event JSON
visible before the persistence path can advertise the event through the
replication stream. The focused Complement worker read-after-write failure no
longer reproduces with this change, but the write still occurs before the SQL
transaction commits and can therefore leave an orphan on rollback. The exact
cross-process visibility guarantee of `mdb_sync` remains a separate mtxdb
contract to verify.

Synapse remains pinned to mtxdb commit
`e939711faf2ab15fa031449f3bad2ae37302c482e`. The fix uses the sync API already
exposed by that revision and does not require updating the dependency.

## Atomicity caveat

The mtxdb write and sync happen before the surrounding SQL transaction commits.
If the SQL transaction subsequently rolls back, an embedded event record may
remain without a corresponding SQL `events` row. It should normally be
unreachable by Synapse, but recovery, index rebuild, or diagnostic scans may
encounter the orphaned record. This is a consistency caveat, not a SQL fallback:
embedded-exclusive event JSON has no SQL copy to restore from.

The narrow barrier does not introduce this risk; it has the same pre-SQL-commit
write ordering as the earlier full `EVENT_DAG` barrier. The staged-batch
protocol below is what removes it.

## Performance caveat

The sync is per `event_json` persistence batch, not necessarily per event, but
small persistence batches can make it behave like an fsync per event. This may
be expensive on HDD-backed stores. The narrow event-JSON barrier intentionally
does not drain coalesced edge-write queues; full barriers still do, adding
namespace/queue lock and CPU cost. Measure batch sizes, sync latency, and
edge-drain cost before making this policy broader.

## Intended long-term contract

The durable design should have one publication barrier for the complete
`EVENT_DAG` batch, with an explicit staged/unpublished state:

1. Append all related pack records.
2. Stage the batch under a transaction/batch id; readers must not expose it.
3. Commit the SQL transaction.
4. Publish an mtxdb commit marker/LSN for that batch and make it readable by
   other handles.
5. Publish the Synapse replication stream token.
6. Checkpoint asynchronously.

A plain `txn.call_after` is not sufficient if replication can publish before
that callback runs. The mtxdb batch protocol must define who publishes the
commit marker after SQL success, how a failed SQL transaction abandons the
staged batch, and how abandoned batches are reclaimed during recovery or
compaction. The preferred implementation is committed pack-tail visibility with
batch framing/commit markers, avoiding a second payload WAL and avoiding an
unconditional fsync for every event.

This requires an mtxdb visibility spike before implementation: verify the
writer-owned flush/commit operation, the IPC or shared notification that
advertises a committed LSN to worker readers, and restart/recovery behavior when
that notification is lost. A miss-triggered request to flush through the
expected LSN is part of the recovery path, not merely an optional optimization.

## Follow-up validation

- Keep the mtxdb lock at `e939711` until a specific required core fix is
  identified and tested.
- Run worker read-after-write, rollback, crash/interruption, and recovery/index
  scan tests.
- Benchmark throughput and p95/p99 persistence latency on the target HDD.
- Verify that `mdb_sync` guarantees the cross-process read visibility required
  by `open_read_committed`, not only local durability.
- Use `event_dag_sync_requests`, `event_dag_sync_completed`,
  `event_dag_sync_errors`, and `event_dag_sync_duration` to measure actual
  native EVENT_DAG sync work. `event_dag_sync_coalesced` and
  `event_dag_sync_coalesced_errors` describe coalescer batches; the latter is a
  coarse subset of `event_dag_sync_errors`, not an independent error count.
  `event_dag_sync_duration` is the canonical named duration metric;
  `ffi_sync_event_dag` is the equivalent legacy FFI timing span.
