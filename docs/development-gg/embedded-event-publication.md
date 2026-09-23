# Embedded event publication ordering

## Current behavior

When embedded event JSON is enabled, `event_json` is written exclusively to
mtxdb. Synapse does not populate SQL `event_json`, so SQL cannot provide a
fallback if a worker reads the event before the embedded record is visible.

The current short-term fix is in `events.py`: the event JSON batch calls
`put_event_json_batch(..., sync=True)`. This synchronously syncs the `EVENT_DAG`
pool before the persistence path can advertise the event through the replication
stream. The focused Complement worker read-after-write failure no longer
reproduces with this change. The exact cross-process visibility guarantee of
`mdb_sync` remains a separate mtxdb contract to verify.

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

## Performance caveat

The sync is per `event_json` persistence batch, not necessarily per event, but
small persistence batches can make it behave like an fsync per event. This may
be expensive on HDD-backed stores. Measure batch sizes and sync latency before
making this policy broader.

## Intended long-term contract

The durable design should have one publication barrier for the complete
`EVENT_DAG` batch:

1. Append all related pack records.
2. Commit the mtxdb batch and make its committed tail readable by other handles.
3. Commit the SQL transaction.
4. Publish the Synapse replication stream token.
5. Checkpoint asynchronously.

A plain `txn.call_after` is not sufficient if replication can publish before
that callback runs. The final barrier must be explicitly post-SQL-commit and
pre-replication-publication, and must cover all related EVENT_DAG records—not
only event JSON. The preferred mtxdb implementation is committed pack-tail
visibility with batch framing/commit markers, avoiding a second payload WAL and
avoiding an unconditional fsync for every event.

## Follow-up validation

- Keep the mtxdb lock at `e939711` until a specific required core fix is
  identified and tested.
- Run worker read-after-write, rollback, crash/interruption, and recovery/index
  scan tests.
- Benchmark throughput and p95/p99 persistence latency on the target HDD.
- Verify that `mdb_sync` guarantees the cross-process read visibility required
  by `open_read_committed`, not only local durability.
- Use `event_dag_sync_requests`, `event_dag_sync_completed`,
  `event_dag_sync_errors`, `event_dag_sync_duration`, and the coalescer success
  and error counters to separate actual sync work from no-op calls.
