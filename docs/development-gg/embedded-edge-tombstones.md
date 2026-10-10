# Embedded edge tombstones and lazy forward filtering

## Problem

Embedded event edges currently maintain two related records:

```text
child event  -> parent events       (backward edge)
parent event -> child events        (forward adjacency list)
```

A purge can resolve a child’s parents directly through its backward edge, but
removing that child from every parent’s packed forward list requires a
read-modify-write for each affected parent. The backward lookup is already the
child-to-parent index; adding another reverse index would not remove this cost.

## Current design

`event_edges_delete` tombstones the purged event’s backward edge and leaves
parent forward lists unchanged. `event_edges_get_forward` treats those lists as
a lossy cache and validates each returned child against its backward edge:

- a non-empty backward record means the child is live;
- a present empty record is an explicit deletion tombstone and is filtered;
- a missing backward record is an incomplete/legacy mirror and must preserve the
  SQL-fallback path rather than being silently dropped.

If the forward result is incomplete because a child’s backward record is
missing, the read returns a miss so Synapse can query SQL. If all returned
children are tombstoned, the result is empty/missing according to the existing
fallback contract.

The purge path also avoids draining the entire namespace edge queue. Rows owned
by purged events are canceled under the namespace lock; unrelated rows remain
queued for the normal coalescer. This prevents a queued unrelated write from
resurrecting a deleted child while avoiding an O(queue-size) drain on every
purge.

## Tradeoffs

This moves work from delete time to forward-read time:

- deletes avoid reading and rewriting parent adjacency lists;
- forward reads perform batched backward-edge liveness checks;
- packed forward lists can grow stale and retain deleted child IDs;
- every write/delete and read must preserve the tombstone contract;
- recovery must distinguish explicit tombstones from absent legacy records.

The implementation uses existing Sithnapse Rust glue and mtxdb reads/writes; no
mtxdb-core change is required.

## Validation requirements

Before treating this as a permanent storage policy, measure and test:

- deletion latency with large parent lists;
- forward-read latency and batch sizes;
- deleting one child while retaining siblings;
- deleting all children;
- missing/legacy backward records and SQL fallback;
- concurrent puts and deletes;
- restart and recovery behavior;
- forward-list growth over time.

## Open work: compaction

Compaction is intentionally separate from the lazy-filter change. A future
compactor must periodically rewrite stale forward lists, remove tombstoned
children, and reclaim obsolete tombstone records. It needs defined rules for
batching, crash recovery, concurrent puts/deletes, tombstone retention, and
existing data migration.

Until that design exists, forward lists are a deliberately stale cache and must
never be consumed without backward-edge filtering.

## Status

The lazy tombstone/filtering change is implemented and compiles. Full-suite
validation and production-like latency measurements are required before claiming
a performance improvement. Compaction remains unimplemented.
