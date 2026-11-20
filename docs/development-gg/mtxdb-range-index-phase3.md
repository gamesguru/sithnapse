# Phase 3 design: range indexes over mtxdb

Status: investigation and design proposal. This document makes no performance
claims and does not authorize an implementation or migration.

This note connects the event-table cutover to mtxdb's ordered-index capability.
The pinned mtxdb version provides a persistent `TimelineIndex`, but Synapse does
not currently expose or use it. First determine whether that index can serve
Synapse's required timeline queries. A general secondary-index engine is only
justified for query shapes the existing primitive cannot support.

Related documents:

- `docs/development-gg/events-cutover.md`
- `docs/development-gg/mtxdb-performance-master-plan.md`
- `res/docs/2026-09-30-ordered-partitioned-append-stream.md`

## Design objective

Support ordered range scans over logical keys while keeping index order
independent of the physical locations of event records in packs. The index
should identify records by stable logical identity; a separate locator resolves
that identity to the current record location. Repacking must not require
rewriting logical index entries.

The correctness target is one coherent view of an index root, event metadata,
and record locations. The proposal must reuse mtxdb's actual transaction,
snapshot, and recovery guarantees rather than assume that an index WAL or
individually published roots provide atomicity.

## Capability boundary

The pinned library has an ordered `TimelineIndex` API. The immediate gap for
Synapse is integration and validation of its scope, ordering, cursor, and
mutation semantics. Do not describe mtxdb as lacking range indexes, and do not
build a second generic index before comparing the API with the event access
inventory in `events-cutover.md`.

Questions to resolve against the pinned API and implementation:

- Does it support the required per-room and global index scopes?
- Can it represent negative backfill positions and the required composite
  orderings?
- Are cursors stable across concurrent commits, refresh, and repack?
- What mutation, deletion, and range-delete operations are available?
- Can one transaction publish event records, all relevant index entries, and the
  root visible to readers?
- What are its on-disk layout, memory use, write amplification, and cold-scan
  behavior?

If it covers the access patterns, adapt it. If a measured query shape remains
unsupported, specify a general secondary index for that shape rather than
assuming every caller needs a new index family.

## Logical keys and physical locations

Keep logical ordering separate from pack coordinates:

```text
(IndexId, SortBytes) -> stable ObjectKey -> current physical locator
```

Unsigned lexicographic byte ordering is a plausible encoding contract. Fixed
width unsigned integers can use big-endian encoding. Signed integers can use
big-endian two's-complement with the sign bit flipped. Composite fields need
unambiguous encodings; variable-width strings require escaping or length
encoding, and hashed identifiers need collision handling. Prefix scans also need
a precise upper-bound rule.

For non-unique sort values, include a stable unique suffix so pagination has a
total order and a resumable cursor. Specify whether the suffix is an object key
or an independent sequence, and how updates that change a sort value remove the
old association.

Do not assume the current `LossyIndex` is an appropriate stable locator until
its identity, overwrite, deletion, and visibility semantics are verified. If the
logical object key survives redaction while the physical frame changes, the
transaction must publish the new locator and record atomically with the visible
index root. Reads must detect a stale index-to-record association; whether this
needs a version or fingerprint is an open implementation question.

## Candidate read path

A range scan can separate logical selection from physical reads:

1. Pin a consistent index snapshot and merge its relevant runs/cursors in
   requested logical order.
2. Apply visibility and deletion rules, selecting the first `K` live entries.
3. Resolve those entries to physical locators.
4. Sort only the selected locators by pack and offset, then coalesce reads under
   explicit maximum gap and window sizes.
5. Decode and validate records, then restore logical result order.

This can reduce random blob reads when selected records are physically close,
while retaining early `LIMIT K` termination. It is not guaranteed to help every
query: scattered locators may still require many reads, and coalescing can read
unused bytes. Benchmark both index work and blob bytes/read count.

The pseudo-code in the proposal is illustrative, not an implementation
specification. In particular, cursor lifetimes, borrowed keys across heap
mutation, error handling, cancellation, and snapshot pinning need compileable
interfaces and tests before the algorithm is accepted.

## Mutation, deletion, and purge

The index and record/locator updates must share the same atomic visibility
boundary. Define behavior for:

- insertion, out-of-order backfill, and multi-writer sequence allocation;
- redaction or overwrite that changes a record's physical frame;
- changing or removing a secondary sort key;
- event and room purge, including readers holding older snapshots;
- rebuild and recovery after an interrupted migration or compaction.

Range tombstones may efficiently skip a contiguous logical prefix, such as a
room-local index whose keys begin with a room identifier. They do not make a
global stream's per-room purge contiguous when the key begins with stream
ordering. Define the index key layout and purge semantics together. Lazy
validation against the primary record map is an option, but its cost under large
purges must be measured; it cannot be described as constant-time purge work.

## Storage alternatives

| Option                         | Appropriate when                                              | Main question                                                                |
| ------------------------------ | ------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Existing `TimelineIndex`       | Its API covers the required event orderings                   | Can Synapse integrate it with the needed transactional and cursor semantics? |
| General key-only ordered index | Multiple secondary orderings are required and unsupported     | What is its consistency, compaction, and recovery design?                    |
| Per-pack sidecar               | Sort order correlates with immutable pack layout              | Does that correlation hold for the target query and survive mutation?        |
| External relational index      | Another database intentionally owns visibility and predicates | Does retaining that dependency meet the cutover objective?                   |

A per-pack sidecar can require probing many packs when logical sort order and
pack order are uncorrelated. A key-only LSM could avoid moving blob payloads
during compaction, but requires a defined manifest, transaction boundary,
recovery protocol, tombstone handling, and compaction policy. These are
alternatives to evaluate, not established properties of the pinned library.

## Measurement plan

Before choosing an implementation:

1. Map each ordered query in `events-cutover.md` to the `TimelineIndex` API and
   record supported, unsupported, and semantically uncertain cases.
2. Prototype representative per-room stream, per-room topological, and global
   cursor scans, including negative backfill and equal-key pagination.
3. Measure cold and warm scans across several limits and result distributions:
   clustered records, scattered records, backfill outliers, and purged entries.
4. Record wall time, index blocks read, locator probes, pack reads, bytes read
   versus bytes returned, memory, write amplification, and compaction cost.
5. Compare against the current SQL indexes on the same data and query shapes.
6. Exercise concurrent writes, refresh/reopen, interrupted publication,
   redaction, and purge while readers hold cursors.

Claims such as a fixed number of block reads per level, a particular HDD seek
latency, a speedup factor, or a fixed memory cost are hypotheses until these
measurements exist. Range scans can cross multiple blocks per run, and merged
runs can interleave; do not infer a one-block-per-level bound without proving
the exact cursor and run layout.

## Decision gate

Prefer integrating `TimelineIndex` if it meets the required ordering and
snapshot semantics. Add a general secondary-index primitive only for access
patterns that the existing API cannot serve and that measurements show matter.
Keep the event-table cutover plan as the source of truth for Synapse query
semantics; this note is the design and evaluation framework for its ordered
index layer.
