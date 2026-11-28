# Event range-index design for the events cutover

Status: investigation and design proposal. This document makes no performance
claims and does not authorize an implementation or migration.

This note is a prerequisite design for the event-table cutover, not a separate
phase of the client-facing fan-out plan. It connects that cutover to mtxdb's
ordered-index capability. The pinned mtxdb version provides a persistent
`TimelineIndex`, but Synapse does not currently expose or use it. First
determine whether that index can serve Synapse's required timeline queries. A
general secondary-index engine is only justified for query shapes the existing
primitive cannot support.

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
- Does publishing an updated root rewrite a manifest proportional to the number
  of levels or runs? Measure append latency at 10k, 100k, and 1M events in one
  room, as well as import throughput and recovery time. A small-room prototype
  is not sufficient to validate the publication path.

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
total order and a resumable cursor. Prefer event identity as the index target,
not a content hash for the stored body. Event IDs are stable across redaction
for room versions whose IDs are derived from the redacted event; older room
versions need their existing event-ID semantics preserved. The mtxdb record
identity must therefore derive from the event ID, not from the current body
bytes. This makes the logical event index independent of replacement body
frames. It does not settle whether bodies are deduplicated underneath: if
multiple events can share one body object, purge needs reference tracking or
another safe ownership rule.

Specify how updates that change a sort value remove the old association. If
index entries are derived from record fields, version the extractor/index
definition: changing its encoding or extraction rule requires a rebuild, since
old and new deltas may not cancel correctly.

Do not assume the current `LossyIndex` is an appropriate stable locator until
its identity, overwrite, deletion, and visibility semantics are verified. If a
logical object key survives redaction while the physical frame changes, the
transaction must publish the new locator and record atomically with the visible
index root. Reads must detect a stale index-to-record association; whether this
needs a version or fingerprint is an open implementation question.

## Candidate read path

A range scan can separate logical selection from physical reads:

1. Pin a consistent index snapshot and merge its relevant runs/cursors in
   requested logical order.
2. Apply visibility and deletion rules, including the committed
   `persisted_up_to` boundary, selecting the first `K` visible entries.
3. Resolve those entries to physical locators.
4. Sort only the selected locators by pack and offset, then coalesce reads under
   explicit maximum gap and window sizes.
5. Decode and validate records, then restore logical result order.

Packs should be shared across small rooms, with a slot directory ordered by
`(room_id, topological_ordering, stream_ordering)`. A large room may span many
packs, and later backfill can make their per-room ranges overlap; the query then
merges the relevant pack directories. Locator resolution is still needed only
for changed/overridden entries if slot directories carry direct record
locations. Outliers can be excluded from sealed timeline packs because they do
not appear in `/messages`, but de-outliering must re-place the event through an
override or repack path.

The slot directory may carry compact flags and type tags so timeline filters can
skip bodies before reading them. `lazy_load_members` affects membership events
in the response's `state` section; it does not filter `m.room.member` events out
of the timeline `chunk`. Timeline filters such as `types` and `not_types` do
affect the chunk and are the relevant case for slot-level type tags.

For an overridden slot, the directory's `RecordId` can resolve whether the event
was purged or moved to a redacted frame. A bitset alone cannot identify the
replacement location. Measure cold primary-directory page faults as well as
logical locator probes; when that directory exceeds RAM, `P_loc` includes real
storage reads.

This can reduce random blob reads when selected records are physically close,
while retaining early `LIMIT K` termination. It is not guaranteed to help every
query: scattered locators may still require many reads, and coalescing can read
unused bytes. Benchmark both index work and blob bytes/read count.

Cursor lifetimes, borrowed keys across heap mutation, error handling,
cancellation, the `persisted_up_to` boundary, and snapshot pinning need
compileable interfaces and tests before an implementation is accepted. With
prefix-compressed blocks, decoded keys must remain valid as cursors advance;
heap operations cannot invalidate borrowed key storage.

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
ordering. A purged-room filter can avoid locator probes, but it does not avoid
popping and merging the dead entries. A scan crossing `D_window` dead entries
still pays for those entries and their index blocks, in addition to the `K`
visible results. Define the index key layout and purge semantics together. Lazy
validation against the primary record map is an option, but its cost under large
purges must be measured; it cannot be described as constant-time purge work.

## Storage alternatives

| Option                         | Appropriate when                                              | Main question                                                                                                                  |
| ------------------------------ | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Existing `TimelineIndex`       | Its API covers the required event orderings                   | Can Synapse integrate it with the needed transactional and cursor semantics?                                                   |
| General key-only ordered index | Multiple secondary orderings are required and unsupported     | Can the existing sorted-run design (fences, Bloom filters, and footer) be reused? What are its consistency and recovery costs? |
| Per-pack sidecar               | Sort order correlates with immutable pack layout              | Does that correlation hold for the target query and survive mutation?                                                          |
| External relational index      | Another database intentionally owns visibility and predicates | Does retaining that dependency meet the cutover objective?                                                                     |

A per-pack sidecar can require probing many packs when logical sort order and
pack order are uncorrelated. A key-only sorted-run index could avoid moving blob
payloads during compaction, but requires a defined manifest, transaction
boundary, recovery protocol, tombstone handling, and compaction policy. Prefer
journaled manifest deltas over rewriting a manifest proportional to all runs if
measurement confirms that publish cost. Evaluate trivial moves for monotone
append ranges before adding merge work. These are alternatives to evaluate, not
established properties of the pinned library. Reconcile this path with the
existing `FRAME-DISPOSITION-AND-SORTED-RUNS` design (fences, Bloom filters, and
footer) before specifying another run format.

## Measurement plan

Before choosing an implementation:

1. Map each ordered query in `events-cutover.md` to the `TimelineIndex` API and
   record supported, unsupported, and semantically uncertain cases.
2. Prototype representative per-room stream, per-room topological, and global
   cursor scans, including negative backfill and equal-key pagination.
3. Measure cold and warm scans across several limits and result distributions:
   clustered records, scattered records, backfill outliers, and purged entries.
4. For a representative room export, measure the number of maximal contiguous
   body-read runs for pages of 50 events under append order, exact
   `(topological_ordering, stream_ordering)` order, and local topological rank.
   Run the measurement for both `/messages` and stream-order reads. This tests
   whether pack layout, rather than a different index, is the dominant cost.
5. Record wall time, index blocks read, locator probes, pack reads, bytes read
   versus bytes returned, primary-index page faults, memory, write
   amplification, and compaction cost. Report `P_loc` separately from body
   reads.
6. Measure append and import latency at 10k, 100k, and 1M events per room,
   including root/manifest publication cost.
7. Compare against the current SQL indexes on the same data and query shapes.
8. Exercise concurrent writes, refresh/reopen, interrupted publication,
   redaction, de-outliering, and purge while readers hold cursors.

Claims such as a fixed number of block reads per level, a particular HDD seek
latency, a speedup factor, or a fixed memory cost are hypotheses until these
measurements exist. Range scans can cross multiple blocks per run, and merged
runs can interleave; do not infer a one-block-per-level bound without proving
the exact cursor and run layout. The seek/transfer break-even gap also depends
on device, queue depth, concurrent load, and read-window caps; benchmark a range
of coalescing policies instead of hard-coding a single HDD estimate.

## Decision gate

Prefer integrating `TimelineIndex` if it meets the required ordering and
snapshot semantics at realistic room sizes. If it does not, compare a
generalized sorted-run index with clustering selected records in pack order for
the dominant read path. A per-room pack laid out exactly in
`(topological_ordering, stream_ordering)` order may make `/messages` a direct
contiguous scan, but this is only useful if measurements show that it reduces
body-read runs without harming stream-order reads, forks, or write/repack cost.
Keep the event-table cutover plan as the source of truth for Synapse query
semantics; this note is the design and evaluation framework for its ordered
index layer.
