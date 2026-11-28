# Synapse cutover map for mtxdb

Status: adapter-side inventory and measurement plan. The reusable mtxdb
primitives, format proposals, and cross-engine roadmap live in the mtxdb
repository at `docs/docs/2026-11-19-ordered-workloads-roadmap.md`. This file
records Synapse-specific query semantics and current integration status so the
two roadmaps do not duplicate the engine design.

## Objective

Move as much suitable Synapse storage and query work as possible into mtxdb. For
each query family, preserve Matrix ordering, visibility, pagination, backfill,
purge, and worker semantics; measure actual work; then remove the retired SQL
path after mtxdb is authoritative. Separate algorithmic gains from I/O and
constant-factor gains.

## Synapse query families

| Family                          | Current path in this fork                                                                                                                                                                                                   | mtxdb integration target                                                                                                                                              | Expected gain to measure                                                                                                                                              |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Event identity and metadata     | SQL `events` point/batch reads; event JSON is already in mtxdb                                                                                                                                                              | RecordId-keyed metadata and batched point reads                                                                                                                       | Fewer SQL round trips; expected O(1) identity lookup does not beat a B-tree asymptotically.                                                                           |
| Room/global ordered timelines   | SQL B-tree ranges over stream and `(room, topological, stream)`; pinned `TimelineIndex` is not wired into Synapse                                                                                                           | Integrate the pinned timeline API; preserve stream tokens, negative backfill, outlier/rejection visibility, and `persisted_up_to`                                     | Same O(log N + k) range bound as SQL; measure publish cost, cold index reads, body seeks, and bytes read.                                                             |
| Auth-chain closure/difference   | With mtxdb enabled, room-scoped direct auth edges and process-local cached ancestor bitmaps; cold misses traverse the graph. SQL-only mode uses legacy SQL traversal. The SQL chain-cover tables were retired in this fork. | Measure closure cache hit/miss, closure size, state-res set count, cache memory, and invalidation/purge behavior before selecting a maintained closure representation | Potential output-sensitive difference only if compact labels/closures let shared prefixes be skipped; current cold traversal remains proportional to visited closure. |
| State groups and state-at-event | SQL state-group identity/delta metadata with mtxdb HAMT roots/nodes mirrored by existing code                                                                                                                               | Move state identity, roots, deltas, reads, and purge lifecycle under a coherent mtxdb authority                                                                       | Point lookups may avoid SQL delta-chain work; structural-sharing diff gains depend on an implemented root-to-root diff and stable node identity.                      |
| Event prev-DAG and extremities  | SQL event metadata/extremity tables plus mtxdb event-DAG edges                                                                                                                                                              | Move graph queries and required metadata together; retain room scoping and support backfill                                                                           | Adjacency-only traversal is proportional to visited vertices/edges; backfill can add ancestors and may change descendants' derived labels.                            |
| Event disposition predicates    | SQL outlier/rejection/redaction flags and joins                                                                                                                                                                             | Room-scoped flag bitmaps over stable event ordinals                                                                                                                   | Bitmap algebra avoids body/row scans for combined predicates; measure density, range restrictions, and update cost.                                                   |
| Sync/state-delta streams        | SQL current-state delta and stream-position tables                                                                                                                                                                          | Evaluate only after timeline/state authority and worker visibility contracts are defined                                                                              | Likely fan-out and scheduling reduction; ordering semantics make this a separate stream migration.                                                                    |

This table is a starting map, not a complete access inventory.
`events-cutover.md`, `auth-chain-cutover.md`, `state-groups-out-of-sql.md`, and
the state delta workstream remain the detailed Synapse references.

## Auth-DAG assumptions to validate

Auth events are expected to be available before a referencing event is accepted
and persisted, including outliers. If this holds for all supported ingest and
repair paths, auth-DAG insertion is append-only with respect to reachability:
adding a new node does not change reachability among existing nodes. This is a
useful property for immutable labels, but it is not yet a proof that a
chain-label representation is efficient or correct for this fork's workloads.

Before adopting per-chain prefix labels, establish:

- whether every supported event-persistence path satisfies the parent-before-
  child invariant;
- how a new label is formed from parents and how many labels are touched;
- sparse-label storage and chain-count growth on large rooms;
- whether auth-chain difference is exactly representable as per-chain prefix
  union/intersection and output intervals for the selected chain decomposition;
- behavior for missing, rejected, partially fetched, and purged events.

A chain-cover implementation could reduce repeated common-prefix work compared
with Conduwuit's full closure-set materialization. Synapse's historical SQL
chain-cover implementation had the same high-level bound, so this would not be a
general asymptotic win over Synapse unless the current mtxdb path or specific
fallback is the measured baseline. State the comparison target per deployment.

Do not apply this append-only assumption to the prev-event DAG. Backfill can
insert an ancestor of already persisted events, so derived reachability labels
may require repair or a structure that supports such insertions. Bounded
extremity and missing-event walks should be compared against the amount they
actually visit, not against all events in the room.

## State-map diff target

The current adapter has HAMT root/node persistence and lookup/materialization
paths. A root-to-root diff that skips identical shared subtrees is a promising
target, but the claimed `O(|Δ| log_32 N)` bound requires that unchanged nodes
retain comparable identities and that the implementation can descend only
changed branches. Verify this in the current `hamt_store` and its callers before
putting the bound on a production path.

Compare against Synapse's bounded SQL state-group delta chains (currently capped
at 100 hops in this tree), full-state materialization, and existing caches.
Measure point lookup, two-root diff, n-way state resolution, write bytes per
single-key update, and periodic snapshot/materialization work. State is a
`(type, state_key) -> event` map; Elias–Fano applies only to separately encoded
monotone key/id sets, not the whole state map.

## Timeline and physical-I/O measurements

The detailed ordered-index design is in `synapse-event-range-index.md`. First
map each Synapse query to the pinned `TimelineIndex`, including per-room and
global ordering, negative backfill, equal-key pagination, filters, and the
`persisted_up_to` visibility boundary. Measure root publication and append
latency at 10k, 100k, and 1M events per room.

For representative room exports, measure pages of 50 events under append order,
`(room, topological_ordering, stream_ordering)`, and local topological rank.
Include `/messages`, `types`/`not_types` filtered `/messages`, and forward
stream reads. `lazy_load_members` controls membership events in the response's
state section; it does not remove member events from the timeline chunk.

Track `C_body` (coalesced body reads), `P_loc` (cold primary-index pages touched
while resolving record IDs), index pages, bytes read/useful bytes, cross-pack
range overlap, CPU, and wall time. Include small rooms sharing packs, large
rooms spanning packs, late backfill, outlier promotion, redaction, and purge.
Keep coalescing gap and window settings as benchmark variables.

## Migration sequence

1. Build query-family baselines from real Synapse database exports and profiles.
2. Measure timeline body layout and `TimelineIndex` append/publication costs.
3. Profile auth closure hits/misses and state-root diffs by closure/state-set
   sizes, then verify the append-only auth-DAG premise separately from prev-DAG
   backfill.
4. Select mtxdb structures and define shared commit, snapshot, purge, and
   recovery boundaries before enabling a cutover.
5. Migrate by query family, remove duplicate SQL writes and runtime reads, and
   verify the retired path is no longer used.

Do not infer performance from aggregate SQL table time alone. Record wall time,
query counts, rows visited, graph nodes visited, index/page faults, body reads,
bytes, memory, write amplification, and compaction/rebuild cost.
