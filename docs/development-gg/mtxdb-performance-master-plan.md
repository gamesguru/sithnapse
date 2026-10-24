# mtxdb Performance Master Plan

## Purpose

This document describes the plan for reducing worker and client-sync latency in
Sithnapse while using mtxdb for the read-heavy data which currently creates
PostgreSQL and disk pressure.

The goal is not to move every table into mtxdb. The goal is to keep ordering,
filtering, per-user queues, and distributed coordination correct while moving
shared immutable or mostly-read data onto a fast, refreshable store.

## Current evidence

The representative worker Complement run used six CPUs, `go test -p4`, 902
passing tests, 36 skips, and a wall time of 8:48.

The important aggregate diagnostics were:

| Metric                    |    Value |
| ------------------------- | -------: |
| SQL time, all processes   |  632.8 s |
| SQL calls                 |  666,597 |
| SQL average               | 0.949 ms |
| SQL pool scheduling delay |  372.9 s |
| SQL pool p99 delay        |  20.3 ms |
| CPU stall                 |    28.1% |
| I/O stall                 |    25.0% |
| iowait                    |    13.9% |

These are sums across processes, not wall-clock time. They identify pressure,
not an additive breakdown of the 8:48 run.

The largest measured SQL areas were:

| Area                                    | Aggregate time | Assessment                                                      |
| --------------------------------------- | -------------: | --------------------------------------------------------------- |
| `events`                                |         90.0 s | Event index/metadata access; event JSON is already mtxdb-backed |
| `server_keys_json`                      |         44.9 s | Large repeated federation-key blob writes; immediate target     |
| `current_state_events`                  |         38.2 s | Room-state lookup and persistence                               |
| `stream_positions`                      |         26.0 s | Worker stream coordination                                      |
| `current_state_delta_stream`            |         19.6 s | State-change fan-out; strong future candidate                   |
| `cache_invalidation_stream_by_instance` |         17.3 s | Worker cache replication                                        |
| `delayed_events`                        |         15.8 s | Scheduled-event workload                                        |
| `worker_read_write_locks`               |         15.5 s | Coordination; improved substantially after lock-order fix       |
| device-list tables                      |        ~27.7 s | Repeated federation/device-list polling                         |
| startup/schema queries                  |          ~25 s | Per-process initialization overhead                             |

The slowest individual statement was the batched `server_keys_json` upsert, with
42.3 seconds aggregate time. mtxdb itself was not the dominant cost: event/state
FFI operations were generally sub-millisecond, and the measured mtxdb sync/fsync
totals were well under one second per pool.

## Existing mtxdb layout

The current Synapse wrapper has four physical pools:

```text
state
event_dag
auth_chain
server_info
```

Each pool can contain multiple logical collections. The existing model is a
parent/group identity with sibling member collections. For a room, the useful
shape is conceptually:

```text
room parent
├── state HAMT
├── event JSON
├── auth-chain links
└── previous-event edges
```

This is different from inventing a new metacollection. A metacollection would be
an additional sibling containing indexes or manifests for the other collections;
it is not required for the initial design.

## Design principles

1. Legacy baseline, not the agreed direction: SQL remains the source of truth
   for data whose ordering and transactional semantics have not been reproduced
   in mtxdb. For data migrated to mtxdb, mtxdb is the authority and duplicate
   SQL writes are removed (see `perf-track-b.md` B2). Data that deliberately
   stays SQL-authoritative keeps this rule, and only that data may use an SQL
   outbox.
2. mtxdb records must be refreshable by read-only workers without reopening the
   process or rebuilding the entire store.
3. Every offload must have an explicit fallback and rebuild path.
4. Do not put ephemeral high-churn data into durable mtxdb merely because it is
   readable from multiple workers.
5. Measure wall time and I/O pressure in addition to aggregate SQL/FFI time.
6. Preserve Matrix protocol behavior first; speedups which weaken consistency
   are not acceptable.

## Phase 0: protect the baseline

Keep a repeatable benchmark profile containing:

```text
database: PostgreSQL and SQLite where applicable
topology: monolith, workers, hybrid mtxdb
CPUs: six
worker parallelism: -p4
tests: full Complement set
diagnostics: SQL table totals, pool delay, load/pressure, mtxdb stats
```

Record both wall time and the following counters:

- `worker_read_write_locks` calls and time;
- SQL pool scheduling p50/p95/p99/max;
- I/O stall and iowait;
- `server_keys_json` calls/time;
- mtxdb refresh counts and refresh bytes;
- mtxdb sync, checkpoint, sidecar, and fsync time;
- per-suite Complement timings.

Aggregate timings must not be described as wall-clock savings.

## Phase 1: worker coordination

The worker-lock context manager was changed so that waiters are notified only
after the database lock row has been deleted. Notifying before deletion wakes
workers into guaranteed failed acquisition queries.

This reduced the measured lock-table aggregate from about 130 seconds to 15.5
seconds in the compared runs. The total run was still approximately one second
slower because HDD/PostgreSQL I/O pressure increased, so the change must be
judged by repeated wall-clock runs as well as lock metrics.

Next worker improvements:

- coalesce release notifications per lock key;
- avoid waking every waiter when a single writer can proceed;
- retain simultaneous wakeups for compatible read-lock waiters;
- instrument retry counts and failed acquisition attempts;
- avoid per-worker startup/schema work where possible.

mtxdb should not become the authority for distributed worker locks. Lock
ownership, expiry, and cross-worker conflict resolution are coordination
semantics, not read-cache semantics.

## Phase 2: eliminate federation-key write amplification

`server_keys_json` is currently the clearest SQL/HDD hotspot. A naive
conditional upsert is not sufficient: `ts_added_ms` is the fetch time and
therefore advances on every repeated response. The real SQL mitigation must
compare the existing response before deciding whether to rewrite it, while
preserving source-order semantics. The long-term fix is to move the blob
workload into mtxdb.

The long-term design is a fourth physical pool:

```text
state
event_dag
auth_chain
server_info
```

The name `server_info` is preferred over `srvinfo` for readability.

Within that pool, use sibling member collections under a server parent:

```text
server_info pool
└── server_name parent
    ├── KEYS  raw signed key responses
    └── SIGN  selected authoritative signing keys
```

Possible future members, only if profiling justifies them:

```text
    ├── DEST  federation destination metadata
    └── CAPS  cached server/federation capabilities
```

`DEST` and `CAPS` are not part of the initial migration.

### KEY records

```text
key:   (server_name, key_id, from_server)
value: (ts_added_ms, ts_valid_until_ms, key_json)
```

Reads need an efficient lookup by `(server_name, key_id)`, so the collection
must provide a secondary index or an equivalent parent-local lookup structure.

### SIGN records

```text
key:   (server_name, key_id)
value: (key body, source/provenance, validity, version)
```

`SIGN` must contain one authoritative selected binding, not every response.

### MSC4499 requirements

The mtxdb implementation is MSC4499-aligned only if it preserves:

- one key-ID to key-body binding per server;
- first-seen-wins behavior;
- permitted direct-origin replacement of provisional/notary data;
- validity timestamps that cannot move backwards;
- atomic concurrent compare-and-set/upsert behavior;
- crash-safe publication and refresh;
- signature validation before insertion;
- no stale worker-visible binding after a committed replacement;
- a migration that cannot silently lose an existing binding.

The `KEYS` collection may retain multiple raw source responses. `SIGN` must
remain the authoritative result of conflict resolution.

The `ServerInfo` shard and its shared-database pool are present in mtxdb and
opened by the Synapse Rust wrapper. The pool is currently empty and has no
KEYS/SIGN collections or Synapse read/write path. This plan is therefore a
design proposal for the data API and migration, not a claim of completed MSC4499
compliance.

The steady-state authority must be mtxdb, not SQL. Keeping SQL as the writer
authority and duplicating every response into mtxdb would retain the measured
`server_keys_json` write hotspot and create an inefficient dual-write path. SQL
may remain temporarily as a migration/read-recovery fallback, but normal key
writes must perform MSC4499 resolution in mtxdb and must not write the SQL blob
table on every fetch.

This also means a Python read-then-write sequence is insufficient for `SIGN`:
the mtxdb layer needs an atomic compare-and-set/upsert primitive, or a single
serialized mtxdb writer that owns the resolution. A standalone pool without such
a primitive cannot safely claim MSC4499 concurrency compliance.

`SIGN` and `KEYS` publication are atomic within a single `DatabaseTransaction`
across the shared pools. An accepted `SIGN` binding with a failed `KEYS` blob
write would leave the authoritative collections inconsistent; using
`SharedDatabase` guarantees joint publication across all four pools.

**Pool and transaction integration already present:** `ShardType::ServerInfo`
(`mtpl-server-info`) is included in the layout, pool policies, and
`SharedDatabase`; existing roots create/upgrade the pool on open. The remaining
work is to add the logical collections and use the shared transaction API for
atomic KEYS/SIGN publication. Confirm the exact crash and refresh guarantees of
the shared transaction path while implementing the collection API; do not assume
pool membership alone proves those guarantees.

## Phase 3: event and room-state fan-out

The largest client-facing opportunity is not the polling request itself. It is
serving the same active-room data repeatedly to many clients.

Prioritize these shared reads:

```text
event JSON bodies
room state snapshots / HAMT roots
state-group materialization
event DAG and auth-chain reads
room state-delta history
```

The desired flow is:

```text
new event
  ├── persist authoritative stream/index records
  ├── publish one room/state update
  ├── wake affected sync requests
  └── serve repeated client reads from mtxdb
```

mtxdb can reduce repeated PostgreSQL reads, but it does not automatically remove
per-client work such as filters, membership checks, sync tokens, or long-poll
wakeups.

### Already implemented

Event JSON is already written to mtxdb for new events when the embedded engine
is enabled. The read path checks mtxdb first, with SQL fallback for rows that
predate the embedded engine or are otherwise absent from the mirror. SQL still
holds the `events` index/metadata and some legacy direct-join paths, so the
`events` table total must not be interpreted as proof that event bodies remain
in PostgreSQL.

### Strong candidates

- state HAMT reads and state-group materialization;
- current-state delta access;
- auth-chain and previous-event reads.

### Keep in SQL initially

- per-user sync tokens;
- push-action queues;
- stream positions;
- cache invalidation streams;
- worker lock rows;
- device-list stream ownership and retry state.

These require ordering, filtering, or coordination semantics that should be
optimized before they are relocated.

## Phase 4: high-client-count sync work

For many local users with devices polling active rooms, optimize in this order:

1. Batch state/event reads across simultaneous sync requests.
2. Coalesce sync wakeups per room and stream position.
3. Use mtxdb for shared event and state reads.
4. Reduce repeated per-client membership and filter queries.
5. Batch push-action and receipt lookups.
6. Reduce repeated stream-position and cache-invalidation queries.

Typing is out of scope for the durable mtxdb plan. Synapse typing is already an
in-memory room-to-users map on the writer, replicated over the worker TCP path;
putting it in mtxdb would add durable-write and refresh overhead without fixing
its existing fan-out path.

### Presence-specific plan

Presence is explicitly in scope as a high-client-count performance workload. The
relevant path is the `presence_stream` table, per-user stream-token tracking,
`StreamChangeCache`, notifier wakeups, sync filtering, and outbound federation
batching. A large active population can create fan-out pressure even when the
database query itself is inexpensive: one presence transition may wake many
long-polling clients and federation queues.

The first presence optimizations should be Synapse-level:

- suppress duplicate state transitions before assigning a stream ID;
- coalesce multiple updates for the same user before notifying clients;
- batch presence reads across concurrent sync requests;
- avoid waking clients whose presence filters cannot match the update;
- keep offline-expiry and last-active updates from producing unnecessary visible
  stream entries;
- batch outbound federation presence EDUs per destination;
- measure notifier wakeups, stream rows written, rows read, and serialized
  presence bytes separately.

Presence should not be put into the durable `server_info` pool or the room/event
pools. Its state is mutable and high-churn, and its main cost is fan-out and
notification scheduling rather than large shared blobs.

### Presence mtxdb design, staged

The first experiment should be a CURRENT-only design in a separate fifth
`ephemeral` pool:

```text
ephemeral pool
└── presence
    └── CURRENT: user_id -> latest presence value and expiry
```

This version deliberately has no per-update presence stream ID and no change
history. It measures whether shared current-state reads are a meaningful
PostgreSQL cost before adding a more complicated incremental-read structure.
Reads fall back to SQL on a miss, and writes are coalesced per user before a
batch commit.

If CURRENT-only reads are insufficient for incremental `/sync`, version two adds
a bounded pool-level change index rather than putting change records under each
user:

```text
PRESENCE_BATCH/<revision_u64_be> -> coalesced changed user IDs
PRESENCE_HORIZON                -> oldest retained revision
```

The revision is one per coalesced batch, not one per heartbeat. A sync client
whose revision predates `PRESENCE_HORIZON` receives a full relevant snapshot;
otherwise the worker scans changed user IDs and batch-reads CURRENT values.

This requires explicit horizon tracking, range reads, bounded retention, and a
well-defined refresh contract for read-only workers. A pool commit revision is
not automatically a usable sync token until those semantics exist.

Presence does not need an atomic transaction with event/state writes, so an
independent pool may be the simpler first implementation. Either choice must
define checkpoint, refresh, `request_durable`, wait-durable, compaction, and
crash-recovery behavior. If pursued, this presence experiment would use a
separate ephemeral pool (a fifth physical pool), not `server_info`.

## Phase 5: device lists and federation metadata

The device-list tables are a better SQL optimization target than most of the
small tables in the report. Investigate:

- batching empty `device_lists_remote_resync` scans;
- indexing by retry time and server;
- coalescing repeated device-list changes;
- reducing per-worker polling;
- separating durable device-list state from transient retry scheduling.

`destination_rooms` is a possible `server_info/DEST` candidate, but it should
not move until its update and retry semantics are documented and measured.

## Phase 6: worker startup and PostgreSQL pressure

The `pg_database` and schema transaction costs indicate repeated process
initialization. Audit:

- schema/version checks performed by every worker;
- connection-pool creation and warmup;
- redundant background updates;
- per-worker database feature probes;
- PostgreSQL connection count and pool sizing;
- whether Complement is amplifying startup cost with excessive process
  parallelism.

This work is independent of mtxdb and may produce a faster result than moving
small tables.

## Phase 2 implementation plan: server keys authority

The physical pool integration is complete; the logical data path is not. Do the
work in these stages, keeping each stage reviewable and preserving SQL as the
recovery source until the final cutover:

1. **Specify records and invariants.** Finalize KEYS/SIGN key encoding,
   serialization/versioning, lookup by `(server_name, key_id)`, source
   provenance, validity rules, and MSC4499 conflict outcomes. Document how
   legacy SQL rows map, including conflicts or incomplete rows.
2. **Instrument the pool and APIs.** Add counters for KEYS/SIGN reads,
   hits/misses, writes, CAS outcomes, refresh count/bytes/time, and fallback
   reads. Expose them through the existing diagnostics path. Keep labels bounded
   (no server names or key IDs).
3. **Implement the mtxdb collection API.** Add Rust collection operations and
   narrow PyO3 wrappers. Reads must support point and batched lookups. Writes
   must return explicit CAS results and errors; do not expose an API that lets
   callers bypass SIGN conflict resolution.
4. **Make publication atomic.** In one shared `DatabaseTransaction`, write the
   raw KEYS response and apply SIGN first-seen-wins/CAS. Define behavior for
   duplicate responses, direct-origin replacement of provisional data, rejected
   conflicts, expired bindings, retries, and transaction aborts. Verify
   committed data becomes visible to read-only workers through the documented
   refresh mechanism.
5. **Integrate writes behind a migration mode.** Route validated responses
   through mtxdb. Keep existing SQL behavior as an explicit compatibility mode
   during development; do not perform a Python-level read-then-write CAS. Record
   enough diagnostics to compare accepted writes and fallback activity.
6. **Read mtxdb first with SQL fallback.** Serve KEYS and authoritative SIGN
   from mtxdb; fall back to SQL for misses while migration is incomplete. Define
   whether a fallback result is merely returned or repaired into mtxdb, and
   ensure repair cannot overwrite a newer authoritative binding.
7. **Backfill safely.** Add a resumable, idempotent backfill from both SQL
   tables. Resolve rows through the same SIGN rules as live writes. Track
   progress and conflicts, and make restart/retry safe. Do not delete or stop
   updating SQL during this stage.
8. **Validate failure and concurrency cases.** Cover simultaneous first
   sightings, provisional-to-direct replacement, conflicting direct keys,
   duplicate/reordered responses, invalid signatures, transaction abort, crash
   around commit, restart, stale read-only worker refresh, partial backfill, and
   SQL fallback. Assert SQL and mtxdb agree wherever both have data.
9. **Measure the hot path.** Compare matched PostgreSQL-only and hybrid runs
   with the same test set, worker topology, compiler, and runner conditions.
   Report wall time separately from aggregate SQL time. Confirm the targeted SQL
   upsert cost falls, fallback stays bounded, refresh cost is bounded, and no
   new contention or I/O regression appears.
10. **Cut over authority, then retire SQL use.** Only after correctness,
    recovery, and performance gates pass, make mtxdb the normal write authority
    and stop steady-state SQL blob writes. Keep SQL fallback and backfill
    available for a defined compatibility window. Remove SQL fallback and cease
    maintaining the two SQL tables only in a separately reviewed final
    migration, with an operational recovery procedure.

Current status: physical `ServerInfo` pool/layout and wrapper opening are
present; collection schema/API, server-info counters, Synapse integration,
backfill, and migration tests are not implemented.

## Acceptance gates

A change is ready to land only when:

- full Complement passes with no new skips or flaky failures;
- two or more controlled runs improve or preserve wall time;
- I/O stall and iowait do not materially worsen;
- worker lock retries do not increase;
- mtxdb refresh counters show bounded refresh cost;
- restart during pending writes recovers cleanly;
- read-only workers see committed records without manual restart;
- SQL fallback and backfill paths are tested;
- MSC4499 collision and concurrent-writer tests pass for key storage;
- no production data loss is possible if mtxdb is absent or rebuilt;
- server-key SQL writes are removed from the steady-state path before claiming a
  performance win for this phase;
- CAS outcomes, SQL fallback, backfill progress, and refresh cost are visible in
  diagnostics without high-cardinality labels.

## Immediate next actions

1. Finalize KEYS/SIGN record and MSC4499 transition semantics.
2. Add server-info counters and specify the collection/API (the physical pool
   already exists).
3. Implement atomic KEYS/SIGN operations and Synapse routing with temporary SQL
   fallback; then backfill and validate before authority cutover.
4. Profile device-list polling and worker startup separately.
5. Implement the presence CURRENT-only experiment in a separate ephemeral pool
   only if its read profile justifies it.
6. Only then evaluate moving `current_state_delta_stream` and additional
   client-sync reads.
