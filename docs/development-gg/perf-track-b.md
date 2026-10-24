# Performance track B: speedups

Measurement work (track A: process cpu vs wall, `:cpu` span twins, stall review
line, `scripts-dev/compare_trial_logs.py`) is kept separate from this list. Each
performance change needs its own before/after measurement. Findings here come
from saved logs in `res/logs` and small profiled test runs; no full suite was
run for this investigation.

## B1. Trial per-test setup

Per test (small profiled samples, FILE_COPY recycle): homeserver setup was about
53-84 ms, depending on run and instrumentation. DB reset and store init were the
largest identified components.

- DB reset (`tests/server.py::_reset_recycled_postgres_db`) now reuses one
  autocommit connection per recycled test database. An alternating 8-test A/B
  measured reset at 21.9 ms before and 15.2 ms after; homeserver setup moved
  from 59.3 ms to 53.7 ms. This is committed as `24c1221afd`. The test-only
  reset path still has about 10 ms/test in TRUNCATE (21 tables per reset, about
  0.5 ms each; 18 of them have no owned sequence or foreign key). Tried and
  dropped: a plain DELETE for small tables of that kind. Alternating A/B (3 runs
  each) cut the reset from 15.3 to 12.7 ms but left homeserver setup unchanged
  (54.2 ms both), so it was not committed. Even eliminating the TRUNCATE
  entirely is only about 15 s of wall time across 5,767 tests at `-j4`.
- Generator startup: `_load_current_ids` and `check_consistency` were profiled
  in a small run (8 tests, 9 homeserver setups). `_load_current_ids` took about
  109 ms over 153 calls (about 12 ms per setup); calls issue 4–9 queries. Only
  the caches and cross-signing generators used the empty-stream `MAX()` path.
  `check_consistency` took about 14 ms total (about 1.6 ms per setup).
- A small change in `synapse/storage/util/id_generators.py` now selects
  `stream_positions` before deleting stale rows, and only issues the DELETE when
  stale rows are found. Alternating measurements moved `_load_current_ids` from
  107/121 ms to 96/94 ms, about 1–3 ms saved per setup. It is committed as
  `6def69aee4`; `tests.storage.test_id_generators` passed (25 tests), and ruff
  and mypy passed. This is an upstream-file divergence. Do not batch the
  generators further without evidence that the extra few milliseconds justify
  its maintenance cost.

- Startup migration enqueues (`422ea06275`). Three hooks re-queued a migration
  by an idempotent upsert into `background_updates` on every homeserver start,
  then called `start_doing_background_updates()`, which restarts the updater's
  poll loop. They are dual-mode scaffolding for databases that predate the
  embedded engine, not state-family work.

  | Hook                                                | Checks                                      | Fresh test DB                |
  | --------------------------------------------------- | ------------------------------------------- | ---------------------------- |
  | `enqueue_event_to_state_group_embedded_migration`   | legacy `event_to_state_groups` rows to copy | no-op: migrate step ~0.08 ms |
  | `enqueue_event_auth_chain_links_embedded_migration` | legacy auth chain link rows                 | no-op: select step ~0.08 ms  |
  | `enqueue_state_hamt_embedded_migration`             | legacy `state_hamt_*` SQL rows              | no-op: select step ~0.08 ms  |

  In the 984 s slice each ran 2,323 times, with 20-22 ms of thread-pool
  scheduling delay per call (148.5 s summed, about 30% of all scheduling delay)
  against about 0.6 ms of SQL. That is accumulated queue time that overlaps with
  other work, not a wall-time saving. A one-time completion marker would have
  replaced the per-start check, but with no deployments the migrations are not
  needed at all, so the hooks were removed and old databases are rebuilt. The
  handlers, enqueue functions and tests remain until the A/B (homeserver setup,
  `main_store_init`, total scheduling delay) is run; the Rust module must be
  rebuilt first.

The two B1 changes are measured setup improvements, not production request-path
improvements. No full suite was run for these changes.

## Shared design constraint: one mtxdb writer

Synapse currently treats every configured `writers.events` instance as an mtxdb
writer (`synapse/storage/databases/state/store.py`). mtxdb's database directory
has an exclusive writer lock, so a second event writer cannot open it writable.
Supporting multiple SQL event writers therefore requires a separate, explicit
mtxdb owner setting, validated to identify exactly one instance. Keep one shared
mtxdb database; per-event-writer databases would require fan-out reads and
merging.

The likely write route is acknowledged RPC to the owner. This enables multiple
SQL event writers; it does not make mtxdb writes faster. All mtxdb writes would
queue at one owner, and the RPC adds latency. There is no measurement showing
that a single event writer is currently the throughput limit, so treat this as a
scaling capability, not a speedup claim.

Any RPC design must specify:

- **Visibility:** after an acknowledged write, the caller must not serve a stale
  local mtxdb read. Define a visibility barrier for read-only handles.
- **Failure gap:** SQL and mtxdb cannot commit atomically across the RPC. A
  crash between the SQL commit and mtxdb apply needs durable retry or
  reconciliation.
- **Ordering and backpressure:** define ordering between writers, duplicate
  request handling, queue bounds, and behavior when the owner is unavailable.
- **Ownership lifecycle:** define startup, restart, and failover behavior for
  the configured owner.

These constraints are shared by B2 and B4, but their payloads and consistency
rules differ. Resolve the shared architecture once; keep each feature's own
design and measurements separate.

## B2. Make mtxdb authoritative, one data family at a time

**Decision:** for each migrated data family, mtxdb is the source of truth for
committed values. Remove duplicate SQL writes and reads where the semantics
allow it. SQL may retain metadata, indexes, or rebuildable projections, but
those must not become a second authority. A SQL outbox is not the default: it
would preserve SQL as the durable source for queued mtxdb updates and duplicate
the work this migration is intended to remove.

Current behavior still writes synchronously to mtxdb while persisting related
SQL state. Do not move those writes to an asynchronous mirror. First choose a
bounded data family and map its write/read dependencies, then define how its
mtxdb commit relates to any SQL metadata that must remain. If cross-store
atomicity is needed, make that a property of the chosen family’s protocol; do
not assume a generic SQL outbox is the answer.

For each family, the design must specify:

- the exact authoritative records and the SQL reads/writes removed;
- atomicity and ordering for writes, including any retained SQL metadata;
- read-after-write visibility through read-only mtxdb handles;
- restart, import, backup, and recovery behavior;
- migration of existing SQL data and rollback/cutover strategy;
- before/after measurements of SQL work, mtxdb work, and end-to-end latency.

Family order (SQL stays for ordering/coordination data: `events` index,
`current_state_delta_stream`, stream positions, cache invalidation, worker
locks, device-list coordination, push queues, delayed events):

1. Federation keys (`server_keys_json`, `server_signature_keys`): about 44.9 s
   aggregate SQL in the profiled run; see B4.
2. `event_to_state_groups`: finish migrating old rows, then drop the SQL
   fallback, scaffolding and table.
3. `event_json`: backfill, convert remaining SQL joins, drop fallback and table.
   The SQL `events` table stays.
4. `event_edges`, `edge_index_outbox`, `room_edge_source_version`: rebuild edges
   in mtxdb from canonical event JSON; remove the SQL write, outbox and gate
   once recovery is proven. `forward-index-design-spec.md` must be revised
   first.
5. `state_groups`, `state_group_edges`, `state_group_id_seq`
   ([family-5 state map](perf-track-b-family5-state-map.md),
   [state-groups-out-of-sql.md](state-groups-out-of-sql.md)): broadest change,
   last.

The SQL `events` table is not a candidate yet. Event bodies are already in mtxdb
for new events; `events` is the relational index (event/room IDs, stream and
topological order, type, sender, depth, timestamps, state key, flags) behind
pagination, federation, relations, membership, purging and stream queries (about
197 SQL references in the main storage code). mtxdb offers point and batch
lookups, not range scans or joins, so dropping it would mean building those
indexes and query paths in Python/Rust. Revisit only after mapping its query
patterns to mtxdb indexes and measuring the replacement cost.

SQL cost per family (`res/logs/2026-10-21-trial-tests-530s.log`, 5,767 tests,
`-j4`, sync off, pin `0efca0a`; summed over 25 processes, total 494.9 s). This
is a unit-test run, not a federation workload, and the log does not split reads
from writes:

- Family 4: `event_edges` 18.3 s (43,754 calls, 31,282 rows) plus
  `edge_index_outbox` 4.5 s = 22.7 s (4.6%).
- Family 5: `state_groups` 12.8 s, `state_groups_persisting` 8.7 s,
  `state_groups_pending_deletion` 7.4 s (42,728 calls, 206 rows) = 28.9 s
  (5.8%). `state_group_edges` is under the top-30 cutoff (4.2 s).

Spread over four parallel jobs each is about 1% of wall time, so 4 and 5 are
structural simplifications, not large speedups. Order 4 before 5 by risk, not by
SQL time.

Items 2 and 3 retire legacy paths, 4 removes an active duplicate write, 5 is a
redesign. The claims in this list (for example that new writes already go only
to mtxdb) are unverified until each family's dependency map is done.

The shared single-writer owner and visibility constraints above apply. Do not
implement a generic outbox or change another family until one migration has
demonstrated correctness and a measured benefit.

## B3. Native `remove_many` for HAMT root deletes

Synapse deletes by writing empty-value tombstones
(`delete_state_hamt_roots_for_room`, event JSON and flat-KV deletes) because
mtxdb exposed no per-key delete. The pinned mtxdb now has
`DatabaseTransaction::remove_many` (`FLAG_REMOVED` frame, reclaimed at repack,
no write for keys that are not live). Before switching: check the removed flag
is visible through the read-journal overlay to read-only workers, and redo the
locking in the binding (it uses `StorageEngine`, not `DatabaseTransaction`). The
auth-chain link delete rewrites a manifest and does not fit. Deletes are rare,
so expect cleanup, not speed. Also in `TODO.txt`.

## B4. Server-key authority in mtxdb

See `mtxdb-server-info-plan.md`. Blocked on the shared writer/visibility design
above and on the payload choice (opaque payload with Synapse-owned merge and
versioned compare-and-set). B4 also needs idempotent key updates and a defined
response contract so the fetching worker receives the authoritative resolved
keys. Do not assume the B2 outbox is automatically suitable for key writes: the
caller needs the resolution result synchronously.

## B5. Thread caps and CPU pinning (experiments)

Unverified. Candidates: the 4-worker tokio runtime started lazily per homeserver
(`rust/src/runtime.rs`, `synapse/server.py:1109`), mtxdb background threads, and
`taskset` for Postgres. First check whether tests start the runtime at all;
pinning can starve Synapse or Postgres if cores are split badly. Measure with
alternating A/B runs and `compare_trial_logs.py`.
