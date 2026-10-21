# Performance track B: speedups

Measurement work (track A: process cpu vs wall, `:cpu` span twins, stall review
line, `scripts-dev/compare_trial_logs.py`) is kept separate from this list. Each
item here needs its own measurement before and after; none has been implemented
yet. Findings come from saved logs in `res/logs` and from two small profiled
test files (8 tests); no full suite was run.

## B1. Trial per-test setup

Per test (8-test sample, FILE_COPY recycle): hs setup 75-84 ms, of which DB
reset about 25 ms, store init about 28 ms, unattributed about 17 ms.

- DB reset (`tests/server.py::_reset_recycled_postgres_db`) is already
  limited to dirty tables. Per test: connect 2-3.5 ms, terminate backends
  2-3 ms, dirty scan 5-6 ms, truncate 13-14 ms, reseed + sequences 3 ms.
  Remaining ideas are small (fewer round trips, skipping terminate when no other
  backend exists).
- Store init: in a cProfile of `DataStore.__init__` (9 calls, 0.296 s),
  `MultiWriterIdGenerator._load_current_ids` accounts for 0.151 s (153 calls,
  about 17 per store, about 1 ms each). Each does a DELETE, a SELECT on
  `stream_positions`, a sequence `last_value` read and `MAX()` queries, one
  round trip each. Batching these across generators could save roughly
  13 ms per test (about 60 s of worker time over 4,582 tests), but it changes
  upstream constructor code in `synapse/storage/util/id_generators.py` and every
  store that builds a generator, so it conflicts with upstream merges. Not
  started; needs a decision on whether to carry that divergence.
- Ordering: do reset round-trip trims first (test-only code), then decide on
  the id-generator batching.

## B2. Move mtxdb writes off the request path (outbox)

mtxdb writes (`put_many`) currently happen synchronously inside the SQL
transaction; the coalescer defers only fsync. Design: commit the SQL state plus
a durable write record together, apply mtxdb updates from one bounded background
writer in batches. Open questions that must be answered first:

- which state reads can fall back to SQL while a write is queued;
- retry, ordering and backpressure limits;
- recovery after a crash between the SQL commit and the mtxdb apply;
- read-only workers (single mtxdb writer) and the replication route.

No code until a design is agreed.

## B3. Native `remove_many` for HAMT root deletes

Synapse deletes by writing empty-value tombstones (`delete_state_hamt_roots_for_room`,
event JSON and flat-KV deletes) because mtxdb exposed no per-key delete. The
pinned mtxdb now has `DatabaseTransaction::remove_many` (`FLAG_REMOVED` frame,
reclaimed at repack, no write for keys that are not live). Before switching:
check the removed flag is visible through the read-journal overlay to read-only
workers, and redo the locking in the binding (it uses `StorageEngine`, not
`DatabaseTransaction`). The auth-chain link delete rewrites a manifest and does
not fit. Deletes are rare, so expect cleanup, not speed. Also in `TODO.txt`.

## B4. Server-key authority in mtxdb

See `mtxdb-server-info-plan.md`. Blocked on the writer route (forward over HTTP
replication to the single mtxdb writer is the leading option) and on the payload
choice (opaque payload with Synapse-owned merge and versioned compare-and-set).

## B5. Thread caps and CPU pinning (experiments)

Unverified. Candidates: the 4-worker tokio runtime started lazily per homeserver
(`rust/src/runtime.rs`, `synapse/server.py:1109`), mtxdb background threads, and
`taskset` for Postgres. First check whether tests start the runtime at all;
pinning can starve Synapse or Postgres if cores are split badly. Measure with
alternating A/B runs and `compare_trial_logs.py`.

## Multiple event writers and the mtxdb owner (design constraints for B2 and B4)

Today `synapse/storage/databases/state/store.py:193` opens mtxdb writable on
every instance in `writers.events`. mtxdb's writable open takes an exclusive
lock, so a second events writer cannot open it. Supporting several SQL event
writers needs a separate mtxdb owner setting in worker configuration and
routing of every mtxdb mutation to it over acknowledged replication.

- Routing does not make mtxdb writes faster: it adds an RPC hop and queues all
  mtxdb writes at one owner. Its only benefit is letting several event writers do
  SQL work in parallel, and nothing measured shows the single events writer is
  saturated. Treat it as a scaling option.
- Keep one shared mtxdb database with one writing process; each process reads
  through its read-only handle and the shared journal refresh. A database per
  event writer would need fan-out and merge reads, so it is rejected.
- Read-after-write: a process must not acknowledge a write and then serve a
  stale local read. Define a visibility barrier (the acknowledgement carries a
  position the reader waits for) before implementing.
- Crash gap: the SQL commit and the owner's mtxdb write cannot commit
  atomically. Needs a durable retry or reconciliation path (the B2 outbox) or a
  stated fallback to SQL.
- Verify: owner restart, duplicate requests, updates from different event
  writers, reader visibility.
- The alternative, multi-process writes inside mtxdb (coordinated WAL appends,
  commits and publication), is a much larger mtxdb project.
