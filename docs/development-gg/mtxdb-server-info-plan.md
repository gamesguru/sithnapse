# mtxdb server-info (KEYS/SIGN) migration plan

Detailed plan for Phase 2 of `mtxdb-performance-master-plan.md`: make mtxdb the
authority for federation signing keys, and remove the SQL reads and writes for
them. This is the first domain where mtxdb replaces SQL work instead of
mirroring it.

## Why this domain

- CI history shows hybrid Postgres+mtxdb tied with monolith Postgres in every
  era (median +0.8% in the mtxdb era, hybrid faster in 16 of 33 runs). The
  hybrid mode only mirrors data that SQL still serves, so it cannot win.
- `server_keys_json` upserts cost 44.9 s aggregate in the profiled worker
  Complement run, and the batched upsert is the slowest single statement (42.3
  s). The payload is a large blob repeated per key id.
- The access pattern is simple: write-rare, read-often, keyed by
  `(server_name, key_id)`. It has no stream ordering and no per-user filtering.

Domains that stay in SQL for now: stream positions, cache-invalidation streams,
worker locks, device-list stream ownership, push queues (they need ordering or
coordination semantics).

## Current state (verified)

mtxdb:

- `ShardType::ServerInfo` (`mtpl-server-info`, magic `SINF`) exists in
  `layout.rs`, `ShardType::ALL`, the pool policy (uncompressed) and the journal.

sithnapse:

- `rust/src/database/mtxdb_syn.rs` maps shard index 3 to `ServerInfo` and opens
  the pool (`server_info_db()`), but read-only workers open it as a durable
  snapshot because no read path exists.
- No KEYS/SIGN collections, PyO3 functions, compare-and-set or server-info
  counters.
- `synapse/storage/databases/main/keys.py` reads and writes
  `server_signature_keys` (MSC4499 first-seen-wins) and `server_keys_json`
  through SQL.

## Data model

Two collections in the `ServerInfo` pool, keyed per server:

- `SIGN`: `(server_name, key_id)` ->
  `{verify_key bytes, ts_valid_until_ms, ts_added_ms, from_server}`. This
  replaces `server_signature_keys`. The authoritative binding for MSC4499.
- `KEYS`: `(server_name, key_id, from_server)` ->
  `{ts_added_ms, ts_valid_until_ms, key_json}`. This replaces
  `server_keys_json`. The signed response is stored once per fetch, not
  duplicated per key id where avoidable (see Open questions).

## MSC4499 semantics to preserve

Taken from `store_server_keys_response` in `keys.py`:

1. Insert if absent; first committer wins.
2. Same key body (refresh): keep the original `from_server`; extend
   `ts_valid_until_ms` and set `ts_added_ms` only if the new validity is later.
3. Different body, stored `from_server != server_name`, candidate
   `from_server == server_name`: direct origin fetch replaces the provisional
   notary binding (log a warning with both sha256 values).
4. Otherwise: first seen wins; return the stored key body and do not persist the
   candidate `KEYS` record for that key id.

The compare-and-set must be atomic across `SIGN` and `KEYS` inside one mtxdb
`DatabaseTransaction`, so a reader never sees a `KEYS` record without its `SIGN`
binding.

## Steps

0. Document the plan (this file).
1. Counters: server-info reads, misses, refreshes and bytes, exposed through the
   existing stats path (`reset_stats` / `set_stats_enabled` already cover the
   pool). Record a baseline from a run before any behavior change.
2. mtxdb: KEYS/SIGN collection API on `ShardType::ServerInfo`, with the atomic
   SIGN compare-and-set in `DatabaseTransaction`. Unit tests in the mtxdb repo
   for first-seen-wins, notary override, refresh, and concurrent writers.
3. `mtxdb_syn.rs`: PyO3 functions for get / get-many / apply-key-response. Wire
   read-only workers to read through the active overlay (replace the snapshot
   open at the `has no read path` comment), so workers see committed records
   without restart.
4. `keys.py`: write path calls the mtxdb compare-and-set instead of the SQL
   insert/select/update sequence when the embedded engine is on. No steady-state
   SQL dual writes. Keep the `_get_server_keys_json` cache and replication
   invalidation as they are.
5. Reads: mtxdb first, SQL fallback for pre-migration rows only.
6. Backfill existing `server_keys_json` and `server_signature_keys` rows into
   mtxdb (background update or one-shot, idempotent, safe to rerun).
7. Verification: crash/recovery (restart during a pending write), concurrent
   MSC4499 writers, read-only worker visibility, backfill and fallback tests,
   `TestMSC4499Key` in Complement.
8. After validation: remove the SQL fallback reads and stop writing the two
   tables. Only in a later change, and only with a rebuild path from SQL dumps
   or a remote re-fetch (keys can be re-fetched, so loss is recoverable).

## Measurement

Success is that SQL work disappears and the saving exceeds mtxdb's added write
and commit cost. For each step that changes behavior:

- Run Complement (workers lane, where federation load is highest) at least twice
  before and after; compare the suite `TIME` line and `server_keys_json` SQL
  aggregate, not just the CI step duration (it includes a Docker build).
- Check I/O stall, iowait and worker lock retries do not worsen.
- Check mtxdb refresh counters stay bounded.
- Expect the SQL `server_keys_json` and `server_signature_keys` lines to drop to
  zero. If they do not, the path is still mirroring.

Runs are slow, so you run them; I do not launch heavy jobs.

## Risks and open questions

- The pool currently opens as a snapshot on read-only workers because overlay
  coverage raced with checkpoints when the pool was empty. Wiring reads must fix
  that race, not reintroduce it.
- `KEYS` stores the full signed response per `(server, key_id, from_server)`.
  Storing it once per response with key ids pointing at it would cut write size
  further, at the cost of a different layout from the SQL table.
- Existing mtxdb database roots need the layout upgrade path to keep working
  (the pool is already in `ShardType::ALL`).
- Federation key fetch frequency in the test workloads may be too low to move
  total wall time; the counters in step 1 will tell us before we commit to the
  rest.
- Cross-repo work: the mtxdb changes land in `../mtxdb` and the dependency pin
  in this repo is bumped afterwards.

## Acceptance gates

Same as the master plan: Complement green with no new skips, two or more
controlled runs that improve or preserve wall time, no worse I/O stall or lock
retries, bounded refresh cost, clean restart during pending writes, read-only
workers see committed records, tested SQL fallback and backfill, MSC4499
collision and concurrent-writer tests pass, no data loss if mtxdb is absent or
rebuilt.
