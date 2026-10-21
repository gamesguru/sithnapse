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

## Existing mtxdb key model (reuse, do not replace)

mtxdb already defines the signing-key layout in `src/matrix/interop.rs`
(introduced in `6a489d5`, "shared interop layouts"):

- One collection, `server_keys_collection_id()`, in the `ServerInfo` pool.
- One record per `(KeyRecordKind, origin)`, id from `server_key_record_id`.
  Kinds: **Historical** (cumulative `verify_keys` and `old_verify_keys` ever
  seen), **Provisional** (key ids bound only through a notary so far),
  **Corroborated** (key ids observed as currently active by a direct fetch),
  **Notary** (last verbatim self-signed payload, re-served by the notary).
- Value = version byte, length-prefixed origin, opaque payload
  (`encode_server_key` / `decode_server_key`). The payload bytes are not
  interpreted by mtxdb.
- `export_schema.rs` has `server_keys` records (origin, kind, base64 payload),
  and the pool also holds the import marker.

This plan builds on that model. A parallel per-`(server_name, key_id)` SIGN/KEYS
schema (the first version of this document) is dropped.

## Mapping SQL semantics to the kinds (to confirm)

| SQL today | mtxdb kind |
|---|---|
| `server_signature_keys` rows bound via a notary (`from_server != server_name`) | Provisional |
| rows bound by a direct fetch (`from_server == server_name`) | Corroborated |
| first-seen-wins reference of all keys ever bound | Historical |
| `server_keys_json` (signed response, served by the notary endpoint) | Notary |

Open questions, to settle by reading the mtxdb payload contract and tests:

- What is inside each payload (JSON? per-key-id map with `valid_until_ts`?).
  Per-origin records mean a read-modify-write of the whole origin record on
  every key refresh, inside one `DatabaseTransaction`.
- `server_keys_json` is keyed `(server, key_id, from_server)` and holds one
  row per fetch source. Notary is one record per origin. Check whether
  `get_all_server_keys_json_for_remote` and `remote_key_resource.py` need the
  per-`from_server` rows. If they do, Notary alone does not replace the table.
- Does the first-seen-wins + provisional->corroborated transition (MSC4499
  steps 1-4 above) fit as one transaction over those records?

## Writer access (settled)

- mtxdb's writable open takes an exclusive lock, so only one process writes.
  `state/store.py:193` picks it: the instance is the writer iff
  `hs.get_instance_name() in hs.config.worker.writers.events`. Every other
  process calls `open_client_read_only`.
- `store_server_keys_response` can run on any process that fetches keys
  (federation readers, the notary endpoint, the main process), not only on the
  events writer. So a direct mtxdb write is not possible from those processes.
- Consequence for the design: key writes need a route to the single writer, or
  key authority needs its own writer lock/role separate from the events
  writer. Options to evaluate: (a) a replication command or HTTP replication
  endpoint that forwards `store_server_keys_response` to the writer; (b) a
  distinct `ServerInfo` writer process lock (the pool is separate from state);
  (c) keep SQL as the write path for non-writer processes during migration.
  This choice comes before any Rust API work.

## Repo and pin

- Synapse pins mtxdb `dev@f41d02a91bb58f1aa91abeadde8ed549297b3e05` (Cargo.lock).
- That commit is not in the local `../mtxdb` clone (it is on
  `laptop/lru-cache`, unrelated). Before any mtxdb change: fetch, confirm the
  key model is present at the pin, and cut a dedicated branch from exactly that
  commit. Keep Synapse's pin aligned with that branch's commit. If the pin
  predates `6a489d5`, decide explicitly whether to include it.

## Steps

0. Document the plan (done).
1. Server-info counters in the stats report (done, `e819cc7042`).
2. Verify the key model at the pin; create the mtxdb branch from it.
3. Settle the writer route (above).
4. Define the payload contract per kind; add the atomic first-seen-wins /
   provisional->corroborated update over these records, with tests in mtxdb.
5. `mtxdb_syn.rs` PyO3 get / apply functions; wire read-only workers to read
   through the overlay (replace the snapshot open at the "has no read path"
   comment).
6. `keys.py`: mtxdb-first reads and the write route; no steady-state SQL dual
   writes; keep the existing caches and replication invalidation.
7. Backfill from `server_keys_json` and `server_signature_keys` (idempotent).
8. Verification: crash/recovery, concurrent writers, read-only visibility,
   `TestMSC4499Key`. Then remove SQL fallbacks.

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
