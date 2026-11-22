# Remote server keys in mtxdb: findings and migration plan

Status: design note (nothing implemented). Decisions A-C below are open. Scope:
move remote signing-key storage from SQL (`server_signature_keys`,
`server_keys_json`) into the mtxdb `server-info` pool (`mtpl-srvinfo`), and a
Python-only migration for an already-deployed instance.

Related: `2026-11-20-join-key-prefetch-refactor.md`. The implemented
`Keyring.prefetch_keys` fills the same key store through the existing store,
notary and origin fetcher chain. It is independent of this change and would
benefit from the bulk `read_server_keys_by_ids` once keys move to mtxdb.

## Scope options considered

1. Wire remote server keys into mtxdb (chosen direction; this note): bindings,
   `keys.py` rewrite, config switch with startup hard-fail, migration, tests.
2. Only make the empty pool legible: a log or doc change so an empty
   `mtpl-srvinfo` is not mistaken for data loss. No storage change.
3. Finish the key prefetch instead (see the related doc): per-join
   origin/request budget, wiring the other verification callers
   (`_check_sigs_and_hash_and_fetch`, get_room_state-style paths), a unit test
   for `signature_requests_for_pdus` (restricted-join case), the 700-origin
   benchmark, then the Congruent, Dendrite and partial-state changes. Still
   open; not superseded by option 1.

## Recommended picks (not yet confirmed)

A1 writer-only, B1 per-origin payload, C1 skip migration for a fresh instance,
or C2/C4 (Python script plus CLI import) for the deployed instance. The user has
not answered A-C. The earlier note that there are no deployments is stale: a
deployed instance exists, so keep existing bindings (C2/C4) unless told
otherwise.

## Why `mtpl-srvinfo` is empty today

Nothing in Sithnapse reads or writes the pool.
`synapse/storage/databases/main/keys.py` stores remote keys in SQL only. The
only references to `server_info` are stats reporting (`embedded_common.py`) and
test harnesses. The read-only open path even says "ServerInfo has no read path
in Synapse yet" and opens it as a durable snapshot. An empty listing in a backup
is the expected state, not data loss and not WAL reclaim. The signing key from
`signing_key_path` is our own key and is unrelated to this pool.

## What the mtxdb crate already provides

Pinned rev `30c659dae8` (`Cargo.lock`) has `mtxdb::server_keys`, exported
publicly (`src/lib.rs`). It implements the MSC4499 binding rules, so Python
should call it, not re-implement it:

- `update_server_keys(db, KeyUpdate)`: merge a response, First Seen Wins,
  provisional (notary) vs permanent (direct) tiers, promotion window, digest
  cap, retired-key eviction. Returns the kept keys plus `KeyIssue`s to log.
- `read_server_key`, `read_server_keys`, `read_server_keys_by_ids` (bulk,
  `get_many`), `read_notary_payload`, `put_notary_payload`.
- `KeyLimits::default()`: `retired_retained = 3000`, equal to
  `MAX_RETIRED_SERVER_KEYS` in `keys.py`, so eviction behaviour matches.

Not exposed to Python: no `#[pyfunction]` wraps any of it
(`rust/src/database/mtxdb_syn.rs`).

## Blockers found (need decisions)

### A. Worker writes

`update_server_keys` needs a `Database` handle. Sithnapse holds one
(`shared_database`) only in the WAL writer; read-only workers have `None`, and
their `server-info` handle is a deliberate snapshot without the read-journal
overlay (opening it through the overlay races checkpoint coverage). Today any
worker that verifies events and fetches keys writes straight to Postgres.

1. Writer-only (recommended): route key writes to the main process; fix the
   read-only `server-info` open path so workers read fresh keys.
2. Single-process only: fail at startup if the mtxdb key store is configured
   with workers.

### B. Notary endpoint data

`/_matrix/key/v2/query` (`remote_key_resource.py`) serves the stored `key_json`
per key with `from_server`, added and valid-until timestamps. mtxdb keeps one
verbatim notary payload per origin, replaced when a newer one arrives.

1. Serve the stored per-origin payload (recommended): smallest change, exact
   signed bytes. An older response holding keys absent from the latest one can
   disappear; a notary query may refetch them or return fewer. Key records are
   still kept.
2. Rebuild `key_json` from key records: keeps every key, more logic, loses the
   original bytes and signatures, so it must re-sign on demand.
3. Extend the crate with per-key verbatim JSON: closest to today's per-source
   behaviour, but a cross-repo change to mtxdb `dev`.

### C. Migration

See the next section. An mtxdb key record carries more than the key: tier
(provisional or permanent), `first_seen_ms`, `last_active_ms`, `corroborated`,
`inferred_retired`, `expired_ts`, a digest binding that survives eviction, and
an optional verified event-time range. SQL has `from_server`, `seen_active`,
`ts_added_ms`, `ts_valid_until_ms` and the stored response JSON.

1. Skip (cold start): previously bound keys come back as new; First Seen Wins
   restarts empty.
2. Import keys only: derive tier and corroboration from the SQL columns.
3. Import keys plus verified observations: add a signature-verification pass
   over event history. Slow on a large database.
4. Use the existing mtxdb CLI import with a keys export.

For a deployed instance, options 2 and 4 combine: a Python script produces the
keys export and the CLI imports it (below).

## Python conversion script (planned, for the deployed instance)

mtxdb already defines a keys export (`mtxdb-cli/src/interchange.rs`,
`export_schema.rs`): JSONL, no header or end record, every line
`"t": "server_keys"`. `mtxdb import` runs `import_keys_export`, which writes
into the `server-info` pool in the layout the server will read. No Rust change
or new binding is needed for the migration itself.

Key line fields: `origin`, `kind` (`provisional` | `permanent`), `key_id`,
base64 `key`, `valid_until_ts`, `source`, `ts_added_ms`; optional `expired_ts`,
`first_seen_ms`, `last_active_ms`, `corroborated`, `inferred_retired`,
`promote_by_ms` (provisional only). An evicted key carries base64 `digest` and
no `key`. A `kind: "notary"` line carries `origin` and base64 `payload`, the
exact bytes.

Planned script, `scripts-dev/export_server_keys_for_mtxdb.py` (not written):

1. Read `server_signature_keys` and `server_keys_json` read-only. Run against a
   copy first.
2. Per `(origin, key_id)` keep the earliest-bound body (First Seen Wins).
   `from_server == origin` maps to `permanent`; any other source maps to
   `provisional` with a chosen `promote_by_ms`. `seen_active` maps to
   `corroborated`. Keys under `old_verify_keys` get `expired_ts` from the stored
   JSON.
3. One `notary` line per origin from the newest stored `key_json`, byte for byte
   (re-encoding breaks the signature).
4. Write `keys.jsonl`; run the mtxdb CLI import; compare its "imported N
   signing-key records (P provisional, Q permanent, R notary)" line against the
   source counts.

Limits:

- The migration only moves data. The server keeps reading SQL until the bindings
  and the `keys.py` switch exist. Config must be one authoritative backend, no
  silent dual-write, and startup must fail hard if mtxdb is configured but does
  not hold the migrated keys.
- Per-source notary copies collapse to one per origin (B1).
- Verified event-time ranges are left empty unless the CLI's room-import
  verification pass supplies them.
- Open input: where the Postgres DSN comes from, and the copy-versus-live
  question.

## Implementation order (once A and B are decided)

1. `#[pyfunction]` wrappers in `mtxdb_syn.rs`: `update_server_keys` (returning
   keys and issues), the three reads, notary read and put. Follow the
   `event_json_*` pattern: `py.detach`, `assert_writable`, refresh-on-miss in
   read-only workers.
2. `keys.py`: in mtxdb mode drop the Python merge block (collision handling,
   upsert, eviction) and call the crate. Keep cache invalidation and the
   replication stream calls.
3. Config and startup check.
4. Migration script and its test with synthetic rows.
5. Run the existing keyring and storage key tests against the mtxdb backend.

## Earlier discussion carried over (join-time key acquisition)

Full detail lives in `2026-11-20-join-key-prefetch-refactor.md`; summarised here
so this note stands alone.

### Which project wins for a 700-origin full join

Congruent, by structure, not by measurement. It collects keys from the whole
`send_join` response (`acquire_events_pubkeys`), then does notary batches, then
a `FuturesUnordered` origin fan-out, then validates. Sithnapse's keyring
coalesces only same-origin requests (`_fetch_keys_queue` is sharded by
`server_name`), which barely helps at one key per origin. Dendrite's `FetchKeys`
takes a `map[request]ts`, but nothing shows a whole join is submitted as one
set. Nothing has been benchmarked.

### Track A (safe) and Track B (future)

- Track A keeps trust semantics: every installed event is still
  signature-verified; only key acquisition gets faster.
- Track B, deferred verification, needs strict quarantine of unverified events
  and its own design. Not proposed.

### Implemented in Sithnapse

`Keyring.prefetch_keys` (100 origins in flight, 30s non-cancelling wait),
`signature_requests_for_pdus` in `federation_base.py`, and the `send_join`
wiring for the normal and MSC4242 state-DAG paths. Three tests added. Commits
`c00ecf0938` and `93355a11bb`. Constants untuned.

### Still open on Sithnapse

- Notary ordering by measured coverage and latency, primary plus fallback (today
  all notaries get the same batch concurrently).
- Total per-join origin/request budget; a timed-out prefetch can overlap
  verification, and behaviour for oversized responses is undefined.
- Prefetch in the other callers (`_check_sigs_and_hash_and_fetch`,
  get_room_state-style paths).
- Distinguish "key unavailable" from "signature invalid" in logs and errors.
- Immutable key snapshot and CPU-pool signature verification.
- Unit test for `signature_requests_for_pdus` (restricted-join case) and an
  integration test through `send_join`.
- The 700-origin benchmark (notary coverage 100/50/0%, dead origins 0/5/20%,
  RTTs, cold vs warm cache) to tune the constants. Recommended first.

### Congruent and Dendrite changes (not started)

- Congruent: measure per-key `acquire_locals` cost and add an mtxdb multi-get if
  it matters; primary plus fallback in `acquire_notary` instead of sending every
  notary the full missing set; a concurrency cap and phase deadline on
  `acquire_origins`; make `get_event_keys` use the prefetched snapshot instead
  of serial per-origin lookups.
- Dendrite: first find where a join response triggers key requests; default
  order is perspectives first (`PreferDirectFetch` makes direct first), so leave
  it for bulk work; check first-hit-wins concurrency across fetchers, the 30s
  per-fetcher context, and `DirectKeyFetcher` concurrency in gomatrixserverlib.
- Partial-state joins (MSC3706) are a separate lever with their own semantics;
  benchmark them separately.

### Hostile-origin note

The risk of a response naming many origins already exists in the per-event
verification path. Prefetch makes that fan-out deliberate and bounded per phase
(100 in flight), but there is no total per-join budget.
