# Join-time signing-key acquisition: prefetch refactor

Status: design proposal (not implemented, not benchmarked). Scope: Sithnapse
`synapse/crypto/keyring.py`, `synapse/federation/federation_client.py`
(`send_join`), plus the equivalent changes for Congruent and Dendrite.

## Problem

A full `/send_join` returns thousands of state and auth-chain events. Their
signers are typically hundreds of distinct origins (e.g. 700), usually needing
one key each. Same-origin coalescing, which the current keyring is good at,
barely helps here. What matters is how fast we can get ~700 keys from ~700
origins without letting dead servers hold up the join.

## Current behaviour (as read from code, 2026-11)

|               | Key-acquisition path in a join                                                                                                                                                                                                       | Gaps                                                                                                                                                                                            |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Sithnapse** | `send_join` runs `concurrently_execute(_execute, state+auth_chain, 10000)`. Each event calls `keyring.verify_event_for_server`, which feeds a `BatchingQueue`. Fetchers run in order: store, perspectives (notaries), direct origin. | No explicit prefetch; batching depends on what reaches the queue together. Every notary gets the full set. `ServerKeyFetcher` fan-out has no concurrency cap.                                   |
| **Congruent** | `acquire_events_pubkeys` collects keys from the whole response, then notary batches, then `FuturesUnordered` origin fan-out, then validates.                                                                                         | `acquire_locals` checks cache one key at a time. Every notary gets the entire missing set. Origin fan-out is unbounded with no phase deadline. Per-event fallback (`get_event_keys`) is serial. |
| **Dendrite**  | `VerifyEventSignatures` calls `KeyRing.FetchKeys` with a `map[request]ts`. Local, then DB/cache, then fetchers sequentially (direct first by default).                                                                               | No response-wide prefetch. Fetchers run one after another with a 30s context each. Direct-before-perspective ordering is the wrong default for bulk.                                            |

Verdict: Congruent has the best shape (explicit whole-response prefetch), but
none is optimal.

## Target design: the 8 steps

### 1. Prefetch before verifying

Walk the entire send_join response once and build a deduped set of
`(origin, key_id, min_valid_ts)`:

- `min_valid_ts` is the maximum `origin_server_ts` over the events that need
  that key (room version 5+), or 0 for v1-4.
- Include sender domain, event-ID domain for v1/v2, and restricted-join
  authorising server.
- Verification must not start until this phase completes or hits its deadline.

### 2. One bulk cache read

- Single multi-get against the key store (mtxdb in this tree), not one lookup
  per key.
- Classify each request as **fresh**, **stale** (cached but `valid_until_ts` too
  early for the events that need it) or **missing**.
- Drop origins currently in negative-cache backoff (MSC4499
  `KeyFetchBackoffCache`) from the fetch set immediately; their events go to
  step 7's degraded path.
- Local server keys short-circuit as today.

### 3. Partition across notaries, don't broadcast

- Split the stale+missing set across the configured notaries (stable hash on
  origin). Cap batch size per `/_matrix/key/v2/query` call.
- Only origins a notary failed to satisfy (or omitted) go to the next notary.
- Hedge: if a notary has not answered by a short threshold, send its slice to
  another notary without cancelling the first.
- Validate notary signatures and MSC4499 duplicate-key rules per response entry,
  as now. A bad entry skips only that origin.

### 4. Bounded origin fallback

- Remaining misses go direct to `/_matrix/key/v2/server`.
- Semaphore of roughly 64-128 concurrent requests; per-origin timeout 3-5s (not
  the 10s x multiple retries of the generic path).
- Overall phase deadline (e.g. 10-15s). Anything unresolved at the deadline is
  left for step 7. Failures update the backoff cache.
- Order the queue so origins that signed auth-critical events go first (see step
  7).

### 5. One batched write

- Persist everything fetched in a single batched write (one transaction / one
  mtxdb batch) after steps 3 and 4, not per response.
- Keep the existing "favour largest `valid_until_ts`" merge rules.

### 6. Verify against an immutable snapshot, in parallel

- Build an immutable `origin -> key_id -> (key, valid_until, expired)` snapshot
  from steps 2-5. Per-event checks only read this snapshot: no I/O, no locks.
- Run signature and hash checks on a CPU pool (in Sithnapse the Rust side; in
  Congruent `spawn_blocking`/rayon). Use batch Ed25519 verification where
  available.
- Events with no usable key in the snapshot are not fetched again here; they go
  to step 7.

### 7. Degrade, don't fail

- Do not reject an event just because a key could not be fetched in time.
  Persist it as unverified/outlier and queue a background retry; re-verify and
  promote or reject when the key arrives. (This matches the existing comment in
  `process_remote_join` that failing to validate signatures now does not mean
  never.)
- Verify auth-critical events first: `m.room.create`, power levels, join rules,
  and the memberships those depend on. The join can complete once those verify,
  so stragglers never block it.
- A key that is fetched and proves the signature invalid is a real rejection; a
  key that is merely unavailable is not.

### 8. Avoid the work where possible

- Prefer partial-state joins (MSC3706): most member events, which carry most of
  the distinct origins, aren't verified at join time and are resolved during
  resync.
- During resync, run the same steps 1-7 in bounded chunks instead of one huge
  batch.

## Per-project changes

### Sithnapse

1. Add `KeyRing.prefetch(requests, deadline)` implementing steps 1-5, and call
   it in `federation_client.send_join` immediately before the
   `concurrently_execute` calls.
2. `PerspectivesKeyFetcher._fetch_keys`: replace "query every notary with
   everything" with partition + waterfall + hedge (step 3).
3. `ServerKeyFetcher._fetch_keys`: replace unbounded `yieldable_gather_results`
   with a bounded gather and the shorter timeout (step 4).
4. Make `StoreKeyFetcher` expose a multi-get used by prefetch (step 2).
5. Reuse `process_request`'s checks against the snapshot after prefetch, so
   `verify_event_for_server` becomes a cache hit.
6. Step 7: let `_check_sigs_and_hash_and_fetch_one` distinguish "key
   unavailable" from "signature invalid" and route the former to the
   outlier/retry path.

### Congruent

1. Replace serial `acquire_locals` with one mtxdb multi-get (reuse the approach
   in `event_key_metadata_for_events`).
2. `acquire_notary`: partition the missing set across `trusted_servers()` and
   waterfall instead of sending all of it to each notary.
3. `acquire_origins`: add a semaphore and a phase deadline around
   `FuturesUnordered`.
4. Make `get_event_keys` use the prefetched snapshot rather than per-origin
   sequential lookups.
5. Treat unavailable keys as retryable in `concurrent_validate_and_add_events`
   (currently an unvalidatable state event is dropped from joined state).

### Dendrite

1. Add a prefetch step before `PerformJoin`/`VerifyEventSignatures` that calls
   `KeyRing.FetchKeys` once with the whole request set.
2. Default perspective-first for bulk requests (`PreferDirectFetch: false`).
3. Run fetchers concurrently with first-hit-wins instead of sequentially, and
   shorten the 30s per-fetcher context.
4. Check how much concurrency `DirectKeyFetcher` in gomatrixserverlib allows;
   cap it explicitly if it is unbounded.
5. Same available-vs-invalid distinction as step 7.

## Expected impact (hypotheses to test)

- Biggest: step 3 (no broadcast, no redundant notary work) and step 7 (join not
  blocked by stragglers/dead servers). Both are cheap.
- Medium: steps 1-2 (deterministic batching, bulk read) and step 4 (bounded
  concurrency avoids socket/CPU storms).
- Step 8 changes the problem size rather than the speed of the same work.

## Benchmark plan

Fixture: synthetic send_join response with 700 distinct origins, one key each.
Variables: notary coverage (100% / 50% / 0%), fraction of dead origins (0 / 5 /
20%), notary RTT, origin RTT, cold vs warm cache. Metrics: time to join
complete, time to all events verified, outbound requests, peak concurrent
sockets, notary bytes, events left unverified at the deadline. Compare: current
Sithnapse, prefetch only (1-2), +partitioned notaries (3), +bounded origins and
deadline (4), +degraded path (7).

## Open questions

- Hedge threshold and phase deadline defaults; should they be per-room-size?
- Where unverified-pending state lives (an event status in mtxdb metadata vs a
  separate queue).
- Whether notary partitioning by hash should weight notaries by observed
  latency.
- Dendrite's actual `DirectKeyFetcher` concurrency (unverified).

## Caveats

This document comes from reading code paths, not measurements.
Congruent/Dendrite details were read from sibling checkouts and should be
re-verified before relying on them.
