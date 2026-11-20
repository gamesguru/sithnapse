# Join-time signing-key acquisition: prefetch refactor

Status: design proposal (not implemented, not benchmarked). Revised after
review. Scope: Sithnapse `synapse/crypto/keyring.py`,
`synapse/federation/federation_client.py` (`send_join`), plus the equivalent
changes for Congruent and Dendrite.

The design has two tracks that must not be mixed:

- **Track A, a faster join with unchanged trust semantics.** Every event the
  join installs is still signature-verified. Safe; this is the main proposal.
- **Track B, deferred verification.** Accepting less-verified state. A protocol
  and storage change that needs its own design. Listed only as future work.

## Problem

A full `/send_join` returns thousands of state and auth-chain events. Their
signers are typically hundreds of distinct origins (e.g. 700), usually needing
one key each. Same-origin coalescing, which the current keyring is good at,
barely helps here. What matters is how fast we can get ~700 keys from ~700
origins without letting dead servers hold up the join.

## Current behaviour (as read from code, 2026-11)

|               | Key-acquisition path in a join                                                                                                                                                               | Gaps                                                                                                                                                                                                                                                        |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Sithnapse** | `send_join` calls `Keyring.prefetch_keys` over the full state/auth response before concurrent signature verification. It fetches through the normal store, notary, then direct-origin chain. | Prefetch has a per-phase concurrency bound, but no total per-join origin/request budget. Its timeout stops waiting, not background work, so prefetch and verification can overlap. Notaries are still queried concurrently with the same batch.             |
| **Congruent** | `acquire_events_pubkeys` collects keys from the whole response, then notary batches, then `FuturesUnordered` origin fan-out, then validates.                                                 | Local availability appears to be checked per key (`acquire_locals`). Each notary gets the entire missing set. Origin fan-out has no cap or phase deadline. Per-event fallback (`get_event_keys`) is serial. Whether a mtxdb multi-get exists is unverified. |
| **Dendrite**  | `VerifyEventSignatures` calls `KeyRing.FetchKeys` with a `map[request]ts`: local keys, then DB/cache, then each fetcher in turn.                                                             | Fetcher order is perspectives first, direct last by default (`PreferDirectFetch` true makes direct first). No evidence yet that a whole join response is submitted as one request set. Parallelism _within_ a fetcher is unverified.                        |

Verdict: Congruent has the clearest structure for this workload (explicit
whole-response prefetch), but none is optimal and none is measured.

## Track A: safe fast path

### 1. Collect and dedupe the requests

Walk the entire send_join response once and build a deduped set of
`(origin, key_id, minimum_valid_until_ts)`:

- `minimum_valid_until_ts` is the maximum `origin_server_ts` over the events
  that need that key (room version 5+), or 0 for v1-4.
- Include sender domain, event-ID domain for v1/v2, and restricted-join
  authorising server.
- Verification does not start until this phase finishes.

### 2. Read local/cache results in bulk, where the API supports it

- Use a multi-get against the key store only if the storage layer provides one.
  Otherwise, add it as a prerequisite and measure whether the per-key reads are
  actually significant before doing so.
- Classify each request as **fresh**, **stale** (cached but `valid_until_ts` too
  early for the events that need it) or **missing**.
- Skip origins currently in negative-cache backoff (MSC4499
  `KeyFetchBackoffCache`) when fetching. The events they signed fall under the
  same failure/retry semantics as step 5, not a new one.
- Local server keys short-circuit as today.

### 3. Query notaries with bounded batching and fallback

- Cap batch size per `/_matrix/key/v2/query` call.
- Order notaries by **measured** coverage and latency, tracked per notary, and
  choose a primary. Do not statically partition origins by hash: a notary that
  cannot answer its slice adds a full round trip.
- Send the primary the whole set; send only its misses to the next notary.
  Bounded hedging, firing a second notary after a latency threshold without
  cancelling the first, is an option to benchmark, not a default.
- Keep per-entry validation (notary signature, MSC4499 duplicate keys). A bad
  entry skips only that origin.
- Today every notary is queried concurrently with the same set. That costs
  duplicate work but wins on latency; benchmark before replacing it.

### 4. Fetch remaining origins concurrently, bounded

- Remaining misses go direct to `/_matrix/key/v2/server`.
- Configurable concurrency cap and an overall phase deadline. Start values for
  the benchmark (not defaults): cap 64-128, per-origin timeout 3-10s, phase
  deadline 10-30s. Right values depend on DNS/TLS setup, geography, retries and
  destination rate limits.
- Failures update the backoff cache.

### 5. Verify as usual

- Build an immutable `origin -> key_id -> (key, valid_until, expired)` snapshot
  and have per-event checks read only that (no I/O, no locks). Run signature and
  hash checks on a CPU pool, and use batch Ed25519 verification where available.
- If required keys are still unavailable after step 4, **keep the join's
  existing failure, retry, or incomplete-state semantics.** The events are not
  installed as verified state. Concretely:
  - Sithnapse: `_check_sigs_and_hash_and_fetch_one` failures drop the event from
    the signed set, as now.
  - Congruent: unvalidatable state events are dropped from joined state, as now.
  - Dendrite: `VerifyEventSignatures` errors fail the verification, as now.
- A cheap, safe improvement here is to distinguish "key unavailable" from
  "signature invalid" in logs, metrics and error types. It changes reporting
  only, not trust decisions.

### 6. Partial-state joins are a separate lever

- Partial-state joins (MSC3706) reduce how many events, and so origins, must be
  verified at join time. But they are an intentional join mode with their own
  semantics, not a transparent speed switch. Optimize and benchmark them
  separately.
- Resync can reuse steps 1-5 in bounded chunks.

## Per-project changes (Track A)

### Sithnapse

1. Add `KeyRing.prefetch(requests, deadline)` implementing steps 1-4 and call it
   in `federation_client.send_join` immediately before the
   `concurrently_execute` calls, so `verify_event_for_server` becomes a cache
   hit.
2. `ServerKeyFetcher._fetch_keys`: bound the concurrency and add the phase
   deadline (step 4).
3. `PerspectivesKeyFetcher._fetch_keys`: add per-notary coverage/latency
   tracking and the primary + fallback ordering (step 3), after benchmarking
   against the current concurrent broadcast.
4. Inspect `StoreKeyFetcher` for a real multi-get before claiming one (step 2).
5. Split "key unavailable" from "signature invalid" in error reporting (step 5).

### Congruent

1. Measure whether the per-key `acquire_locals` cost matters; if so, add a mtxdb
   multi-get (the `event_key_metadata_for_events` approach may be reusable; not
   yet checked).
2. `acquire_notary`: primary + fallback instead of sending the full missing set
   to every notary, after benchmarking.
3. `acquire_origins`: add a concurrency cap and a phase deadline around
   `FuturesUnordered`.
4. Make `get_event_keys` use the prefetched snapshot rather than per-origin
   sequential lookups.

### Dendrite

1. First verify where a join response triggers key requests. The `FetchKeys` map
   API alone does not show a whole join is submitted as one set. Only then add a
   prefetch before `PerformJoin` verification.
2. Default order is already perspectives first; `PreferDirectFetch` is the
   opt-in direct-first. Leave it perspective-first for bulk workloads.
3. Check whether fetchers could run concurrently with first-hit-wins, and
   whether the 30s per-fetcher context should shrink.
4. Check how much concurrency `DirectKeyFetcher` in gomatrixserverlib allows and
   cap it if unbounded.

## Track B: deferred verification (future work, not part of this proposal)

Letting a join finish before all keys are fetched would speed it up, but it
cannot be done by "persist as unverified and retry later" alone. An event whose
signing key is unavailable is not trusted room state. A design would need, at
minimum:

- A strict quarantine: unverified events must not affect state resolution, auth
  checks, membership, joined hosts, or anything client-visible until verified.
- A defined result when verification later fails (rewrite state? re-resolve?
  notify clients?).
- A decision on which events may be deferred at all; the create, power-levels,
  join-rules and the memberships they depend on must be verified before the join
  is accepted.
- Storage for pending events and a background re-verifier.

This is a substantial protocol and storage change. It needs its own design and
review; it is not a cheap optimization.

## Expected impact (hypotheses to test)

- Likely largest: explicit prefetch with a bounded, deadline-limited origin
  phase (steps 1 and 4), because it makes batching deterministic and stops
  stragglers fanning out without limit.
- Notary ordering by measured coverage (step 3) saves duplicate work but may
  cost latency; undecided until measured.
- Partial-state (step 6) changes the problem size rather than the speed of the
  same work.

## Benchmark plan

Fixture: synthetic send_join response with 700 distinct origins, one key each.
Variables: notary coverage (100% / 50% / 0%), fraction of dead origins (0 / 5 /
20%), notary RTT, origin RTT, cold vs warm cache. Metrics: time to join
acceptance, time until all response events are verified, outbound requests by
destination, peak in-flight requests, cache hit rate, events that could not be
verified.

Compare at least: current behaviour; explicit prefetch; prefetch plus bounded,
deadline-limited fetching; prefetch plus coverage-aware notary ordering. This
also answers whether Congruent's deterministic batch actually beats Sithnapse's
concurrent queue on a realistic 700-origin response.

## Sithnapse code-reading corrections and status (implemented)

Re-read `synapse/crypto/keyring.py` and `batching_queue.py` directly:

- `Keyring._fetch_keys_queue` is sharded by `server_name`, so that queue only
  coalesces requests for the _same_ origin. Cross-origin batching happens in the
  per-fetcher queues (`StoreKeyFetcher`, `PerspectivesKeyFetcher`, which use the
  shared key), one reactor tick at a time, so it is real but non-deterministic.
  `ServerKeyFetcher.get_keys` shards per origin, so direct fetches never batch.
- `StoreKeyFetcher` already does a bulk read (`get_server_keys_json` is a
  `list_name` cached method), so step 2 needs no new storage API in Sithnapse.
- The negative cache (`KeyFetchBackoffCache`) short-circuits a second attempt
  for a failed origin, so a failed prefetch is not double-counted by the later
  verification.

Implemented (steps 1, 2 and the bounded part of 4; steps 3 and 5 not yet):

- `Keyring.prefetch_keys(requests)`: de-duplicates `(origin, key_id, min ts)`,
  skips our own server, looks each origin up through the normal fetcher chain
  with `PREFETCH_CONCURRENCY` (100) in flight, and stops waiting after
  `PREFETCH_TIMEOUT_SECONDS` (30) without cancelling. Errors are swallowed; it
  only fills the store, so verification and trust decisions are unchanged.
- `signature_requests_for_pdus` in `federation_base.py` mirrors the three checks
  in `_check_sigs_on_pdu`; `send_join` calls prefetch before verifying
  state/auth_chain (and the MSC4242 state DAG).
- Both constants are untuned starting points. Not yet benchmarked. The
  100-origin concurrency bound applies to prefetch work; it is not a cap on
  total distinct origins or total key requests in a join. The old verification
  path also looked up origins named by the response, so the hostile-origin
  exposure is not new. However, prefetch makes that fan-out deliberate, and
  after the 30-second wait expires its background work can overlap verification.
  A separate per-join origin/request budget should bound aggregate work; define
  its behavior for oversized responses before implementing it.

## Open questions

- Do the Sithnapse store fetcher and the Congruent mtxdb adapter offer a real
  multi-get, and is per-key reading a measurable cost?
- Best primary-notary selection: static config, observed coverage, observed
  latency?
- Hedge threshold, concurrency cap and phase deadline values, and whether they
  should scale with response size.
- Where in Dendrite's join path keys are requested, and its `DirectKeyFetcher`
  concurrency.
- Whether Track B is worth designing at all.

## Caveats

This document comes from reading code paths, not measurements.
Congruent/Dendrite details were read from sibling checkouts and should be
re-verified before relying on them.
