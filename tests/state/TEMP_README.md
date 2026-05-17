# State Resolution Tests

## Background: The Supplemental Merge Bug (V2, Room Versions 1–11)

State resolution V2 has a "supplemental merge" step in `_iterative_auth_checks`
that overlays the accumulated _resolved state_ on top of each event's own auth
chain before running auth checks. This can cause **state resets** — legitimate
members get evicted when join_rules change across a DAG fork.

### Mechanism

1. Two forks diverge. Fork A changes `join_rules` from `public` →
   `invite`. Fork B is a stale server that doesn't have the change.
2. At merge, both `join_rules` and some memberships end up in the conflicted set.
3. The control pass resolves `join_rules` first (power event) — picks
   `invite` (newer timestamp).
4. The supplemental merge then **replaces** each leftover event's
   `join_rules=public` auth reference with the resolved `invite`.
5. A member who joined legitimately under public rules now fails auth against
   `invite` (not invited) → **evicted from state**.

This is the root cause of the catgirl.cloud state reset where `@bot:nutra.tk`
disappeared from room state despite being a legitimate member.

### V2.1 Fix (MSC4297, Room Versions HydraV11 / V12+)

Two changes, both gated on `StateResolutionVersions.V2_1`:

1. **Skip the supplemental merge** — each event authenticates purely against its
   own `auth_events` chain, not the accumulated resolved state.
2. **Start iterative auth from `{}`** instead of `unconflicted_state` — events
   populate base state organically from their own auth chains.

See `synapse/state/v2.py`, specifically `_iterative_auth_checks()`.

### Remediation for V11 Rooms

The V2.1 fix is gated behind `StateResolutionVersions.V2_1`, which only V12+
rooms use. **Standard V11 rooms still run V2 state resolution and remain
vulnerable to the supplemental merge bug.** There is no in-place fix because
room version semantics are immutable once shipped per the Matrix spec.

Options for affected V11 rooms:

1. **Room upgrade to V12** — the intended path. Creates a new room with V2.1
   state resolution. Members must re-join via the upgrade tombstone.
2. **Surgical DAG repair** — use `force-set-state` / `unreject-room` admin
   tools (continuwuity) to manually restore evicted membership on the affected
   server. This doesn't fix the algorithm, just patches the symptoms.
3. **Spec amendment** — propose retroactively changing V11 to use V2.1.
   Extremely unlikely to be accepted since it would change resolution behavior
   for all existing V11 rooms across the federation.

---

## Test Files

### `test_v2.py` — V2 State Resolution (Room Versions 1–11)

Existing upstream tests plus:

| Test                                      | What It Proves                                                                                                                                                   |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `test_state_reset_join_rules_eviction`    | The supplemental merge bug: Eve joins a public room, JR changes to `invite`, a stale fork forces conflict. V2 evicts Eve. This is the documented V2 deficiency.  |
| `test_v2_self_corrects_corrupted_state`   | The supplemental merge _upside_: a missing member is restored when join_rules stay `PUBLIC`, because the supplemental merge re-injects the correct auth context. |
| `test_v2_unauthorized_event_not_restored` | Safety proof: an unauthorized join from a rogue fork is correctly rejected by the supplemental merge.                                                            |

### `test_v2.py::DAGReplayTestCase` — Real-World DAG Replay

Replays 848 production events from `tests/state/remote-dag-tgmfqAWaBc978M80V9_nutra.tk-v11-merged.jsonl`
through Synapse's `resolve_events_with_store()` using `RoomVersions.V2`:

| Test                                           | Scenario                                                                                                      | Finding                                                                                                              |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `test_replay_nutra_tk_dag_bot_membership`      | Full DAG (omniscient view), traces `@bot:nutra.tk` through all 21 fork merges                                 | Bot survives — no eviction with complete information                                                                 |
| `test_replay_nutra_tk_dag_catgirl_perspective` | Bot removed from state at depth 336 (simulating catgirl's corrupted `/state` response), then replay continues | Bot **never recovers** — permanently absent. V2 has no mechanism to surface a member that isn't in the conflict set. |
| `test_replay_nutra_tk_dag_catgirl_v21`         | Same corrupted state, replayed through V2.1 (V12) AND V2 in the same test                                     | V2.1 **self-heals** at depth 412; V2 does NOT recover. Proves the fix works and that V2 is the broken baseline.      |

The JSONL contains real PDUs from the `#general:nutra.tk` room (V11), loaded
natively as V11 `FrozenEventV3` events. Non-canonical fields (`event_id`,
`signatures`, `unsigned`) are stripped; the `hashes` field is preserved as it's
part of the reference hash. Synapse recomputes event_ids from the content hash,
which are asserted to match the originals.

#### Baseline Verification (unpatched v2.py)

With the V2.1 supplemental merge patch reverted (`git show 788e6aedc2^:synapse/state/v2.py`),
the full suite was re-run to confirm:

| Test                                                        | Unpatched Result                                                  | Patched Result |
| ----------------------------------------------------------- | ----------------------------------------------------------------- | -------------- |
| `test_supplemental_merge_does_not_clobber_auth_chain` (V21) | **FAILED** — Bob evicted by supplemental merge                    | PASSED         |
| `test_replay_nutra_tk_dag_catgirl_v21`                      | PASSED — self-heals via start-from-`{}` (pre-existing V12 change) | PASSED         |
| All other tests (24)                                        | PASSED                                                            | PASSED         |

The catgirl self-healing is driven by the V2.1 start-from-empty-set change
(line 182 of v2.py), which is part of the broader V12 room version support.
The supplemental merge patch is specifically needed for the synthetic
join_rules eviction scenario (`test_supplemental_merge_does_not_clobber_auth_chain`).

### `test_v21.py` — V2.1 State Resolution (V12+)

| Test                                                  | What It Proves                                                                                                                                                                                                                                                                  |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `test_state_reset_replay_conflicted_subgraph`         | Dodgy `auth_events` citing old power levels don't cause PL regression. PL3 (bob:50, charlie:50) survives.                                                                                                                                                                       |
| `test_state_reset_start_empty_set`                    | Conflicted join_rules resolve correctly even when the sender's membership isn't conflicted.                                                                                                                                                                                     |
| `test_supplemental_merge_does_not_clobber_auth_chain` | **Key regression test**: Bob joins under PUBLIC, fork flips JR to INVITE. V2 would evict Bob (supplemental merge poisons his auth); V2.1 keeps him (auths against own chain).                                                                                                   |
| `test_conflicted_subgraph_preserves_power_levels`     | Ported from Complement (`TestMSC4297StateResolutionV2_1_includes_conflicted_subgraph`). Dodgy Eve citing old PLs doesn't regress PL state.                                                                                                                                      |
| `test_v21_cve_auth_bypass_without_supplemental_merge` | **Security proof**: removing the supplemental merge doesn't open a CVE. Eve can't replay old PLs to change the topic -- the control pass still resolves PLs first, and Eve is PL 0 under the resolved state.                                                                    |
| `test_v21_self_corrects_corrupted_state`              | Corrupted server missing a ban self-heals when merging with a healthy server.                                                                                                                                                                                                   |
| `test_incomplete_dag_picks_stale_membership`          | **Data completeness proof**: with incomplete DAG (missing rejoin), Eve resolves as LEFT (wrong). With complete DAG, Eve resolves as JOINED with updated profile. Same algorithm, different inputs -- proves state resets are a federation ingestion issue, not a state res bug. |

The V2.1 test harness (`get_resolution_and_verify_expected`) drip-feeds events
into the DB one-by-one, re-resolving after each persistence to verify:

- Mixed persisted/unpersisted event handling
- Auth chain difference stability across persistence boundaries
- Create event never appears in auth diff (guards against conflicted subgraph over-expansion)

---

## Test Output

```log
tests/state/test_v2.py::StateTestCase::test_ban_vs_pl PASSED
tests/state/test_v2.py::StateTestCase::test_join_rule_evasion PASSED
tests/state/test_v2.py::StateTestCase::test_mainline_sort PASSED
tests/state/test_v2.py::StateTestCase::test_offtopic_pl PASSED
tests/state/test_v2.py::StateTestCase::test_state_reset_join_rules_eviction PASSED
tests/state/test_v2.py::StateTestCase::test_topic PASSED
tests/state/test_v2.py::StateTestCase::test_topic_basic PASSED
tests/state/test_v2.py::StateTestCase::test_topic_reset PASSED
tests/state/test_v2.py::StateTestCase::test_v2_self_corrects_corrupted_state PASSED
tests/state/test_v2.py::StateTestCase::test_v2_unauthorized_event_not_restored PASSED
tests/state/test_v2.py::LexicographicalTestCase::test_simple PASSED
tests/state/test_v2.py::SimpleParamStateTestCase::test_event_map_none PASSED
tests/state/test_v2.py::AuthChainDifferenceTestCase::test_get_power_level_for_sender PASSED
tests/state/test_v2.py::AuthChainDifferenceTestCase::test_multiple_unpersisted_chain PASSED
tests/state/test_v2.py::AuthChainDifferenceTestCase::test_simple PASSED
tests/state/test_v2.py::AuthChainDifferenceTestCase::test_unpersisted_events_different_sets PASSED

tests/state/test_v2.py::DAGReplayTestCase::test_replay_nutra_tk_dag_bot_membership
============================================================
REPLAY: 848 events, 21 merges
Bot was NEVER evicted with full DAG
Final: join (depth=820)
============================================================
PASSED

tests/state/test_v2.py::DAGReplayTestCase::test_replay_nutra_tk_dag_catgirl_missing_events PASSED
tests/state/test_v2.py::DAGReplayTestCase::test_replay_nutra_tk_dag_catgirl_perspective PASSED
Correct state at depth 336:
  Bot in state: True
  Bot membership: join (depth=24)
  Corrupted state: removed bot (had_bot=True)
============================================================
CATGIRL SIMULATION:
  Bot removed from state at depth 336
  Bot NEVER recovered
  Final: join (depth=820)
============================================================

tests/state/test_v2.py::DAGReplayTestCase::test_replay_nutra_tk_dag_catgirl_v21 PASSED
tests/state/test_v2.py::V12DAGReplayTestCase::test_replay_v12_nex_missing_events SKIPPED
tests/state/test_v21.py::StateResV21TestCase::test_conflicted_subgraph_preserves_power_levels PASSED
tests/state/test_v21.py::StateResV21TestCase::test_incomplete_dag_picks_stale_membership PASSED
tests/state/test_v21.py::StateResV21TestCase::test_state_reset_replay_conflicted_subgraph PASSED
tests/state/test_v21.py::StateResV21TestCase::test_state_reset_start_empty_set PASSED
tests/state/test_v21.py::StateResV21TestCase::test_supplemental_merge_does_not_clobber_auth_chain PASSED
tests/state/test_v21.py::StateResV21TestCase::test_v21_cve_auth_bypass_without_supplemental_merge PASSED
tests/state/test_v21.py::StateResV21TestCase::test_v21_self_corrects_corrupted_state PASSED

================== 27 passed, 1 skipped, 9 warnings in 24.53s ==================
```

### Interpreting the DAG Replay Results

The two `DAGReplayTestCase` tests replay 848 real production events from
`#general:nutra.tk` (room version 11) through Synapse's V2 state resolution.

**Full DAG replay** (`test_replay_nutra_tk_dag_bot_membership`): With the
complete DAG visible (all 848 events from all servers), V2 state res never
evicts `@bot:nutra.tk`. The bot has a clean auth chain citing
`join_rules=public`, and the supplemental merge actually _helps_ here by
re-injecting correct auth context at each fork merge. 21 merges, zero
evictions.

**Catgirl perspective** (`test_replay_nutra_tk_dag_catgirl_perspective`):
catgirl.cloud joined at depth 336 and received state via `/state` that was
missing the bot's membership. Once the bot is absent from catgirl's state, it
is **permanently lost** — V2 state resolution never naturally surfaces a member
that isn't in the conflict set. The `Final: join (depth=820)` is from the bot
re-joining later, not from state res recovering it.

| Scenario                                                               | Bot evicted?      | Root cause                                             |
| ---------------------------------------------------------------------- | ----------------- | ------------------------------------------------------ |
| Full DAG (omniscient view)                                             | No                | Supplemental merge works correctly with public JR      |
| Catgirl perspective (corrupted initial state)                          | Yes — permanently | Missing from initial state, never enters conflict set  |
| Synthetic fork with JR change (`test_state_reset_join_rules_eviction`) | Yes               | Supplemental merge poisons auth with resolved `invite` |

The catgirl bug is two problems composing: (1) corrupted initial state from
`/state` or `/send_join`, and (2) V2 has no mechanism to recover state that was
never conflicted. The `heal-room` / force re-resolution admin command is the
only recovery for case 2. The V2.1 fix addresses the supplemental merge poison
case, but the corrupted initial state problem is a separate ingestion-layer
issue.

**IMPORTANT**: The `test_replay_nutra_tk_dag_catgirl_v21` test proves V2.1
self-heals corrupted _state tracking_ (bot removed from state_at but events
still exist in event_map). This is NOT the same as missing events. The
`test_replay_nutra_tk_dag_catgirl_missing_events` test proves that when events
are completely absent from the DAG, neither V2 nor V2.1 can recover -- state
resolution cannot surface a member whose events are backfilled or isolated
by a discontinuity.

### Category 3: Per-Server Auth Chain Divergence

The `test_incomplete_dag_picks_stale_membership` test proves a third failure
mode observed in production (c10y eggtopic room, `@stratself:muoi.me`): servers
that rejected events in a user's auth chain (content hash mismatch, signature
failure, transient fetch failure) permanently resolve that user's membership to
a stale or absent state. With ~160 membership updates forming a 160-link auth
chain, the probability of at least one link being rejected on any given server
approaches 1. The fix is not in state resolution (which is correct given its
inputs) but in the federation ingestion layer: aggressive auth chain
pre-fetching, retry on transient failures, and admin tools (`unreject-room`,
`set-state-event`) for manual reconciliation.

### Category 4: V12 Federation Gaps (Spam Attack Aftermath)

**Room**: `!sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE` (unredacted lounge, V12)
**Scale**: 81K events, 2494 fork merges, 100+ servers

**Forensic finding**: `ruma-lean` confirms V2 and V2.1 produce **identical**
resolved state (1789 events) on the full merged DAG. Both include
`@nex:nexy7574.co.uk` as joined with the correct avatar. The divergence between
servers is purely data completeness:

| Server             | `@nex:nexy7574.co.uk` (main)      | `@nex:synapse.nexy7574.co.uk` |
| ------------------ | --------------------------------- | ----------------------------- |
| unredacted.org     | [ok] depth 69605, correct avatar  | [ok] depth 31795              |
| matrix.org         | [!] depth 31842, **stale** avatar | [ok] depth 31795              |
| starstruck.systems | [ok] present                      | [x] **missing entirely**      |

Every server has a different "swiss cheese" pattern of missing events from the
spam attacks. No state resolution algorithm change can recover events that were
never ingested. The fix is ingestion-side: multi-server backfill with auth chain
pre-fetching on fork merges.

---

## Running

```bash
# All state res tests
.venv/bin/python -m pytest tests/state/test_v2.py tests/state/test_v21.py -v -s

# Just the DAG replay (requires JSONL file in tests/state/)
.venv/bin/python -m pytest tests/state/test_v2.py::DAGReplayTestCase -v -s

# Just the V2.1 fix validation
.venv/bin/python -m pytest tests/state/test_v21.py -v -s

# Just the V2 supplemental merge bug proofs
.venv/bin/python -m pytest tests/state/test_v2.py::StateTestCase::test_state_reset_join_rules_eviction \
    tests/state/test_v2.py::StateTestCase::test_v2_self_corrects_corrupted_state \
    tests/state/test_v2.py::StateTestCase::test_v2_unauthorized_event_not_restored -v -s
```

---

## Architectural Principle: Never Mix Network I/O with State Resolution

State resolution must remain a **pure, deterministic, CPU-bound function**:
`f(state_sets, event_map) → resolved_state`. It must never trigger network
calls (`/backfill`, `/get_missing_events`, `/event/{id}`) internally.

### Why

1. **Distributed Deadlocks**: Server A waits on Server B to resolve state,
   while Server B is paused waiting on Server A for a different room.
2. **Transaction Exhaustion**: State resolution runs inside database
   transactions or tight locks. Suspending for HTTP round-trips starves the
   reactor and exhausts connection pools during spam storms.
3. **Denial of Service (Tarpitting)**: An adversary sends an event citing
   thousands of missing `auth_events`. If state res blocks to fetch them,
   the server dies.

### The Correct Architecture

Gap detection and fetching must happen in the **ingestion pipeline**, before
state resolution is invoked:

1. When the federation handler computes `state_sets` for a fork merge, it
   checks for missing `auth_events` in the event store.
2. If missing, it **parks the PDU**, fires an asynchronous fetcher to
   retrieve the missing subgraph from federation peers.
3. Only when the auth chain is complete does it hand the data to
   `resolve_events_with_store()`.

This keeps the state resolution engine testable, deterministic, and safe
from network-induced failures.

---

## Design: `heal-room` Admin API

### Problem

Rooms with "swiss cheese" DAGs (missing event subgraphs from federation
gaps) have permanently divergent state. No state resolution algorithm can
recover events that don't exist in the local store. Server admins currently
have no automated way to repair these rooms.

### Architecture: Outlier Injection

Do **not** feed missing historical events through `handle_new_event`. That
function is designed for strict, linear, real-time timeline ingestion and
will reject events with broken `prev_events` chains. Instead, use the
**outlier persistence path**.

```
POST /_synapse/admin/v1/rooms/{roomId}/heal
{
    "peers": ["matrix.org", "codestorm.net"],
    "dry_run": true
}
```

### Steps

1. **Diff**: Query `/state_ids` from N healthy federation peers at the
   room's current forward extremities. Diff against local state to identify
   missing event IDs.

2. **Fetch**: Retrieve missing events via
   `GET /_matrix/federation/v1/event/{eventId}` from whichever peer has
   them. Validate signatures and content hashes.

3. **Persist as Outliers**: Insert via `outlier=True` persistence. Outliers
   bypass the strict `prev_events` DAG continuity checks but are still
   validated for signatures and `auth_events`. They sit in the database
   purely to satisfy state and auth-chain lookups.

4. **Re-resolve**: Force state recalculation (`compute_state_after_events`)
   at the forward extremities. The newly injected outliers are seamlessly
   pulled into the conflict set during resolution.

### Why Outliers

- Outliers bypass `prev_events` ordering — critical because the missing
  events may be from arbitrary depths in the DAG.
- Outliers are still signature-verified and auth-validated — no security
  compromise.
- State resolution already knows how to include outlier events in auth
  chain walks via `get_auth_chain_difference`.
- `unreject-room` already demonstrates the pattern of bulk-clearing
  rejection markers and forcing re-resolution.

### Response

```json
{
    "status": "healed",
    "events_fetched": 42,
    "events_persisted": 38,
    "events_rejected": 4,
    "state_keys_recovered": [
        ["m.room.member", "@nex:nexy7574.co.uk"],
        ["m.room.member", "@someone:example.com"]
    ],
    "peers_queried": ["matrix.org", "codestorm.net"]
}
```

### Future: Anti-Entropy Protocol

The `heal-room` API is a tactical admin tool. The strategic fix is an
**anti-entropy protocol** where servers periodically exchange lightweight
hashes of their room state (Merkle Search Trees or Invertible Bloom Lookup
Tables), detect divergence in O(log N) time, and automatically trigger
outlier fetches to self-heal in the background. See MSC2286 and Pinecone/P2P
Matrix research for prior art.

---

## Rejection Cascade Firewall (Future Work)

### Problem

Current behavior: if event A fails signature verification, every event B
where `A ∈ B.prev_events` is also rejected. This cascading rejection is
correct for security but devastating for data completeness during spam
storms — a single malformed spam event can orphan thousands of legitimate
events.

### Proposed Mitigation: Decouple Timeline from State Integrity

Stop conflating conversational topology (`prev_events`) with cryptographic
state validity (`auth_events`):

- If event B arrives and its `prev_event` A is rejected or missing, **but
  B's `auth_events` are perfectly valid**, persist B as a `soft_failed`
  outlier.
- B loses its chronological placement in the UI timeline, but because state
  resolution only cares about `auth_events`, B's state mutations (e.g., a
  membership join) survive the next state resolution merge.
- This confines rejection blast radius to the `auth_events` chain (where
  it's cryptographically meaningful) rather than the `prev_events` chain
  (where it's merely topological).
