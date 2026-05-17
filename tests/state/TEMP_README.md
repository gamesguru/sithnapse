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

The JSONL contains real PDUs from the `#general:nutra.tk` room (V11), loaded as V1
`FrozenEvent`s to preserve the original `event_id`s (V4+ format computes IDs from
content hashes, breaking DAG references). After construction, `room_version` is
patched to V11 so auth checks use the correct semantics.

### `test_v21.py` — V2.1 State Resolution (HydraV11 / V12+)

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

tests/state/test_v2.py::DAGReplayTestCase::test_replay_nutra_tk_dag_catgirl_perspective
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
PASSED

tests/state/test_v21.py::StateResV21TestCase::test_conflicted_subgraph_preserves_power_levels PASSED
tests/state/test_v21.py::StateResV21TestCase::test_incomplete_dag_picks_stale_membership PASSED
tests/state/test_v21.py::StateResV21TestCase::test_state_reset_replay_conflicted_subgraph PASSED
tests/state/test_v21.py::StateResV21TestCase::test_state_reset_start_empty_set PASSED
tests/state/test_v21.py::StateResV21TestCase::test_supplemental_merge_does_not_clobber_auth_chain PASSED
tests/state/test_v21.py::StateResV21TestCase::test_v21_cve_auth_bypass_without_supplemental_merge PASSED
tests/state/test_v21.py::StateResV21TestCase::test_v21_self_corrects_corrupted_state PASSED

======================== 25 passed in 5.82s ========================
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
