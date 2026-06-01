# State Resolution: V2 vs V2.1 (MSC4297)

This diagram visualizes the two critical changes introduced for State Resolution V2.1 that prevent the state reset vulnerability.

```text
===================================================================================
                STATE RESOLUTION: ITERATIVE AUTH CHECKS
===================================================================================

                    [ V2 (Vulnerable / Develop) ]
                    
                      1. Start with Base State
                      ┌──────────────────────┐
                      │ base_state =         │
                      │ unconflicted_state   │ ◄── FLAW 1: Base state is polluted
                      └──────────┬───────────┘     early by unverified events.
                                 │
                                 ▼
                     2. For each conflicted event
                      ┌──────────────────────┐
                      │ Fetch event's own    │
                      │ auth_events chain    │
                      └──────────┬───────────┘
                                 │
                                 ▼
                      3. SUPPLEMENTAL MERGE
                      ┌──────────────────────┐
                      │ auth_events.update(  │ ◄── FLAW 2: Resolved state (like
                      │   resolved_state     │     a new join_rule) overrides the
                      │ )                    │     event's historical auth chain!
                      └──────────┬───────────┘
                                 │
                                 ▼
                        4. Auth & Persist
                      ┌──────────────────────┐
                      │ Run Auth Checks      │ ◄── FAILS: Honest users evicted 
                      │ resolved_state += ev │     because they are judged against
                      └──────────────────────┘     future state, not the past.


===================================================================================

                        [ V2.1 (Fixed / PR) ]
                        
                      1. Start with Base State
                      ┌──────────────────────┐
                      │ base_state = { }     │ ◄── FIX 1: Start from an empty,
                      │ (Empty Dictionary)   │     sterile foundation.
                      └──────────┬───────────┘
                                 │
                                 ▼
                     2. For each conflicted event
                      ┌──────────────────────┐
                      │ Fetch event's own    │
                      │ auth_events chain    │
                      └──────────┬───────────┘
                                 │
                                 ▼
                       3. PURE AUTHENTICATION
                      ┌──────────────────────┐
                      │ Run Auth Checks      │ ◄── FIX 2: No supplemental merge!
                      │ (Using ONLY the      │     The event is judged purely on
                      │ event's auth_events) │     the state of the room when it
                      └──────────┬───────────┘     was originally sent.
                                 │
                                 ▼
                        4. Accumulate State
                      ┌──────────────────────┐
                      │ resolved_state += ev │ ◄── PASSES: Honest users survive.
                      └──────────┬───────────┘
                                 │
                                 ▼
                        5. Final Re-assembly
                      ┌──────────────────────┐
                      │ resolved_state +=    │ ◄── Unconflicted state is safely
                      │ unconflicted_state   │     applied at the very end.
                      └──────────────────────┘
```

### The Two Changes Explained:

1. **`base_state = {}` instead of `unconflicted_state`:**
   In V2, the resolution loop started with `unconflicted_state`. This meant that if a malicious or broken fork contained unconflicted garbage, it was present from step 1 and could accidentally fail auth checks for later events. V2.1 starts from a sterile `{}` and builds up strictly from what passes auth.
2. **Skipping the Supplemental Merge (`auth_events.update(...)`):**
   In V2, Synapse constantly smeared the newly `resolved_state` (like a resolved `join_rules: invite` event) backward over the `auth_events` of older messages. V2.1 skips this entirely (gated by `if room_version.state_res != StateResolutionVersions.V2_1:`), guaranteeing an event is only ever authenticated against the actual history it points to.
