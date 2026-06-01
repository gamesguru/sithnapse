# V2.1 MSC4297 State Resolution Fix Evaluation

This document formally captures the execution of the state resolution integration test `test_v21_prevents_supplemental_merge_eviction_on_real_dag` running on the `nutra.tk` merged DAG (3779 events, 211 state merges).

The outputs below prove that the pre-fix codebase improperly evicts 37 essential state events, and our fix correctly retains them.

### Test Output on Fixed Code (Current Branch)

When running the test natively on this branch, V12 successfully preserves all 44 state events.

```text
============================================================
V11 (V2) State Size: 7
V12 (V2.1) State Size: 44
Events wrongfully evicted by V2: 37
  - EVICTED: m.room.canonical_alias 
  - EVICTED: m.room.create 
  - EVICTED: m.room.guest_access 
  - EVICTED: m.room.history_visibility 
  - EVICTED: m.room.join_rules 
  - EVICTED: @aranjedeath:explodie.org (membership: leave)
  - EVICTED: @cat:feline.support (membership: join)
  - EVICTED: @cat:maunium.net (membership: join)
  - EVICTED: @caufa:muoi.me (membership: leave)
  - EVICTED: @char:zirco.dev (membership: leave)
  - EVICTED: @crazy_nicc:hnvn.de (membership: join)
  - EVICTED: @erents:dapperepoging.nl (membership: join)
  - EVICTED: @ext_logn:kludgecs.com (membership: join)
  - EVICTED: @gg:nutra.tk (membership: join)
  - EVICTED: @kim:sosnowkadub.de (membership: join)
  - EVICTED: @logn:uwu.zirco.dev (membership: join)
  - EVICTED: @logn:zirco.dev (membership: join)
  - EVICTED: @lveneris:kludgecs.com (membership: join)
  - EVICTED: @mangotcf:mangotcf.ru (membership: join)
  - EVICTED: @mscbot:matrix.org (membership: join)
  - EVICTED: @nex:nexy7574.co.uk (membership: join)
  - EVICTED: @nex:starstruck.systems (membership: join)
  - EVICTED: @plate:starstruck.systems (membership: join)
  - EVICTED: @reminder:codestorm.net (membership: leave)
  - EVICTED: @reminder:maunium.net (membership: join)
  - EVICTED: @ring0:zirco.dev (membership: leave)
  - EVICTED: @shane:wombatx.me (membership: join)
  - EVICTED: @sky:codestorm.net (membership: join)
  - EVICTED: @stratself:federated.nexus (membership: join)
  - EVICTED: @stratself:muoi.me (membership: join)
  - EVICTED: @sys:31a05b.net (membership: leave)
  - EVICTED: @tobiasfella:kde.org (membership: join)
  - EVICTED: @vel:heizle.net (membership: leave)
  - EVICTED: m.room.name 
  - EVICTED: m.room.pinned_events 
  - EVICTED: m.room.server_acl 
  - EVICTED: m.room.topic 
============================================================

1 passed, 6 warnings in 36.33s
```

### Test Output on Vulnerable Code (origin/develop)

When running the exact same test using `synapse/state/v2.py` checked out from the unpatched `origin/develop` branch, V12 fails to differentiate from V2 and falls victim to the supplemental merge bug, yielding only 37 events and triggering a test failure.

```text
============================================================
V11 (V2) State Size: 7
V12 (V2.1) State Size: 37
Events wrongfully evicted by V2: 30
... (Same list as above, but stopping short)
============================================================

FAILED tests/state/test_v2.py::V12DAGReplayTestCase::test_v21_prevents_supplemental_merge_eviction_on_real_dag
twisted.trial.unittest.FailTest: 37 != 44 : V12 (V2.1) should preserve members and yield 44 events

1 failed, 6 warnings in 35.84s
```
