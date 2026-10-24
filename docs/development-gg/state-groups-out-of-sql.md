# Retiring SQL for state groups

## Problem

`mtxdb-exclusive` already moves the HAMT payload itself (`state_hamt_roots`,
`state_hamt_nodes`) out of SQL when the embedded engine is configured -- see
`_persist_state_hamt_txn`/`_persist_state_hamt_incremental_txn` in
`synapse/storage/databases/state/store.py`. What's left in SQL is thin
bookkeeping, not state content:

- `state_groups`: one row per group, `(id, room_id, event_id)`.
- `state_group_edges`: ancestry (`state_group -> prev_state_group`), used by
  purge/deletion safety checks, not by state reads.
- `state_group_id_seq`: the sequence generator that allocates `id`.

The `id` column is the load-bearing part: every other table, replication stream,
and cache addresses a state group by this small integer, not by its
content-derived identity (see below). "We don't need SQL state groups" means
moving all three of the above into mtxdb too, so state has no SQL footprint at
all under the embedded engine.

## Why this isn't a quick swap

### 1. `id` allocation can move to mtxdb, but needs a one-time bootstrap

mtxdb already exposes a generic atomic counter primitive,
`increment_counters_batch` (`rust/src/database/mtxdb_syn.rs`), used today for
per-state-group reference counts
(`increment_state_group_refcounts_batch`/`decrement_state_group_refcounts_batch`
in `synapse/storage/databases/main/embedded_event_to_state_group.py`). The same
primitive can allocate `state_group` ids: call it with a namespaced
`state_group_id_seq` key and treat the returned value as the new id (a batch of
N ids is one call with `delta=N`, taking the last N values below the returned
max).

The trap: any server that already has history predates this and has been
allocating ids from `state_group_id_seq` (the SQL sequence) for as long as it's
existed. Pointing new allocation at a fresh, independent mtxdb counter starting
from 0 would immediately collide with existing SQL-allocated ids (e.g. mtxdb
hands out `id=1` while SQL row `id=1` already exists for a different group),
corrupting `state_groups`/`state_group_edges`/every FK against them. This needs
a one-time reconciliation the first time an instance allocates via mtxdb: seed
the mtxdb counter to
`max(current SQL sequence value, mtxdb counter's current value)` before handing
out any new id from it. Also only the writer instance
(`self._embedded_db_is_writer`) can do this -- `increment_counters_batch` calls
`assert_writable()`, so read-only workers stay on the SQL sequence generator for
this, same as they already do for HAMT root mirror writes (see
`skip_mirror_write`).

### 2. `state_groups`/`state_group_edges` are read directly by other modules

Not just `store.py`. A repo-wide search for `table="state_groups"` /
`table="state_group_edges"` (or raw `FROM state_groups` / `JOIN state_groups`)
currently turns up:

- `synapse/storage/controllers/purge_events.py` -- purge safety checks.
- `synapse/storage/databases/state/deletion.py` -- deletion bookkeeping.
- `synapse/storage/databases/state/bg_updates.py` -- background migrations.
- `synapse/storage/schema/state/delta/47/state_group_seq.py` -- schema history.
- `synapse/_scripts/synapse_port_db.py` -- SQLite -> Postgres port tooling.

Each of these would need its own mtxdb-backed read/write path (or an mtxdb-aware
fallback), not just `store.py`'s persist path.

### 3. Existing SQL rows need a real migration, not just a cutover

A server with years of `state_groups` history has real data --
`room_id`/`event_id` per group, and the full `prev_state_group` edge chain --
that mtxdb would need to serve reads for once SQL is no longer the source of
truth. This is the same shape of problem `_background_backfill_state_hamt_roots`
already solved for HAMT roots (reconstruct via the legacy walk, write it into
mtxdb, mark progress), so that background-update pattern is the template, but it
needs its own pass for `id`/`room_id`/`event_id`/edges specifically.

## Suggested order of work

1. Bootstrap-safe id allocation via mtxdb (writer-only, SQL fallback for
   read-only workers), landed and soak-tested on its own first -- this is useful
   independent of the rest, and de-risks the trickiest correctness trap
   (bootstrap seeding) in isolation.
2. Mirror `state_groups`/`state_group_edges` writes into mtxdb alongside SQL
   (dual-write, matching how HAMT roots/nodes were transitioned), with reads
   still preferring SQL.
3. Background migration to backfill mtxdb for pre-existing groups, mirroring
   `_background_backfill_state_hamt_roots`.
4. Flip reads in `purge_events.py`/`deletion.py`/`bg_updates.py` to mtxdb, one
   call site at a time, each with its own test coverage.
5. Drop the SQL writes/tables once nothing reads them, and update
   `synapse_port_db.py` accordingly.

Not scoped or started yet -- this document exists to capture the shape of the
problem discussed alongside the `guru/feat/mtxdb-exclusive` test-isolation fix
in `tests/storage/test_state.py` (see that commit's message), not to claim any
of the above is implemented.

## Audit: what SQL state remains (partly verified)

Scope is family 5 of the B2 list in `perf-track-b.md`. Not in scope:
`event_to_state_groups` (family 2), `current_state_events` and
`current_state_delta_stream` (ordering/stream data that stays in SQL), and
`state_hamt_*` (payload already in mtxdb; SQL writes are rare, 21 of about
12,000 node writes in the 984 s slice).

The family is these tables in the state schema
(`synapse/storage/schema/state/`):

| Table                           | Seen in code                                                                                                 | Status                                    |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------ | ----------------------------------------- |
| `state_groups`                  | written in `state/store.py` ~1855                                                                            | written on persist                        |
| `state_group_edges`             | written ~1866, read by purge/deletion                                                                        | written on persist                        |
| `state_group_id_seq`            | sequence behind `id`                                                                                         | allocator                                 |
| `state_groups_persisting`       | `deletion.py` protocol, startup clear at ~108                                                                | coordination, not state content           |
| `state_groups_pending_deletion` | `deletion.py`, `store.py` ~2428, ~2688                                                                       | coordination, not state content           |
| `state_groups_state`            | deleted/purged (`store.py` ~2397-2416, ~2662); a comment at ~1775 says it is not written for embedded groups | legacy; still read/deleted for old groups |
| `state_hamt_*` queue tables     | not traced here                                                                                              | unverified                                |

`persisting` and `pending_deletion` are missing from the problem statement above
but are the hottest of the set. `deletion.py` (~52-76) documents the protocol: a
group is marked as about to be used, persisted, then unmarked, and deletion
rechecks `pending_deletion` in its own transaction. Any replacement has to keep
that guarantee against a concurrent deletion.

### Cost, with the caveat

The 530 s unit-test run summed `state_groups` 12.8 s, `persisting` 8.7 s and
`pending_deletion` 7.4 s (28.9 s, 5.8%). That is summed SQL time, not wall time.
The 984 s slice shows about 14 s across the same three, the same ratio at
smaller scale.

### Blockers, in order

1. **Single writer.** State groups are created wherever state is resolved and a
   context is built, which can be a worker, not only the events writer. Workers
   use the SQL sequence today. If only the mtxdb owner may allocate ids and
   write, those workers need an acknowledged call to the owner. This is the
   shared constraint in `perf-track-b.md`.
2. **Integer id coupling.** Tables, replication streams and caches address a
   group by the integer id. Keep integer ids; seed the mtxdb counter above the
   SQL sequence at the one-time import.
3. **Coordination.** The `persisting` and `pending_deletion` protocol needs an
   mtxdb equivalent, including the `FOR KEY SHARE` row lock taken in
   `mark_state_groups_as_persisting`.
4. **Crash gap.** A group written before its event is persisted can be orphaned.
   That is what `persisting` and `pending_deletion` clean up today.

### Order

Last. Behind family 2 and `event_json`, and not before the single-writer
decision. Per `goals.txt` there is no dual-write phase: a one-time import with a
verifier and a durable completion marker, startup refusing to run if it is
incomplete, then delete the SQL tables and the migration scaffolding.

### Unverified

The table list above is not a traced write-path audit. Still to check:
`state_groups_state` writes under non-embedded configs, and the `state_hamt_*`
queue tables.
