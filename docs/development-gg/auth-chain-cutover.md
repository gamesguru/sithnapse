# Auth-chain cutover: SQL `event_auth` -> mtxdb

Status: design note. Nothing here is implemented yet. This document maps every
consumer of the SQL `event_auth` table and the mtxdb auth-edge collections, then
defines the invariants and migration shape for making mtxdb the sole runtime
source of direct auth edges.

Related documents:

- `docs/development-gg/mtxdb-performance-master-plan.md`
- `docs/development-gg/event-status-mtxdb-metadata.md`
- `goals.txt` (event-edge backend ownership and migration rules)

## Why this is the first cutover

Auth chains already have the right derived structures in mtxdb:

- direct outbound edges (`auth_chain_edges_get` / `auth_chain_edges_put`);
- inbound child edges (`auth_chain_children_get` /
  `auth_chain_children_append`);
- short-id allocation and reverse mapping;
- the in-process ancestor `ClosureCache`;
- the v2.1 forward-reachability BFS.

The missing piece is ownership. SQL still holds the only durable copy of
`event_auth`, and mtxdb direct edges are populated lazily by the cold-import
path, which reads SQL. That makes SQL a permanent runtime dependency no matter
how fast the BFS is.

Event-status metadata is the larger remaining SQL win, but it is a separate
project: the design note is not implemented, the Rust status collection does not
exist, and every status mutation path would need auditing. Auth chains have a
smaller, already-understood surface and finish work that is in flight, so they
should go first.

## Current state

### Durable data

SQL `event_auth(event_id, room_id, auth_id)` is authoritative. Only _state_
events get rows, including rejected state events (state res v2 needs them).
Non-state events have no rows and are treated as leaves by the cold import.

mtxdb holds:

- per-room short ids (`event_id <-> u32`);
- direct outbound auth edges, keyed by the child event's short id;
- inbound child edges, derived from the outbound writes;
- no durable transitive closure (RAM-only `ClosureCache`).

The chain-cover index (`event_auth_chains`, `event_auth_chain_to_calculate`,
`event_auth_chain_links`) is a _different_ structure. Its links already prefer
mtxdb when configured; its chain-id rows and calculation queue are still SQL.
That index is derived from direct auth edges and must be kept working across
this cutover.

### Critical finding: there is no persist-time auth-edge write today

`embed_auth_edges_batch()` is called only from `_cold_import_auth_edges()`. New
events do not write their direct auth edges into mtxdb at persistence. Every
event not previously visited by a closure walk is therefore a cold node, and
every cold walk pays a SQL read to populate mtxdb.

There is consequently no dual-write to remove. The cutover has to _add_ the
authoritative mtxdb write, import existing rows, and only then remove the SQL
write.

### Purge is currently unwired

`embedded_event_auth_chains.purge_room()` and `bump_room_generation()` exist but
have no callers. `purge_events.purge_room()` deletes SQL `event_auth` and the
chain-cover links, but leaves mtxdb auth edges and short ids behind. This is a
correctness gap independent of the cutover and must be fixed as part of it.

## Consumer map

### SQL `event_auth` readers

| Site                                                                                  | Purpose                                                                                | Cutover action                                                                         |
| ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `embedded_event_auth_chains._cold_import_auth_edges`                                  | Lazy SQL -> mtxdb import for closures                                                  | Delete after the import marker is complete; before that, keep as the migration reader  |
| `event_federation._get_auth_chain_ids_txn`                                            | Legacy auth-chain BFS when there is no chain cover                                     | Read direct edges from mtxdb (closure cache or edge batch)                             |
| `event_federation._get_auth_chain_ids_using_cover_index_txn`                          | Completeness check: direct auth ids must be covered by the cover index                 | Read direct edges from mtxdb                                                           |
| `event_federation._get_auth_chain_difference_txn`                                     | Legacy auth-difference BFS fallback                                                    | Read direct edges from mtxdb                                                           |
| `event_federation._get_auth_chain_difference_using_cover_index_txn`                   | Walks `event_auth_chain_to_calculate` joined to `event_auth` for un-indexed events     | Read direct edges from mtxdb; keep queue handling in SQL or move it separately         |
| `event_federation._calculate_chain_cover_index_for_events` / `_add_chain_cover_index` | Fetches auth events for a batch before allocating chain ids                            | Use the auth-event list already carried by the persist path, or mtxdb direct edges     |
| `events_bg_updates._rejected_events_metadata`                                         | Finds rejected state events missing metadata and inserts `event_auth`                  | Read/write mtxdb edges; keep only the `state_events` repair in SQL if that table stays |
| `events_bg_updates._chain_cover_index`                                                | Room-wide chain-cover backfill; delegates to `_calculate_chain_cover_index_for_events` | Same as above                                                                          |

### SQL `event_auth` writers

| Site                                          | Purpose                                             | Cutover action                                        |
| --------------------------------------------- | --------------------------------------------------- | ----------------------------------------------------- |
| `events.py::_persist_event_auth_chain_txn`    | Inserts direct rows for state events during persist | Delete after the mtxdb write becomes authoritative    |
| `events_bg_updates._rejected_events_metadata` | Backfills rows for old rejected events              | Rewrite to mtxdb, or retire once the import covers it |
| `synapse_port_db.py` (`APPEND_ONLY_TABLES`)   | SQLite -> Postgres copy list                        | Remove when the table is no longer written            |

### SQL `event_auth` deletes

| Site                                                       | Purpose                              | Cutover action                                                                |
| ---------------------------------------------------------- | ------------------------------------ | ----------------------------------------------------------------------------- |
| `purge_events.purge_room_tables_with_event_id_index`       | Room purge                           | Replace with `embedded_event_auth_chains.bump_room_generation` + `purge_room` |
| `purge_events._purge_history_txn`                          | History purge of non-state events    | Same                                                                          |
| `event_federation._background_delete_non_state_event_auth` | Historical cleanup of non-state rows | Retire; add the same cleanup to the import if any rows remain                 |

### mtxdb auth-edge readers/writers

| Site                                                                      | Purpose                                                                             |
| ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `embedded_event_auth_chains.embed_auth_edges_batch`                       | Writes outbound + inbound edges, idempotent                                         |
| `embedded_event_auth_chains.get_embedded_auth_edges_batch`                | Direct outbound reads; `None` = not embedded, `[]` = leaf                           |
| `embedded_event_auth_chains.get_embedded_auth_children_batch`             | Inbound reads for the v2.1 forward BFS                                              |
| `embedded_event_auth_chains._cold_import_auth_edges`                      | Lazy import (to be removed)                                                         |
| `embedded_event_auth_chains.ClosureCache`                                 | Ancestor closures over direct edges                                                 |
| `embedded_event_auth_chains.get_forward_reachable_short_ids`              | v2.1 conflicted-subgraph forward pass                                               |
| `event_federation._get_auth_chain_difference_using_embedded_closures_txn` | Primary v2/v2.1 auth-difference path                                                |
| `embedded_event_auth_chain_links.*`                                       | Chain-cover links (separate structure, already mtxdb-authoritative when configured) |

## Invariants

1. **One owner per config.** Either SQL or mtxdb is authoritative, selected by
   the configured embedded engine, exactly as for event edges. There is no
   dual-write. Before the cutover, SQL is authoritative and mtxdb is populated
   only by the manual import; after the marker flips, no runtime path reads
   `event_auth` and no runtime path writes it.

2. **Every embedded event has an edge record.** For each event whose SQL
   `event_auth` rows would exist, mtxdb must have either a non-empty outbound
   list or an explicit empty list. `None` means "not imported yet" before the
   marker is complete, and corruption after it. The import and the persist write
   must both create empty records for leaves so `None` cannot be confused with a
   genuine leaf.

3. **Both directions agree.** Every outbound edge `A -> B` has `A` in `B`'s
   inbound child list, and vice versa. `embed_auth_edges_batch` is the only
   writer of either direction; no caller may append one side alone.

4. **The write shares the event's durability boundary.** Direct edges must be
   staged in the same mtxdb transaction as event JSON when the shared WAL is
   available, matching the event-edge design in `goals.txt`. If a mode cannot
   share the transaction, the write must be covered by an explicit durability
   barrier before the event row is visible. After cutover there is no repair
   sweep and no SQL copy to fall back to, so a gap is a fault, not a lazy miss.

5. **Rejected state events are included.** State res v2 needs auth edges for
   rejected state events. The persist write and the import must both cover them.
   Non-state events intentionally have no rows; the empty record is the leaf
   marker.

6. **Purge wins.** `bump_room_generation(namespace, room_id)` runs before any
   deletion; `purge_room(engine, namespace, room_id)` deletes counters, short
   ids, outbound edges, and inbound edges. A concurrent closure read may waste
   work but must never serve a stale bitmap after the purge lands.

7. **Gaps are explicit.** `IncompleteAuthGraph` remains the only signal that a
   graph is genuinely missing data. `None` outbound edges before the marker is
   complete are importable; `None` after the marker is corruption and must not
   silently fall back to SQL.

8. **Chain cover is derived, not authoritative.** Moving direct edges must not
   change chain-cover semantics. Its readers should consume mtxdb direct edges
   (or the caller-provided auth lists) rather than `event_auth`; its own tables
   can be migrated separately.

9. **Workers see the same marker.** The import completion marker and schema
   version must be visible to every process that can serve state resolution. A
   process that sees an incomplete or mismatched marker keeps using the SQL
   backend; it must not half-read mtxdb.

10. **Idempotence.** Persist writes, retries, and the import may re-apply the
    same edge rows. `embed_auth_edges_batch` is already idempotent; the import
    must be resumable and safe to restart.

## Migration shape

### Phase 0: make the boundary observable

- Add a backend marker analogous to the event-edge marker:
  `backend = sql | mtxdb`, plus the import state and schema version.
- Add counters/metrics for: cold imports per room, import progress, zero-edge
  records written, inbound/outbound mismatches detected by verification, and
  `IncompleteAuthGraph` fallbacks.
- Audit every `event_auth` string in the tree, including scripts and tests, so
  the list above cannot silently go stale.

### Phase 1: add the mtxdb write path behind the backend switch

- Add a write path that stages direct edges for every state event, accepted or
  rejected, in the persist's mtxdb transaction / `SyncTier.DURABLE` barrier.
- Select exactly one write backend from config, matching the event-edge design:
  SQL backend -> SQL `event_auth` only; mtxdb backend -> auth-edge collections
  only. Do not write both, even temporarily.
- Until the migration is run and the marker flipped, the configured backend
  remains SQL, so this path is dormant.
- Add tests for accepted state events, rejected state events, de-outliered
  events, leaves (empty lists), and a rolled-back persist (no ghost edges).

### Phase 2: manual import and cutover

No dual-write. The migration is a manual operation, like
`synapse_event_edges_migrate`, not a background update.

- Add a command, for example
  `synapse_auth_edges_migrate --to mtxdb --config ... [--room ...]`.
- It must run with writers paused (maintenance window) or under a write lock, so
  the SQL snapshot is complete. Do not attempt a live online migration with a
  best-effort shadow write.
- For each room, read `event_auth` in `event_id` order, allocate short ids, and
  write both edge directions in batches.
- Write empty records for in-scope events in `events` that have no `event_auth`
  rows (state events with zero auth events, e.g. create).
- Verify:
  - every SQL row has a matching mtxdb outbound edge;
  - every mtxdb outbound edge has a matching SQL row;
  - inbound lists are the exact reverse of outbound lists;
  - sampled closures match the SQL legacy BFS.
- Record the completed rooms / last processed event and write the completion
  marker (`backend=mtxdb`, import high-water mark, schema version).
- Start the server with the mtxdb backend. Startup refuses a config/marker
  mismatch or an incomplete marker.

### Phase 3: readers select one backend

Readers follow the same switch: SQL config uses `event_auth`; mtxdb config uses
the auth-edge collections. There is no runtime SQL fallback in the mtxdb
backend, exactly as for event edges. `IncompleteAuthGraph` is a genuine data
error after the marker is complete, not a cue to read SQL.

Flip these in order, each with its own tests:

1. `_get_auth_chain_ids_using_cover_index_txn` completeness check: read direct
   auth ids from mtxdb.
2. `_calculate_chain_cover_index_for_events` / `_chain_cover_index`: read auth
   lists from mtxdb (or the persist path's existing list) instead of SQL.
3. `_get_auth_chain_ids_txn` and `_get_auth_chain_difference_txn` legacy BFS:
   run over the closure cache / edge batch.
4. `embedded_event_auth_chains` cold import: delete. A `None` outbound record is
   corruption, never a cold event.
5. `events_bg_updates._rejected_events_metadata`: switch its auth repair to
   mtxdb and stop inserting `event_auth`.

### Phase 4: remove the SQL writer and dead paths

- Delete the `event_auth` insert in `_persist_event_auth_chain_txn` (or make it
  the SQL-backend branch only).
- Remove the SQL delete/cleanup background update.
- Wire purge to `bump_room_generation` + `purge_room` for auth chains (this can
  and should land before Phase 4, because it is a live correctness fix).
- Remove `event_auth` from `purge_events` table lists once nothing writes it.
- Remove it from `synapse_port_db` once it is no longer part of the schema's
  live state.
- Delete the now-unused cold-import SQL code and the `_AuthChainTxn` protocol
  surface if nothing else needs it.

### Phase 5: rebuild and recovery

- A rebuild must be possible from event JSON alone: each event body already
  carries `auth_events`, so the direct-edge graph can be reconstructed without
  `event_auth`. This is the recovery path if mtxdb is lost or a write gap is
  suspected after cutover.
- Rebuild is resumable, verifies against a sample / full count, and writes a new
  marker generation only after verification.
- Pre-cutover, the SQL import remains the recovery source; post-cutover, event
  JSON is. The marker records which generation/recovery source it was built
  from.

## Chain-cover interaction

`event_auth_chain_to_calculate` and `event_auth_chains` still live in SQL. Two
options, to be decided before Phase 4:

- Keep chain cover as a SQL-derived index for now, but source its auth lists
  from mtxdb. This preserves the existing rollout and is the smaller change.
- Move chain cover ids/queue to mtxdb too. This is a separate project; do not
  couple it to the direct-edge cutover.

The direct-edge cutover must not remove `event_auth` rows until the chain-cover
code no longer reads them.

## Tests and measurement

- Unit: persist accepted/rejected/de-outliered/leaf edges; rollback leaves no
  edges; import resume/verify; purge removes counters, short ids, and both edge
  directions; marker mismatch refuses startup.
- Storage integration: `tests.storage.test_event_federation` with the embedded
  closures and conflicted-subgraph cases, including the embedded-leaf fixture.
- Sytest: `33room-get-missing-events.pl`, `34room-backfill.pl`, `36state.pl`,
  `50no-deextrem-outliers.pl`.
- Complement: the MSC4297 state-resolution tests and the federation gap tests.
- Measurement: after the cutover, the isolated MSC4297 profile should have no
  `event_auth` SQL at all. Track cold-import count (must be zero after the
  marker), closure walk time, and mtxdb FFI batch sizes.

## Open questions

1. Transaction boundary: can every persist mode stage auth edges in the same
   mtxdb transaction as event JSON, or are there modes that need the explicit
   durability barrier? The event-edge work already answered part of this; reuse
   that answer.
2. Rejected events written outside the normal state persist path: confirm all
   sites that can add `event_auth` rows are covered by the persist write or the
   import.
3. Workers: confirm whether read-only processes can read the auth-edge
   collections directly, or whether they must go through the same namespace
   marker and cache refresh as event edges.
4. Chain cover: keep SQL ids/queue or migrate in the same project?
5. Table retirement: leave dead `event_auth` rows in place (as with
   `event_edges`) or provide a cleanup command? The event-edge precedent says
   leave the SQL dead data until a separate cleanup.
