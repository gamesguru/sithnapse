# Auth-chain cutover: mtxdb is authoritative

Status: implemented. With `embedded_db.engine` configured, mtxdb owns direct
auth edges and all runtime auth-chain queries. SQL `event_auth` is kept only as
the source snapshot for the one-shot offline migration; it is not read or
written by the runtime after the cutover.

Related documents:

- `docs/development-gg/mtxdb-performance-master-plan.md`
- `docs/development-gg/event-status-mtxdb-metadata.md`
- `goals.txt` (event-edge backend ownership and migration rules)

## End state

mtxdb holds:

- per-room short ids (`event_id <-> u32`);
- direct outbound auth edges (`auth_chain_edges_get` / `_put`), keyed by the
  child event's short id;
- inbound child edges, derived from the outbound writes;
- explicit empty outbound records for leaves (e.g. the create event);
- no durable transitive closure; the ancestor closure is a RAM-only
  `ClosureCache` derived from the direct edges;
- the v2.1 forward-reachability BFS over inbound child edges.

The persist path stages direct edges in the same mtxdb transaction as event JSON
and event-DAG edges. A committed state event therefore always has its auth-edge
record, accepted or rejected; a rolled-back persist leaves none.

There is no runtime SQL fallback, no lazy cold import, and no dual write. If the
graph is missing an event, `IncompleteAuthGraph` is a data-corruption error and
surfaces as `RuntimeError`, never as a SQL read.

The SQL chain-cover index (`event_auth_chains`, `event_auth_chain_links`,
`event_auth_chain_to_calculate`) is retired. It is no longer written, read, or
repaired. Auth-chain queries use the closure cache and forward BFS directly, so
there is no chain-id/sequence-number derivation to maintain and no background
chain-cover backfill to run. Schema delta 98 drops the obsolete tables and
sequence after retiring any queued chain-cover background updates.

## Ownership and migration marker

`auth_chain_backend` (schema delta 97) records `sql` or `mtxdb` in one row.

- A fresh database adopts the configured backend at boot.
- An existing database with a mismatched marker refuses to start with
  `AuthChainBackendMismatchError`.
- The marker flips only after `synapse_auth_edges_migrate` has rebuilt and
  verified the mtxdb graph.

`configured_backend()` is derived from `embedded_db.engine`; there is no
separate auth-chain backend opt-in. Configuring the embedded engine _is_ the
declaration that mtxdb owns auth chains.

## Migration

`synapse_auth_edges_migrate --config ... [--batch-size ...]` is an offline
operation. Stop every Synapse process first.

For each room the command:

1. bumps the in-process generation and purges the room's mtxdb auth-chain
   collections, so an interrupted run can be repeated without stale children;
2. reads the union of `state_events` and `event_auth`, guaranteeing both real
   edges and explicit empty records for zero-auth state events and rejected
   state events;
3. allocates short ids and writes outbound + inbound edges in batches;
4. verifies every imported outbound edge and every SQL inbound edge;
5. flips `auth_chain_backend` to `mtxdb` only after verification succeeds.

If the marker already says `mtxdb`, the command returns immediately. A failed or
interrupted run leaves the marker at `sql`, so the server keeps refusing to boot
in mtxdb mode until a complete run succeeds.

## Invariants

1. **One owner.** Configuring `embedded_db.engine` selects mtxdb for auth edges.
   No runtime path writes SQL `event_auth` and mtxdb edges together.
2. **Every embedded event has a record.** Each event that would have an
   `event_auth` row has either a non-empty outbound list or an explicit empty
   list. `None` is corruption after migration.
3. **Both directions agree.** Every outbound edge `A -> B` appears in `B`'s
   inbound child list. `embed_auth_edges_batch` is the only writer of either
   direction.
4. **Durability matches event JSON.** Edges share the persist transaction when
   the shared WAL is available; otherwise they are written before the SQL
   COMMIT.
5. **Rejected state events are included.** State res v2 needs their auth edges.
6. **Purge wins.** `bump_room_generation` runs before deleting the room's short
   ids, outbound edges, and inbound children, so no cached closure survives.
7. **Gaps are explicit.** `IncompleteAuthGraph` has one meaning: genuine missing
   data. It never triggers a SQL fallback.
8. **Idempotence.** Persist writes, retries, and migration re-application are
   safe; short-id allocation and edge writes are idempotent.

## SQL backend

Without `embedded_db.engine`, the direct SQL `event_auth` graph remains the
backend. State res v2.1 computes forward reachability with a bounded SQL BFS; it
does not need the retired chain-cover index. This compatibility mode is not the
mtxdb performance path.
