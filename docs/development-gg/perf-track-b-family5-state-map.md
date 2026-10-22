# Family 5 map: state-group bookkeeping

Status: dependency map only; no implementation proposed here. Family 5 is
deferred until after family 2 (`event_to_state_groups`) and `event_json`, and
must not start before the single-writer ownership decision. The detailed
existing analysis is in
[`state-groups-out-of-sql.md`](state-groups-out-of-sql.md).

## Scope

Family 5 covers the SQL bookkeeping needed to identify, connect, coordinate, and
delete state groups:

| Table                           | Role / current understanding                                                                              |
| ------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `state_groups`                  | Integer ID, room ID, and event ID for each group.                                                         |
| `state_group_edges`             | Parent-group links used by purge and deletion paths.                                                      |
| `state_group_id_seq`            | Allocates the integer IDs used across Synapse tables, streams, and caches.                                |
| `state_groups_persisting`       | Coordinates groups being persisted against concurrent deletion.                                           |
| `state_groups_pending_deletion` | Tracks deletion work and protects the deletion protocol.                                                  |
| `state_groups_state`            | Legacy table; still read/deleted for old groups. Whether any configuration still writes it is unverified. |

Out of scope: `event_to_state_groups` is family 2; `current_state_events` and
`current_state_delta_stream` are SQL ordering/stream data; `state_hamt_*`
payload is already in mtxdb, with rare SQL writes. The `state_hamt_*` queue
tables still need a direct-path audit before claiming they are fully out of SQL.

## Decoupling workstreams

1. Replace integer ID allocation without collisions, preserving the existing
   integer IDs referenced by tables, streams, and caches.
2. Move group identity and ancestry records, plus persisting/deletion
   coordination, while preserving their concurrent-use and deletion guarantees.
3. Trace and replace every direct reader/writer, including purge, state
   deletion/background-update paths, and `synapse_port_db.py`.
4. Import existing SQL records once, verify parity, and make startup refuse to
   serve if import or verification is incomplete.

This is a map of required surfaces, not an implementation sequence or a claim
that these paths have been fully audited.

## Blockers

1. **Single writer:** state resolution and context construction can happen on
   workers outside the events writer. A single mtxdb writer therefore needs an
   explicit owner and acknowledged write route before state-group writes move.
2. **Integer ID coupling:** IDs are externally referenced throughout Synapse;
   allocation must continue above the imported maximum and remain globally
   unique.
3. **In-flight coordination:** `state_groups_persisting` and
   `state_groups_pending_deletion` currently protect persistence/deletion races,
   including SQL row-lock semantics. The replacement must preserve that
   guarantee.
4. **Crash-created orphans:** a state group can be written before its event is
   persisted. The current coordination tables support cleanup; recovery must
   retain that property.

## Priority and measured cost

Keep this family last, behind family 2 and `event_json`. In the 5,767-test,
530-second run, `state_groups`, `state_groups_persisting`, and
`state_groups_pending_deletion` accounted for 28.9 seconds (5.8%) of summed SQL
time across processes—not wall time. In the 2,339-test, 984-second slice, the
same three accounted for about 14 seconds of summed SQL time, a similar share at
smaller scale. These are unit-test workloads, not production state-resolution
profiles.

The table list and call sites above are not a complete read/write audit. In
particular, verify whether `state_groups_state` is still written under any
configuration and trace the `state_hamt_*` queue tables before implementation.

## Related work kept separate

The three per-start `enqueue_*_embedded_migration` hooks are B1 test-setup and
dual-mode-scaffolding work, not Family 5. Their measured scheduling delay is
accumulated across homeserver starts and overlaps other work; removing obsolete
hooks needs its own setup-and-total-time A/B. Removing those hooks does not
replace or waive a one-time state import and startup completion gate.
