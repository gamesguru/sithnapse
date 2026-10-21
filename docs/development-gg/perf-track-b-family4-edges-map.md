# Family 4 map: `event_edges`, `edge_index_outbox`, `room_edge_source_version`

Read-only dependency map for B2 family 4 (see `perf-track-b.md`). Nothing here
is implemented. Line numbers are from the tree at the time of writing.

This family already has a detailed plan:
`res/docs/2026-09-28-event-edges-sql-removal-plan.md` (cutover gates, strict
cross-process freshness, startup check `check_event_edges_migration_complete`
behind `EVENT_EDGES_SQL_INSERT_REMOVED = False`). That plan keeps the SQL insert
and the SQL fallback until a release window proves the mtxdb path. This map adds
what that plan does not: every SQL caller, hot-path status, and the read/write
cost split. It also covers the outbox and version gate, which that plan does not
remove.

## Measured cost: reads versus writes

Source: `res/logs/2026-10-21-trial-tests-530s.log` (5,767 tests, `-j4`, sync
off, pin `0efca0a`, 25 processes, SQL total 494.9 s). The report gives only
per-table totals, not per-statement splits, so the split below is an estimate
from call counts, not a measurement.

| Table                      | Time                    | Calls  | Rows   | Avg      |
| -------------------------- | ----------------------- | ------ | ------ | -------- |
| `event_edges`              | 18.3 s                  | 43,754 | 31,282 | 0.418 ms |
| `edge_index_outbox`        | 4.5 s                   | 23,422 | 25,810 | 0.190 ms |
| `room_edge_source_version` | not in top 30 (< 4.2 s) |        |        |          |

- `ffi_event_edges_put` ran 13,435 times. Each is one post-commit batch from one
  `_handle_mult_prev_events` call, which issues one `event_edges` INSERT (plus
  one outbox INSERT and one version upsert per room). So SQL writes are about
  13.4k of the 43.8k `event_edges` calls (about 31%); the other roughly 30k
  calls are reads (or deletes/other).
- `event_edges` returned 31.3k rows. Outbox rows are 25.8k, which is close to
  the insert count, so the inserted edge rows are probably most of the 31.3k and
  the read calls return few rows each.
- Writes are many small statements at 0.4 ms each. Removing the write path
  removes at most the write share of 18.3 s plus the outbox and version-gate
  statements, not the whole table time. The read share stays until the SQL joins
  below are replaced.
- **Not measurable from this log:** exact read/write time per statement. To get
  it, add a label to the edge statements (the `sched_label` breakdown from
  `98439a4cdd` already tags by transaction description) and take one trial run.

## SQL callers

Hot path means it runs per persisted event or per request in a normal workload.
"Unverified" means I read the code but did not trace call frequency.

### Writes

| Caller                                                                               | Table                                              | Hot path                                    | Disappears at cutover                                                                  |
| ------------------------------------------------------------------------------------ | -------------------------------------------------- | ------------------------------------------- | -------------------------------------------------------------------------------------- |
| `events.py:4725` `_handle_mult_prev_events` INSERT                                   | `event_edges`                                      | yes, every persisted event with prev_events | the INSERT (Postgres `execute_values`, SQLite `executemany`)                           |
| same, `bump_room_edge_source_version`                                                | `room_edge_source_version`                         | yes, once per room per batch                | the upsert, and `embedded_event_edges.py:82`                                           |
| same, `simple_insert_many_txn`                                                       | `edge_index_outbox`                                | yes, once per room per batch                | the outbox INSERT                                                                      |
| `record_edge_index_deletes_txn` from `purge_events.py:401, 707`                      | outbox, version                                    | no, purge only                              | reads `event_edges` (line 123) then bumps version and writes delete rows to the outbox |
| `purge_events.py:51, 403` table delete loop                                          | `event_edges`                                      | no                                          | the `event_edges` entry                                                                |
| `drain_edge_index_outbox` (`embedded_event_edges.py:935`)                            | outbox DELETE, SELECT                              | background, writer only                     | whole drain loop, `events_worker.py:340` registration                                  |
| `repair_edge_index_from_sql` (`:1059`), called from `event_federation.py:3001`       | reads `event_edges` and outbox                     | repair on fallback only                     | whole function                                                                         |
| `rebuild_room_forward_index` (`:1262`) and rebuild checkpoint helpers (`:1117-1260`) | reads `event_edges` JOIN `events`, outbox, version | no, manual or maintenance                   | rebuild source changes (see below)                                                     |
| `_background_migrate_event_edges_mtxdb` (`events_bg_updates.py:1986`)                | reads `event_edges`                                | one-off                                     | after migration is universal                                                           |
| `events_bg_updates.py:323` index `event_edges_event_id_prev_event_id_idx`            | `event_edges`                                      | one-off                                     | the update                                                                             |

### Reads

| Caller                                                                          | Shape                                                           | Hot path                                                     | mtxdb-first today                 | Replacement difficulty                                    |
| ------------------------------------------------------------------------------- | --------------------------------------------------------------- | ------------------------------------------------------------ | --------------------------------- | --------------------------------------------------------- |
| `event_federation.py:2972` / `3033` `get_successor_events`                      | point lookup by prev id                                         | per federation backfill request (unverified frequency)       | yes, with SQL fallback and repair | easy: drop fallback after gates                           |
| `events_worker.py:2746` `is_event_next_to_forward_gap`                          | EXISTS with LEFT JOIN `rejections`                              | per request, `handlers/room.py:2223`                         | yes, with SQL fallback            | easy for the edge, but needs the rejection join in Python |
| `persist_events.py:1235` -> `events.py:1051` `_get_events_which_are_prevs`      | `event_edges` JOIN `events` JOIN `rejections` JOIN `event_json` | **yes, every persist that computes new forward extremities** | **no, SQL only**                  | hard: multi-table join                                    |
| `persist_events.py:1183, 1241` -> `events.py:1140` `_get_prevs_before_rejected` | same join, recursive loop                                       | **yes, same path**                                           | **no, SQL only**                  | hard                                                      |
| `event_federation.py:2101` `get_backfill_points_in_room_txn`                    | `events` JOIN `event_edges` JOIN `event_backward_extremities`   | backfill only                                                | no                                | hard: three-way join                                      |
| `event_federation.py:2487` `_get_connected_prev_event_backfill_results_txn`     | `event_edges` JOIN `events`, ordered by depth                   | backfill only                                                | no                                | hard: needs depth, order, outlier flag                    |
| `event_federation.py:2857` `_get_missing_events`                                | iterative `event_edges` JOIN `events`                           | `get_missing_events` federation endpoint                     | no                                | medium: loop already iterative                            |
| `events_worker.py:2935` partial-state query                                     | `NOT EXISTS` over `event_edges` JOIN `partial_state_events`     | partial-state join only                                      | no                                | hard                                                      |
| `purge_events.py:288`, `:553`                                                   | joins with `events_to_purge`, `event_backward_extremities`      | purge only                                                   | no                                | hard; temp tables                                         |
| `state_repair.py:123`                                                           | COUNT with LEFT JOIN `events`                                   | admin script                                                 | no                                | easy to change or drop                                    |
| `controllers/stats.py:108`                                                      | table size estimate                                             | admin                                                        | n/a                               | remove the name                                           |
| `synapse_port_db.py:118, 160`                                                   | port tables list                                                | one-off tool                                                 | n/a                               | remove the name after the table is dropped                |

`events.py:1480` also carries a comment about edges not being written to SQL or
the embedded engine in one case; check it when the insert is removed.

## What this means for cutover

1. **The persist-path reads are the real blocker.**
   `_get_events_which_are_prevs` and `_get_prevs_before_rejected` run on every
   persist, join four tables, and have no mtxdb path. Removing the SQL INSERT
   without replacing them leaves them reading an empty table. They also join
   `event_json` (family 3), so families 3 and 4 are coupled here.
2. **Replacing them is the expensive part.** They need the event's outlier flag,
   internal metadata and rejection status. Options: (a) keep `event_edges` in
   SQL for these queries (then nothing is removed), (b) have mtxdb return prevs
   and do the filter against the already-loaded event cache in Python, (c) add
   an mtxdb range call. None is designed or measured.
3. **Backfill, purge and partial-state readers are rare**, but they use SQL
   joins with `events` and `event_backward_extremities`. They also need a Python
   or mtxdb path, or a defined "unsupported in embedded mode" answer.
4. **Duplicate-write saving is real but small.** Only the write share of the
   18.3 s table time and the outbox 4.5 s go away at cutover, and the reads
   remain until item 1 is solved.

## Rebuild, purge, restart, read-only workers

- **Rebuild.** `rebuild_room_forward_index` takes a snapshot from SQL
  `event_edges` JOIN `events` at `room_edge_source_version` V, builds a new
  forward-index generation, then replays outbox rows after V. At cutover there
  is no SQL snapshot, so rebuild must read canonical prev_events from event JSON
  (the backward-edge record or the event body in mtxdb) and has no V or outbox
  to catch up against. A rebuild that races live writes then needs a different
  consistency rule (for example the single writer pausing edge writes for the
  room, or rebuilding from a log position). That rule is not designed.
- **Purge.** `delete_event_edges_batch` already deletes mtxdb edges for purged
  events. Today the SQL purge writes a delete outbox row first
  (`record_edge_index_deletes_txn`). At cutover it needs only the mtxdb delete,
  and the plan notes purge was found to be already correct. Prune the
  `event_edges` entry from the purge table loop. `remove_many` (B3) is a
  separate decision.
- **Restart and crash gap.** Today the outbox covers a crash between the SQL
  commit and the post-commit mtxdb write
  (`txn.call_after(put_event_edges_batch, ...)`). Without it, an event can be
  committed to SQL (`events`) with no edges. This is the shared "failure gap"
  constraint in `perf-track-b.md`. The mtxdb-authoritative answer must be that
  edges are derivable from the event body: on writer startup, find events newer
  than the last durable edge position and re-derive them. That recovery scan
  does not exist.
- **Read-only workers.** Already covered by the freshness plan: strict publish
  signal (`mtxdb@af82b409`), `synapse_event_edges_publish_signal_active` and
  `synapse_event_edges_reads_total`. Open items there: append-to-existing-parent
  partial-hit test, backward-edge liveness, production callers through Python,
  and the migration-completion authority gate (not enforced by
  `event_edges_fwd_is_authoritative`).
- **Write routing.** `_handle_mult_prev_events` writes mtxdb only when
  `_embedded_event_edges_writable` (events writer). That matches the single
  writer today. The shared multi-writer constraint in `perf-track-b.md` applies
  only if more than one SQL event writer is configured.

## What disappears, by stage

1. SQL fallback reads in `get_successor_events` and
   `is_event_next_to_forward_gap`, plus `repair_edge_index_from_sql` and the
   `room_edge_source_version` read on every gated hit (per the target-clock
   note). Requires the freshness gates in the removal plan.
2. SQL INSERT into `event_edges`, `bump_room_edge_source_version`, the outbox
   INSERT, `record_edge_index_*` in purge, `drain_edge_index_outbox`, and the
   rebuild's SQL snapshot and replay. Requires items 1 and 2 of "What this means
   for cutover" to be solved, the startup check
   (`EVENT_EDGES_SQL_INSERT_REMOVED = True`) and a rollback window.
3. Table, tables `edge_index_outbox` and `room_edge_source_version`, schema
   delta, background updates (`EVENT_EDGES_DROP_INVALID_ROWS`,
   `EVENT_EDGES_REPLACE_INDEX`), stats and port-db entries.

## Open questions

- Are `_get_events_which_are_prevs` / `_get_prevs_before_rejected` inputs small
  enough that a Python filter over mtxdb prevs beats the SQL join? Needs a
  per-statement timing, not a guess.
- How often do get_successor_events, backfill and get_missing_events run in the
  Complement workload? The table time may sit mostly in the persist-path joins.
- What is the recovery scan's position marker once the outbox is gone?
- Does removing the table break any third-party module or admin API reading it?
  Not checked.
