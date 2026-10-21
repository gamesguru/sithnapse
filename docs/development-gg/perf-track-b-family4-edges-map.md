# Family 4 map: `event_edges`, `edge_index_outbox`, `room_edge_source_version`

Dependency map for B2 family 4 (see `perf-track-b.md`). Only the measurement
labels are implemented (`a912896dd7`). Line numbers are from the tree at the
time of writing.

**Model.** Edges are derived from each event's immutable `prev_events`. The
snapshot, version gate and outbox exist only because SQL is the authority and
mtxdb a mirror. With mtxdb authoritative, an event's body and edges commit in
one mtxdb transaction; there is no snapshot, version gate or outbox, and no
automatic recovery scan. The accepted crash trade is a manual repair tool that
re-derives edges from event bodies.

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

## Measured: the persist-path reads dominate

Run: `tests.storage` only (787 tests, `-j4`, sync off, pin `f41d02a`; 25
processes, summed SQL 57.2 s). Not a full-suite or federation workload, and
storage tests are persist-heavy, so do not scale it up.

| Label              | Calls | SQL exec | Total await | Candidates | Rows  |
| ------------------ | ----- | -------- | ----------- | ---------- | ----- |
| `persist_prevs`    | 2,371 | 1.33 s   | 1.99 s      | 5,102      | 8     |
| `persist_rejected` | 2,029 | 0.97 s   | 1.62 s      | 2,808      | 1,927 |
| combined           | 4,400 | 2.30 s   | 3.61 s      |            |       |

- `event_edges` had 6,590 SQL calls (2.48 s); the INSERT ran 2,082 times. These
  two queries are about two thirds of the calls, so reads dominate and removing
  the duplicate write alone saves little. Table attribution is approximate.
- About 1.3 s (36%) of the 3.6 s is outside statement execution (pool and
  overhead); an FFI call avoids that part.
- Pool scheduling delay per query is not known: both fall below the 15-entry
  "top scheduling delay" cutoff.
- About 1.4% of wall time here (3.6 s over 4 jobs in 66.6 s).
- `persist_prevs` returned 8 rows for 5,102 candidates: nearly every call finds
  nothing. A forward-children lookup (`ffi_event_edges_get_forward`, about 0.04
  ms) would answer almost all of them. Hypothesis, not measured on the
  replacement.

## Replacement plan

1. **`_get_events_which_are_prevs`.** One batched call in the Synapse Rust
   binding over mtxdb: candidate ids in, forward children out; read the
   Edge-pool status record for children only. See
   `event-status-mtxdb-metadata.md`. Keep the SQL path as fallback until
   measured.
2. **`_get_prevs_before_rejected`.** After the above, once the filter inputs are
   verified (below). Returns many rows, so it needs the full filter.
3. Add an mtxdb API only if the binding cannot batch these reads efficiently.

The persist path no longer reads `internal_metadata` or the rejection mirror; it
reads the new Edge-pool status record (outlier, rejected, soft-failed). The
rejection mirror's completeness still matters for the rejected bit and its own
migration; see the note's mutation-path table.

## What this means for cutover

1. **The persist-path reads are the blocker.** Removing the SQL INSERT without
   replacing them leaves them reading an empty table. They also join
   `event_json` (family 3), so families 3 and 4 are coupled here.
2. **Backfill, purge and partial-state readers are rare** but use SQL joins with
   `events` and `event_backward_extremities`. They need a Python or mtxdb path,
   or a defined "unsupported in embedded mode" answer.
3. **Rebuild** becomes a manual repair tool that re-derives edges from event
   bodies. There is no snapshot, version or outbox to catch up against.
4. **Purge.** `delete_event_edges_batch` already deletes mtxdb edges; drop the
   `record_edge_index_deletes_txn` outbox/version writes and the `event_edges`
   entry from the purge table loop.
5. **Restart / crash gap.** An event can be committed to SQL `events` with no
   edges if the process dies before the mtxdb commit. Accepted; the repair tool
   covers it. No startup scan.
6. **Read-only workers** are covered by the freshness plan (publish signal
   `mtxdb@af82b409`, `synapse_event_edges_*` metrics); its migration-completion
   gate is not enforced by `event_edges_fwd_is_authoritative`.
7. **Write routing.** mtxdb is written only when
   `_embedded_event_edges_writable` (the events writer); see the shared
   single-writer constraint in `perf-track-b.md`.

## What disappears, by stage

1. The `persist_prevs` and `persist_rejected` SQL joins (via the FFI lookup),
   the SQL fallback in `get_successor_events` and
   `is_event_next_to_forward_gap`, and `repair_edge_index_from_sql`.
2. SQL INSERT into `event_edges`, `bump_room_edge_source_version`, the outbox
   INSERT, `record_edge_index_*` in purge, `drain_edge_index_outbox`, and the
   snapshot/replay rebuild. Needs stage 1, the startup check
   (`EVENT_EDGES_SQL_INSERT_REMOVED = True`) and a rollback window.
3. Tables `event_edges`, `edge_index_outbox`, `room_edge_source_version`, schema
   delta, background updates, stats and port-db entries.

## Open questions

- Is the outlier flag in mtxdb always in step with `events.outlier`?
- Full-suite cost of the two queries, and their pool scheduling delay.
- How often do `get_successor_events`, backfill and `get_missing_events` run in
  a federation workload?
- Does removing the table break any third-party module or admin API?
