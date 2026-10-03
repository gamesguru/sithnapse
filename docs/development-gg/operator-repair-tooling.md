# Operator repair and DAG tooling

Sithnapse currently has the storage and federation primitives needed to build
room-repair tooling, but it does not yet expose a Congruent-like operator
console or a supported command set for repairing room DAGs.

This document records the current boundary and the intended future operator
surface. It is deliberately documentation-only: no repair command should be
considered available merely because the underlying storage APIs exist.

## Current capabilities

The following are available today:

- The Admin API can read room state, event context, room messages, and forward
  extremities.
- Federation backfill and event retrieval are part of the normal server
  machinery, but there is no bulk operator workflow for exporting or importing a
  room DAG.
- The `synapse_state_repair` entry point provides read-only `check-room`,
  `list-rejected`, and `list-outliers` commands.
- The Synapse manhole provides an SSH/TCP Python shell for low-level
  introspection. It is not a Unix-socket admin command console and should not be
  treated as a repeatable repair interface.

The state-repair tool currently reports discovery information only. Its
`--publish` option is reserved and intentionally not implemented.

## Not currently exposed

Sithnapse does not currently provide supported commands for:

- comparing a room's state with one or more remote servers;
- resetting or force-setting room state from local or remote input;
- rebuilding room state by replaying the persisted DAG;
- reordering or reindexing a room timeline topologically;
- bulk backfill/export of a room DAG;
- importing JSONL PDU/DAG files or outlier exports;
- repairing derived membership/state caches after state surgery; or
- attaching to a running server through a local Unix-socket admin console.

These operations should not be performed by directly editing database tables.
They affect state groups, current-state rows, event ordering, rejection state,
cache invalidation, federation visibility, and client sync behavior.

## Congruent comparison

The adjacent Congruent tree provides the model for the desired operator surface.
Its admin console includes commands corresponding to:

- `yolo compare-room-state`;
- `debug force-set-state` and `yolo force-set-state`;
- `yolo rebuild-state`;
- `yolo reorder-timeline`;
- `yolo get-remote-dag`;
- `yolo import-pdus` and `yolo import-outliers`; and
- a Unix-socket console attach/headless execution mode.

Sithnapse should converge on equivalent _capabilities_, but should adapt the
implementation to Synapse's SQL state tables, replication/cache machinery, and
the embedded state-HAMT/event-DAG storage rather than copying the command names
or assumptions mechanically.

## Proposed operator surface

The preferred interface is a local, authenticated Unix-socket console with an
optional headless CLI mode. It should work while the homeserver is running and
should make command output suitable for logs and automation.

An initial command namespace could be:

```text
room state-compare <room_id> [<server>...]
room state-rebuild <room_id> [--dry-run]
room state-force-set <room_id> [<server>...] [--input FILE] [--dry-run]
room dag-export <room_id> --output FILE.jsonl
room dag-backfill <room_id> [<server>...] [--import] [--reorder]
room dag-import FILE.jsonl [--room-id ROOM_ID] [--dry-run]
room timeline-reorder <room_id> [--dry-run]
room repair-report <room_id>
```

Names are provisional. The important properties are explicit dry-run support,
structured output, room-level locking, and clear distinction between:

- read-only diagnosis;
- fetching and staging remote data;
- normal auth-checked insertion; and
- emergency force insertion that bypasses selected checks.

## Safety requirements

Before any write-capable repair command is added, it should:

1. refuse partial-state rooms unless a separately designed recovery mode is
   selected;
2. verify that required predecessor, state, and auth events are present or
   explicitly stage them first;
3. acquire a room-scoped repair lock, initially allowing offline operation if
   online fencing is not yet reliable;
4. default to a deterministic dry-run diff;
5. preserve old state groups and repair metadata for audit/rollback;
6. update current-state, rejection, replication, and cache state through the
   same audited machinery used by normal persistence; and
7. warn prominently when timeline ordering or stream ordering changes can
   invalidate client sync tokens or caches.

JSONL import/export should include enough metadata to distinguish timeline
events, outliers, rejected/soft-failed events, state hashes, and local ordering
metadata. Raw event JSON alone is not a sufficient repair record.

## Suggested implementation order

1. Add a Unix-socket admin console with headless command execution.
2. Add read-only room DAG/state reports and remote state comparison.
3. Add DAG export and authenticated remote backfill staging.
4. Add dry-run JSONL import validation.
5. Add offline state replay/rebuild and state publication for one room.
6. Add force-set, cache repair, rejection repair, and timeline reorder behind
   explicit restricted/emergency controls.

The existing design note in
[`state-repair-from-dag.md`](state-repair-from-dag.md) describes the deeper
state publication model and should be updated when the first write-capable
milestone is implemented.
