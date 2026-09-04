# Room state repair from the persisted DAG

This note describes the recovery path we need when a state-resolution bug has
already committed bad room state. The target is an organic in-place repair: keep
the room, keep users joined, and require at most client cache refreshes.

## Problem

Room state groups and HAMT roots are materialized results of state resolution.
If the state-resolution algorithm has a systematic bug, it can persist a
structurally valid but logically wrong state group. Rebuilding the HAMT from
that state group only preserves the damage.

For full-state rooms, the durable source is the room DAG:

```
events + event_edges + auth events + state_events + room version
    -> state resolution
    -> (type, state_key) -> event_id
    -> state group
    -> HAMT root/nodes
```

Therefore the recovery path must be able to ignore existing state groups for the
affected range and recompute state from persisted events using fixed code.

Partial-state rooms are out of scope for this repair mode. If the server does
not have the complete local state inputs, the tool must refuse to publish.

## Requirements

The repair must:

- preserve room identity;
- preserve local membership rows unless the recomputed state explicitly changes
  them;
- not require users to leave/rejoin;
- default to dry-run;
- produce a deterministic diff before publication;
- publish replacement state groups and HAMTs before repointing live tables;
- update `current_state_events` via the same stream/cache machinery used by
  normal persistence, or an equivalent audited repair path;
- leave old state groups intact for audit and rollback until an explicit later
  cleanup;
- refuse to run if the room is partial-state or the local DAG is incomplete.

## Recovery Model

The repair command should operate in phases.

This is the same broad recovery model as conduwuit's YOLO admin tools:

- `yolo rebuild-state` incrementally rebuilds room state from the beginning of
  the timeline without changing room membership or timeline order;
- `yolo force-set-state` re-resolves and applies a room state snapshot;
- `yolo manage-rejected`, `yolo unreject-room`, and `yolo list-rejected` expose
  rejection repair as an explicit operator step;
- `yolo rebuild-membership-cache` repairs derived membership caches after state
  surgery.

The Synapse version needs the same phases, but its publication target is
`state_groups`, `event_to_state_groups`, `current_state_events`, rejection rows,
current-state streams, cache invalidations, and HAMT roots/nodes.

### 1. Freeze

Acquire a per-room repair lock and stop new event persistence for the room. This
can initially require the homeserver to be offline. Online repair can come later
once room-scoped persistence fencing exists.

### 2. Discover

For each target room:

- verify the room is not in `partial_state_rooms`;
- read the room version from `rooms`;
- read current forward extremities from `event_forward_extremities`;
- read room DAG edges from `event_edges`;
- read state event metadata from `state_events`;
- identify candidate divergence points from operator input, bad state groups, or
  event stream ranges;
- verify that every needed prev event, previous state event, and auth event is
  present locally.

If any required event is missing, stop. Do not synthesize state from federation
during publication.

### 3. Replay

Topologically walk the affected portion of the room DAG. For each event whose
state must be repaired:

- compute state before the event from repaired predecessor state;
- run event auth using repaired auth-state inputs;
- decide rejection status using the fixed code;
- for accepted state events, apply the state event to produce state after the
  event;
- for merges, run fixed state resolution over repaired predecessor state sets;
- materialize the resulting state as a new state group and HAMT.

Rejected events must not be blindly preserved or blindly un-rejected. Their
rejection status is part of replay output and must be included in the dry-run
diff.

### 4. Compare

Before publication, emit:

- old and new state group IDs;
- changed `(type, state_key)` entries;
- rejection status changes;
- current-state row changes;
- local membership changes;
- event count and stream range covered;
- missing-input checks.

The current `synapse_state_repair check-room` command is read-only: it reports
discovery metadata and counts (room version, forward extremities, state edges,
state events, outliers, rejected events) but does not yet produce a diff or
comparison against a proposed repair. Compare output is planned as a future
phase to make repair plans auditable before write support is designed.

### 5. Future publication design

Publication order:

1. write all replacement HAMT nodes;
2. write replacement `state_groups` rows;
3. write replacement root pointers;
4. update `event_to_state_groups` for affected events;
5. update `rejections` for events whose rejection result changed;
6. rebuild `current_state_events` from repaired forward extremities;
7. append `current_state_delta_stream` entries for the current-state diff;
8. invalidate state, current-state, membership, room-summary, and sync caches;
9. release the room repair lock.

The old branch should remain present. A future repair implementation should
create a new branch and repoint live metadata only after all replacement data
verifies.

## CLI Shape

Initial interface:

```
synapse_state_repair --config homeserver.yaml check-room --room '!room:id'
synapse_state_repair --config homeserver.yaml check-room --room-file rooms.txt
synapse_state_repair --config homeserver.yaml list-rejected
synapse_state_repair --config homeserver.yaml list-outliers
```

Useful current flags:

```
--write-report report.json
```

## First Implementation Milestone

Build the read-only `synapse_state_repair check-room` command that:

- loads target rooms;
- refuses partial-state rooms;
- reports room version and forward extremities;
- reports state event and state edge counts;
- identifies events in the affected range;
- writes a JSON report.

The first write-capable milestone should only publish repaired HAMTs/state
groups for one room while the homeserver is offline. Online organic repair
should wait until room-scoped persistence fencing is explicit.
