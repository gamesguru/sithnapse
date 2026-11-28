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

## Narrow repair: `repair-member`

For the case where a single member event was dropped at join time (for example a
notary key-window problem made a `send_join` state event fail verification),
`synapse_state_repair repair-member` repoints one `m.room.member` key without a
full DAG replay:

```
synapse_state_repair --config homeserver.yaml repair-member \
    --room '!room:id' --event-id '$leave' --via unredacted.org            # dry run
synapse_state_repair --config homeserver.yaml --publish --homeserver-is-stopped \
    repair-member --room '!room:id' --event-id '$leave' --via unredacted.org
```

It fetches the event through the normal federation client (signatures and hashes
checked against the keyring), persists it as an outlier through the normal
auth/rejection path, then for each distinct state group among the forward
extremities stores a new group (`prev_group` plus a one-key delta), repoints the
extremities at it, and recalculates current state with the normal persistence
code. Patching `current_state_events` alone would not stick, because the next
event inherits its state group from its prev events. Old state groups are left
in place.

It refuses when the room is partial-state or an MSC4242 state-DAG room, when the
event is rejected or not a member event of that room, or when the current value
for the key is not one of the replacement event's `auth_events` (i.e. the
replacement does not supersede it). `--publish` requires
`--homeserver-is-stopped`.

## Importing a state diff: `repair-state import`

`repair-member` fixes one known event. To find out _which_ keys were dropped,
`repair-state import` compares local state with a remote server's state at the
room's single forward extremity:

```
synapse_state_repair --config homeserver.yaml repair-state import \
    --room '!room:id' --via unredacted.org                    # dry run
synapse_state_repair --config homeserver.yaml --publish --homeserver-is-stopped \
    repair-state import --room '!room:id' --via unredacted.org
```

It fetches `/_matrix/federation/v1/state_ids` at the extremity, diffs by event
ID, and fetches only the symmetric difference. Remote events already held
locally are reused; the rest go through `get_pdu`, so signatures and hashes are
checked against the keyring. The report lists every differing key with the local
and remote event, the action (`import`, `replace`, or `keep-local`), and the
state-group rewrite plan.

Publishing imports the missing/differing events through the normal outlier auth
path and applies them to the forward extremities with the same state-group
rewrite as `repair-member`. Keys that exist only locally are reported but never
removed, so local-only memberships survive. It refuses partial-state and MSC4242
rooms, a multi-extremity room without `--at-event`, and a diff larger than
`--max-diff`. The remote's `/state_ids` answer is the state _before_ the
comparison event; the tool adds the accepted event itself before comparing, and
ignores the predecessor that event supersedes.

An import whose seed state already matches the remote produces no plan and makes
no changes. That includes re-running after a publish that stopped partway: the
remaining forward-extremity groups are not reconciled automatically, because
there is no recorded plan to resume and applying the seed's historical snapshot
to another extremity could roll back state that legitimately changed after it.
Re-seed at the affected extremity with `--at-event` instead.

This is the import half, and it only finds differences at the seed event. A key
that was wrong early and was overwritten later will not appear in its diff.

### Propagating the diff: `--propagate`

`repair-state import --propagate --at-event <join>` seeds the comparison at a
historical event instead of the tip. Any local non-outlier event is accepted,
including a non-extremity. After importing the missing events, every descendant
state group that still holds the seed's value for a changed key is rewritten as
`old_group + delta`, and the events on that group are repointed:

```
synapse_state_repair --config homeserver.yaml repair-state import \
    --room '!room:id' --via unredacted.org \
    --at-event '$local-join' --propagate                    # dry run
```

Descendant groups are found through `state_group_edges`, not by replaying the
event DAG, so the repair works when the dropped event itself is only a stateless
outlier. A group whose value for a key was legitimately changed later is left
alone and reported under `stops`. Groups with no local event are counted but not
rewritten. `--max-events` bounds the descendants considered (default 200000). An
interrupted propagation has no durable resume record. If the seed already
matches on a later invocation, the tool leaves other groups unchanged rather
than applying the historical snapshot to forward extremities; review and plan a
new repair explicitly.

In the JSON report, `plan` is a list of step objects with group IDs, affected
event IDs, and `changes`; the tuple-keyed internal delta is omitted. With
`--propagate`, the same steps are also under `propagation.groups`. A propagation
that could not finish reports `partial: true`, `published: false` and the
unfinished work under `propagation.incomplete_work`; save the report, since it
is the only record.

A valid local non-outlier seed can still produce no publishable plan. If any
planned group would repoint a state event to a group that replaces that event's
own key with a different event ID, the store cannot keep both the historical and
the repaired mapping, so `_import_state` refuses the entire plan, including in
dry-run mode, and reports the offending events under `unrepresentable`.

The report also lists events whose `auth_events` name the replaced event
(`auth_review`), including their current rejection status. That list is
report-only; published repairs never change existing rejections. The leave is
still not inserted into the timeline at its historical position.

The full `repair-state sweep` remains unimplemented. Unlike `--propagate`, it
would recompute every event's state from its prev groups (with real state
resolution at merges) and report auth decisions, rather than applying a known
key delta to the groups that descend from one seed.
