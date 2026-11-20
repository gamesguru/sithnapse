# Room timeline ordering and untrusted event depth

Status: design note only. Nothing here is implemented.

## Observed case

Room `!xk891i99RF7AvawRic:wombatx.me` on `sith.nutra.tk` returns one timestamp
inversion from `/messages?dir=b` (98 events, no gaps, no repeats):

| event                             | origin_server_ts | stored depth | stream_ordering |
| --------------------------------- | ---------------- | ------------ | --------------- |
| `$GeX7hVm5-…` display-name change | 2026-04-08       | 22           | -5940           |
| `$xSMIUY7n0…` invite              | 2026-04-13       | 22           | -5975           |

Both events have the same single `prev_events` parent (`$5QVetOCRzNI…`, depth
21), read from the stored event edges. That shared parent is what establishes
that they are siblings: neither is an ancestor of the other, so either relative
order is valid. The equal depth values agree with this, but on their own do not
prove it.

Both are backfilled. Within one depth the order falls to `stream_ordering`, and
negative backfill stream values record arrival order, not time. So the relative
order is an artifact of backfill arrival. It is not a pagination bug, and it is
unrelated to the initial-sync `prev_batch` cursor fix. The room has all 98
events; the single inversion is not missing history.

## What pagination actually orders by

- Pagination order is `ORDER BY topological_ordering, stream_ordering`
  (`synapse/storage/databases/main/stream.py:2401-2409`). Continuation tokens
  encode `t<topological>-<stream>`.
- On persist, both `events.depth` and `events.topological_ordering` are set to
  the received `event.depth` verbatim
  (`synapse/storage/databases/main/events.py:3334-3335`). It is not recomputed
  from the stored DAG.
- Locally created events derive depth from the parents:
  `max(prev_event.depth) + 1`, capped at `MAX_DEPTH`
  (`synapse/events/builder.py:283-288`).
- Received depths are only range-checked: an `int` with
  `0 <= depth <= MAX_DEPTH`, otherwise the PDU is rejected
  (`synapse/federation/federation_base.py:393-400`). There is no
  parent-consistency check.

So `/messages` and `/sync` ordering trusts the sending server's depth claim for
remote events. That is the current trust model, not a locally verified
topological rank.

Consequences of an implausible received depth:

- A very high depth moves the event to the apparent end of the room. Honest
  descendants inherit it, potentially reaching `MAX_DEPTH`; legacy values above
  the cap need the existing `fixup_max_depth_cap` background update
  (`synapse/storage/databases/main/events_bg_updates.py:2543-2601`).
- A very low depth lowers `room_depth.min_depth`
  (`synapse/storage/databases/main/events.py:4273-4286`), which can stop
  backfill early.
- Equal depth does not generally prove events are incomparable: a sender can
  give an ancestor and its descendant the same depth.
- A large depth jump trips the gap heuristic
  (`synapse/handlers/pagination.py:739-762`) and can cause repeated foreground
  backfill attempts.

Where causality matters, inspect `prev_events`/stored edges (or their transitive
closure), not depth. For the pair above, the shared parent is the proof.

## Ordering options

These are alternatives discussed for making concurrent events look more
chronological. They are design options, not current behavior.

| Option                                | Ordering                                                                                             | Trade-off                                                                                                                                                                                                                                                                                                                             |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A — keep current ordering             | `(wire depth, stream_ordering)`                                                                      | Existing behavior. Tokens and indexes unchanged. Equal-depth order follows local stream/backfill arrival order.                                                                                                                                                                                                                       |
| B — backfill-only timestamp tie-break | `(wire depth, origin_server_ts, stream_ordering)` for backfilled events (`stream_ordering < 0`) only | Can make backfilled siblings look chronological. Still trusts received depth and timestamp; gives backfilled and live events different tie-break rules; needs a token/key change; does not fix rows already stored unless they are rewritten.                                                                                         |
| C — timestamp tie-break everywhere    | `(wire depth, origin_server_ts, stream_ordering)` for all events                                     | Consistent among equal-depth events, but still trusts both sender-supplied fields and changes established ordering for every room.                                                                                                                                                                                                    |
| D — local DAG rank, then timestamp    | `(locally computed rank from stored prev_events, origin_server_ts, stable tie-break)`                | The only option that does not trust wire depth for the primary order. Requires a defined rank algorithm that stays valid as backfill adds edges, recomputation, a new column and index, and matching pagination cursors. Largest and most structurally sound if chronological ordering among concurrent events becomes a requirement. |

`origin_server_ts` is sender-supplied too. Options B and C only use it inside an
equal-depth group, which preserves causal order only if received depth is
honest. Option D avoids that assumption, but its rank must handle incomplete
histories, newly discovered edges, and non-ordinary DAG shapes before it can be
treated as stable.

## Pagination and forward-skip hazards

A sort key and its cursor key must agree. If a query orders equal-depth events
by timestamp but the continuation token still records only depth and stream
position, a page boundary can repeat or skip events. Every consumer of the
topological comparator must use the same order: backward and forward
`/messages`, `/context`, initial and incremental `/sync` windows, foreground gap
refills, and any other pagination path.

Forward pagination has a specific existing hazard. With `live_from_key`, the
`from_key` is a topological anchor for backfilled events (negative stream) while
live events use the caller's stream position
(`synapse/storage/databases/main/stream.py:2313-2336`). When a page ends on a
backfilled event, the next token keeps the live stream position rather than that
event's negative one, so a same-depth backfilled sibling can look already-seen
(`stream.py:2458-2551`). The current code fetches the whole depth group into the
page to avoid this, capped at `_MAX_BACKFILL_BOUNDARY_FETCH` (10000) with a
warning if a group is split. A new comparator must preserve this property or
encode the full sort key in the token.

A timestamp tie-break adds a second forward-skip case: an event that arrives
late with an earlier timestamp sorts before an already-issued forward cursor and
can be skipped by a forward walk. Any timestamp-based option must either define
how such events are rediscovered or explicitly accept the skip.

Tests for any such change should cover:

- page boundaries that split an equal-depth group, in both directions;
- an equal-depth group whose timestamp order crosses stream order;
- events arriving late with an earlier timestamp (the forward-skip case);
- gap refills and `/sync` windows using the same order as `/messages`;
- rooms with implausible received depths.

## Current decision

Keep option A. The Wombat Central inversion is a valid ordering of sibling
events, and there is no demonstrated product requirement to order concurrent
events by timestamp. The initial-sync cursor fix and the stale-outlier
pagination fix address separate bugs and are unaffected by this note.

If chronological ordering among concurrent events becomes a product requirement,
scope option D rather than adding `origin_server_ts` to one `ORDER BY`. Do not
assume honest received depth anywhere in the new design.
