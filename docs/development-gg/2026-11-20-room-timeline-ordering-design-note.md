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
chronological. Option C is now implemented; A, B, and D remain alternatives.

| Option                                | Ordering                                                                                             | Trade-off                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A — keep current ordering             | `(wire depth, stream_ordering)`                                                                      | Existing behavior. Tokens and indexes unchanged. Equal-depth order follows local stream/backfill arrival order.                                                                                                                                                                                                                                                                         |
| B — backfill-only timestamp tie-break | `(wire depth, origin_server_ts, stream_ordering)` for backfilled events (`stream_ordering < 0`) only | Can make backfilled siblings look chronological. Still trusts received depth and timestamp; gives backfilled and live events different tie-break rules; needs a token/key change; does not fix rows already stored unless they are rewritten.                                                                                                                                           |
| C — timestamp tie-break everywhere    | `(wire depth, origin_server_ts, stream_ordering)` for all events                                     | Implemented in `e49e5bd1fb`. Consistent among equal-depth events, but still trusts both sender-supplied fields and changes established ordering for every room. Existing cached two-part historical tokens must be discarded.                                                                                                                                                           |
| D — local DAG rank, then timestamp    | `(locally computed rank from stored prev_events, origin_server_ts, stable tie-break)`                | The only option that does not trust wire depth for the primary order. Its hard part is cursor stability: a local rank can change when backfill discovers older edges, so cursors issued under the old rank can skip or repeat unless the rank is append-only or versioned. Also needs a defined rank algorithm, recomputation, a new column and index, and matching pagination cursors. |

`origin_server_ts` is sender-supplied too. Options B and C only use it inside an
equal-depth group, which preserves causal order only if received depth is
honest. Option D avoids that assumption, but its rank must handle incomplete
histories, newly discovered edges, and non-ordinary DAG shapes before it can be
treated as stable.

Two limits of option C:

- It only reorders events at equal depth. A sibling branch one level deeper with
  an earlier timestamp still sorts after, so C removes the obvious inversions,
  not every one.
- "Concurrent" is only guaranteed for honest senders. A forged equal depth
  between an ancestor and its descendant would let the timestamp reorder a
  causal pair. That is the depth-trust gap D would close, and it has not
  affected this room.

### Cross-depth example

The first limit is not theoretical. In `!4zKUu8M4fstFjTFZ9E:nutra.tk` the
stored edges (read from mtxdb, since this deployment keeps edges outside SQL)
give this shape:

```text
                  $yspw4Ug…  depth 459, 13 Nov 23:40Z
                   /                     \
       $fJT7NvSS…                          $3L-wW0HF…
       depth 460, 15 Nov 02:06Z             depth 460, 15 Nov 21:17Z
       (gg/Shane joined)                    (nex left)
             |
       $wb1OgO1X…
       depth 461, 15 Nov 03:34Z
       (goldencasey joined)
```

| Event | Depth | Timestamp (UTC) | `prev_events` |
| --- | --- | --- | --- |
| `$yspw4Ug7285DHzv_QXd0fm62B-2eaeGHQxZ6OUTlLu0` | 459 | 13 Nov 23:40:22 | `$nGLgjWfk…` (458) |
| `$fJT7NvSSuaoIj26XHACD8E_I6if_MIbwVXMDKk6b90M` | 460 | 15 Nov 02:06:04 | `$yspw4Ug…` |
| `$3L-wW0HFg-PEOhj74wf1p9luw59iH3oGi0jO4WHOKQc` | 460 | 15 Nov 21:17:51 | `$yspw4Ug…` |
| `$wb1OgO1XAo13Gd2yZUllkweNTi0IxfCn45tIKzsOU8I` | 461 | 15 Nov 03:34:01 | `$fJT7NvSS…` |

- The two depth-460 events are siblings: both list `$yspw4Ug…` as their only
  parent, so they are concurrent.
- The depth-461 event is a child of the depth-460 join, so it is causally
  after that join but concurrent with the depth-460 leave.

Chronological order is join (02:06) → goldencasey (03:34) → leave (21:17). The
graph permits it, so this is not a causal timestamp reversal.

| Ordering | Displayed (oldest first) |
| --- | --- |
| A — `(depth, stream_ordering)` | leave, join, goldencasey |
| C — `(depth, origin_server_ts, stream_ordering)` | join, leave, goldencasey |

C fixes the sibling half (join before leave) but still displays the depth-461
join after the depth-460 leave, because depth outranks timestamp. Removing the
remaining inversion needs a timestamp-prioritised topological traversal that
emits the earliest-timestamp ready head: after the join is emitted,
goldencasey becomes ready and sorts before the leave. That cannot be expressed
as a single `ORDER BY`; the cursor would have to encode the traversal frontier,
and it would be unstable if backfill later discovers older edges. C
deliberately does not attempt it.

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

Late backfilled events can still sort before an already-issued forward cursor
and be missed by that walk. This is an existing class of backfill race: negative
stream positions already let later-backfilled events sort before a cursor, and
the timestamp key does not make that guarantee worse. Live events are also
bounded by their stream position during forward pagination, so a late live event
with an earlier timestamp remains eligible for the next page.

Tests for any such change should cover:

- page boundaries that split an equal-depth group, in both directions;
- an equal-depth group whose timestamp order crosses stream order;
- late backfilled events and forward pagination;
- gap refills and newly joined/initial `/sync` windows using the same order as
  `/messages` (incremental `/sync` remains stream-ordered);
- rooms with implausible received depths.

## Current decision

Option C is implemented in `e49e5bd1fb`: historical cursors now carry depth,
origin timestamp, and stream ordering, and pagination uses that three-part key.
The old two-part historical token format is rejected; clients must clear cached
pagination cursors. Option D remains a separate future design if local DAG rank
is needed to avoid trusting wire depth. The initial-sync cursor fix and the
stale-outlier pagination fix address separate bugs and are unaffected by this
change.

The implementation still needs verification on PostgreSQL and mtxdb, especially
the three-column row comparison and the background-built expression index.

The reason C was selected was:

- Depth, timestamp and stream are all fixed when an event is stored, so the
  cursor key is immutable and previously issued cursors keep their meaning.
- C treats live and backfilled events the same; B does not.
- C preserves causal order while depth is honest, because the timestamp only
  acts inside an equal-depth group. It needs no rewrite of existing rows.
- The visible result for the Wombat Central pair is the same as D.

Scope D only if untrusted depth becomes a real requirement. D is not a better
temporal fix; it is a depth-trust fix. Its rank-stability problem is the main
cost and must be designed first, not treated as a detail.

The implementation includes:

- the timestamp in the continuation token, plus a matching
  `(room_id, topological_ordering, COALESCE(origin_server_ts, 0),
  stream_ordering)` index and migration, so the index expression matches the
  query ordering;
- the same comparator in every ordering site: `/messages` in both directions,
  `/context`, newly joined/initial `/sync` windows, and gap refills. Incremental
  `/sync` remains stream-ordered and uses stream tokens;
- handling for the live/backfill forward boundary, plus tests for late live
  events on an older branch and same-depth backfilled events;
- the equal-depth ordering regression test, paged one event at a time in both
  directions.

Still to verify: the three-column comparison and background expression index on
PostgreSQL, and the index/order path on mtxdb.

No ordering can guarantee that displayed dates always increase while preserving
causality when timestamps are sender-supplied. C removes the visible inversions
among concurrent events; it does not make clock skew disappear.
