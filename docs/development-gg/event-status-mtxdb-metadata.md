# Event status in mtxdb frame metadata

Status: design note, nothing implemented. Covers the state needed before the
family-4 persist queries can read from mtxdb without SQL. See
`perf-track-b-family4-edges-map.md` and
`res/docs/2026-09-28-event-edges-sql-removal-plan.md`.

## Problem

The persist-path queries filter persisted children by mutable event state:

- `_get_events_which_are_prevs` needs `NOT events.outlier` and no rejection,
  plus `soft_failed` for the metadata filter.
- `_get_prevs_before_rejected` needs the same rejection/soft-fail state.

Two of those inputs are not in mtxdb today:

- **Outlier** is excluded from the stored `internal_metadata` JSON by design.
  `rust/src/events/internal_metadata.rs:24` documents that `outlier`,
  `received_ts` and `stream_ordering` have their own SQL columns.
  `rust/src/events/internal_metadata.rs:604` repeats it for serialization.
  mtxdb's `event_json_put` stores that JSON verbatim, so mtxdb has no outlier
  bit at all. SQL `events.outlier` is the authority.
- **Rejections** are mirrored only on some paths. `events.py:4575` mirrors new
  rows, and `purge_events.py:430` / `:733` mirror deletes, but the accept/reject
  recheck path at `events_worker.py:2969-2991` deletes or upserts the SQL row
  with no mirror call. `get_rejections_batch` has no production caller.

`soft_failed` is in the metadata JSON, but reading it means reading and parsing
that record. Fine for the handful of children a persist actually finds; not a
reason to keep the SQL join.

## Decision

Put a compact, versioned status field in mtxdb **frame metadata** on the event's
already-separate metadata record (`event_meta_node_id`, see
`rust/src/database/mtxdb_syn.rs:3113`).

Do not put it in `internal_metadata` JSON. That blob is Synapse's event-body
compatibility format, it already excludes outlier, and reading it requires
locating and decoding the record. Frame metadata is stored outside the
compressed payload and is the right place for a few typed bits.

## What the frame format gives us

From the pinned mtxdb checkout (`mtxdb/src/engine/packfile/mod.rs`):

- One flag byte per frame. Known bits: `0x01` compressed, `0x02` CRC disabled,
  `0x04` metadata present, `0x08` removed (`:308-362`). An unknown flag bit is
  fatal, so **do not add a new flag bit** for this.
- `FLAG_METADATA` introduces `[version][length][TLV...]` outside the compressed
  node bytes (`:315-330`). Existing tags: logical id, content digest, role,
  digest algorithm, last-write LSN (`:334-354`).
- Unknown **TLV tags are preserved**, not rejected (`FrameMetadata.unknown`,
  `:576-578`; decoder arm `:706`). So a new tag is forward-compatible: older
  readers round-trip it and newer readers interpret it. This is the opposite of
  the flag-byte rule.
- Reads do not expose frame metadata today. `NodeData` is only
  `{ bytes, children }` (`mtxdb/src/engine/storage.rs:181-207`), and the
  `get`/`get_many`/`get_many_bytes` surface returns no `FrameMetadata`. This is
  the main API cost.

## Recommended change in mtxdb

Yes, make the change in mtxdb, but make it **generic**: mtxdb should round-trip
opaque per-record metadata and should not know what an event, outlier or
rejection is. Synapse owns the tag number and the payload definition.

The current surface is payload-only at every layer, not just at the getter:

| Layer   | Today                                                                                            | Change                                                                                                  |
| ------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------- |
| Write   | `StorageEngine::put` / `put_many` take `NodeData`                                                | add `put_many_with_metadata` (and the transaction equivalent) taking `Option<FrameMetadata>` per record |
| Journal | `Mutation::Put { collection_id, node_id, payload }` (`mtxdb/src/engine/journal.rs:106-115`)      | add `metadata: Option<FrameMetadata>`                                                                   |
| Overlay | `ReadJournal::apply_mutation` stores `(bytes, lsn)` (`packfile/storage/read_journal.rs:364-380`) | store the metadata alongside, so a read after a write sees the same status as a committed read          |
| Read    | `NodeData { bytes, children }`                                                                   | return a record with its `FrameMetadata`, or add `get_many_with_metadata`                               |
| Binding | `event_json_put` / `event_json_get` pass bytes only                                              | pass and return the status TLV                                                                          |

`PackfileStorage` already has internal
`append_put_many_entry(..., metadata, ...)` and
`put_many_internal(..., metadatas, ...)` paths, so the packfile can store it.
What is missing is the public surface, the journal mutation, and the overlay.
That makes this a contained upstream change, not a format redesign.

Constraints for the mtxdb change:

- Do **not** add a frame flag bit. Unknown flags are fatal (`KNOWN_FLAGS`,
  `packfile/mod.rs:325-327`).
- Keep metadata optional: a record with none stays byte-identical.
- Preserve unknown TLV tags on decode and re-encode. Verified at the pinned
  `f41d02a`: `FrameMetadata.unknown` exists and is re-emitted
  (`packfile/mod.rs:576-578`, `:620`). Older read-only readers therefore ignore
  the status tag safely. Any older writer that rewrites the metadata record must
  be on a revision that preserves unknown tags, or it will drop them.
- A plain `put` replaces the frame. "Preserve on rewrite" must be explicit at
  every write site, not assumed from the codec.

Cost versus a sibling status record:

- A sibling `event_status_node_id` record needs **no mtxdb change** and can be
  written in the same collection and transaction. It costs one extra small
  record per event and one extra point read per child in the persist filter.
- The frame TLV costs roughly the TLV bytes (about 10-16) in an existing frame
  and one read that already happens for `soft_failed`; it avoids a second record
  and keeps status tied to the record version (a rewrite cannot leave a stale
  sibling behind).
- Recommendation: take the frame TLV if the generic mtxdb change lands in one
  revision. If it slips, ship the sibling record behind the same
  `event_status_get` API; do not block the rejection-mirror fix or the persist
  read cutover on it.

## Proposed tag

Allocate one new TLV tag from mtxdb upstream, e.g. `META_TAG_EVENT_STATUS`
(proposed `0x10`; tags `0x01-0x05` are taken). Payload:

```text
version u8        = 1
flags   u8        bit 0 outlier
                  bit 1 rejected
                  bit 2 soft_failed (compat mirror, see below)
                  bit 3 out_of_band_membership (optional)
code    u8        rejection category, 0 = none/unknown
reserved u8       = 0
```

Rules:

- Keep the payload fixed-size and versioned. Unknown versions are treated as
  "status unavailable", so callers fall back to SQL during the transition.
- The header carries a rejection **bit and optional category**, not the reason
  text. `rejections.reason` is free text and the reason set is not provably
  closed. Keep the full reason in an authoritative record (the existing
  rejection record body is fine) and only put a stable category in the header if
  callers need to filter without reading it.
- `soft_failed` is already in `internal_metadata` and is rewritten on
  soft-fail/un-soft-fail. Do not make the JSON cleanup a prerequisite. Either
  (a) carry `soft_failed` in the header as a compatibility mirror maintained by
  the same paths, with a documented rule that JSON remains the writer until a
  later cleanup, or (b) leave it out of the header initially and parse the small
  metadata record only for children that have edges. Option (b) is smaller and
  still removes SQL; prefer it for the first cut.

## Write paths

The status field must be written or updated everywhere `outlier` or rejection
state changes, and must commit with the event body in the same mtxdb
transaction.

| Path                                            | What changes                          | Requirement                                                         |
| ----------------------------------------------- | ------------------------------------- | ------------------------------------------------------------------- |
| `event_json_put` / `_persist_events_txn_body`   | new event status                      | stage with `mtxdb_txn`; one transaction with body and edges         |
| `_update_outliers_txn` (`events.py` ~1435-1497) | de-outlier                            | rewrite the small metadata record; do not touch the JSON body       |
| `purge_events.py` outlier flips                 | retained event becomes outlier        | same rewrite                                                        |
| `_store_rejections_txn` (`events.py:4555`)      | rejection insert                      | set `rejected`; write the authoritative rejection record            |
| `events_worker.py:2969-2991`                    | accept (delete) / re-reject (upsert)  | clear/set `rejected`; **currently unmirrored**                      |
| purge `delete_rejections_batch`                 | rejection delete                      | clear `rejected`                                                    |
| soft-fail / un-soft-fail paths                  | metadata JSON update                  | header mirror only if option (a) is chosen                          |
| censoring / expiry / re-signing                 | body rewrite through `event_json_put` | preserve the status field; a body rewrite must not silently drop it |

Two properties matter:

1. **Atomicity with the body.** A newly persisted event's status is part of the
   same record set as its body and edges. An update to outlier/rejection is a
   separate small rewrite; that is unavoidable because those are post-persist
   state changes.
2. **Failure behavior during transition.** SQL remains authoritative until the
   verifier passes. A failed mtxdb status write must not corrupt SQL; it should
   surface as a retry or a reconciliation miss, not a silent divergence.

## Read API

Add one batched call, roughly:

```text
event_status_get(namespace, event_ids) ->
    [(event_id, status_flags, rejection_code, present)]
```

- Implement it in the Rust binding by reading the `event_meta_node_id` records
  with a metadata-capable read.
- It must use the same overlay/refresh path as `event_json_get`, so a read
  immediately after a write (including a read-only worker after publication)
  sees the same status. The pending-write overlay has to carry the metadata too.
- A missing record or an unknown status version returns `present = false`; the
  caller falls back to SQL until migration is complete.
- `_get_events_which_are_prevs` then becomes: one forward-edge batch read plus
  one status batch read for the children that exist, with no `runInteraction`.
  `_get_prevs_before_rejected` uses the same status batch inside its Rust
  recursion.

## Migration and verification

Backfill is a background update over SQL, batched and resumable:

1. `events.outlier` per event id.
2. `rejections` rows: `rejected` bit plus reason record.
3. Optionally `event_json.internal_metadata` for `soft_failed` if option (a).

Then a verifier compares mtxdb status against SQL for a deterministic sample and
for every event touched by an update path. It must cover:

- persisted non-outlier, persisted outlier, and de-outliered events;
- rejected, re-accepted (row deleted), and re-rejected events;
- purged rooms and purged rejections;
- censored/rewritten bodies (status preserved).

Cut over reads only after the verifier is clean and the mirror write paths have
run through a deployment window.

## Sequencing

1. Fix the unmirrored accept/reject path and add a `rejected` bit to the
   existing rejection mirror. This is an independent bug fix; SQL stays
   authoritative.
2. Add the status field to the mtxdb record (new tag or, if the upstream write
   API cannot take per-record metadata yet, a typed status record under a
   sibling node id in the same collection and transaction).
3. Backfill and verify.
4. Switch `_get_events_which_are_prevs` and `_get_prevs_before_rejected` to the
   mtxdb status + edge reads.
5. Proceed with the family-4 single-transaction cutover (body + edges), then
   remove the SQL join fallbacks, outbox and version gate.

Steps 1 and 2 are useful even if the edge cutover slips: they remove a real
mirror bug and give the persist path a SQL-free filter input.

## Open questions

- Allocate the TLV tag upstream and confirm the pinned mtxdb revision accepts
  per-record metadata on the transactional `put` path. Today `NodeData` has no
  metadata slot, and `DatabaseTransaction::put` takes a `NodeData`.
- Confirm the pending-write overlay and read-committed refresh preserve unknown
  TLV tags and expose them through the new read API.
- Confirm a metadata-only read does not decompress or checksum the node payload.
  `get_many_bytes` avoids the `NodeData` wrapper but still returns bytes only.
- Is the rejection reason set closed enough for a category enum? Until it is,
  keep the reason text in the authoritative record.
- Does any censoring/expiry rewrite construct a fresh `FrameMetadata` and drop
  unknown tags? The codec preserves them, but the writer template must too.
- Does the fallback typed-status record change the answer? If the frame API is
  too invasive, a sibling record costs one extra small read and no mtxdb change;
  measure both before committing to the tag.
