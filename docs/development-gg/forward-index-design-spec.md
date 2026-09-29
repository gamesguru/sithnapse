# Design Specification: Forward-Index Engine (`embedded_edges.rs`)

---

## 1. Overview & System Architecture

This specification defines the storage layout, protocol, and concurrency mechanics for the Matrix DAG event edge forward-index in `sithnapse` (`rust/src/database/embedded_edges.rs`).

Because neither Rezzy nor mtxdb has pre-existing production deployments, we target the end-state architecture directly:

1. **Dedicated Forward Namespace (`FWD`):** Forward edges are fully segregated from backward edges (`PREV`), eliminating candidate discriminator hashing on read queries.
2. **Per-Room Active Generation Pointer (`active_generation`):** Full room rebuilds run entirely out of band into Generation $N+1$, followed by an $O(1)$ pointer flip under a short room lock.
3. **In-Place Live Mutations:** Live edge writes update the current `active_generation` directly under the room lock.
4. **Disposable Dev Layout / Re-initialization:** Old development databases that lack the generation layout are rejected and cleanly reinitialized.

---

## 2. Namespace & Collection Derivation

```rust
/// Member namespace tags under the room's group identity in the Edges pool:
/// - `PREV`: Backward event edges `[event_id -> (prev_event_id, is_state)]`
/// - `FWD `: Forward event edges for generation N
pub const BACKWARD_MEMBER_TAG: [u8; 4] = *b"PREV";
pub const FORWARD_MEMBER_TAG: [u8; 4] = *b"FWD ";

/// Derive the collection ID for a room's specific forward generation:
/// BLAKE3-256("mtxdb/member/v1" || "FWD " || group_logical_id || gen_u64_be)[0..16]
#[must_use]
pub fn forward_edges_room_gen_id(room_id: &str, generation: u64) -> [u8; 16] {
    let group_digest = group_full_logical_id(room_id.as_bytes());
    let mut hasher = DigestAlgorithm::Blake3.hasher();
    hasher.update(MEMBER_DOMAIN_PREFIX);
    hasher.update(&FORWARD_MEMBER_TAG);
    hasher.update(&group_digest);
    hasher.update(&generation.to_be_bytes());
    let digest = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}
```

---

## 3. Protocol & Concurrency Workflow

### 3.1 Reader Path (`get_forward_edges`)

1. Read `active_generation` for `room_id` (cached in-memory or read-committed from room metadata).
2. Derive `collection_id = forward_edges_room_gen_id(room_id, active_generation)`.
3. Directly lookup forward edge records by `prev_event_id`.
4. Return decoded children without candidate discriminator hashing (collection is 100% forward edges).

### 3.2 Live Writer Path (Incremental Append)

1. Acquire short `lock_rooms(room_id)`.
2. Read current `active_generation`.
3. Fetch existing forward edge list from `forward_edges_room_gen_id(room_id, active_generation)`.
4. Append & deduplicate new forward edge.
5. Write back to active generation collection.
6. Release `lock_rooms(room_id)`.

### 3.3 Full Out-of-Band Rebuild Workflow

```text
[Step 1: Allocation (Out of Lock)]
  │
  ├── Read current `active_generation = G`.
  └── Target new generation `G_next = G + 1`.

[Step 2: Out-of-Band Population (Zero Lock Contention)]
  │
  ├── 1. Allocate collection: `forward_edges_room_gen_id(room_id, G_next)`.
  ├── 2. Scan backward edges / SQL events for `room_id`.
  ├── 3. Build forward edge index directly in `G_next`.
  └── 4. Sync / stage generation data in mtxdb.

[Step 3: Atomic Pointer Swap (Short Lock Hold)]
  │
  ├── 1. Acquire short `lock_rooms(room_id)`.
  ├── 2. Replay any live edge appends that occurred between Step 1 and Step 3 into `G_next`.
  ├── 3. Write `active_generation = G_next` in room metadata.
  ├── 4. Commit atomic metadata update.
  └── 5. Release `lock_rooms(room_id)`.

[Step 4: Post-Cutover]
  │
  └── All subsequent reader queries instantly query `G_next`.
      Retired generation `G` is marked for deferred epoch GC.
```

---

## 4. Implementation Steps

1. **Namespace Separation:** Add `FORWARD_MEMBER_TAG` (`FWD `) and generation-scoped collection ID derivation in `mtxdb_syn.rs`.
2. **Metadata Pointer Storage:** Store `active_generation: u64` in room metadata records in `mtxdb`.
3. **Dedicated Reader Path:** Update `get_forward_edges` to query the active generation collection directly without candidate hashing.
4. **Live Append Path:** Point live `event_edges_put` mutations to `active_generation`.
5. **Out-of-Band Rebuild Tool:** Build background repair command generating `G+1` and performing the $O(1)$ pointer swap under the short room lock.
6. **Layout Version Guard:** Reject outdated mtxdb dev layouts lacking the generation metadata scheme.
