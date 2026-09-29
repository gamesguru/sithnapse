# Design Specification: Forward-Index Engine (`embedded_edges.rs`)

---

## 1. Overview & System Architecture

This specification defines the storage layout, protocol, and concurrency mechanics for the Matrix DAG event edge forward-index in `sithnapse` (`rust/src/database/embedded_edges.rs`).

Because neither Rezzy nor mtxdb has pre-existing production deployments, we target the end-state greenfield architecture directly:

1. **Dedicated Forward Namespace (`FWD `):** Forward edges are fully segregated from backward edges (`PREV`), eliminating candidate discriminator hashing on read queries.
2. **Per-Room Active Generation Pointer (`active_generation`):** Full room rebuilds run out of band into Generation $N+1$, followed by an atomic pointer flip under `lock_rooms(room_id)`.
3. **Dual-Store Synchronization Protocol:** Live edge mutations and rebuild pointer flips coordinate under the room lock to prevent cross-store race conditions between SQL (authoritative source) and `mtxdb` (accelerated derived index).
4. **Iterative Catch-Up Loop:** Out-of-band rebuilds catch up to live traffic before acquiring the final publication lock, minimizing the lock-hold duration.
5. **Generation Retention & Deferred GC:** Retired generations ($N$) are kept on disk initially so active in-flight reader scans complete safely; automated reader-epoch GC is added in a subsequent phase.

---

## 2. Schema & Collection Definitions

| Store | Collection / Table | Key Structure | Value Structure |
|---|---|---|---|
| **`mtxdb`** | `backward_edges` (`PREV`) | `(room_id, child_event_id)` | `Vec<(prev_event_id, is_state)>` |
| **`mtxdb`** | `forward_edges_v2` (`FWD `) | `(room_id, generation_id, parent_event_id)` | `Vec<child_event_id>` |
| **`mtxdb`** | `room_forward_meta` | `room_id` | `active_generation: u32` |
| **SQL** | `event_edges` | `(room_id, ...)` | Authoritative edges & `source_version: u64` |

```rust
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

## 3. Protocol Specification

### 3.1 Reader Path (`get_forward_edges`)

1. Read `active_generation` from `room_forward_meta` (cached or read-committed).
2. Derive `collection_id = forward_edges_room_gen_id(room_id, active_generation)`.
3. Query `forward_edges_v2` directly by `parent_event_id`.
4. Return decoded child event IDs. (Zero candidate hash discrimination required).

### 3.2 Live Writer Path (Dual-Store Sequencing)

Every live edge write executes sequentially under the room lock:

1. **Acquire `lock_rooms(room_id)`**.
2. Read current `active_generation` from `mtxdb` metadata.
3. Commit authoritative SQL edge mutation and increment SQL `source_version`.
4. Apply the forward-edge append/mutation to `forward_edges_v2` under `(room_id, active_generation)`.
5. **Release `lock_rooms(room_id)`**.

*Invariant:* Because the room lock covers steps 2–4, no pointer swap can land mid-write; mutations deterministically land in whichever generation is active at write time.

---

### 3.3 Full Rebuild Protocol (Iterative Catch-Up Loop)

```text
[Step 1: Out-of-Band Snapshot Build (Unlocked)]
  1. Record V_start = current SQL source_version.
  2. Set G_new = active_generation + 1.
  3. Build G_new in `forward_edges_v2` from SQL snapshot at V_start.

[Step 2: Unlocked Catch-Up Loop]
  4. Fetch current SQL source_version V_latest.
  5. If V_latest > V_start:
       Apply edge deltas for V_start..V_latest onto G_new.
       Set V_start = V_latest.
       Repeat until (V_latest - V_start) is near zero.

[Step 3: Locked Replay & Atomic Pointer Swap]
  6. Acquire lock_rooms(room_id).
  7. Read V_final from SQL.
  8. If V_final > V_latest:
       Replay deltas V_latest..V_final onto G_new under lock.
  9. Atomically update `active_generation = G_new` in mtxdb metadata.
 10. Enqueue G_old into `retired_generations_queue` (timestamped).
 11. Release lock_rooms(room_id).
```

---

## 4. Generation Lifecycle & Retention Policy

* **Publication:** The atomic pointer update immediately routes all new queries to $G_{new}$.
* **Reader Safety:** In-flight reader scans started against $G_{old}$ continue scanning $G_{old}$ without disruption.
* **Deferred GC:** $G_{old}$ is retained on disk and queued. Automated cleanup based on reader epoch tracking will be wired in a follow-up phase.

---

## 5. Implementation Steps

1. **Database Version Guard:** Bump `DATABASE_LAYOUT_VERSION` in `mtxdb` to reject legacy flat/shared dev databases.
2. **Schema & Metadata:** Add `FORWARD_MEMBER_TAG` (`FWD `), generation collection derivation, and `room_forward_meta` storage in `mtxdb_syn.rs`.
3. **Reader Implementation:** Wire `get_forward_edges` to query the active generation directly.
4. **Live Writer Implementation:** Wrap live mutations in `lock_rooms(room_id)` updating SQL and `active_generation` in `mtxdb`.
5. **Rebuild Worker:** Implement the iterative catch-up populator and pointer flip under `lock_rooms(room_id)`.
