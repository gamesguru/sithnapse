# Design Specification: Forward-Index Engine (`embedded_edges.rs`)

---

## 1. Overview & System Guarantees

This specification defines the storage layout, protocol, and concurrency mechanics for the Matrix DAG event edge forward-index in `sithnapse` (`rust/src/database/embedded_edges.rs`).

The system operates across two operational phases:

1. **Slice 1 (In-Place Leased Repair & Incremental Engine):** A crash-safe, bounded-lock repair protocol using a cross-process fallback fence and a three-tier version tracking mechanism.
2. **Slice 2 (Out-of-Band Generation Swap & Namespace Isolation):** A zero-downtime full rebuild protocol utilizing atomic generation pointer swaps and reader-epoch garbage collection.

### Core Guarantees

* **Precision (Soundness):** Read-time candidate hash verification prevents serving stale or corrupted forward edges.
* **Recall (Completeness):** Cross-process `rebuild_in_progress` fences prevent readers from observing transient, incomplete batch states by forcing read fallback to SQL/primary store.
* **Serializability:** A three-tier version check prevents repair workers from overwriting concurrent live mutations.
* **Bounded Lock Latency:** Room lock hold time is strictly $O(\text{chunk size})$ during repairs, avoiding writer starvation.

---

## 2. Data Layout & Type Definitions

```rust
use std::sync::atomic::{AtomicBool, AtomicU64};

/// Metadata record stored in mtxdb for room edge tracking.
#[repr(C)]
pub struct RoomEdgeMeta {
    /// Incremented on every live edge mutation (writes/deletes).
    pub current_source_version: u64,
    /// Active generation ID for Slice 2 generation pointers.
    pub active_generation: u64,
    /// Monotonic version counter for in-place repair batches.
    pub index_repair_version: u64,
}

/// Cross-process shared state or database-backed fence marker.
pub struct RebuildFence {
    /// Indicates whether a room is currently undergoing a full repair or batch update.
    pub is_fenced: bool,
    /// The repair lease token owner.
    pub lease_holder_id: u64,
    /// Version of the source state at the start of the repair pass.
    pub base_source_version: u64,
}

/// Represents an edge mutation batch for chunked execution.
pub struct EdgeBatchChunk {
    pub room_id: [u8; 32],
    pub additions: Vec<ForwardEdgeRecord>,
    pub removals: Vec<ForwardEdgeKey>,
    pub target_base_version: u64,
}
```

---

## 3. Slice 1: Protocol Specification

### 3.1 Reader Path (`get_forward_edges`)

```text
               +----------------------------------+
               |  Read Query for Room Edge Data   |
               +----------------------------------+
                                │
                                ▼
               /──────────────────────────────────\
              < Is `rebuild_in_progress` Fenced?   >
               \──────────────────────────────────/
                               │
                      ┌────────┴────────┐
                 Yes  │                 │ No
                      ▼                 ▼
          +──────────────────────+   +──────────────────────+
          |  Force SQL Fallback  |   |  Scan mtxdb Forward  |
          |  (Bypass Index Read) |   |  Collection          |
          +──────────────────────+   +──────────────────────+
                                                │
                                                ▼
                                     /─────────────────────\
                                    < Candidate Decodes as  >
                                    < Backward & Validates? >
                                     \─────────────────────/
                                                │
                                       ┌────────┴────────┐
                                  Yes  │                 │ No
                                       ▼                 ▼
                           +─────────────────+   +─────────────────+
                           | Return Edge     |   | Drop Candidate  |
                           | (Valid Forward) |   | (Stale/Inert)   |
                           +─────────────────+   +─────────────────+
```

### 3.2 Writer Path (Live Incremental Write)

Live edge mutations do not wait for background rebuilds to complete. When a new event lands:

1. Acquire the short `lock_rooms(room_id)`.
2. Read `RoomEdgeMeta`, increment `current_source_version += 1`.
3. If `rebuild_in_progress == true`:
   * Write edge mutation directly to SQL/primary store and `mtxdb`.
   * The version bump will trigger version-mismatch detection in the background repair loop.
4. If `rebuild_in_progress == false`:
   * Apply edge additions/deletions directly to the active set in `mtxdb`.
5. Release `lock_rooms(room_id)`.

---

### 3.3 Repair Execution Workflow

```text
[Phase 1: Initial Scan & Diff (Out of Lock)]
  │
  ├── 1. Acquire WAL replay lease via `scan_collection_at_snapshot(room_id)`.
  ├── 2. Derive desired edge state from backward edges.
  └── 3. Compute target diff (additions set A, removals set R).

[Phase 2: Fence Initialization (Short Lock Hold)]
  │
  ├── 1. Acquire short `lock_rooms(room_id)`.
  ├── 2. Acquire cross-process rebuild lease.
  ├── 3. Write `rebuild_in_progress = true` to shared fence registry.
  ├── 4. Record `base_source_version = current_source_version`.
  └── 5. Release `lock_rooms(room_id)`.

[Phase 3: Chunked Application Loop (Per Chunk)]
  │
  ├── 1. Chunk diff into bounded sizes (e.g., N = 500 keys).
  ├── 2. Acquire short `lock_rooms(room_id)`.
  ├── 3. Check: Is `current_source_version == base_source_version`?
  │      │
  │      ├── YES:
  │      │    a. Write ADDITIONS chunk to mtxdb.
  │      │    b. Write REMOVALS / Tombstones chunk to mtxdb.
  │      │    c. Increment `index_repair_version += 1`.
  │      │    d. Commit batch.
  │      │    e. Release `lock_rooms(room_id)`. Yield thread.
  │      │
  │      └── NO (Live Write Collision Detected):
  │           a. DO NOT apply chunk.
  │           b. Keep `rebuild_in_progress = true` fence active.
  │           c. Re-read current edge state from primary source.
  │           d. Recompute diff against new state.
  │           e. Set `base_source_version = current_source_version`.
  │           f. Resume chunk loop from step 1.

[Phase 4: Completion & Fence Clear (Short Lock Hold)]
  │
  ├── 1. Acquire short `lock_rooms(room_id)`.
  ├── 2. Verify all chunks applied and version matches `current_source_version`.
  ├── 3. Write `rebuild_in_progress = false` to shared fence registry.
  ├── 4. Release cross-process rebuild lease and WAL replay lease.
  └── 5. Release `lock_rooms(room_id)`.
```

---

## 4. Slice 2: Architecture & Progression

Slice 2 shifts full room rebuilds from in-place diffing to out-of-band generation creation with $O(1)$ atomic pointer swaps.

```text
[ Existing State ]
  Active Pointer: Gen 1
  Collection: `forward_edges_gen_1`
  Readers: Reading Gen 1

[ Out-of-Band Rebuild Step ]
  1. Allocate new collection: `forward_edges_gen_2`.
  2. Populate `forward_edges_gen_2` via background WAL scan (zero lock hold).
  3. Catch up via WAL replay lease.

[ Atomic Cutover Step (Short Lock Hold) ]
  1. Acquire short `lock_rooms(room_id)`.
  2. Update pointer: `active_generation = 2`.
  3. Register Gen 1 for Epoch GC with timestamp `T_swap`.
  4. Release `lock_rooms(room_id)`.

[ Post-Cutover State ]
  Active Pointer: Gen 2
  Readers: Instantly routed to Gen 2 (Zero SQL fallback required).
  Gen 1: Reclaimed by background GC once reader epochs surpass `T_swap`.
```

### 4.1 Namespace Isolation

Forward and backward records are separated into distinct collection templates:

* `backward_edges_v1`: Contains backward room edges (`prev_edges`).
* `forward_edges_v1_gen_{N}`: Dedicated namespace for forward records. Removes candidate discriminator hashing on read paths.

### 4.2 Reader-Epoch Garbage Collection

Old generations ($N-1$) are dropped safely without locking readers:

1. Active readers register an epoch token upon starting a forward-edge scan.
2. The GC coordinator tracks active reader tokens.
3. Once the minimum active epoch across all workers advances past `T_swap`, the old collection `forward_edges_v1_gen_{N-1}` is deleted asynchronously.

---

## 5. Concurrency & Failure Recovery Matrix

| Scenario | Protection Mechanism | Invariant / Outcome Guaranteed |
| --- | --- | --- |
| **Reader queries room mid-rebuild** | Cross-process `rebuild_in_progress` fence | Reader routes to SQL fallback. Zero partial/incomplete edge reads observed. |
| **Live event arrives mid-rebuild** | Three-tier version tracking (`current_source_version`) | Version bump invalidates base version; repair worker re-diffs before applying remaining chunks. |
| **Process crashes during batch loop** | Persistent/Shared Fence + WAL Replay Lease | Fence remains set. Subsequent reader queries fall back to SQL until repair worker or auto-recovery re-runs phase 1-4. |
| **Stale repair worker attempts write** | Rebuild lease validation under short lock | Lock step rejects batch from expired lease holder; no stale overwrites. |
| **Concurrent repair workers attempt same room** | Rebuild lease coordinator | First worker acquires lease; second worker blocks or aborts gracefully. |
| **Corruption in forward record** | Candidate self-verification hash check | Read path rejects invalid candidates even if fence is bypassed; precision is absolute. |
