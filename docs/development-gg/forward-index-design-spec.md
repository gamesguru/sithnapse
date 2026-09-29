# Design Specification: Forward-Index Engine (`embedded_edges.rs`)

---

## 1. Overview & System Guarantees

This specification defines the storage layout, protocol, and concurrency mechanics for the Matrix DAG event edge forward-index in `sithnapse` (`rust/src/database/embedded_edges.rs`).

The system operates across two operational phases:

1. **Slice 1 (In-Place Leased Repair & Incremental Engine):** A crash-safe, bounded-lock repair protocol using a persistent cross-process fallback fence, authoritative primary storage, and a lease-validated three-tier version tracking mechanism.
2. **Slice 2 (Out-of-Band Generation Swap & Namespace Isolation):** A zero-downtime full rebuild protocol utilizing atomic generation pointer swaps and reader-epoch garbage collection.

### Core Guarantees & Invariants

* **Slice 1 Invariant:** While fenced, primary storage (SQL / backward edges) is strictly authoritative for reads and writes. `mtxdb` forward-index mutations are serialized under the room repair lease. A repair batch may commit only when both its lease token and base source version remain valid.
* **Precision (Soundness):** Read-time candidate hash verification prevents serving stale or corrupted forward edges.
* **Recall (Completeness):** Persistent `rebuild_in_progress` fences prevent readers from observing transient, incomplete batch states by routing read queries directly to authoritative fallback.
* **Serializability & No Overwrite:** Any live write advances `current_source_version`. Mismatches force the repair worker to abort the in-flight chunk sequence and recompute the remaining diff from the authoritative source under the active fence.
* **Bounded Lock Latency:** Room lock hold time is strictly $O(\text{chunk size})$ during repairs, avoiding live worker starvation.
* **Crash Resilience:** Leases carry explicit millisecond TTLs. Expired leases trigger background scan-and-rebuild upon startup or subsequent room access, preventing permanent fallback deadlock.

---

## 2. Data Layout & Type Definitions

```rust
use std::time::Duration;

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

/// Persistent, cross-process fence record stored in mtxdb / SQL.
#[repr(C)]
pub struct PersistentRebuildFence {
    /// Indicates whether a room is currently fenced for repair.
    pub is_fenced: bool,
    /// Unique lease holder token (e.g., worker PID + monotonic counter).
    pub lease_holder_token: u64,
    /// Monotonic timestamp (ms) after which the lease is considered expired.
    pub lease_expires_at_ms: u64,
    /// Version of the authoritative source state when the repair pass began.
    pub base_source_version: u64,
}

/// Represents an edge mutation batch for chunked execution.
pub struct EdgeBatchChunk {
    pub room_id: [u8; 32],
    pub additions: Vec<ForwardEdgeRecord>,
    pub removals: Vec<ForwardEdgeKey>,
    pub lease_holder_token: u64,
    pub target_base_version: u64,
}
```

---

## 3. Slice 1: Protocol Specification

### 3.1 Authoritative Source Definition

During Slice 1:
* **Primary / Authoritative Source:** PostgreSQL `event_edges` and `mtxdb` backward edge collections (`prev_edges_room_id`).
* **Secondary / Derived Index:** `mtxdb` forward edge records.
* All repair diff calculations read exclusively from the primary source.

### 3.2 Reader Path (`get_forward_edges`)

```text
               +----------------------------------+
               |  Read Query for Room Edge Data   |
               +----------------------------------+
                                │
                                ▼
               /──────────────────────────────────\
              < Is `rebuild_in_progress` Fenced?   >
              < (and lease has NOT expired)?       >
               \──────────────────────────────────/
                               │
                      ┌────────┴────────┐
                 Yes  │                 │ No (or Expired)
                      ▼                 ▼
          +──────────────────────+   +──────────────────────+
          |  Force SQL Fallback  |   |  Scan mtxdb Forward  |
          |  (Primary Store)     |   |  Collection          |
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

### 3.3 Writer Path (Live Incremental Write)

Live edge writes must not mutate the forward index concurrently without lease serialization:

1. Acquire the short `lock_rooms(room_id)`.
2. Increment `current_source_version += 1` in `RoomEdgeMeta`.
3. Commit mutation directly to primary authoritative storage (SQL / backward edges).
4. Read `PersistentRebuildFence`:
   * **If fenced (and lease active):** DO NOT write to `mtxdb` forward collection. Primary store is authoritative; version advance signals the repair worker to re-diff.
   * **If not fenced (or lease expired):** Apply forward edge update directly to `mtxdb` forward collection.
5. Release `lock_rooms(room_id)`.

---

### 3.4 Repair Execution Workflow

```text
[Phase 1: Initial Scan & Diff (Out of Lock)]
  │
  ├── 1. Read authoritative backward edges / SQL state for `room_id`.
  └── 2. Compute target diff (additions set A, removals set R).

[Phase 2: Fence & Lease Acquisition (Short Lock Hold)]
  │
  ├── 1. Acquire short `lock_rooms(room_id)`.
  ├── 2. Check existing `PersistentRebuildFence`:
  │      - If active and unexpired with another token: ABORT/YIELD.
  │      - If expired or inactive: Claim lease with new `lease_holder_token` and TTL.
  ├── 3. Write `is_fenced = true`, `lease_expires_at_ms = now + TTL`.
  ├── 4. Set `base_source_version = current_source_version`.
  └── 5. Release `lock_rooms(room_id)`.

[Phase 3: Chunked Application Loop (Per Chunk)]
  │
  ├── 1. Chunk diff into bounded sizes (e.g., N = 500 keys).
  ├── 2. Acquire short `lock_rooms(room_id)`.
  ├── 3. Validate Lease & Version:
  │      a. Is `lease_holder_token` still owner and unexpired?
  │      b. Is `current_source_version == base_source_version`?
  │      │
  │      ├── YES (Clean State):
  │      │    i. Apply ADDITIONS chunk to mtxdb.
  │      │   ii. Apply REMOVALS / Tombstones chunk to mtxdb.
  │      │  iii. Increment `index_repair_version += 1`.
  │      │   iv. Extend `lease_expires_at_ms = now + TTL`.
  │      │    v. Commit chunk transaction.
  │      │   vi. Release `lock_rooms(room_id)`. Yield thread.
  │      │
  │      └── NO (Live Write Collision or Expired Lease):
  │           i. DO NOT apply chunk.
  │          ii. If lease lost: ABORT repair.
  │         iii. If version mismatch:
  │              - Keep `is_fenced = true`.
  │              - Re-read authoritative primary storage.
  │              - Recompute entire remaining diff.
  │              - Update `base_source_version = current_source_version`.
  │              - Resume chunk loop from step 1.

[Phase 4: Completion & Fence Clear (Short Lock Hold)]
  │
  ├── 1. Acquire short `lock_rooms(room_id)`.
  ├── 2. Confirm `current_source_version == base_source_version` and lease ownership.
  ├── 3. Write `is_fenced = false`, clear `lease_holder_token`.
  └── 4. Release `lock_rooms(room_id)`. Readers resume direct mtxdb queries.
```

---

## 4. Crash Recovery & Stale Lease Handling

1. **Worker Crash Mid-Repair:**
   * The `PersistentRebuildFence` persists in `mtxdb` / storage. Readers observe `is_fenced == true` and route safely to primary SQL fallback.
   * As long as `now < lease_expires_at_ms`, the fence prevents split-brain repair workers.
2. **Lease Expiry & Auto-Recovery:**
   * Once `now >= lease_expires_at_ms`, the next reader query or background maintenance pass detects the expired lease.
   * Auto-recovery claims a fresh lease, marks `is_fenced = true`, and recomputes the entire index from the authoritative primary source.
   * Prevents indefinite stall or permanent fallback mode.

---

## 5. Slice 2: Architecture & Progression

Slice 2 shifts full room rebuilds from in-place diffing to out-of-band generation creation with $O(1)$ atomic pointer swaps once Slice 1 metrics and recovery tests validate.

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

### 5.1 Namespace Isolation

Forward and backward records are separated into distinct collection templates:
* `backward_edges_v1`: Contains backward room edges (`prev_edges`).
* `forward_edges_v1_gen_{N}`: Dedicated namespace for forward records. Removes candidate discriminator hashing on read paths.

### 5.2 Reader-Epoch Garbage Collection

* Active readers acquire epoch tickets during forward-edge scans.
* Old generation collections `forward_edges_v1_gen_{N-1}` are dropped asynchronously only after all reader tickets advance beyond `T_swap`.

---

## 6. Implementation Sequence & Acceptance Gates

Slice 1 must be implemented in the following strict order:

1. **Persistent Lease & Fence Record:** Define `PersistentRebuildFence` with TTL in mtxdb/SQL.
2. **Reader Fallback Gate:** Wire `get_forward_edges` to route to SQL when fenced with an active lease.
3. **Lease-Validated Batch Mutation API:** Expose chunked write functions requiring valid lease tokens and base versions.
4. **Collision Abort & Re-diff:** Implement strict version-mismatch abortion and full diff recomputation under the fence.
5. **Crash Recovery & Stale-Lease Tests:** Validate recovery on simulated worker crashes, expired lease overrides, and concurrent repair races.
6. **Chunk Size & Lock Tuning:** Benchmark and calibrate lock durations against live traffic.
