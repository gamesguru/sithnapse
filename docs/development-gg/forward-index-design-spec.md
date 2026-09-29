# Design Specification: Forward-Index Engine (`embedded_edges.rs`)

---

## 1. Overview & System Architecture

This specification defines the storage layout, protocol, and concurrency mechanics for the Matrix DAG event edge forward-index in `sithnapse` (`rust/src/database/embedded_edges.rs`).

Because neither Rezzy nor mtxdb has pre-existing production deployments, we target the end-state greenfield architecture directly:

1. **Dedicated Forward Namespace (`FWD `):** Forward edges are fully segregated from backward edges (`PREV`), eliminating candidate discriminator hashing on read queries.
2. **Per-Room Active Generation Pointer (`active_generation`):** Full room rebuilds run out of band into Generation $N+1$, followed by an atomic pointer flip under `lock_rooms(room_id)`.
3. **Durable SQL Outbox Change Stream:** Live edge writes commit an outbox mutation alongside the SQL edge write and increment `source_version` in the same transaction, eliminating dual-store crash-inconsistency.
4. **Published-Version Validity Gate:** Readers verify `published_source_version == source_version` before querying `mtxdb`, seamlessly falling back to SQL if a worker crashed before applying outbox changes.
5. **Iterative Catch-Up Loop:** Out-of-band rebuilds replay the SQL outbox stream to catch up before acquiring the final publication lock.
6. **Generation Retention & Deferred GC:** Retired generations ($N$) are kept on disk initially so active in-flight reader scans complete safely; automated reader-epoch GC is added in a subsequent phase.

---

## 2. Schema & Collection Definitions

| Store | Collection / Table | Key Structure | Value Structure |
|---|---|---|---|
| **`mtxdb`** | `backward_edges` (`PREV`) | `(room_id, child_event_id)` | `Vec<(prev_event_id, is_state)>` |
| **`mtxdb`** | `forward_edges_v2` (`FWD `) | `(room_id, generation_id, parent_event_id)` | `Vec<child_event_id>` |
| **`mtxdb`** | `room_forward_meta` | `room_id` | `active_generation: u32`, `published_source_version: u64` |
| **SQL** | `event_edges` | `(room_id, ...)` | Authoritative edges |
| **SQL** | `room_edge_source_version` | `room_id` | `source_version: u64` |
| **SQL** | `edge_index_outbox` | `(room_id, stream_id)` | `parent_event_id`, `child_event_id`, `is_state`, `op` (insert/delete) |

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

1. Read `source_version` from SQL / cache, and read `(active_generation, published_source_version)` from `room_forward_meta`.
2. **Validity Check:**
   * If `published_source_version == source_version`:
     Query `forward_edges_v2` at `forward_edges_room_gen_id(room_id, active_generation)`.
   * If `published_source_version != source_version` (outbox lagging or worker crashed post-commit):
     Route query to primary SQL fallback (or trigger immediate outbox drain).

### 3.2 Live Writer Path (Outbox Publication)

Every live edge write executes atomically in SQL:

1. **SQL Transaction Commit:**
   - Insert/delete edge in `event_edges`.
   - Increment `source_version += 1` in `room_edge_source_version`.
   - Insert mutation record into `edge_index_outbox` tagged with `source_version`.
   - Commit SQL transaction.
2. **mtxdb Outbox Drain (Same process or worker loop):**
   - Acquire `lock_rooms(room_id)`.
   - Read `active_generation`.
   - Apply pending outbox records to `forward_edges_v2` under `(room_id, active_generation)`.
   - Update `published_source_version = source_version` in `room_forward_meta` within the same mtxdb transaction.
   - Prune applied rows from `edge_index_outbox`.
   - Release `lock_rooms(room_id)`.

*Crash Invariant:* If a crash occurs between step 1 and step 2, `published_source_version < source_version` forces readers to SQL fallback until the outbox is drained on recovery. Zero stale or corrupted reads.

---

### 3.3 Full Rebuild Protocol (Iterative Outbox Catch-Up)

```text
[Step 1: Out-of-Band Snapshot Build (Unlocked)]
  1. Record V_start = current SQL source_version.
  2. Set G_new = active_generation + 1.
  3. Build G_new in `forward_edges_v2` from authoritative SQL snapshot at V_start.

[Step 2: Unlocked Outbox Replay Loop]
  4. Query `edge_index_outbox` for records > V_start.
  5. Apply outbox delta stream onto G_new.
  6. Update V_start to latest stream ID replayed.
  7. Repeat until remaining outbox backlog is near zero.

[Step 3: Locked Final Replay & Atomic Pointer Swap]
  8. Acquire lock_rooms(room_id).
  9. Read V_final from SQL and drain any final outbox rows onto G_new.
 10. Update `active_generation = G_new` and `published_source_version = V_final` in mtxdb transaction.
 11. Enqueue G_old into `retired_generations_queue` (timestamped).
 12. Release lock_rooms(room_id).
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
3. **SQL Outbox Schema:** Add `edge_index_outbox` and `room_edge_source_version` tables.
4. **Outbox Drainer & Writer:** Implement the transactional outbox commit and mtxdb drainer with `published_source_version` updates.
5. **Reader Fallback Gate:** Wire `get_forward_edges` to verify `published_source_version == source_version` before querying `mtxdb`.
6. **Rebuild Worker:** Implement the iterative outbox replay populator and pointer flip under `lock_rooms(room_id)`.
