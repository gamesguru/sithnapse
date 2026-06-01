# Federated DAG State Resolution: Performance Guidance

This document outlines architectural strategies for optimizing Matrix State Resolution (V2 and V2.1) under pathological conditions, such as "Storm" rooms with tens of thousands of forks and massive auth chains.

## 1. High-Performance Auth Chain Differences (Roaring Bitmaps)

Calculating `_get_auth_chain_difference` is one of the most CPU-intensive operations in state resolution. Naive implementations using string-based set intersections suffer from significant hashing overhead and poor cache locality.

### Strategy:
- **Integer Mapping:** Map every `EventId` to a unique `uint32_t` integer index.
- **Roaring Bitmaps:** Use Roaring Bitmaps (e.g., `CRoaring` in C++ or `roaring` in Rust) to represent auth chains as sets of integers.
- **SIMD Acceleration:** Utilize SIMD-accelerated bitwise operations (`AND`, `OR`, `AND NOT`) for set reconciliation. Finding the difference between two 100,000-event chains becomes a microsecond operation.

## 2. Lexicographical Fast-Paths (Conflict Pruning)

In large merges, a significant portion of the room state is often identical across all conflicting branches (e.g., historical room name, topic, or avatars that haven't changed in years).

### Strategy:
- **Aggrerssive Pruning:** Before initiating heavy topological sorts or auth checks, compare the `EventId`s for each `(type, state_key)` across all forks.
- **Commonality Fast-Path:** If all forks agree on the exact same event for a state key, move it immediately to the resolved set and remove it from the conflict set. This reduces the problem size for the $O(N^2)$ sort phase by orders of magnitude.

## 3. Memoized Mainline Depth Calculation

Sorting events in State Res V2 requires calculating the "Mainline Depth" relative to a power level ancestor. Naive implementations walk the graph for every event being sorted.

### Strategy:
- **Scoped Cache:** Implement a hashmap (`depth_cache[event_id] -> depth`) scoped to the resolution transaction.
- **Path Sharing:** Many events in a storm will share the same power-level ancestors. Memoization transforms redundant $O(N^2)$ graph walks into $O(1)$ lookups, significantly speeding up Kahn's algorithm.

## 4. Summary of Pathologies Addressed

- **The "Vanish" Anomaly:** Addressed by treating reconciliation as a **Set Problem** rather than just a **Graph Problem**, ensuring missing auth-critical events are fetched even if the topological path avoids them.
- **Power Level Replay (CVE):** Patched in V2.1 by restricting the "supplemental merge" to `m.room.power_levels` only, forcing malicious events to authenticate against the newly resolved state while protecting legitimate historical state from eviction.
