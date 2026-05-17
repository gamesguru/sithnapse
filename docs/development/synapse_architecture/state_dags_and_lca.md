# State DAGs and Sublinear LCA Walks

This document provides architectural context on how Synapse approaches the problem of finding common ancestors (merge-bases) in room graphs, the limitations of current implementations, and the path forward with MSC4242 (State DAGs).

## The `event_edges` Bottleneck

Synapse currently stores the room's event DAG as a raw adjacency list in the `event_edges` table (representing `prev_events`). While simple, this structure is inefficient for certain operations.

To find the Lowest Common Ancestor (LCA) or merge-base of two divergent forks in a room's history, the server must perform a linear, $O(N)$ walk back through these edges. Synapse mitigates the cost of these walks slightly by using the `depth` column to prune branches that are deeper than the target, but the worst-case complexity remains linear. For large rooms with deep histories and numerous concurrent forks, this linear walk can become a significant bottleneck, particularly during state resolution.

## Sublinear LCA Optimizations

To overcome the linear walk bottleneck, computer science offers several "sublinear" traversal techniques.

### 1. Jump Pointers (Binary Lifting)
Instead of only storing pointers to immediate parents ($d=1$), an event could store "jump pointers" to ancestors at exponentially increasing distances ($d=2, 4, 8, 16 \dots$). This technique, often referred to as binary lifting, allows the server to skip large sections of the graph, reducing the time complexity of finding an LCA from $O(N)$ to $O(\log N)$.

### 2. Transitive Closure / Chain Cover Index
For queries that only need to determine reachability (e.g., "Is Event A an ancestor of Event B?"), a **Chain Cover Index** is highly effective. The DAG is decomposed into linear chains, and reachability is pre-calculated. Synapse currently uses a variant of this (based on the Jagadish algorithm) exclusively for the **Auth DAG** (via `event_auth_chains`). It provides $O(1)$ reachability checks, drastically speeding up the "auth chain difference" algorithm used in State Resolution v2.

## MSC4242: State DAGs

Applying these sublinear optimizations (like Jump Pointers) to the entire `event_edges` DAG is impractical due to database overhead; indexing every chat message in a high-traffic room would cause massive write amplification.

This is where **MSC4242 (State DAGs)** comes in. MSC4242 proposes separating state events into their own distinct DAG, linked via a new `prev_state_events` field.

### Why State DAGs Enable Sublinear Walks

1. **Reduced Graph Size:** State transitions (e.g., membership changes, power levels) represent a tiny fraction of total room events. By isolating them into a State DAG, the number of nodes ($N$) to traverse or index is reduced by orders of magnitude.
2. **Provable Completeness:** MSC4242 mandates that servers must possess all paths back to the `m.room.create` event for the State DAG. This eliminates the "holes" typical in the standard room DAG, allowing for deterministic and reliable indexing.
3. **Targeted Optimization:** Because LCA and merge-base calculations are almost exclusively required for resolving state conflicts, applying heavy indexing (like Jump Pointers or Chain Indices) strictly to the State DAG provides the maximum performance benefit with minimal database overhead.

## Finding the Precise Delta

Finding the LCA is not the end goal; it is the anchor point. When two branches converge, the server needs to calculate the state delta—the exact set of state changes that occurred on each branch since they diverged.

By utilizing a sublinear LCA walk on a fully synchronized State DAG (MSC4242), Synapse can instantly locate this divergence point. From the LCA, it can efficiently extract the "precise delta" of state events and feed only those relevant changes into the State Resolution algorithm. This shifts state resolution from a slow, full-history comparison to a rapid, surgical merge.
