# Proposal: Federated Set Reconciliation (Matrix 2.0)

## The "Set Problem" vs. "Graph Path" Fallacy

Traditional Matrix synchronization (v1.x) treats room reconciliation as a **Graph Problem**. Mechanisms like `get_missing_events` and `backfill` are designed to navigate the "DAG Path" by following `prev_events` links.

As documented in the "Federated DAG Membership Anomalies" report, this approach is architecturally fragile. It leads to the **"Set Problem"**:
- A server possesses a merge event but is missing a critical authorization dependency (the "parent" join) because that dependency sits on a different branch or was never served in a shallow subgraph.
- Because the resolver only sees the graph and not the set, it "routes around" the missing authority, leading to permanent membership divergence (e.g., Anomaly 1: `@bot:nutra.tk` vanishing from catgirl.cloud).

## Limitations of Current Mechanisms

### 1. `get_missing_events` / `backfill`
Both endpoints are hard-coded to ignore the auth path:
- `synapse/storage/databases/main/event_federation.py` explicitly filters out state edges: `AND NOT event_edges.is_state`.
- These mechanisms are "Path-Complete" (they bridge topological gaps) but "Set-Incomplete" (they ignore the recursive auth set required for resolution).

### 2. The DoS Risk of Recursive Resolution
A naive "v2.2" fix (implementing a BFS recursive walk in the state resolution engine) is **unscalable and vulnerable to DoS attacks**. A malicious actor could craft a "Tower DAG" with thousands of dummy auth events, forcing every resolver in the room into $O(N^2)$ or $O(N^3)$ complexity walls.

## The Solution: Set Reconciliation (RBSR)

Instead of making the resolver "smarter" (which introduces DoS risk), we must make the federation layer **"Set-Complete."**

### 1. Range-Based Set Reconciliation (RBSR)
We propose implementing **RBSR** (using protocols like **Negentropy**) for federation sync. Instead of walking the graph, servers reconcile their entire event set for a room by recursively partitioning the ID space and comparing fingerprints (hashes).

- **Efficiency:** Reconciles 100,000+ events in $O(d \log n)$ time, where $d$ is the number of differences.
- **Completeness:** Automatically identifies missing auth events even if they are far removed from the current topological frontier.
- **Robustness:** Eliminates the "Set Problem" by ensuring the resolver never encounters an event without possessing its entire recursive auth set.

### 2. Bloom Filter Metadata
For smaller transactions, servers can include a Bloom filter of their known auth chain in federation requests. The sender can then proactively attach the missing pieces of the set to the outbound transaction.

## Benefits and Alignment

-   **Consensus Convergence:** Enables automatic "Healing" of diverged rooms by filling DAG holes that current graph-walking ignores.
-   **Security:** Fixes the Power Level Replay vulnerability by ensuring the "Consensus PL" is always present in the local set during resolution.
-   **Protocol Roadmap:** Directly aligns with **MSC4186 (Simplified Sliding Sync)** and the broader **Matrix 2.0** effort to move toward instant, set-complete synchronization.

## Summary

The "magic" re-appearance of members is not a result of recursive resolution logic, but of **Set Integrity**. By adopting Set Reconciliation at the federation layer, we eliminate the complexity bottlenecks of state resolution while guaranteeing a single, verifiable truth across the entire DAG.
