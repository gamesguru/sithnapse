# Synapse Performance Tuning and Infrastructure Guidelines

This document outlines the strategy for rolling out performance optimizations for this Synapse instance. The goal is to safely apply these changes within a short maintenance window by isolating them into distinct phases (Pull Requests). This approach minimizes the "blast radius" and allows for quick reversions if any issues arise.

## Phase 1: Application-Layer Tuning ("Quick Wins")

These optimizations focus on adjusting Synapse's internal configurations to better manage memory, garbage collection, and federation performance. These are pure application configurations with no new infrastructure dependencies.

### Key Changes in `homeserver.yaml`
1.  **Internal Cache Sizing:** Increase `caches.global_factor` to replicate in-memory speedups.
2.  **Cache Pruning:** Set an `expiry_time` under the `caches` section to prevent memory leaks from the expanded caches.
3.  **Taming Python's Garbage Collector:** Configure `gc_min_interval` (e.g., to `[10.0, 10.0, 10.0]`) to amortize the CPU cost of GC sweeps.
4.  **Mitigate Dead Server Drag:** Adjust `federation_client_timeout` to drop connections to offline servers faster.
5.  **Faster Joins:** Ensure `faster_joins: true` is enabled under `experimental_features` to offload the computational burden of joining federated rooms.

## Phase 2: Database Infrastructure (PgBouncer & UNIX Sockets)

This phase addresses the critical bottleneck of PostgreSQL connection management by introducing a connection pooler.

### The Problem: Connection Storms
Synapse workers can create "connection storms" during bursts of federation traffic. PostgreSQL's process-per-connection model causes immense CPU and memory overhead when handling hundreds of concurrent, bursty connections.

### The Solution: PgBouncer
PgBouncer acts as a lightweight proxy, multiplexing thousands of incoming connections from Synapse workers down to a small, persistent pool of connections to PostgreSQL.

### Implementation Steps
1.  Deploy and configure PgBouncer (`pgbouncer.ini`, `userlist.txt`).
2.  Configure Synapse and PgBouncer to communicate via UNIX domain sockets (`/var/run/postgresql`) to eliminate local TCP/IP overhead.
3.  Update the `database:` section in `homeserver.yaml` to connect through PgBouncer.

## Phase 3: Background Maintenance Automation (State Compressor)

This phase focuses on long-term database health by automating the compression of highly redundant state groups.

### Implementation Steps
1.  Set up the `rust-synapse-compress-state` tool on the host.
2.  Create a wrapper script to execute the tool with the correct database credentials.
3.  Schedule the script via a `cron` job or systemd timer to run during off-peak hours. This reclaims disk space and speeds up room history fetches without impacting live performance.
