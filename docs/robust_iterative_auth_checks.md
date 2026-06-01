# Proposal: State Resolution v2.2 (for Room v12.1)

## Overview

This document proposes **State Resolution v2.2**, an evolution of the v2.1 algorithm (MSC4297), targeted for **Room Version 12.1**. The primary goal is to make iterative auth checks robust against incomplete federation data while maintaining strict security against Power Level Replay attacks.

Currently, Synapse's state resolution follows the Matrix specification strictly by only inspecting the immediate `auth_event_ids()` provided by an event. While this is spec-compliant, it makes the resolution process fragile when dealing with incomplete auth chains from remote servers.

## The v2.2 Algorithm: Robust BFS Recursive Walk

State Res v2.2 introduces a **Breadth-First Search (BFS) recursive walk** of the local `auth_events` during the iterative check phase. This ensures that the resolver has the most complete possible picture of the author's intended authorization context.

### Why BFS?

As identified in the `ruma-lean` implementation, the choice of BFS over Depth-First Search (DFS) is critical for correctness in a distributed DAG:

1.  **Nearest-Wins Property:** BFS ensures that we encounter the "closest" authorization event for a given `(type, state_key)` pair first.
2.  **Anti-Clobbering:** In a messy DAG, an author might (accidentally or maliciously) cite both a recent and a stale Power Level event. By using BFS and a "set-if-absent" pattern, we ensure the resolver uses the most direct dependency, preventing a "grandparent" from clobbering the "parent's" restrictions.

## Algorithm Details

The v2.2 logic would be applied during the iterative auth check for each event:

1.  **Initialize `local_auth_map`**: A dictionary to store the author's provided auth context.
2.  **Initialize `queue`**: Populated with the event's immediate `auth_event_ids`.
3.  **Recursive Walk (BFS)**:
    - While `queue` is not empty:
      - Pop `auth_id`.
      - Fetch the event `ev` from the local `event_map` or store.
      - If `ev` is valid and not rejected:
        - If `(ev.type, ev.state_key)` is not in `local_auth_map`:
          - Insert `ev` into `local_auth_map`.
          - Add `ev.auth_event_ids()` to the `queue` to continue the walk.
4.  **Ancestry Check (Mandatory in v2.2)**:
    - Only apply the resolved consensus `m.room.power_levels` if the `local_auth_map` **already contains** a Power Level event.
    - This check prevents authors from bypassing demotions by omitting all Power Level events from their chain—a vulnerability present in naive v2.1 implementations.
5.  **Final Auth Check**:
    - Pass the resulting `local_auth_map` to `check_state_dependent_auth_rules`.

## Versioning Summary

| Version  | Room Version        | Supplemental Merge | Ancestry Check | Auth Walk         |
| :------- | :------------------ | :----------------- | :------------- | :---------------- |
| **v2.0** | 2 - 11              | All Auth Types     | No             | Immediate only    |
| **v2.1** | 12                  | PL Only            | No             | Immediate only    |
| **v2.2** | **12.1 (Proposed)** | PL Only            | **Yes**        | **Recursive BFS** |

## Benefits

- **Robustness against Gaps:** The room can "heal" more easily if a single PDU in an auth chain is missing but deeper ancestors are known.
- **Strict Security:** By combining BFS with the mandatory ancestry check, we close the "Time-Travel Promotion" CVE while remaining resilient to topological noise.
- **Implementation Parity:** Aligns Synapse with more rigorous, formally-inspired resolution strategies.

## Performance Considerations

A recursive walk could theoretically become $O(N^2)$ in a pathological "deep" auth chain. To mitigate this:

- **Limit Depth:** We can cap the BFS depth to a reasonable limit (e.g., 5-10 hops), as Matrix auth chains are rarely deeper than 3-4 events (`create` -> `member` -> `pl` -> `jr`).
- **Memoization:** Results of the auth chain walk for common ancestors can be cached within the scope of a single state resolution transaction.
