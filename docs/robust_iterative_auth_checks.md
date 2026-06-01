# Proposal: State Resolution v2.2 (for Room v12.1)

## Overview

This document proposes **State Resolution v2.2**, an evolution of the v2.1 algorithm (MSC4297), targeted for **Room Version 12.1**. The primary goal is to make iterative auth checks robust against incomplete federation data while maintaining strict security against Power Level Replay attacks.

Currently, Synapse's state resolution follows the Matrix specification strictly by only inspecting the immediate `auth_event_ids()` provided by an event. While this is spec-compliant, it makes the resolution process fragile when dealing with incomplete auth chains from remote servers.

## The v2.2 Algorithm: Transitive Authorization (via recusive BFS)

State Res v2.2 introduces a **Breadth-First Search (BFS) recursive walk** of the local `auth_events` during the iterative check phase. This formally codifies the **Principle of Transitive Authorization**: if an author cites a chain of authority, the resolver should utilize the most specific (nearest) ancestors available in the local DAG to validate the author's intent.

By walking the auth chain recursively, v2.2 ensures that the resolver has the most complete possible picture of the author's intended authorization context, even if intermediate "parent" events are missing.

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

## Determinism, Discord, and DAG Healing

A critical requirement of Matrix state resolution is that it must be a **pure, deterministic function** of the DAG. However, as documented in the "Federated DAG Membership Anomalies" report, Room Version 12 is currently in a state of **active discord**.

Servers frequently disagree on membership (e.g., Anomaly 1: matrix.org vs. nutra.tk) because the v2.1 algorithm is too fragile—it rejects events when a single "parent" auth event is missing, even when a valid "grandparent" is available to provide the necessary authority.

### The "Healing" Argument for v2.2

While v12.1 is the formal standard for mandating these checks, there is a strong pragmatic case for allowing v2.2 logic to run on existing v12 rooms:

1.  **Resolution of Existing Divergence:** V12 rooms are _already_ diverged because different servers have different "holes" in their local DAGs. The current algorithm (v2.1) is unable to bridge these holes.
2.  **Convergence via Robustness:** State Res v2.2's BFS recursive walk acts as a **DAG Healer**. By utilizing transitive authority, it allows servers with slightly different DAG coverage to reach the **same mathematically correct state**.
3.  **Correcting the "Set Problem":** v2.2 shifts the resolution from a "Graph Link" problem (which fails at every gap) to a "Set Reconciliation" problem (which succeeds as long as the necessary authority exists somewhere in the set).

In this light, upgrading to v2.2 is not just a "change in logic"—it is the implementation of a more robust mathematical model that **enables consensus** in environments where v2.1 currently guarantees discord.

## Self-Correction of Existing Corruption

A common concern in distributed systems is whether a protocol change can fix a state that is **already corrupt**. In the case of Matrix State Resolution, the answer for v2.2 is a resounding **yes**.

### How v2.2 Heals "Catgirl and Bot"

In Anomaly 2 (catgirl.cloud vs. @bot), the room is currently "corrupt" because catgirl.cloud has dropped the bot's membership from its resolved state. Under v2.1, any new events from the bot or attempts to "re-join" fail because the immediate parent auth event is missing from the local context, and v2.1 is too fragile to look further.

**State Res v2.2 fixes this via "The Fresh Start" property:**

1.  **Pure Function:** State resolution does not "update" the previous state; it re-calculates the state of a conflict set from scratch using the available DAG.
2.  **Transitive Recovery:** When a new merge event arrives, a server running v2.2 will re-evaluate the conflicted membership events. Even if the immediate "parent" join is still missing, the **BFS Recursive Walk** will reach back to the "grandparent" authority (e.g., the `m.room.create` event or a stable `m.room.power_levels`).
3.  **Admission of the "Vished":** Because v2.2 finds this transitive authority, it can successfully authorize and admit the bot's membership into the new resolved state.

By allowing previously "un-authorizable" events to finally pass authentication, v2.2 enables a diverged server to **automatically align its reality** with the rest of the federation as soon as a merge occurs. Corruption is not permanent; it is merely a result of the algorithm being too blind to see the proof of authority that already exists in the DAG.

### The Mechanism of Re-appearance: How the "Magic" Works

To understand how members "magically" re-appear, we must look at the transition from **rejection** to **admission** during a fork merge:

1.  **The Event is Present, but the Parent is Missing:** In a diverged room like "Catgirl and Bot," the bot's join event is physically present in the server's DAG (having been received via federation), but it is currently **excluded** from the resolved state because the server is missing the immediate PDU that authorized it.
2.  **Iterative Re-evaluation:** When a new merge event triggers state resolution, the bot's join event enters the **Conflict Set**.
3.  **v2.1 Failure (The Wall):** As the resolver iterates through the conflict set, it reaches the bot's join. It looks for the immediate `auth_event_id` (the "parent"). It's missing. The resolver logs a warning, considers the event unauthorized, and **skips it**. The bot remains missing.
4.  **v2.2 Success (The Bridge):** Under v2.2, the resolver reaches the same join event. It sees the parent is missing, but instead of giving up, it performs the **BFS Recursive Walk**. It finds the "grandparent" (e.g., the room's `m.room.create` event).
5.  **Admission to State:** Because the "grandparent" provides valid authority, the bot's join event **passes authentication**. The resolver then inserts this event into the room's `resolved_state` map.

**The Result:** The member "re-appears" because they are now part of the room's active state dictionary. Any subsequent messages from that member, which were previously being rejected as "unauthorized" by the server, will now find the member's join in the resolved state and pass auth instantly. The "magic" is simply the algorithm choosing to see the proof of authority that was there all along.

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
