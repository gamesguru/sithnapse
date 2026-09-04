#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from synapse.api.constants import EventTypes  # noqa: E402
from synapse.synapse_rust import state_hamt  # noqa: E402
from synmark.state_fixtures import StateFixture, make_state_fixture  # noqa: E402


def entries(state: dict[tuple[str, str], str]) -> list[tuple[str, str, str]]:
    return [(typ, state_key, event_id) for (typ, state_key), event_id in state.items()]


def build(fixture: StateFixture) -> tuple[bytes, list[tuple[bytes, bytes]]]:
    root, nodes = state_hamt.build_root_handle(
        fixture.room_id, entries(dict(fixture.state))
    )
    return bytes(root[0]), [
        (bytes(node_hash), bytes(blob)) for node_hash, blob in nodes
    ]


def materialize(
    root_hash: bytes, nodes: list[tuple[bytes, bytes]]
) -> dict[tuple[str, str], str]:
    node_map = dict(nodes)
    result = state_hamt.materialize_state_entries(node_map[root_hash], nodes)
    return {(typ, state_key): event_id for typ, state_key, event_id in result}


def selective_lookup(
    fixture: StateFixture,
    root_hash: bytes,
    nodes: list[tuple[bytes, bytes]],
    keys: tuple[tuple[str, str], ...],
) -> tuple[dict[tuple[str, str], str], int]:
    all_nodes = dict(nodes)
    loaded = {root_hash: all_nodes[root_hash]}
    while True:
        result, missing = state_hamt.lookup_state_entries(
            fixture.room_id,
            loaded[root_hash],
            list(loaded.items()),
            keys,
        )
        missing = [bytes(node_hash) for node_hash in missing if node_hash not in loaded]
        if not missing:
            return (
                {(typ, state_key): event_id for typ, state_key, event_id in result},
                len(loaded),
            )
        loaded.update((node_hash, all_nodes[node_hash]) for node_hash in missing)


def time_case(
    name: str, iterations: int, operation: Callable[[], object]
) -> dict[str, object]:
    samples: list[float] = []
    operation()
    for _ in range(iterations):
        start = time.perf_counter()
        operation()
        samples.append(time.perf_counter() - start)
    ordered = sorted(samples)
    return {
        "case": name,
        "iterations": iterations,
        "mean_ms": statistics.mean(samples) * 1000,
        "median_ms": statistics.median(samples) * 1000,
        "p95_ms": ordered[max(0, int(len(ordered) * 0.95) - 1)] * 1000,
    }


def positive_int(value: str) -> int:
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark the current flat state HAMT"
    )
    parser.add_argument("--members", type=int, default=10_000)
    parser.add_argument("--other-state", type=int, default=32)
    parser.add_argument("--mutations", type=positive_int, default=100)
    parser.add_argument("--iterations", type=positive_int, default=20)
    args = parser.parse_args()

    fixture = make_state_fixture(args.members, args.other_state, args.mutations)
    root_hash, nodes = build(fixture)
    expected = dict(fixture.state)
    if materialize(root_hash, nodes) != expected:
        raise RuntimeError("HAMT fixture failed round-trip validation")

    def full() -> dict[tuple[str, str], str]:
        return materialize(root_hash, nodes)

    # These deliberately materialize first, matching today's production path.
    def exact_auth() -> dict[tuple[str, str], str]:
        state = materialize(root_hash, nodes)
        return {key: state[key] for key in fixture.auth_keys}

    def exact_auth_selective() -> tuple[dict[tuple[str, str], str], int]:
        return selective_lookup(fixture, root_hash, nodes, fixture.auth_keys)

    def all_members() -> dict[tuple[str, str], str]:
        state = materialize(root_hash, nodes)
        return {
            key: value for key, value in state.items() if key[0] == EventTypes.Member
        }

    mutation_key, mutation_event_id = fixture.mutations[0]

    def rebuild_one_key() -> object:
        state = dict(fixture.state)
        state[mutation_key] = mutation_event_id
        return state_hamt.build_root_handle(fixture.room_id, entries(state))

    results = [
        time_case("full_materialize", args.iterations, full),
        time_case("exact_auth_5_current_path", args.iterations, exact_auth),
        time_case("exact_auth_5_selective", args.iterations, exact_auth_selective),
        time_case("all_members_current_path", args.iterations, all_members),
        time_case("rebuild_one_key_current_path", args.iterations, rebuild_one_key),
    ]
    metadata = {
        "members": args.members,
        "entries": len(fixture.state),
        "nodes": len(nodes),
        "node_bytes": sum(len(blob) for _, blob in nodes),
        "selective_auth_nodes": exact_auth_selective()[1],
    }
    print(json.dumps({"metadata": metadata, "results": results}, indent=2))


if __name__ == "__main__":
    main()
