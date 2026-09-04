from __future__ import annotations

from dataclasses import dataclass

from synapse.api.constants import EventTypes
from synapse.types import StateMap

StateKey = tuple[str, str]


@dataclass(frozen=True)
class StateFixture:
    room_id: str
    state: StateMap[str]
    auth_keys: tuple[StateKey, ...]
    mutations: tuple[tuple[StateKey, str], ...]


def make_state_fixture(
    member_count: int,
    other_state_count: int = 32,
    mutation_count: int = 100,
) -> StateFixture:
    """Build deterministic, Matrix-shaped state without constructing events."""
    if member_count < 2:
        raise ValueError("member_count must be at least 2")
    if other_state_count < 0 or mutation_count < 0:
        raise ValueError("fixture sizes must be non-negative")

    state: dict[StateKey, str] = {
        (EventTypes.Create, ""): "$create:benchmark",
        (EventTypes.PowerLevels, ""): "$power_levels:benchmark",
        (EventTypes.JoinRules, ""): "$join_rules:benchmark",
        (EventTypes.RoomHistoryVisibility, ""): "$history_visibility:benchmark",
        (EventTypes.Name, ""): "$name:benchmark",
        (EventTypes.RoomAvatar, ""): "$avatar:benchmark",
    }
    for i in range(member_count):
        state[(EventTypes.Member, f"@user-{i}:benchmark")] = f"$member-{i}-0:benchmark"
    for i in range(other_state_count):
        state[(f"com.example.state.{i % 8}", f"key-{i}")] = f"$other-{i}:benchmark"

    auth_keys = (
        (EventTypes.Create, ""),
        (EventTypes.PowerLevels, ""),
        (EventTypes.JoinRules, ""),
        (EventTypes.Member, "@user-0:benchmark"),
        (EventTypes.Member, "@user-1:benchmark"),
    )
    mutations = tuple(
        (
            (EventTypes.Member, f"@user-{i % member_count}:benchmark"),
            f"$member-{i % member_count}-{i + 1}:benchmark",
        )
        for i in range(mutation_count)
    )
    return StateFixture(
        room_id="!state-hamt-benchmark:test",
        state=state,
        auth_keys=auth_keys,
        mutations=mutations,
    )
