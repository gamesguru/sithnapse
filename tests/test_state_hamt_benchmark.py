from typing import Any
from unittest import SkipTest

from synapse.api.constants import EventTypes
from synapse.synapse_rust import state_hamt


def _make_state_fixture(*args: Any, **kwargs: Any) -> Any:
    # The Debian package deliberately does not install the developer-only
    # synmark package, but its test runner still copies this test module.
    try:
        from synmark.state_fixtures import make_state_fixture
    except ModuleNotFoundError as e:
        if e.name == "synmark":
            raise SkipTest("synmark is not installed") from e
        raise

    return make_state_fixture(*args, **kwargs)


def test_state_hamt_benchmark_fixture_round_trips() -> None:
    fixture = _make_state_fixture(20, other_state_count=8, mutation_count=4)
    entries = [
        (typ, state_key, event_id)
        for (typ, state_key), event_id in fixture.state.items()
    ]
    root, nodes = state_hamt.build_root_handle(fixture.room_id, entries)
    node_map = {bytes(node_hash): bytes(blob) for node_hash, blob in nodes}
    result = state_hamt.materialize_state_entries(
        node_map[bytes(root[0])],
        list(node_map.items()),
    )
    actual = {(typ, state_key): event_id for typ, state_key, event_id in result}
    assert actual == fixture.state
    assert len([key for key in actual if key[0] == EventTypes.Member]) == 20


def test_state_hamt_benchmark_fixture_is_deterministic() -> None:
    assert _make_state_fixture(20, 8, 4) == _make_state_fixture(20, 8, 4)
