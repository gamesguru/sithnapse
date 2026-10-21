#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

from typing import Any
from unittest import mock

from synapse.api.room_versions import RoomVersions
from synapse.synapse_rust import state_res as rust_res

from tests import unittest
from tests.handlers import test_federation_event


class BackfilledStateDagRustStateResTests(
    test_federation_event.BackfilledStateDagEventTests
):
    """Runs the backfilled state DAG scenario and checks that the current state is
    resolved by the Rust resolver in its MSC4242 mode, rather than by the Python
    fallback, which would also give the right answer and so hide a Rust bug.

    `rezzy` once rejected the backfilled state event because the auth chain
    prefetch did not follow `prev_state_events`, and then (when pointed at its
    `V2_1` mode) because that mode does not model the state DAG at all. Either
    gave a wrong current state with no error.
    """

    @unittest.override_config({"experimental_features": {"msc4242_enabled": True}})
    def test_backfilled_state_events_are_resolved_by_rust_state_dag_mode(self) -> None:
        real = rust_res.resolve_v2_via_lattice_fold
        calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        results: list[Any] = []

        def spy(*args: Any, **kwargs: Any) -> Any:
            calls.append((args, kwargs))
            results.append(real(*args, **kwargs))
            return results[-1]

        with mock.patch.object(rust_res, "resolve_v2_via_lattice_fold", spy):
            # The scenario asserts the current state itself.
            self.test_backfilled_state_events_update_current_state()

        self.assertGreaterEqual(
            len(results), 1, "the Rust resolver was never used for the state DAG room"
        )
        for args, kwargs in calls:
            room_version = kwargs.get(
                "room_version", args[3] if len(args) > 3 else None
            )
            self.assertEqual(room_version, RoomVersions.MSC4242v12.identifier)
