#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 Element Creations Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Central resolver for the embedded HAMT engine's PyO3 module.

Every consumer of the embedded engine (the state HAMT layer in
`state/store.py`, `state/bg_updates.py`, and the mirrors in
`embedded_event_auth_chain_links.py`, `embedded_event_json.py`,
`embedded_event_to_state_group.py`) used to import the engine module
directly by name. That scattered the engine name across a dozen files and
made swapping engines a whole-repo refactor.

This module is the single place that maps an `embedded_hamt.engine` config
value to its PyO3 submodule and validates unknown names with a consistent
error. Consumers resolve the module here and never import it directly, so
switching engines (or renaming the module) is a one-file change.
"""

from types import ModuleType


def get_embedded_engine(engine_name: str | None) -> ModuleType:
    """Return the PyO3 module implementing the named embedded HAMT engine.

    Args:
        engine_name: the `embedded_hamt.engine` config value (e.g. "mtxdb").
            Callers only invoke this after gating on the engine being
            configured, so `None` shouldn't reach here; if it does, it fails
            loudly below rather than as an AttributeError in a hot path.

    Returns:
        The PyO3 submodule exposing the engine's point/batch API.

    Raises:
        RuntimeError: if `engine_name` is not a known engine, so a typo'd
            config fails loudly rather than as an AttributeError in a hot
            path.
    """
    if engine_name == "mtxdb":
        from synapse.synapse_rust import mtxdb_engine

        return mtxdb_engine
    raise RuntimeError(
        f"Unknown embedded_hamt_engine: {engine_name!r} (supported engine(s): mtxdb)"
    )
