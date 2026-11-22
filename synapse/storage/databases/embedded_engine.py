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

"""Central resolver for the embedded DB engine's PyO3 module.

Every consumer of the embedded engine (the state HAMT layer in
`state/store.py`, `state/bg_updates.py`, and the mirrors in
`embedded_event_json.py`,
`embedded_event_to_state_group.py`) used to import the engine module
directly by name. That scattered the engine name across a dozen files and
made swapping engines a whole-repo refactor.

This module is the single place that maps an `embedded_db.engine` config
value to its PyO3 submodule and validates unknown names with a consistent
error. Consumers resolve the module here and never import it directly, so
switching engines (or renaming the module) is a one-file change.
"""

import os
from types import ModuleType

from synapse.logging.context import defer_to_thread
from synapse.types import ISynapseReactor


def _get_embedded_db_disk_bytes(path: str | None) -> int | None:
    """Return the on-disk size of an embedded DB path, in bytes.

    Prefer the open embedded driver's own shard sizes, which avoids walking the
    database directory. Falls back to the filesystem for an older extension or
    when the engine is not open. Returns `None` if the path is unset.
    """
    if not path:
        return None

    try:
        from synapse.synapse_rust import mtxdb_engine

        stats = mtxdb_engine.stats()
        sizes = [
            int(pool["disk_bytes"])
            for pool in stats.values()
            if isinstance(pool, dict) and isinstance(pool.get("disk_bytes"), int)
        ]
        if sizes:
            return sum(sizes)
    except Exception:
        pass

    try:
        if os.path.isfile(path):
            return os.path.getsize(path)
        if not os.path.isdir(path):
            return None

        total = 0
        for root, _dirs, files in os.walk(path):
            for name in files:
                try:
                    total += os.path.getsize(os.path.join(root, name))
                except OSError:
                    pass
        return total
    except OSError:
        return None


async def get_embedded_db_disk_bytes(
    reactor: ISynapseReactor, path: str | None
) -> int | None:
    """Collect embedded DB disk size without blocking the request thread."""
    return await defer_to_thread(reactor, _get_embedded_db_disk_bytes, path)


def get_embedded_engine(engine_name: str | None) -> ModuleType:
    """Return the PyO3 module implementing the named embedded DB engine.

    Args:
        engine_name: the `embedded_db.engine` config value (e.g. "mtxdb").
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
        f"Unknown embedded_db_engine: {engine_name!r} (supported engine(s): mtxdb)"
    )
