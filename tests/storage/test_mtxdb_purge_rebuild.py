#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2026 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Purged room data must stay purged when the room is written again and the
mtxdb indexes are rebuilt from the packfiles.

The engine is driven from subprocesses: mtxdb's binding owns process-global pools.
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from tests import unittest

_WRITE = """
import sys
from synapse.synapse_rust import mtxdb_engine as m

room = "!purged:test"
m.open_client(sys.argv[1])
m.event_json_put([(room, "$old", b"old body")])
old_id, new_id = m.get_or_create_short_ids(room, ["$old", "$new"])
m.auth_chain_edges_put(room, [(old_id, [new_id])])
m.sync()
m.event_json_purge_room(room)
m.auth_chain_purge_room(room)
# Writing the room again clears the deletion markers.
m.event_json_put([(room, "$new", b"new body")])
m.auth_chain_edges_put(room, [(new_id, [])])
m.sync()
m.close_client()
"""

_READ = """
import json, sys
from synapse.synapse_rust import mtxdb_engine as m

room = "!purged:test"
m.open_client(sys.argv[1])
bodies = dict(m.event_json_get(["$old", "$new"]))
old_id, new_id = m.get_or_create_short_ids(room, ["$old", "$new"])
old_edges, new_edges = m.auth_chain_edges_get(room, [old_id, new_id])
m.close_client()
print(json.dumps({
    "old_body": None if bodies.get("$old") is None else bodies["$old"].decode(),
    "new_body": None if bodies.get("$new") is None else bodies["$new"].decode(),
    "old_edges": old_edges is not None,
    "new_edges": new_edges is not None,
}))
"""


class MtxdbPurgeRebuildTestCase(unittest.TestCase):
    def _engine(self, script: str, store: str) -> str:
        result = subprocess.run(
            [sys.executable, "-c", script, store],
            capture_output=True,
            text=True,
            env=dict(os.environ, SYNAPSE_MTXDB_WAL="1"),
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr[-1000:])
        return result.stdout

    def _rebuild_after_purge(self, drop_generation_fences: bool) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="test-mtxdb-purge-rebuild-") as store:
            self._engine(_WRITE, store)

            # Without the saved indexes, reopening has to scan the packfiles,
            # which still hold the purged records.
            derived = [
                path
                for pattern in ("index.checkpoint", "index.delta*", "shard_coll*")
                for path in Path(store).rglob(pattern)
            ]
            self.assertTrue(derived, "found no saved indexes to remove")
            for path in derived:
                path.unlink()

            if drop_generation_fences:
                fences = list(Path(store).rglob("collection.generations"))
                self.assertTrue(fences, "the purge recorded no deletion")
                for path in fences:
                    path.unlink()

            out: dict[str, Any] = json.loads(
                self._engine(_READ, store).strip().splitlines()[-1]
            )
            return out

    def test_purged_records_stay_gone_after_rebuild(self) -> None:
        self.assertEqual(
            self._rebuild_after_purge(drop_generation_fences=False),
            {
                "old_body": None,
                "new_body": "new body",
                "old_edges": False,
                "new_edges": True,
            },
        )

    def test_rebuild_would_resurrect_without_generation_fences(self) -> None:
        """Control: proves the rebuild above really does see the old records."""
        out = self._rebuild_after_purge(drop_generation_fences=True)
        self.assertEqual((out["old_body"], out["old_edges"]), ("old body", True))
