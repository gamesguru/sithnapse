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

"""Concurrent event-edge merges under mtxdb collection OCC.

Races many ``event_edges_put`` calls against one room's forward list with the
shared WAL open, so the optimistic path in ``embedded_edges.rs`` is exercised:
the first commit advances the room collection's logical version, the other
writers' staged expectations go stale, and ``run_edge_occ`` replays the losers
on fresh transactions. The forward list must end up with every child exactly
once -- a torn merge or a lost update drops one of the racers.

Scope: this covers RMW concurrency *within* mtxdb collections. It deliberately
does not touch the PREV/FWD generation split or the SQL ``source_version``
completeness gate; binding that SQL watermark clock to mtxdb collection
versions needs a separate coordinator and is out of scope here, so
``test_sql_fallback_and_repair_on_missing_mtxdb_edges`` stays a documented
known failure rather than a target of this change.
"""

import os
import subprocess
import sys
import tempfile

from tests import unittest

_SCRIPT = """
import sys
import threading

from synapse.synapse_rust import mtxdb_engine as m

m.open_client(sys.argv[1])
conflicts_before = m.event_edges_occ_conflicts()

namespace = "test-occ-edge-merge"
room_id = "!occ-merge:test"
parent = "$parent"
children = [f"$child-{index:02d}" for index in range(16)]

# Align every writer on one barrier so they read the same forward-list version
# and race the same commit instead of serializing by chance.
barrier = threading.Barrier(len(children))
failures = []


def worker(child):
    try:
        barrier.wait()
        # Mirror the production caller's `_retry_on_contention` contract: an
        # exhausted optimistic loop surfaces a retryable `BlockingIOError`.
        for attempt in range(5):
            try:
                m.event_edges_put(namespace, [(room_id, child, parent, False)])
                return
            except BlockingIOError:
                if attempt == 4:
                    raise
    except BaseException as error:
        failures.append(repr(error))


threads = [threading.Thread(target=worker, args=(child,)) for child in children]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()

assert not failures, failures

forward = dict(m.event_edges_get_forward(namespace, [parent]))
merged = forward.get(parent)
assert merged is not None, forward
# No child lost and none duplicated: a torn merge would drop a racer.
assert sorted(merged) == sorted(children), (sorted(merged), sorted(children))
conflicts_after = m.event_edges_occ_conflicts()
assert conflicts_after > conflicts_before, (
    "the race did not exercise EdgeOccOutcome::Conflict and run_edge_occ replay: "
    f"before={conflicts_before}, after={conflicts_after}"
)
"""


class MtxdbOccEdgeMergeTestCase(unittest.TestCase):
    def test_concurrent_forward_list_merges_without_lost_updates(self) -> None:
        with tempfile.TemporaryDirectory(prefix="test-mtxdb-occ-edge-merge-") as store:
            result = subprocess.run(
                [sys.executable, "-c", _SCRIPT, store],
                capture_output=True,
                text=True,
                env=dict(os.environ, SYNAPSE_MTXDB_WAL="1"),
                timeout=120,
            )

        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
