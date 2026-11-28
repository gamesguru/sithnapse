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

"""Pure unit tests for the embedded redactions FFI layer.

The mtxdb client is process-global, so each test opens this module's fixed
root and closes it again rather than relying on it staying open for the life
of the process (other test modules close and reopen the client too). The root
itself is removed only at process exit.
"""

import atexit
import shutil
import tempfile

from synapse.storage.databases.main.embedded_redactions import (
    delete_redactions_batch,
    get_redactions_batch,
    put_redaction_batch,
    set_have_censored_batch,
)
from synapse.synapse_rust import mtxdb_engine

from tests import unittest
from tests.utils import EMBEDDED_DB_ENGINE, EMBEDDED_DB_PATH

if EMBEDDED_DB_ENGINE and EMBEDDED_DB_PATH:
    _TEST_ENGINE_TMPDIR = EMBEDDED_DB_PATH
else:
    _TEST_ENGINE_TMPDIR = tempfile.mkdtemp(prefix="test-embedded-redactions-")
    atexit.register(shutil.rmtree, _TEST_ENGINE_TMPDIR, ignore_errors=True)
_ENGINE = "mtxdb"


class EmbeddedRedactionsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # The client is process-global and tests elsewhere close it; open this
        # module's fixed root for the duration of each test. Data persists in
        # it across tests (each uses unique ids), so a close/reopen is cheap
        # and does not change what any test can see.
        mtxdb_engine.close_client()
        mtxdb_engine.open_client(_TEST_ENGINE_TMPDIR)

    def tearDown(self) -> None:
        mtxdb_engine.close_client()

    def test_redaction_put_get_round_trip(self) -> None:
        put_redaction_batch(
            _ENGINE,
            [
                ("$rt-target1", "$rt-redaction1", False),
                ("$rt-target2", "$rt-redaction2", True),
            ],
        )

        found = get_redactions_batch(
            _ENGINE, ["$rt-target1", "$rt-target2", "$rt-missing"]
        )
        self.assertEqual(found["$rt-target1"], ("$rt-redaction1", False))
        self.assertEqual(found["$rt-target2"], ("$rt-redaction2", True))
        self.assertNotIn("$rt-missing", found)

    def test_have_censored_flips_both_directions(self) -> None:
        put_redaction_batch(_ENGINE, [("$hc-target", "$hc-redaction", False)])

        set_have_censored_batch(_ENGINE, ["$hc-target"], True)
        self.assertEqual(
            get_redactions_batch(_ENGINE, ["$hc-target"])["$hc-target"],
            ("$hc-redaction", True),
        )

        # Flipping back (the "original event re-persisted unredacted" path)
        # must preserve the redaction event id.
        set_have_censored_batch(_ENGINE, ["$hc-target"], False)
        self.assertEqual(
            get_redactions_batch(_ENGINE, ["$hc-target"])["$hc-target"],
            ("$hc-redaction", False),
        )

    def test_put_is_create_only(self) -> None:
        """A second redaction targeting the same event must not clobber the
        existing slot's aggregate `have_censored` -- SQL's `_store_redaction`
        upsert preserves it, and `redactions.redacts` is non-unique."""
        put_redaction_batch(_ENGINE, [("$co-target", "$co-redaction1", False)])
        set_have_censored_batch(_ENGINE, ["$co-target"], True)

        # A second redaction of the same event: the slot stays censored.
        put_redaction_batch(_ENGINE, [("$co-target", "$co-redaction2", False)])
        self.assertEqual(
            get_redactions_batch(_ENGINE, ["$co-target"])["$co-target"],
            ("$co-redaction1", True),
        )

    def test_set_have_censored_skips_absent_ids(self) -> None:
        # No mirror record for the id: a no-op, not an error.
        set_have_censored_batch(_ENGINE, ["$absent-target"], True)
        self.assertEqual(get_redactions_batch(_ENGINE, ["$absent-target"]), {})

    def test_latest_write_wins_for_a_shared_slot(self) -> None:
        put_redaction_batch(_ENGINE, [("$lw-target", "$lw-redaction-a", False)])
        put_redaction_batch(_ENGINE, [("$lw-target", "$lw-redaction-b", True)])

        self.assertEqual(
            get_redactions_batch(_ENGINE, ["$lw-target"])["$lw-target"],
            ("$lw-redaction-a", False),
        )

    def test_redaction_delete(self) -> None:
        put_redaction_batch(_ENGINE, [("$del-target", "$del-redaction", False)])
        delete_redactions_batch(_ENGINE, ["$del-target"])
        self.assertEqual(get_redactions_batch(_ENGINE, ["$del-target"]), {})


class EmbeddedReopenTestCase(unittest.TestCase):
    """`close_client` must release *all* per-root state, including caches that
    live outside the pool handles.

    The room index keeps shard file handles and a state-group -> prefix cache
    in process-global statics keyed only by shard. If those survive a close,
    reopening a different root silently appends to (or answers from) the first
    root's index files. Reusing the same key in both roots makes that visible.
    """

    def setUp(self) -> None:
        mtxdb_engine.close_client()

    def tearDown(self) -> None:
        mtxdb_engine.close_client()

    def test_close_and_reopen_does_not_share_state(self) -> None:
        root_a = tempfile.mkdtemp(prefix="test-mtxdb-reopen-a-")
        root_b = tempfile.mkdtemp(prefix="test-mtxdb-reopen-b-")
        self.addCleanup(shutil.rmtree, root_a, ignore_errors=True)
        self.addCleanup(shutil.rmtree, root_b, ignore_errors=True)

        group = 987_654_321
        prefix_a = b"ROOMAAAA"
        prefix_b = b"ROOMBBBB"

        mtxdb_engine.open_client(root_a)
        put_redaction_batch(_ENGINE, [("$reopen-target", "$reopen-a", False)])
        mtxdb_engine.put_room_index([(group, prefix_a)])
        self.assertEqual(mtxdb_engine.get_room_index([group]), [prefix_a])
        mtxdb_engine.close_client()

        # Same key, same state-group id, different root: neither the store
        # nor the room-index caches may leak the first root's values.
        mtxdb_engine.open_client(root_b)
        self.assertNotIn(
            "$reopen-target", get_redactions_batch(_ENGINE, ["$reopen-target"])
        )
        self.assertEqual(mtxdb_engine.get_room_index([group]), [None])
        put_redaction_batch(_ENGINE, [("$reopen-target", "$reopen-b", False)])
        mtxdb_engine.put_room_index([(group, prefix_b)])
        mtxdb_engine.close_client()

        # Reopening the first root must still read its own write, not the
        # second root's later one.
        mtxdb_engine.open_client(root_a)
        self.assertEqual(
            get_redactions_batch(_ENGINE, ["$reopen-target"])["$reopen-target"],
            ("$reopen-a", False),
        )
        self.assertEqual(mtxdb_engine.get_room_index([group]), [prefix_a])
