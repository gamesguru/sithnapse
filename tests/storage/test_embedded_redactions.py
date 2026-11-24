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

Like test_embedded_event_edges.py, mtxdb_engine.open_client() is backed by a
Rust OnceCell: the engine is opened exactly once per process and the data
directory must survive for the life of the process. The module-level fixture
opens one store and removes it only at process exit.
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
mtxdb_engine.open_client(_TEST_ENGINE_TMPDIR)

_ENGINE = "mtxdb"


class EmbeddedRedactionsTestCase(unittest.TestCase):
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
