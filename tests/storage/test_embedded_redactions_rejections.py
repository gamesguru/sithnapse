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

"""Pure unit tests for the embedded redactions/rejections FFI layer.

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
from synapse.storage.databases.main.embedded_rejections import (
    delete_rejections_batch,
    get_rejections_batch,
    put_rejection_batch,
)
from synapse.synapse_rust import mtxdb_engine

from tests import unittest
from tests.utils import EMBEDDED_HAMT_ENGINE, EMBEDDED_HAMT_PATH

if EMBEDDED_HAMT_ENGINE and EMBEDDED_HAMT_PATH:
    _TEST_ENGINE_TMPDIR = EMBEDDED_HAMT_PATH
else:
    _TEST_ENGINE_TMPDIR = tempfile.mkdtemp(prefix="test-embedded-redactions-")
    atexit.register(shutil.rmtree, _TEST_ENGINE_TMPDIR, ignore_errors=True)
mtxdb_engine.open_client(_TEST_ENGINE_TMPDIR)

_ENGINE = "mtxdb"


class EmbeddedRedactionsRejectionsTestCase(unittest.TestCase):
    def test_redaction_put_get_round_trip(self) -> None:
        ns = "test-redactions-round-trip"
        put_redaction_batch(
            _ENGINE,
            ns,
            [
                ("$target1", "$redaction1", False),
                ("$target2", "$redaction2", True),
            ],
        )

        found = get_redactions_batch(_ENGINE, ns, ["$target1", "$target2", "$missing"])
        self.assertEqual(found["$target1"], ("$redaction1", False))
        self.assertEqual(found["$target2"], ("$redaction2", True))
        self.assertNotIn("$missing", found)

    def test_have_censored_flips_both_directions(self) -> None:
        ns = "test-redactions-flip"
        put_redaction_batch(_ENGINE, ns, [("$target", "$redaction", False)])

        set_have_censored_batch(_ENGINE, ns, ["$target"], True)
        self.assertEqual(
            get_redactions_batch(_ENGINE, ns, ["$target"])["$target"],
            ("$redaction", True),
        )

        # Flipping back (the "original event re-persisted unredacted" path)
        # must preserve the redaction event id.
        set_have_censored_batch(_ENGINE, ns, ["$target"], False)
        self.assertEqual(
            get_redactions_batch(_ENGINE, ns, ["$target"])["$target"],
            ("$redaction", False),
        )

    def test_put_is_create_only(self) -> None:
        """A second redaction targeting the same event must not clobber the
        existing slot's aggregate `have_censored` -- SQL's `_store_redaction`
        upsert preserves it, and `redactions.redacts` is non-unique."""
        ns = "test-redactions-create-only"
        put_redaction_batch(_ENGINE, ns, [("$target", "$redaction1", False)])
        set_have_censored_batch(_ENGINE, ns, ["$target"], True)

        # A second redaction of the same event: the slot stays censored.
        put_redaction_batch(_ENGINE, ns, [("$target", "$redaction2", False)])
        self.assertEqual(
            get_redactions_batch(_ENGINE, ns, ["$target"])["$target"],
            ("$redaction1", True),
        )

    def test_set_have_censored_skips_absent_ids(self) -> None:
        ns = "test-redactions-absent"
        # No mirror record for the id: a no-op, not an error.
        set_have_censored_batch(_ENGINE, ns, ["$never-written"], True)
        self.assertEqual(get_redactions_batch(_ENGINE, ns, ["$never-written"]), {})

    def test_redaction_namespace_isolation(self) -> None:
        put_redaction_batch(_ENGINE, "ns-a", [("$shared", "$redaction-a", False)])
        put_redaction_batch(_ENGINE, "ns-b", [("$shared", "$redaction-b", True)])

        self.assertEqual(
            get_redactions_batch(_ENGINE, "ns-a", ["$shared"])["$shared"],
            ("$redaction-a", False),
        )
        self.assertEqual(
            get_redactions_batch(_ENGINE, "ns-b", ["$shared"])["$shared"],
            ("$redaction-b", True),
        )

    def test_redaction_delete(self) -> None:
        ns = "test-redactions-delete"
        put_redaction_batch(_ENGINE, ns, [("$target", "$redaction", False)])
        delete_redactions_batch(_ENGINE, ns, ["$target"])
        self.assertEqual(get_redactions_batch(_ENGINE, ns, ["$target"]), {})

    def test_rejection_put_get_round_trip(self) -> None:
        ns = "test-rejections-round-trip"
        put_rejection_batch(
            _ENGINE,
            ns,
            [
                ("$e1", "bad signature", "1700000000000"),
                ("$e2", "not allowed", "1700000000001"),
            ],
        )

        found = get_rejections_batch(_ENGINE, ns, ["$e1", "$e2", "$missing"])
        self.assertEqual(found["$e1"], ("bad signature", "1700000000000"))
        self.assertEqual(found["$e2"], ("not allowed", "1700000000001"))
        self.assertNotIn("$missing", found)

    def test_rejection_last_check_rewrite(self) -> None:
        ns = "test-rejections-rewrite"
        put_rejection_batch(_ENGINE, ns, [("$e1", "reason", "1700000000000")])
        put_rejection_batch(_ENGINE, ns, [("$e1", "reason", "1700000009999")])
        self.assertEqual(
            get_rejections_batch(_ENGINE, ns, ["$e1"])["$e1"],
            ("reason", "1700000009999"),
        )

    def test_rejection_namespace_isolation(self) -> None:
        put_rejection_batch(_ENGINE, "rns-a", [("$shared", "reason-a", "1")])
        put_rejection_batch(_ENGINE, "rns-b", [("$shared", "reason-b", "2")])

        self.assertEqual(
            get_rejections_batch(_ENGINE, "rns-a", ["$shared"])["$shared"],
            ("reason-a", "1"),
        )
        self.assertEqual(
            get_rejections_batch(_ENGINE, "rns-b", ["$shared"])["$shared"],
            ("reason-b", "2"),
        )

    def test_rejection_delete(self) -> None:
        ns = "test-rejections-delete"
        put_rejection_batch(_ENGINE, ns, [("$e1", "reason", "1")])
        delete_rejections_batch(_ENGINE, ns, ["$e1"])
        self.assertEqual(get_rejections_batch(_ENGINE, ns, ["$e1"]), {})
