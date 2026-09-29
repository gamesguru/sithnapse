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

"""End-to-end tests for mtxdb's collection OCC Python bindings."""

import os
import subprocess
import sys
import tempfile

from tests import unittest

_SCRIPT = """
import sys
from synapse.synapse_rust import mtxdb_engine as m

m.open_client(sys.argv[1])
collection = bytes([0xA1]) * 16
key = bytes([0x01]) * 16
other_key = bytes([0x02]) * 16
payload = b"initial"

def txn():
    result = m.begin_transaction()
    assert result is not None, "OCC bindings require the shared WAL"
    return result

# Seed a collection through the generic transaction put.
seed = txn()
seed.put(0, collection, key, payload)
seed.commit()

# The versioned point read returns data and the token from one boundary.
stale = txn()
records, expected = stale.get_with_collection_version(0, collection, [key])
assert records == [payload], records

# A concurrent transaction changes the collection version.
writer = txn()
writer.put(0, collection, other_key, b"concurrent")
writer.commit()
assert not m.recheck_collection_version(0, collection, expected)

# The stale expectation is surfaced as the registered typed exception, with
# the structured conflict fields retained in args.
stale.expect_collection_version(0, collection, expected)
stale.put(0, collection, key, b"must-not-publish")
try:
    stale.commit()
except m.StaleReadError as error:
    assert error.args[1:] == ("mtpl-state", collection, expected, expected + 1), error.args
else:
    raise AssertionError("stale transaction unexpectedly committed")

# A fresh read/expectation succeeds, and its token passes the lazy-scan recheck.
retry = txn()
records, current = retry.get_with_collection_version(0, collection, [key, other_key])
assert records == [payload, b"concurrent"], records
retry.expect_collection_version(0, collection, current)
retry.put(0, collection, key, b"retried")
retry.commit()
assert m.recheck_collection_version(0, collection, current + 1)
"""


class MtxdbOccBindingsTestCase(unittest.TestCase):
    def test_versioned_transaction_api_and_stale_error(self) -> None:
        with tempfile.TemporaryDirectory(prefix="test-mtxdb-occ-bindings-") as store:
            result = subprocess.run(
                [sys.executable, "-c", _SCRIPT, store],
                capture_output=True,
                text=True,
                env=dict(os.environ, SYNAPSE_MTXDB_WAL="1"),
                timeout=120,
            )

        self.assertEqual(result.returncode, 0, result.stderr[-2000:])
