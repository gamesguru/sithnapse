#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#

"""Tests for the de-salt dump/restore pair in `scripts-dev/mtxdb/`.

The dump half needs prod's old build and cannot run in CI, which has one build.
What CI can check is everything that does not depend on the old FFI: the old
key derivations, the dump framing, and the restore half against a real store.
The end-to-end proof is the rehearsal on a snapshot of prod's store.
"""

import importlib.util
import json
import shutil
import struct
import sys
import tempfile
from pathlib import Path
from typing import Any
from unittest import TestCase

from synapse.storage.databases.main.embedded_event_json import (
    _encode_event_json_record,
)
from synapse.storage.databases.main.embedded_event_to_state_group import (
    _event_to_state_group_key,
    _state_group_refcount_key,
)
from synapse.storage.databases.state.bg_updates import _encode_state_hamt_root
from synapse.synapse_rust import mtxdb_engine, state_hamt

SCRIPTS = Path(__file__).parents[2] / "scripts-dev" / "mtxdb"
RESTORE = SCRIPTS / "restore_desalt-001-4db1a920f-11c6125cab.py"


def _load() -> tuple[Any, Any]:
    """Load `_desalt` and the restore script on demand (the Debian build ships
    no `scripts-dev/`)."""
    sys.path.insert(0, str(SCRIPTS))
    try:
        # A dynamic import: mypy excludes scripts-dev/, so a static one cannot
        # resolve in the repo-wide run.
        _desalt = importlib.import_module("_desalt")

        spec = importlib.util.spec_from_file_location("restore_desalt", RESTORE)
        assert spec is not None and spec.loader is not None
        restore = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(restore)
    finally:
        sys.path.remove(str(SCRIPTS))
    return _desalt, restore


class DesaltKeyTestCase(TestCase):
    def setUp(self) -> None:
        if not RESTORE.exists():
            self.skipTest("scripts-dev/mtxdb is absent")
        self.desalt, _ = _load()

    def test_old_keys_match_prods_salted_layout(self) -> None:
        # Literal values derived from 4db1a920f's `embedded_common.namespace_hash`
        # (sha256(namespace)[:16], hex) and its two key helpers.
        self.assertEqual(
            self.desalt.old_event_to_state_group_key("example.org", "$abc"),
            b"event_to_state_group:bfabc37432958b063360d3ad6461c9c4:$abc",
        )
        self.assertEqual(
            self.desalt.old_state_group_refcount_key("example.org", 7),
            b"state_group_refcount:bfabc37432958b063360d3ad6461c9c4:"
            + struct.pack(">q", 7),
        )

    def test_new_keys_match_the_running_build(self) -> None:
        self.assertEqual(
            self.desalt.new_event_to_state_group_key("$abc"),
            _event_to_state_group_key("$abc"),
        )
        self.assertEqual(
            self.desalt.new_state_group_refcount_key(7), _state_group_refcount_key(7)
        )

    def test_framing_rejects_truncated_and_miscounted_dumps(self) -> None:
        header = {"type": "header", "format": self.desalt.EXPORT_FORMAT}
        record = {"type": "event", "event_id": "$a", "room_id": "!r:x", "value": ""}
        counts = dict.fromkeys(self.desalt.RECORD_TYPES, 0)
        counts["event"] = 1

        self.assertEqual(
            self.desalt.count_records(
                [header, record, {"type": "end", "counts": counts}]
            ),
            counts,
        )
        with self.assertRaisesRegex(RuntimeError, "truncated"):
            self.desalt.count_records([header, record])
        with self.assertRaisesRegex(RuntimeError, "disagree"):
            self.desalt.count_records(
                [header, record, record, {"type": "end", "counts": counts}]
            )
        with self.assertRaisesRegex(RuntimeError, "no header"):
            self.desalt.count_records([record])


class DesaltRestoreTestCase(TestCase):
    def setUp(self) -> None:
        if not RESTORE.exists():
            self.skipTest("scripts-dev/mtxdb is absent")
        self.desalt, self.restore = _load()
        self.dir = Path(tempfile.mkdtemp(prefix="test-desalt-restore-"))
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        mtxdb_engine.close_client()
        mtxdb_engine.open_client(str(self.dir))
        self.addCleanup(mtxdb_engine.close_client)

    def _dump(self) -> Path:
        d = self.desalt
        room_id = "!desalt:example.org"
        room_prefix = state_hamt.room_hamt_prefix(room_id, False)
        root_hash, _, lattice, _ = state_hamt.build_root_handle_with_lattice(
            room_id, [("m.test", "", "$one")]
        )
        body = _encode_event_json_record("{}", '{"type":"m.test"}', 10)
        rows: list[dict[str, Any]] = [
            {"type": "header", "format": d.EXPORT_FORMAT},
            {
                "type": "event",
                "event_id": "$one",
                "room_id": room_id,
                "value": d.b64(body),
            },
            {
                "type": "event_to_state_group",
                "event_id": "$one",
                "value": d.b64(struct.pack(">q", 41)),
            },
            {
                "type": "state_group_refcount",
                "state_group": 41,
                "value": d.b64(struct.pack(">q", 3)),
            },
            {
                "type": "room_index",
                "state_group": 41,
                "room_prefix": d.b64(room_prefix),
            },
            {
                "type": "root",
                "state_group": 41,
                "room_prefix": d.b64(room_prefix),
                "value": d.b64(
                    _encode_state_hamt_root(room_prefix, root_hash, lattice, room_id)
                ),
            },
        ]
        counts = dict.fromkeys(d.RECORD_TYPES, 0)
        for row in rows[1:]:
            counts[row["type"]] += 1
        rows.append({"type": "end", "counts": counts})
        path = self.dir / "dump.jsonl"
        path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        return path

    def test_restore_writes_unsalted_keys_and_verifies(self) -> None:
        path = self._dump()
        room_id = "!desalt:example.org"
        room_prefix = state_hamt.room_hamt_prefix(room_id, False)
        root_hash, _, _, nodes = state_hamt.build_root_handle_with_lattice(
            room_id, [("m.test", "", "$one")]
        )
        # HAMT nodes are intentionally unchanged by the namespace migration;
        # seed them as they would already exist in the source store.
        mtxdb_engine.put_state_hamt_nodes(room_prefix, nodes)
        written = self.restore.apply_dump(mtxdb_engine, path, batch_size=2)
        self.assertEqual(
            written, self.desalt.count_records(self.desalt.read_dump(path))
        )

        # The running build reads what the restore wrote, through its own keys.
        self.assertEqual(self.restore.verify_dump(mtxdb_engine, path, 2), [])
        found = dict(mtxdb_engine.batch_get([_event_to_state_group_key("$one")]))
        self.assertEqual(
            struct.unpack(">q", found[_event_to_state_group_key("$one")])[0], 41
        )
        roots = mtxdb_engine.get_state_hamt_roots_for_room(room_prefix, [41])
        self.assertIsNotNone(roots[0])
        self.assertEqual(
            mtxdb_engine.materialize_state_hamt(room_prefix, root_hash, room_id),
            [("m.test", "", "$one")],
        )

        # Running it again is safe.
        self.restore.apply_dump(mtxdb_engine, path, batch_size=2)
        self.assertEqual(self.restore.verify_dump(mtxdb_engine, path, 2), [])

    def test_verify_reports_a_record_that_did_not_land(self) -> None:
        path = self._dump()
        self.restore.apply_dump(mtxdb_engine, path, batch_size=10)
        mtxdb_engine.batch_delete([_state_group_refcount_key(41)])

        bad = self.restore.verify_dump(mtxdb_engine, path, 10)

        self.assertEqual(len(bad), 1)
        self.assertIn("refcount", bad[0])

    def test_old_room_index_is_moved_aside_once(self) -> None:
        index = self.dir / "room_index"
        index.mkdir()
        (index / "index-2d.bin").write_bytes(b"old")

        self.assertTrue(self.restore.retire_old_room_index(self.dir))
        self.assertFalse(index.exists())
        self.assertEqual(
            (self.dir / "room_index.pre-desalt" / "index-2d.bin").read_bytes(), b"old"
        )

        # A re-run must not move the new-format index the first run wrote.
        index.mkdir()
        (index / "index-2d.bin").write_bytes(b"new")
        self.assertFalse(self.restore.retire_old_room_index(self.dir))
        self.assertEqual((index / "index-2d.bin").read_bytes(), b"new")
