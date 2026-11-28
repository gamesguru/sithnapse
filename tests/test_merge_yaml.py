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

import importlib.util
from pathlib import Path
from unittest import TestCase, skipUnless

SCRIPT = Path(__file__).parent.parent / "scripts-dev" / "merge_yaml.py"


def _load():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location("merge_yaml", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# The Debian package build runs the tests from a tree that has `tests/` but not
# `scripts-dev/`, so there is nothing to load there.
@skipUnless(SCRIPT.exists(), "scripts-dev/merge_yaml.py is not part of this tree")
class MergePlainSampleTests(TestCase):
    def test_commented_and_active_options_are_enabled(self) -> None:
        mod = _load()
        sample = [
            "# Prose: not an option\n",
            "#commented: 1\n",
            "active: 2\n",
        ]
        lines = mod.enable_disabled(sample, mod.SAMPLE_DISABLED_RE)
        out, used = mod.merge(lines, {"commented": 10, "active": 20})
        text = "".join(
            mod.commented(ln) if isinstance(ln, mod.Off) else ln for ln in out
        )
        self.assertIn("# Prose: not an option\n", text)
        self.assertIn("commented: 10\n", text)
        self.assertNotIn("#commented: 10", text)
        self.assertIn("active: 20\n", text)
        self.assertEqual(used, {("commented",), ("active",)})

    def test_renamed_option_is_moved_to_current_name(self) -> None:
        mod = _load()
        user = mod.apply_renames(
            {"stats": {"public_user_count_enabled": True, "enabled": True}}
        )
        self.assertEqual(
            user,
            {"stats": {"enabled": True, "public_stats_enabled": True}},
        )

    def test_current_name_wins_over_renamed_option(self) -> None:
        mod = _load()
        user = mod.apply_renames(
            {
                "stats": {
                    "public_user_count_enabled": True,
                    "public_stats_enabled": False,
                }
            }
        )
        self.assertEqual(user, {"stats": {"public_stats_enabled": False}})

    def test_user_comments_and_quotes_in_block_lists_survive(self) -> None:
        mod = _load()
        user_text = (
            "federation:\n"
            "  accept_unverifiable_event_ids:\n"
            "    # room !abc (alias: #x:example.org)\n"
            '    - "$one"\n'
            "    # second origin\n"
            '    - "$two"\n'
            "    # END ROOM\n"
            "  other: 1\n"
        )
        user = mod.yaml.safe_load(user_text)
        sample = [
            "federation:\n",
            "  #accept_unverifiable_event_ids: []\n",
            "  #other: 0\n",
        ]
        lines = mod.enable_disabled(sample, mod.SAMPLE_DISABLED_RE)
        out, _ = mod.merge(lines, user, user_text.splitlines(True))
        text = "".join(out)
        self.assertIn("    # room !abc (alias: #x:example.org)\n", text)
        self.assertIn('    - "$one"\n', text)
        self.assertIn("    # second origin\n", text)
        self.assertIn("    # END ROOM\n", text)

    def test_block_list_falls_back_to_dump_when_text_is_not_found(self) -> None:
        mod = _load()
        text = mod.user_block([], ("a", "b"), ["x"], 4)
        self.assertIsNone(text)

    def test_template_comments_are_aligned_like_prettier(self) -> None:
        mod = _load()
        schema = {
            "properties": {
                "block": {
                    "type": "object",
                    "default": {},
                    "description": "Block docs.",
                    "properties": {
                        "first": {"type": "integer", "default": 1, "description": "A."},
                        "last": {"type": "integer", "default": 2, "description": "B."},
                    },
                },
                "other": {"type": "integer", "default": 3, "description": "C."},
            }
        }
        lines = mod.enable_disabled(mod.render_schema(schema))
        out, _ = mod.merge(lines, {"block": {"first": 10}})
        mod.align_comments(out)
        text = "".join(
            mod.commented(ln) if isinstance(ln, mod.Off) else ln for ln in out
        )
        # Documentation for a set option sits at the option's column ...
        self.assertIn("  ##   A.\n  first: 10\n", text)
        # ... and for an option left at its default after the last set one, at column 0.
        self.assertIn("##   B.\n#  last: 2\n", text)

    def test_dumped_lists_are_indented_and_double_quoted(self) -> None:
        mod = _load()
        self.assertEqual(
            mod.dump_block({"a": ["!x:y", "plain"]}, 2),
            '  a:\n    - "!x:y"\n    - plain\n',
        )

    def test_leading_document_start_marker_is_kept(self) -> None:
        mod = _load()
        self.assertTrue(mod.has_document_start(["---\n", "a: 1\n"]))
        self.assertTrue(mod.has_document_start(["# c\n", "\n", "---\n", "a: 1\n"]))
        self.assertFalse(mod.has_document_start(["a: 1\n", "---\n"]))
        self.assertFalse(mod.has_document_start(["a: 1\n"]))
