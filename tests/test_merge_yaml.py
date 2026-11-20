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
from unittest import TestCase

SCRIPT = Path(__file__).parent.parent / "scripts-dev" / "merge_yaml.py"


def _load():  # type: ignore[no-untyped-def]
    spec = importlib.util.spec_from_file_location("merge_yaml", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
        text = "".join("#" + ln if isinstance(ln, mod.Off) else ln for ln in out)
        self.assertIn("# Prose: not an option\n", text)
        self.assertIn("commented: 10\n", text)
        self.assertNotIn("#commented: 10", text)
        self.assertIn("active: 20\n", text)
        self.assertEqual(used, {("commented",), ("active",)})
