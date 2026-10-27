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

from synapse.http.matrixfederationclient import StrictJsonParser
from synapse.util.json import strict_json_decoder

from tests import unittest


class StrictJsonDecoderTestCase(unittest.TestCase):
    def test_accepts_ordinary_json(self) -> None:
        self.assertEqual(
            strict_json_decoder.decode('{"a": {"b": [1, {"c": 2}]}, "d": "e"}'),
            {"a": {"b": [1, {"c": 2}]}, "d": "e"},
        )

    def test_rejects_top_level_duplicate_key(self) -> None:
        with self.assertRaises(ValueError):
            strict_json_decoder.decode('{"a": 1, "a": 2}')

    def test_rejects_nested_duplicate_key(self) -> None:
        with self.assertRaises(ValueError):
            strict_json_decoder.decode('{"a": {"key": "x", "key": "x"}}')

    def test_rejects_duplicate_key_inside_list(self) -> None:
        with self.assertRaises(ValueError):
            strict_json_decoder.decode('{"a": [{"b": 1, "b": 1}]}')

    def test_still_rejects_nan(self) -> None:
        with self.assertRaises(ValueError):
            strict_json_decoder.decode('{"a": NaN}')


class StrictJsonParserTestCase(unittest.TestCase):
    def test_parses_an_object(self) -> None:
        parser = StrictJsonParser()
        parser.write(b'{"a": 1}')
        self.assertEqual(parser.finish(), {"a": 1})

    def test_rejects_duplicate_keys(self) -> None:
        parser = StrictJsonParser()
        parser.write(b'{"a": {"key": "x", "key": "x"}}')
        with self.assertRaises(ValueError):
            parser.finish()

    def test_rejects_non_objects(self) -> None:
        parser = StrictJsonParser()
        parser.write(b"[1, 2]")
        with self.assertRaises(ValueError):
            parser.finish()
