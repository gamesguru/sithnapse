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
# <https://www.gnu.org/licenses/agpl_3.0.html>.
#

import json

from immutabledict import immutabledict

from synapse.synapse_rust.events import JsonObject
from synapse.util.json import json_decoder, json_encoder

from tests.unittest import TestCase


class OrjsonTestCase(TestCase):
    """Test cases for synapse.util.json Orjson wrapper integration"""

    def test_encode_decode_basic(self) -> None:
        """Test encoding and decoding basic types."""
        data = {"string": "hello", "integer": 42, "boolean": True, "list": [1, 2, 3]}
        encoded = json_encoder.encode(data)
        self.assertIsInstance(encoded, str)

        decoded = json_decoder.decode(encoded)
        self.assertEqual(decoded, data)

    def test_encode_immutabledict(self) -> None:
        """Test encoding of immutabledict wrapper."""
        d = immutabledict({"key": "value"})
        encoded = json_encoder.encode(d)
        self.assertEqual(encoded, '{"key":"value"}')

        decoded = json_decoder.decode(encoded)
        self.assertEqual(decoded, {"key": "value"})

    def test_encode_json_object(self) -> None:
        """Test encoding of Rust-based JsonObject wrapper."""
        obj = JsonObject({"key": "value"})
        encoded = json_encoder.encode(obj)
        self.assertEqual(encoded, '{"key":"value"}')

        decoded = json_decoder.decode(encoded)
        self.assertEqual(decoded, {"key": "value"})

    def test_decode_invalid(self) -> None:
        """Test that invalid JSON strings raise JSONDecodeError."""
        with self.assertRaises(json.JSONDecodeError):
            json_decoder.decode("not valid json")

    def test_decode_nan_infinity(self) -> None:
        """Test that NaN, Infinity, and -Infinity are rejected as invalid JSON."""
        for invalid_const in ["NaN", "Infinity", "-Infinity"]:
            with self.assertRaises(json.JSONDecodeError):
                json_decoder.decode(invalid_const)

    def test_unsupported_type_error(self) -> None:
        """Test that serializing an unsupported type raises TypeError."""

        class Unsupported:
            pass

        with self.assertRaises(TypeError):
            json_encoder.encode(Unsupported())
