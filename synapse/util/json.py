#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright (C) 2025 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl_3.0.html>.
#
#

import json
from typing import (
    Any,
)

from immutabledict import immutabledict

from synapse.synapse_rust.events import JsonObject


def _reject_invalid_json(val: Any) -> None:
    """Do not allow Infinity, -Infinity, or NaN values in JSON."""
    raise ValueError("Invalid JSON value: '%s'" % val)


def _handle_extra_mappings(obj: Any) -> dict[Any, Any]:
    """Helper for json_encoder. Makes immutabledicts and JsonObjects
    serializable
    """
    match obj:
        case immutabledict():
            # fishing the protected dict out of the object is a bit nasty,
            # but we don't really want the overhead of copying the dict.
            try:
                # Safety: we catch the AttributeError immediately below.
                return obj._dict
            except AttributeError:
                # If all else fails, resort to making a copy of the immutabledict
                return dict(obj)
        case JsonObject():
            # Just convert to a dict.
            return dict(obj)
    raise TypeError(
        "Object of type %s is not JSON serializable" % obj.__class__.__name__
    )


# A custom JSON encoder which:
#   * handles immutabledicts
#   * produces valid JSON (no NaNs etc)
#   * reduces redundant whitespace
json_encoder = json.JSONEncoder(
    allow_nan=False, separators=(",", ":"), default=_handle_extra_mappings
)

# Create a custom decoder to reject Python extensions to JSON.
json_decoder = json.JSONDecoder(parse_constant=_reject_invalid_json)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build an object from its key/value pairs, refusing duplicate keys.

    The standard decoder silently keeps the last value for a repeated key, which
    lets an attacker present different content to parsers that disagree about
    which value wins.

    Raises:
        ValueError: If any key occurs more than once, even with the same value.
    """
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key: %r" % (key,))
        result[key] = value
    return result


# Like `json_decoder`, but also rejects any object (at any depth) that contains
# the same key more than once.
strict_json_decoder = json.JSONDecoder(
    parse_constant=_reject_invalid_json, object_pairs_hook=_reject_duplicate_keys
)
