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

from typing import Any

import orjson
from immutabledict import immutabledict

from synapse.synapse_rust.events import JsonObject


def _handle_extra_mappings(obj: Any) -> Any:
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


class OrjsonEncoder:
    """A custom high-performance JSON encoder wrapper that uses orjson."""

    def encode(self, obj: Any) -> str:
        # orjson.dumps returns bytes; we decode it to a UTF-8 string to be
        # compatible with json.JSONEncoder.encode().
        return orjson.dumps(obj, default=_handle_extra_mappings).decode("utf-8")


class OrjsonDecoder:
    """A custom high-performance JSON decoder wrapper that uses orjson."""

    def decode(self, s: str | bytes) -> Any:
        return orjson.loads(s)


json_encoder = OrjsonEncoder()
json_decoder = OrjsonDecoder()
