#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2014-2016 OpenMarket Ltd
# Copyright (C) 2023 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#

import logging

import attr
from signedjson.types import VerifyKey

logger = logging.getLogger(__name__)


@attr.s(slots=True, frozen=True, auto_attribs=True)
class FetchKeyResult:
    verify_key: VerifyKey  # the key itself
    valid_until_ts: int  # how long we can use this key for
    # Other bodies observed for the same (server, key ID), with their own
    # validity windows. A key ID collision must not make earlier bodies
    # impossible to use for historical event signatures.
    alternate_keys: tuple[tuple[VerifyKey, int], ...] = ()

    def candidates(self) -> tuple[tuple[VerifyKey, int], ...]:
        return ((self.verify_key, self.valid_until_ts), *self.alternate_keys)


def merge_fetch_key_results(
    primary: FetchKeyResult, *others: FetchKeyResult
) -> FetchKeyResult:
    """Combine observed bodies without changing which body is authoritative.

    `primary`'s verify key stays the binding (First Seen Wins); other bodies
    observed for the same key ID become `alternate_keys`, with the longest
    validity seen for each body. Only the primary body's validity window is
    widened. Alternates are ordered by body bytes so the result is stable.
    """
    primary_body = primary.verify_key.encode()
    primary_validity = primary.valid_until_ts
    alternates: dict[bytes, tuple[VerifyKey, int]] = {}
    for result in (primary, *others):
        for key, valid_until_ts in result.candidates():
            body = key.encode()
            if body == primary_body:
                primary_validity = max(primary_validity, valid_until_ts)
                continue
            previous = alternates.get(body)
            if previous is None or valid_until_ts > previous[1]:
                alternates[body] = (key, valid_until_ts)

    ordered = tuple(alternates[body] for body in sorted(alternates))
    return FetchKeyResult(primary.verify_key, primary_validity, ordered)


@attr.s(slots=True, frozen=True, auto_attribs=True)
class FetchKeyResultForRemote:
    key_json: bytes  # the full key JSON
    valid_until_ts: int  # how long we can use this key for, in milliseconds.
    added_ts: int  # When we added this key, in milliseconds.
