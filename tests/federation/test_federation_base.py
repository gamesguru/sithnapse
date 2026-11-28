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
#


from unittest.mock import patch

from signedjson.key import encode_verify_key_base64, get_verify_key

from synapse.api.room_versions import (
    EventFormatVersions,
    RoomVersion,
    RoomVersions,
)
from synapse.crypto import keyring
from synapse.crypto.event_signing import add_hashes_and_signatures
from synapse.events import EventBase, make_event_from_dict
from synapse.federation import federation_base
from synapse.federation.federation_base import InvalidEventSignatureError

from tests import unittest


class FederationBaseTestCase(unittest.HomeserverTestCase):
    def test_events_signed_by_banned_key_are_refused(self) -> None:
        """Ensure that event JSON signed using a banned server_signing_key fails verification."""
        event_dict = {
            "content": {"body": "Here is the message content"},
            "event_id": "$0:domain",
            "origin_server_ts": 1000000,
            "type": "m.room.message",
            "room_id": "!r:domain",
            "sender": f"@u:{self.hs.config.server.server_name}",
            "auth_events": [],
            "prev_events": [],
            "depth": 1,
            "signatures": {},
            "unsigned": {"age_ts": 1000000},
        }

        add_hashes_and_signatures(
            self.hs.config.server.default_room_version,
            event_dict,
            self.hs.config.server.server_name,
            self.hs.signing_key,
        )
        event = make_event_from_dict(event_dict)
        fs = self.hs.get_federation_server()

        # Ensure the signatures check out normally
        self.get_success(
            fs._check_sigs_and_hash(self.hs.config.server.default_room_version, event)
        )

        # Patch the list of banned signing keys and ensure the signature check fails
        with patch.object(
            keyring,
            "BANNED_SERVER_SIGNING_KEYS",
            (encode_verify_key_base64(get_verify_key(self.hs.signing_key))),
        ):
            self.get_failure(
                fs._check_sigs_and_hash(
                    self.hs.config.server.default_room_version, event
                ),
                InvalidEventSignatureError,
            )


class OperatorAcceptedEventTestCase(unittest.HomeserverTestCase):
    """`federation.accept_unverifiable_event_ids` waives a signature failure for
    exactly the listed event, and nothing else."""

    def setUp(self) -> None:
        super().setUp()

        async def failing_check(*args: object, **kwargs: object) -> None:
            pdu = args[2]
            assert isinstance(pdu, EventBase)
            raise InvalidEventSignatureError("no usable key", pdu.event_id)

        patcher = patch.object(
            federation_base, "_check_sigs_on_pdu", side_effect=failing_check
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _event(self, room_version: RoomVersion, body: str = "hello") -> EventBase:
        event_dict = {
            "content": {"body": body},
            "origin_server_ts": 1000000,
            "type": "m.room.message",
            "room_id": "!r:domain",
            "sender": f"@u:{self.hs.config.server.server_name}",
            "auth_events": [],
            "prev_events": [],
            "depth": 1,
            "signatures": {},
            "unsigned": {"age_ts": 1000000},
        }
        if room_version.event_format == EventFormatVersions.ROOM_V1_V2:
            # These formats carry an explicit, arbitrary event ID.
            event_dict["event_id"] = f"${body.replace(' ', '')}:domain"
        add_hashes_and_signatures(
            room_version,
            event_dict,
            self.hs.config.server.server_name,
            self.hs.signing_key,
        )
        return make_event_from_dict(event_dict, room_version)

    def test_unlisted_event_with_bad_signature_is_refused(self) -> None:
        room_version = RoomVersions.V10
        fs = self.hs.get_federation_server()
        self.get_failure(
            fs._check_sigs_and_hash(room_version, self._event(room_version)),
            InvalidEventSignatureError,
        )

    def test_listed_event_is_accepted_and_others_are_not(self) -> None:
        room_version = RoomVersions.V10
        event = self._event(room_version)
        other = self._event(room_version, body="a different event")
        self.assertNotEqual(other.event_id, event.event_id)
        self.hs.config.federation.accept_unverifiable_event_ids = frozenset(
            {event.event_id}
        )
        fs = self.hs.get_federation_server()

        accepted = self.get_success(fs._check_sigs_and_hash(room_version, event))
        self.assertEqual(accepted.event_id, event.event_id)
        self.get_failure(
            fs._check_sigs_and_hash(room_version, other), InvalidEventSignatureError
        )

    def test_listing_is_ignored_for_room_versions_without_content_hash_ids(
        self,
    ) -> None:
        room_version = RoomVersions.V1
        event = self._event(room_version)
        self.hs.config.federation.accept_unverifiable_event_ids = frozenset(
            {event.event_id}
        )
        fs = self.hs.get_federation_server()
        self.get_failure(
            fs._check_sigs_and_hash(room_version, event), InvalidEventSignatureError
        )
