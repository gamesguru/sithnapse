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

"""Storage behaviour for cached server-key responses."""

from canonicaljson import encode_canonical_json
from signedjson.key import (
    SigningKey,
    encode_verify_key_base64,
    generate_signing_key,
    get_verify_key,
)

from twisted.internet.testing import MemoryReactor

from synapse.server import HomeServer
from synapse.storage.database import LoggingTransaction
from synapse.storage.keys import FetchKeyResult
from synapse.util.clock import Clock

from tests.unittest import HomeserverTestCase


class ServerKeyResponseStorageTestCase(HomeserverTestCase):
    def prepare(
        self, reactor: MemoryReactor, clock: Clock, homeserver: HomeServer
    ) -> None:
        self.store = homeserver.get_datastores().main

    def _count_rows(self) -> tuple[int, int, int]:
        def _txn(txn: LoggingTransaction) -> tuple[int, int, int]:
            txn.execute("SELECT COUNT(*) FROM server_key_response_blobs")
            blobs = txn.fetchone()[0]  # type: ignore[index]

            txn.execute("SELECT COUNT(*) FROM server_keys_json")
            rows = txn.fetchone()[0]  # type: ignore[index]

            txn.execute(
                "SELECT COUNT(*) FROM server_keys_json WHERE response_id IS NOT NULL"
            )
            refs = txn.fetchone()[0]  # type: ignore[index]
            return blobs, rows, refs

        return self.get_success(
            self.store.db_pool.runInteraction("count_server_key_rows", _txn)
        )

    def _store_response(
        self,
        server_name: str,
        from_server: str,
        signing_key: SigningKey,
        valid_until_ts: int,
        ts_added_ms: int,
    ) -> tuple[str, dict[str, FetchKeyResult]]:
        verify_key = get_verify_key(signing_key)
        key_id = f"{verify_key.alg}:{verify_key.version}"
        verify_keys = {
            key_id: FetchKeyResult(verify_key=verify_key, valid_until_ts=valid_until_ts)
        }
        response = {
            "server_name": server_name,
            "old_verify_keys": {},
            "valid_until_ts": valid_until_ts,
            "verify_keys": {key_id: {"key": encode_verify_key_base64(verify_key)}},
        }
        result = self.get_success(
            self.store.store_server_keys_response(
                server_name=server_name,
                from_server=from_server,
                ts_added_ms=ts_added_ms,
                verify_keys=verify_keys,
                response_json=response,
            )
        )
        return key_id, result

    def _signature_row(self, server_name: str, key_id: str) -> tuple[int, bool]:
        def _txn(txn: LoggingTransaction) -> tuple[int, bool]:
            txn.execute(
                """
                SELECT ts_valid_until_ms, seen_active
                FROM server_signature_keys
                WHERE server_name = ? AND key_id = ?
                """,
                (server_name, key_id),
            )
            row = txn.fetchone()
            assert row is not None
            return int(row[0]), bool(row[1])

        return self.get_success(
            self.store.db_pool.runInteraction("signature_row", _txn)
        )

    def test_stale_shorter_response_keeps_longer_window(self) -> None:
        """A later, shorter response must not shorten the stored validity."""
        now = self.clock.time_msec()
        server_name = "remote.example"
        from_server = "notary.example"
        signing_key = generate_signing_key("stale")
        long_ts = now + 3_600_000
        short_ts = now + 60_000

        key_id, first = self._store_response(
            server_name, from_server, signing_key, long_ts, now
        )
        self.assertEqual(first[key_id].valid_until_ts, long_ts)

        _, second = self._store_response(
            server_name, from_server, signing_key, short_ts, now + 1
        )
        self.assertEqual(second[key_id].valid_until_ts, long_ts)

        local = self.get_success(
            self.store.get_server_keys_json([(server_name, key_id)])
        )
        self.assertEqual(local[(server_name, key_id)].valid_until_ts, long_ts)

        stored_ts, _ = self._signature_row(server_name, key_id)
        self.assertEqual(stored_ts, long_ts)

    def test_seen_active_upgrade_does_not_shorten_window(self) -> None:
        """Upgrading a legacy `seen_active` row must keep its longer expiry."""
        now = self.clock.time_msec()
        server_name = "remote.example"
        from_server = "notary.example"
        signing_key = generate_signing_key("legacy")
        long_ts = now + 3_600_000
        short_ts = now + 60_000

        key_id, _ = self._store_response(
            server_name, from_server, signing_key, long_ts, now
        )

        # Simulate a row written before `seen_active` tracking existed.
        def _clear_seen_active(txn: LoggingTransaction) -> None:
            txn.execute(
                "UPDATE server_signature_keys SET seen_active = ?"
                " WHERE server_name = ? AND key_id = ?",
                (False, server_name, key_id),
            )

        self.get_success(
            self.store.db_pool.runInteraction("clear_seen_active", _clear_seen_active)
        )

        _, second = self._store_response(
            server_name, from_server, signing_key, short_ts, now + 1
        )
        self.assertEqual(second[key_id].valid_until_ts, long_ts)

        stored_ts, seen_active = self._signature_row(server_name, key_id)
        self.assertEqual(stored_ts, long_ts)
        self.assertTrue(seen_active)

    def test_signed_response_is_stored_once_per_response(self) -> None:
        """A response covering several keys keeps one blob, not one per key."""
        now = self.clock.time_msec()
        server_name = "remote.example"

        verify_keys: dict[str, FetchKeyResult] = {}
        response_verify_keys = {}
        for version in ("a", "b"):
            signing_key = generate_signing_key(version)
            verify_key = get_verify_key(signing_key)
            key_id = f"{verify_key.alg}:{verify_key.version}"
            verify_keys[key_id] = FetchKeyResult(
                verify_key=verify_key, valid_until_ts=now + 3600_000
            )
            response_verify_keys[key_id] = {"key": encode_verify_key_base64(verify_key)}

        response = {
            "server_name": server_name,
            "old_verify_keys": {},
            "valid_until_ts": now + 3600_000,
            "verify_keys": response_verify_keys,
        }

        self.get_success(
            self.store.store_server_keys_response(
                server_name=server_name,
                from_server=server_name,
                ts_added_ms=now,
                verify_keys=verify_keys,
                response_json=response,
            )
        )

        blobs, rows, refs = self._count_rows()
        self.assertEqual(blobs, 1)
        self.assertEqual(rows, len(verify_keys))
        self.assertEqual(refs, len(verify_keys))

        # Local verification reads still resolve the key from the shared blob.
        local = self.get_success(
            self.store.get_server_keys_json(
                (server_name, key_id) for key_id in verify_keys
            )
        )
        self.assertEqual(set(local), {(server_name, key_id) for key_id in verify_keys})
        for key_id, result in verify_keys.items():
            self.assertEqual(local[(server_name, key_id)].verify_key, result.verify_key)

        # The notary endpoint can still serve the original signed response.
        remote = self.get_success(
            self.store.get_server_keys_json_for_remote(server_name, list(verify_keys))
        )
        self.assertEqual(set(remote), set(verify_keys))
        expected_json = encode_canonical_json(response)
        for remote_result in remote.values():
            self.assertIsNotNone(remote_result)
            assert remote_result is not None
            self.assertEqual(remote_result.key_json, expected_json)
