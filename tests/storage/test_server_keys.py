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

import hashlib

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
from synapse.storage.databases.main.keys import MAX_KEY_BODIES_PER_ID
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

    def test_refresh_does_not_leak_replaced_blob(self) -> None:
        """A refresh with a new signed response drops the blob it replaces."""
        now = self.clock.time_msec()
        server_name = "remote.example"
        signing_key = generate_signing_key("a")

        for i in range(3):
            key_id, _ = self._store_response(
                server_name,
                "notary.example",
                signing_key,
                valid_until_ts=now + 3600_000 + i,
                ts_added_ms=now + i,
            )
            blobs, rows, refs = self._count_rows()
            self.assertEqual((blobs, rows, refs), (1, 1, 1))

    def test_expired_document_body_is_not_retained(self) -> None:
        """MSC4499 §11 admission: a replayed expired `verify_keys` document is
        not retained, while a retired body is."""
        now = self.clock.time_msec()
        server_name = "remote.example"
        expired = now - 600_000

        active = generate_signing_key("active")
        active_verify = get_verify_key(active)
        active_id = f"{active_verify.alg}:{active_verify.version}"
        retired = generate_signing_key("retired")
        retired_verify = get_verify_key(retired)
        retired_id = f"{retired_verify.alg}:{retired_verify.version}"

        response = {
            "server_name": server_name,
            "valid_until_ts": expired,
            "verify_keys": {
                active_id: {"key": encode_verify_key_base64(active_verify)}
            },
            "old_verify_keys": {
                retired_id: {
                    "key": encode_verify_key_base64(retired_verify),
                    "expired_ts": expired,
                }
            },
        }
        self.get_success(
            self.store.store_server_keys_response(
                server_name=server_name,
                from_server=server_name,
                ts_added_ms=now,
                verify_keys={
                    active_id: FetchKeyResult(active_verify, expired),
                    retired_id: FetchKeyResult(retired_verify, expired),
                },
                response_json=response,
            )
        )

        def _retained(txn: LoggingTransaction) -> set[str]:
            txn.execute(
                "SELECT key_id FROM server_signature_key_candidates"
                " WHERE server_name = ?",
                (server_name,),
            )
            return {row[0] for row in txn.fetchall()}

        retained = self.get_success(
            self.store.db_pool.runInteraction("retained_bodies", _retained)
        )
        self.assertEqual(retained, {retired_id})

    def test_colliding_body_is_retained_but_not_authoritative(self) -> None:
        """MSC4499 §11: a second body for one key ID is kept as an alternate,
        while the First Seen Wins body stays the binding."""
        now = self.clock.time_msec()
        server_name = "remote.example"
        original = generate_signing_key("reuse")
        original_verify = get_verify_key(original)
        key_id, _ = self._store_response(
            server_name, server_name, original, now + 3_600_000, now
        )

        colliding = generate_signing_key("reuse")
        colliding_verify = get_verify_key(colliding)
        self.assertNotEqual(colliding_verify.encode(), original_verify.encode())
        _, colliding_result = self._store_response(
            server_name, server_name, colliding, now + 3_600_000, now + 1
        )

        # The caller keeps the first body as the binding...
        self.assertEqual(
            colliding_result[key_id].verify_key.encode(),
            original_verify.encode(),
        )
        # ...and can see the colliding body as an alternate.
        self.assertEqual(
            [key.encode() for key, _ in colliding_result[key_id].alternate_keys],
            [colliding_verify.encode()],
        )

        local = self.get_success(
            self.store.get_server_keys_json([(server_name, key_id)])
        )
        result = local[(server_name, key_id)]
        self.assertEqual(result.verify_key.encode(), original_verify.encode())
        self.assertEqual(
            [key.encode() for key, _ in result.alternate_keys],
            [colliding_verify.encode()],
        )

        def _binding(txn: LoggingTransaction) -> bytes:
            txn.execute(
                "SELECT verify_key FROM server_signature_keys"
                " WHERE server_name = ? AND key_id = ?",
                (server_name, key_id),
            )
            row = txn.fetchone()
            assert row is not None
            return bytes(row[0])

        stored = self.get_success(
            self.store.db_pool.runInteraction("get_binding", _binding)
        )
        self.assertEqual(stored, original_verify.encode())

    def test_reused_key_id_cap_keeps_the_newest_bodies(self) -> None:
        """The per-ID cap evicts the oldest alternate, not the body just fetched.

        A server that keeps reusing one key ID for new bodies must still have
        its latest body retained, or events it signs cannot be verified.
        """
        now = self.clock.time_msec()
        server_name = "remote.example"
        keys = [generate_signing_key("reuse") for _ in range(MAX_KEY_BODIES_PER_ID + 1)]

        result: dict[str, FetchKeyResult] = {}
        key_id = ""
        for offset, signing_key in enumerate(keys):
            key_id, result = self._store_response(
                server_name,
                server_name,
                signing_key,
                now + 3_600_000,
                now + offset,
            )

        expected_hashes = {
            hashlib.sha256(get_verify_key(key).encode()).hexdigest() for key in keys
        }
        oldest_hash = hashlib.sha256(get_verify_key(keys[0]).encode()).hexdigest()
        newest_verify = get_verify_key(keys[-1])

        def _retained(txn: LoggingTransaction) -> set[str]:
            txn.execute(
                "SELECT key_hash FROM server_signature_key_candidates"
                " WHERE server_name = ? AND key_id = ?",
                (server_name, key_id),
            )
            return {row[0] for row in txn.fetchall()}

        retained = self.get_success(
            self.store.db_pool.runInteraction("retained_candidates", _retained)
        )
        self.assertEqual(len(retained), MAX_KEY_BODIES_PER_ID)
        # The earliest body falls out of the bounded set...
        self.assertNotIn(oldest_hash, retained)
        # ...and every body except it is still retained.
        self.assertEqual(retained, expected_hashes - {oldest_hash})

        # The response that fetched the newest body can verify signatures made
        # with it, even though another body remains the authoritative binding.
        self.assertIn(
            newest_verify.encode(),
            [key.encode() for key, _ in result[key_id].alternate_keys],
        )
