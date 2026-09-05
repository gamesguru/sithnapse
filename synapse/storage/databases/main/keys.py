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

import hashlib
import itertools
import json
import logging
from typing import Iterable, Mapping, cast

from canonicaljson import encode_canonical_json
from signedjson.key import decode_verify_key_bytes
from unpaddedbase64 import decode_base64

from synapse.storage.database import LoggingTransaction
from synapse.storage.databases.main.cache import CacheInvalidationWorkerStore
from synapse.storage.keys import FetchKeyResult, FetchKeyResultForRemote
from synapse.storage.types import Cursor
from synapse.types import JsonDict
from synapse.util.caches.descriptors import cached, cachedList
from synapse.util.iterutils import batch_iter

logger = logging.getLogger(__name__)


db_binary_type = memoryview


class KeyStore(CacheInvalidationWorkerStore):
    """Persistence for signature verification keys"""

    async def store_server_keys_response(
        self,
        server_name: str,
        from_server: str,
        ts_added_ms: int,
        verify_keys: dict[str, FetchKeyResult],
        response_json: JsonDict,
    ) -> dict[str, FetchKeyResult]:
        """Stores the keys for the given server that we got from `from_server`,
        atomically enforcing MSC4499 First Seen Wins.

        Args:
            server_name: The owner of the keys
            from_server: Which server we got the keys from
            ts_added_ms: When we're adding the keys
            verify_keys: The decoded keys
            response_json: The full *signed* response JSON that contains the keys.

        Returns:
            The authoritative map of key_id -> FetchKeyResult (retaining any
            previously-bound key bodies on collision).
        """

        key_json_bytes = encode_canonical_json(response_json)

        def store_server_keys_response_txn(
            txn: LoggingTransaction,
        ) -> dict[str, FetchKeyResult]:
            final_keys: dict[str, FetchKeyResult] = dict(verify_keys)
            keys_to_persist: dict[str, FetchKeyResult] = dict(verify_keys)

            # ----------------------------------------------------------------
            # MSC4499 conflict resolution: insert-and-reload.
            #
            # Instead of a blind ON CONFLICT DO UPDATE SET (which overwrites
            # all columns including from_server on every collision), we:
            #
            #   1. INSERT ... ON CONFLICT DO NOTHING  -- first committer wins
            #   2. SELECT the actual row from the DB   -- see committed state
            #   3. Compare stored key body vs candidate and act accordingly
            #
            # This addresses two bugs:
            #
            # Issue 1 (concurrent race): Two transactions can both read "no
            # row" and both try to upsert. The first to commit wins via
            # ON CONFLICT DO NOTHING; the second re-reads the committed row
            # and applies collision logic against it, so both callers return
            # the same authoritative key body.
            #
            # Issue 2 (provenance downgrade): When the key body is identical
            # (a notary refresh), the original from_server is preserved
            # rather than being overwritten with the notary's identity.
            # ----------------------------------------------------------------

            for key_id in list(keys_to_persist.keys()):
                fetch_result = keys_to_persist[key_id]

                # Step 1: INSERT ... ON CONFLICT DO NOTHING.
                txn.execute(
                    """
                    INSERT INTO server_signature_keys
                        (server_name, key_id, from_server, ts_added_ms,
                         verify_key, ts_valid_until_ms)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (server_name, key_id) DO NOTHING
                    """,
                    (
                        server_name,
                        key_id,
                        from_server,
                        ts_added_ms,
                        db_binary_type(fetch_result.verify_key.encode()),
                        fetch_result.valid_until_ts,
                    ),
                )

                # Step 2: Re-read the authoritative row.
                txn.execute(
                    """
                    SELECT verify_key, ts_valid_until_ms, from_server
                    FROM server_signature_keys
                    WHERE server_name = ? AND key_id = ?
                    """,
                    (server_name, key_id),
                )
                row = cast(tuple[bytes | memoryview, int | None, str], txn.fetchone())
                assert row is not None
                stored_key_raw, stored_ts, stored_from_server = row
                stored_key_bytes = bytes(stored_key_raw)

                # Step 3: Compare stored key body against candidate.
                if stored_key_bytes == fetch_result.verify_key.encode():
                    # Same key body (refresh) -- preserve original from_server.
                    stored_ts = stored_ts if stored_ts is not None else 0
                    if stored_ts < fetch_result.valid_until_ts:
                        txn.execute(
                            """
                            UPDATE server_signature_keys
                            SET ts_valid_until_ms = ?, ts_added_ms = ?
                            WHERE server_name = ? AND key_id = ?
                            """,
                            (
                                fetch_result.valid_until_ts,
                                ts_added_ms,
                                server_name,
                                key_id,
                            ),
                        )
                elif stored_from_server != server_name and from_server == server_name:
                    # Two-Tier override: direct fetch overrides provisional
                    # notary binding (different key body).
                    logger.warning(
                        "MSC4499: overriding provisional notary binding for "
                        "%s %s with direct origin fetch. "
                        "notary_from=%s cached_sha256=%s new_sha256=%s",
                        server_name,
                        key_id,
                        stored_from_server,
                        hashlib.sha256(stored_key_bytes).hexdigest(),
                        hashlib.sha256(fetch_result.verify_key.encode()).hexdigest(),
                    )
                    txn.execute(
                        """
                        UPDATE server_signature_keys
                        SET from_server = ?, ts_added_ms = ?,
                            ts_valid_until_ms = ?, verify_key = ?
                        WHERE server_name = ? AND key_id = ?
                        """,
                        (
                            from_server,
                            ts_added_ms,
                            fetch_result.valid_until_ts,
                            db_binary_type(fetch_result.verify_key.encode()),
                            server_name,
                            key_id,
                        ),
                    )
                else:
                    # Collision: First Seen Wins -- retain original binding.
                    logger.warning(
                        "MSC4499: key ID collision for %s %s -- retaining "
                        "original key body (First Seen Wins). "
                        "cached_sha256=%s new_sha256=%s",
                        server_name,
                        key_id,
                        hashlib.sha256(stored_key_bytes).hexdigest(),
                        hashlib.sha256(fetch_result.verify_key.encode()).hexdigest(),
                    )
                    final_keys[key_id] = FetchKeyResult(
                        verify_key=decode_verify_key_bytes(key_id, stored_key_bytes),
                        valid_until_ts=stored_ts if stored_ts is not None else 0,
                    )
                    keys_to_persist.pop(key_id, None)

            if keys_to_persist:
                self.db_pool.simple_upsert_many_txn(
                    txn,
                    table="server_keys_json",
                    key_names=("server_name", "key_id", "from_server"),
                    key_values=[
                        (server_name, key_id, from_server) for key_id in keys_to_persist
                    ],
                    value_names=(
                        "ts_added_ms",
                        "ts_valid_until_ms",
                        "key_json",
                    ),
                    value_values=[
                        (
                            ts_added_ms,
                            fetch_result.valid_until_ts,
                            db_binary_type(key_json_bytes),
                        )
                        for fetch_result in keys_to_persist.values()
                    ],
                )

                for key_id in keys_to_persist:
                    txn.call_after(
                        self._get_server_keys_json.invalidate,
                        ((server_name, key_id),),
                    )
                self._send_invalidation_to_replication_bulk(
                    txn,
                    self._get_server_keys_json.__name__,
                    [
                        (json.dumps([server_name, key_id]),)
                        for key_id in keys_to_persist
                    ],
                )
                self._invalidate_cache_and_stream_bulk(
                    txn,
                    self.get_server_key_json_for_remote,
                    [(server_name, key_id) for key_id in keys_to_persist],
                )

            return final_keys

        return await self.db_pool.runInteraction(
            "store_server_keys_response", store_server_keys_response_txn
        )

    @cached()
    def _get_server_keys_json(
        self, server_name_and_key_id: tuple[str, str]
    ) -> FetchKeyResult:
        raise NotImplementedError()

    @cachedList(
        cached_method_name="_get_server_keys_json", list_name="server_name_and_key_ids"
    )
    async def get_server_keys_json(
        self, server_name_and_key_ids: Iterable[tuple[str, str]]
    ) -> Mapping[tuple[str, str], FetchKeyResult]:
        """
        Args:
            server_name_and_key_ids:
                iterable of (server_name, key-id) tuples to fetch keys for

        Returns:
            A map from (server_name, key_id) -> FetchKeyResult, or None if the
            key is unknown
        """
        keys = {}

        def _get_keys(txn: Cursor, batch: tuple[tuple[str, str], ...]) -> None:
            """Processes a batch of keys to fetch, and adds the result to `keys`."""

            # batch_iter always returns tuples so it's safe to do len(batch)
            where_clause = " OR (server_name=? AND key_id=?)" * len(batch)

            # `server_keys_json` can have multiple entries per server (one per
            # remote server we fetched from, if using perspectives). Order by
            # `ts_added_ms` so the most recently fetched one always wins.
            sql = f"""
                SELECT server_name, key_id, key_json, ts_valid_until_ms
                FROM server_keys_json WHERE 1=0
                {where_clause}
                ORDER BY ts_added_ms
            """

            txn.execute(sql, tuple(itertools.chain.from_iterable(batch)))

            for server_name, key_id, key_json_bytes, ts_valid_until_ms in txn:
                if ts_valid_until_ms is None:
                    # Old keys may be stored with a ts_valid_until_ms of null,
                    # in which case we treat this as if it was set to `0`, i.e.
                    # it won't match key requests that define a minimum
                    # `ts_valid_until_ms`.
                    ts_valid_until_ms = 0

                # The entire signed JSON response is stored in server_keys_json,
                # fetch out the bits needed. The key may live under either
                # `verify_keys` (current) or `old_verify_keys` (expired, per
                # MSC4499 historical event verification) -- check both.
                key_json = json.loads(bytes(key_json_bytes))
                key_entry = key_json["verify_keys"].get(key_id)
                if key_entry is None:
                    key_entry = key_json.get("old_verify_keys", {}).get(key_id)
                if key_entry is None:
                    # Not present in either section of the stored response
                    # (e.g. server_signature_keys and server_keys_json
                    # disagree). Skip rather than raise.
                    continue
                key_base64 = key_entry["key"]

                keys[(server_name, key_id)] = FetchKeyResult(
                    verify_key=decode_verify_key_bytes(
                        key_id, decode_base64(key_base64)
                    ),
                    valid_until_ts=ts_valid_until_ms,
                )

        def _txn(txn: Cursor) -> dict[tuple[str, str], FetchKeyResult]:
            for batch in batch_iter(server_name_and_key_ids, 50):
                _get_keys(txn, batch)
            return keys

        return await self.db_pool.runInteraction("get_server_keys_json", _txn)

    @cached()
    def get_server_key_json_for_remote(
        self,
        server_name: str,
        key_id: str,
    ) -> FetchKeyResultForRemote | None:
        raise NotImplementedError()

    @cachedList(
        cached_method_name="get_server_key_json_for_remote", list_name="key_ids"
    )
    async def get_server_keys_json_for_remote(
        self, server_name: str, key_ids: Iterable[str]
    ) -> Mapping[str, FetchKeyResultForRemote | None]:
        """Fetch the cached keys for the given server/key IDs.

        If we have multiple entries for a given key ID, returns the most recent.
        """
        rows = cast(
            list[tuple[str, str, int, int, bytes | memoryview]],
            await self.db_pool.simple_select_many_batch(
                table="server_keys_json",
                column="key_id",
                iterable=key_ids,
                keyvalues={"server_name": server_name},
                retcols=(
                    "key_id",
                    "from_server",
                    "ts_added_ms",
                    "ts_valid_until_ms",
                    "key_json",
                ),
                desc="get_server_keys_json_for_remote",
            ),
        )

        if not rows:
            return {}

        # We sort the rows by ts_added_ms so that the most recently added entry
        # will stomp over older entries in the dictionary.
        rows.sort(key=lambda r: r[2])

        return {
            key_id: FetchKeyResultForRemote(
                # Cast to bytes since postgresql returns a memoryview.
                key_json=bytes(key_json),
                valid_until_ts=ts_valid_until_ms,
                added_ts=ts_added_ms,
            )
            for key_id, from_server, ts_added_ms, ts_valid_until_ms, key_json in rows
        }

    async def get_all_server_keys_json_for_remote(
        self,
        server_name: str,
    ) -> dict[str, FetchKeyResultForRemote]:
        """Fetch the cached keys for the given server.

        If we have multiple entries for a given key ID, returns the most recent.
        """
        rows = cast(
            list[tuple[str, str, int, int, bytes | memoryview]],
            await self.db_pool.simple_select_list(
                table="server_keys_json",
                keyvalues={"server_name": server_name},
                retcols=(
                    "key_id",
                    "from_server",
                    "ts_added_ms",
                    "ts_valid_until_ms",
                    "key_json",
                ),
                desc="get_server_keys_json_for_remote",
            ),
        )

        if not rows:
            return {}

        # We sort the rows by ts_added_ms so that the most recently added entry
        # will stomp over older entries in the dictionary.
        rows.sort(key=lambda r: r[2])

        return {
            key_id: FetchKeyResultForRemote(
                # Cast to bytes since postgresql returns a memoryview.
                key_json=bytes(key_json),
                valid_until_ts=ts_valid_until_ms,
                added_ts=ts_added_ms,
            )
            for key_id, from_server, ts_added_ms, ts_valid_until_ms, key_json in rows
        }
