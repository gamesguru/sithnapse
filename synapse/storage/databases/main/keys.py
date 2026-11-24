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
import time
from typing import TYPE_CHECKING, Iterable, Mapping, cast

from canonicaljson import encode_canonical_json
from signedjson.key import decode_verify_key_bytes
from unpaddedbase64 import decode_base64

from synapse.storage.database import (
    DatabasePool,
    LoggingDatabaseConnection,
    LoggingTransaction,
)
from synapse.storage.databases.main.cache import CacheInvalidationWorkerStore
from synapse.storage.keys import (
    FetchKeyResult,
    FetchKeyResultForRemote,
    merge_fetch_key_results,
)
from synapse.storage.types import Cursor
from synapse.types import JsonDict
from synapse.util.caches.descriptors import cached, cachedList
from synapse.util.iterutils import batch_iter

if TYPE_CHECKING:
    from synapse.server import HomeServer

logger = logging.getLogger(__name__)


db_binary_type = memoryview

# MSC4499: keep a bounded number of retired keys per server. Evict keys first
# learned as already retired before those we previously saw active (whose
# signatures may still need verifying), then evict by earliest expiry within
# each tier.
MAX_RETIRED_SERVER_KEYS = 3000

# Bound the number of distinct public-key bodies accepted for one key ID. A
# server can keep reusing an ID, so retaining every collision forever would
# let a signed key endpoint grow this table without limit.
MAX_KEY_BODIES_PER_ID = 8

# MSC4499 §11 retention ceilings, independent of the retired-key ceiling.
# Eviction is deterministic: oldest first-seen observation, then key ID, then
# body digest. Reaching either ceiling is itself the anomaly signal.
MAX_KEY_BODIES_PER_SERVER = 24_000
MAX_KEY_BODIES = 200_000

# A `verify_keys` body is retained only if its document was still unexpired at
# reception, allowing for clock skew. A replayed expired document is not
# admitted: its asserted window cannot be distinguished from a fresh one.
MAX_KEY_RETENTION_SKEW_MS = 5 * 60 * 1000


class KeyBackgroundUpdateStore(CacheInvalidationWorkerStore):
    """Background updates for the key stores.

    This is a separate class from `KeyStore` so that tools which build a
    reduced store (notably `synapse_port_db`) can register the handlers for
    the updates queued by schema deltas without pulling in the whole key
    store.
    """

    def __init__(
        self,
        database: DatabasePool,
        db_conn: LoggingDatabaseConnection,
        hs: "HomeServer",
    ):
        super().__init__(database, db_conn, hs)

        # `server_keys_json.response_id` is an existing column, so the index
        # cannot be created synchronously by the delta (see
        # `scripts-dev/check_schema_delta.py`). The delta only queues this
        # update; build the partial index here.
        self.db_pool.updates.register_background_index_update(
            update_name="server_keys_json_response_id_index",
            index_name="server_keys_json_response_id",
            table="server_keys_json",
            columns=["response_id"],
            where_clause="response_id IS NOT NULL",
        )


class KeyStore(KeyBackgroundUpdateStore):
    """Persistence for signature verification keys"""

    async def store_server_keys_response(
        self,
        server_name: str,
        from_server: str,
        ts_added_ms: int,
        verify_keys: dict[str, FetchKeyResult],
        response_json: JsonDict,
    ) -> dict[str, FetchKeyResult]:
        """Stores the keys for the given server that we got from `from_server`.

        The legacy signature-key row keeps the MSC4499 preferred binding, while
        `server_signature_key_candidates` retains a bounded set of distinct,
        validated key bodies for historical event signature checks.

        Retain at most 3,000 retired keys per server. Evict keys never seen
        active before those that were, then unknown or earliest expiry first.
        Retired means expiry is absent or at most ts_added_ms. Eviction removes
        stored key responses and invalidates caches. Encoding and storage errors
        propagate.

        Args:
            server_name: The owner of the keys
            from_server: Which server we got the keys from
            ts_added_ms: When we're adding the keys, in milliseconds since
                the Unix epoch
            verify_keys: The decoded keys
            response_json: The full *signed* response JSON that contains the keys.

        Returns:
            Map of supplied key IDs to all retained candidate bodies and their
            validity windows. The map can include retired keys evicted by this
            call.
        """

        key_json_bytes = encode_canonical_json(response_json)
        interaction_started = time.perf_counter()

        def store_server_keys_response_txn(
            txn: LoggingTransaction,
        ) -> dict[str, FetchKeyResult]:
            """Resolve key bindings, store signed responses, and evict excess retired keys.

            Schedule cache invalidations and return results for the supplied key IDs,
            including any retired keys evicted in this transaction.
            """
            txn_started = time.perf_counter()
            final_keys: dict[str, FetchKeyResult] = dict(verify_keys)
            keys_to_persist: dict[str, FetchKeyResult] = dict(verify_keys)
            per_key_started = time.perf_counter()

            # Preserve every distinct body we have observed, even when the
            # strict binding table below keeps another body as its preferred
            # value. The response itself was validated by BaseV2KeyFetcher.
            old_key_ids = set(response_json.get("old_verify_keys", {}))
            active_key_ids = set(response_json.get("verify_keys", {}))
            document_valid_until = response_json.get("valid_until_ts")
            retained_any = False
            for key_id, fetch_result in verify_keys.items():
                # MSC4499 §11 admission: a retired body is published evidence;
                # an active body is admitted only while its document is still
                # unexpired, so a replayed expired payload cannot be retained.
                if key_id not in old_key_ids:
                    if key_id not in active_key_ids:
                        continue
                    if (
                        not isinstance(document_valid_until, int)
                        or document_valid_until
                        < ts_added_ms - MAX_KEY_RETENTION_SKEW_MS
                    ):
                        continue
                retained_any = True
                key_bytes = fetch_result.verify_key.encode()
                key_hash = hashlib.sha256(key_bytes).hexdigest()
                active_now = fetch_result.valid_until_ts > ts_added_ms
                txn.execute(
                    """
                    INSERT INTO server_signature_key_candidates
                        (server_name, key_id, key_hash, from_server, ts_added_ms,
                         verify_key, ts_valid_until_ms, seen_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (server_name, key_id, key_hash) DO UPDATE SET
                        ts_added_ms = CASE
                            WHEN server_signature_key_candidates.ts_added_ms
                                > EXCLUDED.ts_added_ms
                            THEN server_signature_key_candidates.ts_added_ms
                            ELSE EXCLUDED.ts_added_ms
                        END,
                        ts_valid_until_ms = CASE
                            WHEN server_signature_key_candidates.ts_valid_until_ms
                                > EXCLUDED.ts_valid_until_ms
                            THEN server_signature_key_candidates.ts_valid_until_ms
                            ELSE EXCLUDED.ts_valid_until_ms
                        END,
                        seen_active = (
                            server_signature_key_candidates.seen_active
                            OR EXCLUDED.seen_active
                        )
                    """,
                    (
                        server_name,
                        key_id,
                        key_hash,
                        from_server,
                        ts_added_ms,
                        db_binary_type(key_bytes),
                        fetch_result.valid_until_ts,
                        active_now,
                    ),
                )

                txn.execute(
                    """
                    SELECT key_hash FROM server_signature_key_candidates
                    WHERE server_name = ? AND key_id = ?
                    ORDER BY seen_active DESC, ts_added_ms DESC, key_hash ASC
                    """,
                    (server_name, key_id),
                )
                excess_candidates = [
                    row[0] for row in txn.fetchall()[MAX_KEY_BODIES_PER_ID:]
                ]
                if excess_candidates:
                    placeholders = ", ".join("?" for _ in excess_candidates)
                    txn.execute(
                        f"DELETE FROM server_signature_key_candidates "
                        f"WHERE server_name = ? AND key_id = ? "
                        f"AND key_hash IN ({placeholders})",
                        (server_name, key_id, *excess_candidates),
                    )

            if retained_any:
                self._prune_key_candidates_txn(txn, server_name)

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
                # Active (not yet retired) when fetched; see the eviction below.
                active_now = fetch_result.valid_until_ts > ts_added_ms

                # Step 1: INSERT ... ON CONFLICT DO NOTHING.
                txn.execute(
                    """
                    INSERT INTO server_signature_keys
                        (server_name, key_id, from_server, ts_added_ms,
                         verify_key, ts_valid_until_ms, seen_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (server_name, key_id) DO NOTHING
                    """,
                    (
                        server_name,
                        key_id,
                        from_server,
                        ts_added_ms,
                        db_binary_type(fetch_result.verify_key.encode()),
                        fetch_result.valid_until_ts,
                        active_now,
                    ),
                )

                # Step 2: Re-read the authoritative row.
                txn.execute(
                    """
                    SELECT verify_key, ts_valid_until_ms, from_server, seen_active
                    FROM server_signature_keys
                    WHERE server_name = ? AND key_id = ?
                    """,
                    (server_name, key_id),
                )
                row = cast(
                    tuple[bytes | memoryview, int | None, str, bool], txn.fetchone()
                )
                assert row is not None
                stored_key_raw, stored_ts, stored_from_server, stored_seen_active = row
                seen_active = bool(stored_seen_active) or active_now
                stored_key_bytes = bytes(stored_key_raw)

                # Step 3: Compare stored key body against candidate.
                if stored_key_bytes == fetch_result.verify_key.encode():
                    # Same key body (refresh) -- preserve original from_server.
                    stored_ts = stored_ts if stored_ts is not None else 0
                    if stored_ts < fetch_result.valid_until_ts:
                        txn.execute(
                            """
                            UPDATE server_signature_keys
                            SET ts_valid_until_ms = ?, ts_added_ms = ?, seen_active = ?
                            WHERE server_name = ? AND key_id = ?
                            """,
                            (
                                fetch_result.valid_until_ts,
                                ts_added_ms,
                                seen_active,
                                server_name,
                                key_id,
                            ),
                        )
                    else:
                        # The candidate does not extend the stored window. It
                        # must never shorten it, even when this response also
                        # upgrades a legacy `seen_active` flag.
                        if seen_active and not stored_seen_active:
                            txn.execute(
                                """
                                UPDATE server_signature_keys
                                SET seen_active = ?
                                WHERE server_name = ? AND key_id = ?
                                """,
                                (seen_active, server_name, key_id),
                            )
                        if stored_ts > fetch_result.valid_until_ts:
                            # A stale or short notary view. Return the longer
                            # stored window, and do not overwrite the cached
                            # response blob for this key with the shorter one.
                            final_keys[key_id] = FetchKeyResult(
                                verify_key=fetch_result.verify_key,
                                valid_until_ts=stored_ts,
                            )
                            keys_to_persist.pop(key_id, None)
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
                            ts_valid_until_ms = ?, verify_key = ?,
                            seen_active = ?
                        WHERE server_name = ? AND key_id = ?
                        """,
                        (
                            from_server,
                            ts_added_ms,
                            fetch_result.valid_until_ts,
                            db_binary_type(fetch_result.verify_key.encode()),
                            seen_active,
                            server_name,
                            key_id,
                        ),
                    )
                else:
                    # Preserve the legacy preferred binding. The candidate
                    # body has already been retained separately above for
                    # verification of events signed before this collision.
                    logger.warning(
                        "MSC4499: key ID collision for %s %s -- retaining "
                        "original preferred body and caching alternate body. "
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

            # Return all retained bodies to the caller so the signature checker
            # can try them against historical events signed with a reused ID.
            for key_id in verify_keys:
                txn.execute(
                    """
                    SELECT verify_key, ts_valid_until_ms
                    FROM server_signature_key_candidates
                    WHERE server_name = ? AND key_id = ?
                    ORDER BY ts_added_ms, key_hash
                    """,
                    (server_name, key_id),
                )
                candidates = [
                    FetchKeyResult(
                        verify_key=decode_verify_key_bytes(key_id, bytes(row[0])),
                        valid_until_ts=row[1],
                    )
                    for row in txn.fetchall()
                ]
                txn.execute(
                    """
                    SELECT verify_key, ts_valid_until_ms
                    FROM server_signature_keys
                    WHERE server_name = ? AND key_id = ?
                    """,
                    (server_name, key_id),
                )
                legacy_row = txn.fetchone()
                if legacy_row is not None:
                    # The First Seen Wins binding stays the primary body; the
                    # candidate rows are alternates for historical signatures.
                    authoritative = FetchKeyResult(
                        verify_key=decode_verify_key_bytes(
                            key_id, bytes(legacy_row[0])
                        ),
                        valid_until_ts=legacy_row[1] or 0,
                    )
                    final_keys[key_id] = merge_fetch_key_results(
                        authoritative, *candidates
                    )
                elif candidates:
                    final_keys[key_id] = merge_fetch_key_results(
                        candidates[0], *candidates[1:]
                    )

            per_key_seconds = time.perf_counter() - per_key_started
            post_key_started = time.perf_counter()
            phase_started = post_key_started
            phases: dict[str, float] = {}

            def mark(name: str) -> None:
                nonlocal phase_started
                now = time.perf_counter()
                phases[name] = now - phase_started
                phase_started = now

            if keys_to_persist:
                # The signed response covers every key in this payload, so
                # storing it once per key would write the same blob thousands
                # of times for a rotation that publishes many old keys.
                # Content-address it instead: the blob table holds one copy,
                # and each per-key row points at it.
                response_id = hashlib.sha256(key_json_bytes).hexdigest()
                txn.execute(
                    """
                    INSERT INTO server_key_response_blobs (response_id, key_json)
                    VALUES (?, ?)
                    ON CONFLICT (response_id) DO UPDATE
                    SET key_json = EXCLUDED.key_json
                    """,
                    (response_id, db_binary_type(key_json_bytes)),
                )

                mark("blob_insert")

                # The upsert below repoints these rows at the new blob, which
                # can leave the blobs they used to reference unreferenced.
                persist_key_ids = list(keys_to_persist)
                key_placeholders = ", ".join("?" for _ in persist_key_ids)
                txn.execute(
                    f"""
                    SELECT DISTINCT response_id FROM server_keys_json
                    WHERE server_name = ? AND key_id IN ({key_placeholders})
                        AND response_id IS NOT NULL
                    """,
                    (server_name, *persist_key_ids),
                )
                replaced_response_ids = {row[0] for row in txn} - {response_id}

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
                        "response_id",
                    ),
                    value_values=[
                        (
                            ts_added_ms,
                            fetch_result.valid_until_ts,
                            db_binary_type(b""),
                            response_id,
                        )
                        for fetch_result in keys_to_persist.values()
                    ],
                )

                mark("keys_json_upsert")
                for old_response_id in replaced_response_ids:
                    txn.execute(
                        """
                        DELETE FROM server_key_response_blobs
                        WHERE response_id = ?
                            AND NOT EXISTS (
                                SELECT 1 FROM server_keys_json
                                WHERE response_id = ?
                            )
                        """,
                        (old_response_id, old_response_id),
                    )
                for key_id in verify_keys:
                    txn.call_after(
                        self._get_server_keys_json.invalidate,
                        ((server_name, key_id),),
                    )
                self._send_invalidation_to_replication_bulk(
                    txn,
                    self._get_server_keys_json.__name__,
                    [(json.dumps([server_name, key_id]),) for key_id in verify_keys],
                )
                self._invalidate_cache_and_stream_bulk(
                    txn,
                    self.get_server_key_json_for_remote,
                    [(server_name, key_id) for key_id in keys_to_persist],
                )
                mark("persist_invalidation")
            elif verify_keys:
                for key_id in verify_keys:
                    txn.call_after(
                        self._get_server_keys_json.invalidate,
                        ((server_name, key_id),),
                    )
                self._send_invalidation_to_replication_bulk(
                    txn,
                    self._get_server_keys_json.__name__,
                    [(json.dumps([server_name, key_id]),) for key_id in verify_keys],
                )

            # MSC4499: bound the number of retired keys retained for a server.
            # Keys we saw active before retirement may have signed events we
            # still need to verify, so evict keys first observed as already
            # retired before them, and within each tier the earliest expiring.
            txn.execute(
                """
                SELECT key_id FROM server_signature_keys
                WHERE server_name = ?
                    AND (ts_valid_until_ms IS NULL OR ts_valid_until_ms <= ?)
                ORDER BY seen_active ASC, ts_valid_until_ms ASC NULLS FIRST
                """,
                (server_name, ts_added_ms),
            )
            retired_key_ids = [row[0] for row in txn]
            mark("retired_select")
            excess = len(retired_key_ids) - MAX_RETIRED_SERVER_KEYS
            if excess > 0:
                evicted_key_ids = retired_key_ids[:excess]
                placeholders = ", ".join("?" for _ in evicted_key_ids)

                # Remember which response blobs the evicted rows referenced, so
                # we can drop any that no other key still uses.
                txn.execute(
                    f"""
                    SELECT DISTINCT response_id FROM server_keys_json
                    WHERE server_name = ? AND key_id IN ({placeholders})
                        AND response_id IS NOT NULL
                    """,
                    (server_name, *evicted_key_ids),
                )
                evicted_response_ids = [row[0] for row in txn]

                txn.execute(
                    f"DELETE FROM server_signature_keys WHERE server_name = ? "
                    f"AND key_id IN ({placeholders})",
                    (server_name, *evicted_key_ids),
                )
                txn.execute(
                    f"DELETE FROM server_signature_key_candidates "
                    f"WHERE server_name = ? AND key_id IN ({placeholders})",
                    (server_name, *evicted_key_ids),
                )
                txn.execute(
                    f"DELETE FROM server_keys_json WHERE server_name = ? "
                    f"AND key_id IN ({placeholders})",
                    (server_name, *evicted_key_ids),
                )
                for response_id in evicted_response_ids:
                    txn.execute(
                        """
                        DELETE FROM server_key_response_blobs
                        WHERE response_id = ?
                            AND NOT EXISTS (
                                SELECT 1 FROM server_keys_json
                                WHERE response_id = ?
                            )
                        """,
                        (response_id, response_id),
                    )
                for key_id in evicted_key_ids:
                    txn.call_after(
                        self._get_server_keys_json.invalidate,
                        ((server_name, key_id),),
                    )
                self._send_invalidation_to_replication_bulk(
                    txn,
                    self._get_server_keys_json.__name__,
                    [
                        (json.dumps([server_name, key_id]),)
                        for key_id in evicted_key_ids
                    ],
                )

                self._invalidate_cache_and_stream_bulk(
                    txn,
                    self.get_server_key_json_for_remote,
                    [(server_name, key_id) for key_id in evicted_key_ids],
                )

            mark("eviction")
            post_key_seconds = time.perf_counter() - post_key_started
            txn_seconds = time.perf_counter() - txn_started
            if txn_seconds >= 0.5:
                logger.info(
                    "Slow server-key response transaction for %s from %s: "
                    "input_keys=%d persisted_keys=%d retired_keys=%d "
                    "evicted_keys=%d per_key_sql=%.3fs post_key_sql=%.3fs "
                    "txn=%.3fs phases=%s",
                    server_name,
                    from_server,
                    len(verify_keys),
                    len(keys_to_persist),
                    len(retired_key_ids),
                    excess if excess > 0 else 0,
                    per_key_seconds,
                    post_key_seconds,
                    txn_seconds,
                    {k: round(v, 3) for k, v in phases.items()},
                )

            return final_keys

        result = await self.db_pool.runInteraction(
            "store_server_keys_response", store_server_keys_response_txn
        )
        interaction_seconds = time.perf_counter() - interaction_started
        if interaction_seconds >= 0.5:
            logger.info(
                "Slow server-key response store await for %s from %s: "
                "input_keys=%d interaction=%.3fs",
                server_name,
                from_server,
                len(verify_keys),
                interaction_seconds,
            )
        return result

    def _prune_key_candidates_txn(
        self, txn: LoggingTransaction, server_name: str
    ) -> None:
        """Evict retained key bodies beyond the per-origin/global ceilings.

        The order is deterministic: oldest first-seen observation, then key ID,
        then body digest. These ceilings are independent of the retired-key
        limit; reaching one is itself the anomaly signal.
        """

        def _evict(count: int, where: str, args: tuple[object, ...]) -> None:
            txn.execute(
                "SELECT server_name, key_id, key_hash "
                "FROM server_signature_key_candidates "
                f"{where} ORDER BY ts_added_ms, key_id, key_hash LIMIT ?",
                (*args, count),
            )
            for row_server, key_id, key_hash in txn.fetchall():
                txn.execute(
                    "DELETE FROM server_signature_key_candidates "
                    "WHERE server_name = ? AND key_id = ? AND key_hash = ?",
                    (row_server, key_id, key_hash),
                )

        txn.execute(
            "SELECT COUNT(*) FROM server_signature_key_candidates "
            "WHERE server_name = ?",
            (server_name,),
        )
        server_count_row = txn.fetchone()
        assert server_count_row is not None
        (server_count,) = server_count_row
        excess = server_count - MAX_KEY_BODIES_PER_SERVER
        if excess > 0:
            _evict(excess, "WHERE server_name = ?", (server_name,))
            logger.warning(
                "MSC4499: evicting %d retained key bodies for %s beyond the "
                "per-origin ceiling of %d",
                excess,
                server_name,
                MAX_KEY_BODIES_PER_SERVER,
            )

        txn.execute("SELECT COUNT(*) FROM server_signature_key_candidates")
        total_count_row = txn.fetchone()
        assert total_count_row is not None
        (total_count,) = total_count_row
        excess = total_count - MAX_KEY_BODIES
        if excess > 0:
            _evict(excess, "", ())
            logger.warning(
                "MSC4499: evicting %d retained key bodies beyond the global "
                "ceiling of %d",
                excess,
                MAX_KEY_BODIES,
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
            where_clause = " OR (r.server_name=? AND r.key_id=?)" * len(batch)

            # `server_keys_json` can have multiple entries per server (one per
            # remote server we fetched from, if using perspectives). Order by
            # `ts_added_ms` so the most recently fetched one always wins.
            #
            # Newer rows point at a shared, content-addressed response blob;
            # older rows still carry the full response inline, so fall back to
            # that when there is no blob reference.
            sql = f"""
                SELECT r.server_name, r.key_id,
                    COALESCE(b.key_json, r.key_json), r.ts_valid_until_ms
                FROM server_keys_json r
                LEFT JOIN server_key_response_blobs b
                    ON b.response_id = r.response_id
                WHERE 1=0
                {where_clause}
                ORDER BY r.ts_added_ms
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
                if not key_json_bytes:
                    # Missing response blob (e.g. mid-upgrade); nothing to use.
                    continue
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

            candidate_where = " OR (server_name=? AND key_id=?)" * len(batch)
            params = tuple(itertools.chain.from_iterable(batch))
            txn.execute(
                f"""
                SELECT server_name, key_id, verify_key, ts_valid_until_ms
                FROM server_signature_key_candidates
                WHERE 1=0 {candidate_where}
                """,
                params,
            )
            for server_name, key_id, key_bytes, valid_until_ts in txn:
                candidate = FetchKeyResult(
                    verify_key=decode_verify_key_bytes(key_id, bytes(key_bytes)),
                    valid_until_ts=valid_until_ts,
                )
                cache_key = (server_name, key_id)
                existing = keys.get(cache_key)
                keys[cache_key] = (
                    merge_fetch_key_results(existing, candidate)
                    if existing is not None
                    else candidate
                )

            txn.execute(
                f"""
                SELECT server_name, key_id, verify_key, ts_valid_until_ms
                FROM server_signature_keys
                WHERE 1=0 {candidate_where}
                """,
                params,
            )
            for server_name, key_id, key_bytes, valid_until_ts in txn:
                candidate = FetchKeyResult(
                    verify_key=decode_verify_key_bytes(key_id, bytes(key_bytes)),
                    valid_until_ts=valid_until_ts or 0,
                )
                cache_key = (server_name, key_id)
                existing = keys.get(cache_key)
                # `server_signature_keys` holds the First Seen Wins binding, so
                # it must stay the primary body even when a stored response
                # blob or a retained candidate carries a colliding one.
                keys[cache_key] = (
                    merge_fetch_key_results(candidate, existing)
                    if existing is not None
                    else candidate
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

    def _select_server_keys_json_for_remote_txn(
        self,
        txn: LoggingTransaction,
        server_name: str,
        key_ids: Iterable[str] | None,
    ) -> list[tuple[str, str, int, int, bytes]]:
        """Read remote-facing key rows, resolving the shared response blob.

        `key_ids=None` means "all keys for this server". Rows written before
        the blob table existed have no `response_id`, so the inline `key_json`
        is used as a fallback.
        """
        # Newer rows point at a shared, content-addressed response blob.
        select = """
            SELECT r.key_id, r.from_server, r.ts_added_ms,
                r.ts_valid_until_ms, COALESCE(b.key_json, r.key_json)
            FROM server_keys_json r
            LEFT JOIN server_key_response_blobs b
                ON b.response_id = r.response_id
        """

        rows: list[tuple[str, str, int, int, bytes]] = []
        if key_ids is None:
            txn.execute(f"{select} WHERE r.server_name = ?", (server_name,))
            result_rows = txn.fetchall()
            rows.extend(
                (
                    key_id,
                    from_server,
                    ts_added_ms,
                    ts_valid_until_ms,
                    # Cast to bytes since postgresql returns a memoryview.
                    bytes(key_json),
                )
                for key_id, from_server, ts_added_ms, ts_valid_until_ms, key_json in result_rows
            )
            return rows

        for batch in batch_iter(key_ids, 50):
            placeholders = ", ".join("?" for _ in batch)
            txn.execute(
                f"{select} WHERE r.server_name = ? AND r.key_id IN ({placeholders})",
                (server_name, *batch),
            )
            for (
                key_id,
                from_server,
                ts_added_ms,
                ts_valid_until_ms,
                key_json,
            ) in txn.fetchall():
                rows.append(
                    (
                        key_id,
                        from_server,
                        ts_added_ms,
                        ts_valid_until_ms,
                        bytes(key_json),
                    )
                )

        return rows

    @cachedList(
        cached_method_name="get_server_key_json_for_remote", list_name="key_ids"
    )
    async def get_server_keys_json_for_remote(
        self, server_name: str, key_ids: Iterable[str]
    ) -> Mapping[str, FetchKeyResultForRemote | None]:
        """Fetch the cached keys for the given server/key IDs.

        If we have multiple entries for a given key ID, returns the most recent.
        """
        rows = await self.db_pool.runInteraction(
            "get_server_keys_json_for_remote",
            self._select_server_keys_json_for_remote_txn,
            server_name,
            key_ids,
        )

        if not rows:
            return {}

        # We sort the rows by ts_added_ms so that the most recently added entry
        # will stomp over older entries in the dictionary.
        rows.sort(key=lambda r: r[2])

        return {
            key_id: FetchKeyResultForRemote(
                key_json=key_json,
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
        rows = await self.db_pool.runInteraction(
            "get_all_server_keys_json_for_remote",
            self._select_server_keys_json_for_remote_txn,
            server_name,
            None,
        )

        if not rows:
            return {}

        # We sort the rows by ts_added_ms so that the most recently added entry
        # will stomp over older entries in the dictionary.
        rows.sort(key=lambda r: r[2])

        return {
            key_id: FetchKeyResultForRemote(
                key_json=key_json,
                valid_until_ts=ts_valid_until_ms,
                added_ts=ts_added_ms,
            )
            for key_id, from_server, ts_added_ms, ts_valid_until_ms, key_json in rows
        }
