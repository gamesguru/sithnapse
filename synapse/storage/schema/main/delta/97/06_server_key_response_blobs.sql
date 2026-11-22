--
-- This file is licensed under the Affero General Public License (AGPL) version 3.
--
-- Copyright (C) 2026 Element Creations Ltd
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as
-- published by the Free Software Foundation, either version 3 of the
-- License, or (at your option) any later version.
--
-- See the GNU Affero General Public License for more details:
-- <https://www.gnu.org/licenses/agpl-3.0.html>.

-- Store each signed server-key response once, content-addressed by its
-- canonical JSON hash. `server_keys_json` previously held a full copy of the
-- response per key, so an origin rotation that publishes thousands of
-- `old_verify_keys` wrote the same blob thousands of times.
CREATE TABLE IF NOT EXISTS server_key_response_blobs (
    response_id TEXT NOT NULL PRIMARY KEY,
    key_json BYTEA NOT NULL
);

-- Nullable so rows written before this delta keep their inline `key_json`.
ALTER TABLE server_keys_json ADD COLUMN response_id TEXT;
