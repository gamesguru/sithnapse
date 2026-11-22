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

-- Finding whether a response blob is still referenced (on eviction and when a
-- refresh repoints a key) would otherwise scan `server_keys_json`.
CREATE INDEX IF NOT EXISTS server_keys_json_response_id
    ON server_keys_json (response_id)
    WHERE response_id IS NOT NULL;
