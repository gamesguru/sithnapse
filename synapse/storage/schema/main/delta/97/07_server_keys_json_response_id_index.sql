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
--
-- `server_keys_json` is an existing table, so build the partial index in the
-- background rather than in this delta. The handler is registered by
-- `KeyStore.__init__`.
INSERT INTO background_updates (ordering, update_name, progress_json) VALUES
    (9707, 'server_keys_json_response_id_index', '{}');
