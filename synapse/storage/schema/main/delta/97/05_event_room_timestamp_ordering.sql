--
-- This file is licensed under the Affero General Public License (AGPL) version 3.
--
-- Copyright (C) 2026 Element Creations Ltd
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
--
-- See the GNU Affero General Public License for more details:
-- <https://www.gnu.org/licenses/agpl-3.0.html>.

-- Build the timeline ordering index in the background. This matches the
-- COALESCE expression used by topological pagination for legacy NULL timestamps.
INSERT INTO background_updates (ordering, update_name, progress_json) VALUES
    (9705, 'events_order_room_ts_idx', '{}');
