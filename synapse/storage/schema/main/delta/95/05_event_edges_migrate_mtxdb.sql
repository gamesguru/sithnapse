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

-- Mirror pre-existing `event_edges` rows into the embedded mtxdb engine, for
-- servers that enabled it after already running (or before this backfill
-- existed). A no-op when the embedded edges engine isn't enabled/writable on
-- this process -- see `_background_migrate_event_edges_mtxdb`.
INSERT INTO background_updates (ordering, update_name, progress_json) VALUES
  (9505, 'event_edges_migrate_mtxdb', '{}');
