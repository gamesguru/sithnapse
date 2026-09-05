--
-- This file is licensed under the Affero General Public License (AGPL) version 3.
--
-- Copyright (C) 2026 Element Creations Ltd
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or (at your
-- option) any later version.
--
-- See the GNU Affero General Public License for more details:
-- <https://www.gnu.org/licenses/agpl-3.0.html>.

-- State groups that existed before the HAMT tables (01_state_hamt.sql, this
-- same schema version) were added have no `state_hamt_roots` row. Give each
-- of them one, reconstructed from the legacy `state_groups_state`/
-- `state_group_edges` tables -- see `_background_backfill_state_hamt_roots`.
--
-- Ordering is deliberately low (rather than the usual "schema-version * 100"
-- convention) so this always runs before older, lower-numbered background
-- updates that read state via `_get_state_groups_from_groups_txn` -- e.g.
-- `sliding_sync_membership_snapshots_bg_update` at ordering 8701 -- which
-- would otherwise hit "state group exists in SQL but has no HAMT root" on
-- any database that predates schema v95.
INSERT INTO background_updates (ordering, update_name, progress_json) VALUES
  (100, 'state_hamt_backfill_roots', '{}');
