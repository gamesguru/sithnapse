-- The state HAMT backend migration no longer exists. Remove any queued legacy
-- background update whose handler is no longer registered.
DELETE FROM background_updates
WHERE update_name = 'state_hamt_embedded_migration';
