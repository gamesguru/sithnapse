-- The event-edge backend migration is now a manual operator command. Remove
-- any queued legacy background update whose handler is no longer registered.
DELETE FROM background_updates
WHERE update_name = 'event_edges_migrate_mtxdb';
