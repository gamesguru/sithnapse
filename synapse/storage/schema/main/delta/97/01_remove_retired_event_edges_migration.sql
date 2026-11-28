-- The event-edge backend migration is now a manual operator command. Remove
-- any queued legacy background update whose handler is no longer registered.
DELETE FROM background_updates
WHERE update_name IN (
    'event_edges_migrate_mtxdb',
    'event_to_state_group_embedded_migration',
    'event_auth_chain_links_embedded_migration',
    'state_hamt_embedded_migration'
);
