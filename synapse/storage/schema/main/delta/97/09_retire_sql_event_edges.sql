-- Event adjacency now lives in mtxdb's CSR store. Remove the obsolete backend
-- marker and consume the SQL index update queued by schema delta 71 before the
-- background updater starts looking for a handler.
DELETE FROM background_updates
WHERE update_name = 'event_edges_replace_index';

DROP TABLE IF EXISTS edge_backend;
