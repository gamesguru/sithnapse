-- HAMT root deletion happens after the SQL room purge commits. Keep the
-- state-group IDs until that cleanup succeeds, so failures can be retried
-- instead of leaving directly-readable stale roots indefinitely.
CREATE TABLE state_hamt_root_deletion_queue (
    state_group BIGINT NOT NULL PRIMARY KEY
);
