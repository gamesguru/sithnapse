-- Records which backend (`sql` or `mtxdb`) holds the authoritative event edges.
-- Existing servers keep their edges in SQL. Switching backends is a manual
-- operator migration that rewrites this row; startup refuses to run when the
-- configured backend differs from it.
CREATE TABLE IF NOT EXISTS edge_backend (
    lock CHAR(1) NOT NULL DEFAULT 'X' UNIQUE CHECK (lock = 'X'),
    backend TEXT NOT NULL
);
INSERT INTO edge_backend (lock, backend) VALUES ('X', 'sql') ON CONFLICT (lock) DO NOTHING;

-- Track each part of a user's room-upgrade transfer so retries do not overwrite
-- tag changes made in the replacement room.
CREATE TABLE IF NOT EXISTS room_upgrade_user_transfers (
    old_room_id TEXT NOT NULL,
    new_room_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    tags_transferred BOOLEAN NOT NULL DEFAULT FALSE,
    push_rules_transferred BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (old_room_id, new_room_id, user_id)
);
