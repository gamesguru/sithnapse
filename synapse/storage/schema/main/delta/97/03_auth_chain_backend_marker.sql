-- Records which backend (`sql` or `mtxdb`) holds authoritative auth edges.
-- Existing databases remain SQL-authoritative until the offline migration
-- verifies the mtxdb copy and flips this marker.
CREATE TABLE IF NOT EXISTS auth_chain_backend (
    lock CHAR(1) NOT NULL DEFAULT 'X' UNIQUE CHECK (lock = 'X'),
    backend TEXT NOT NULL
);
INSERT INTO auth_chain_backend (lock, backend) VALUES ('X', 'sql')
ON CONFLICT (lock) DO NOTHING;
