-- Records which backend (`sql` or `mtxdb`) holds the authoritative event edges.
-- Existing servers keep their edges in SQL. Switching backends is a manual
-- operator migration that rewrites this row; startup refuses to run when the
-- configured backend differs from it.
CREATE TABLE IF NOT EXISTS edge_backend (
    lock CHAR(1) NOT NULL DEFAULT 'X' UNIQUE CHECK (lock = 'X'),
    backend TEXT NOT NULL
);
INSERT INTO edge_backend (lock, backend) VALUES ('X', 'sql') ON CONFLICT (lock) DO NOTHING;
