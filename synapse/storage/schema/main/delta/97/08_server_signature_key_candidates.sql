-- Keep additional authenticated bodies for a server/key ID collision. The
-- legacy table remains the preferred binding for existing readers and
-- migrations; this table holds candidate bodies for event verification.
CREATE TABLE IF NOT EXISTS server_signature_key_candidates (
    server_name TEXT NOT NULL,
    key_id TEXT NOT NULL,
    key_hash TEXT NOT NULL,
    from_server TEXT NOT NULL,
    ts_added_ms BIGINT NOT NULL,
    verify_key BYTEA NOT NULL,
    ts_valid_until_ms BIGINT NOT NULL,
    seen_active BOOLEAN NOT NULL,
    CONSTRAINT server_signature_key_candidates_unique
        UNIQUE (server_name, key_id, key_hash)
);

CREATE INDEX IF NOT EXISTS server_signature_key_candidates_expiry
    ON server_signature_key_candidates(server_name, ts_valid_until_ms);
