-- Whether we ever saw this key bound as active (not yet retired). Retired-key
-- eviction drops keys that were never seen active before ones that were.
ALTER TABLE server_signature_keys ADD COLUMN seen_active BOOLEAN NOT NULL DEFAULT FALSE;
