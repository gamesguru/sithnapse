-- Whether we ever saw this key bound as active (not yet retired). Retired-key
-- eviction drops keys that were never seen active before ones that were.
ALTER TABLE server_signature_keys ADD COLUMN seen_active BOOLEAN NOT NULL DEFAULT FALSE;

-- Existing keys that were valid beyond their added timestamp were active.
-- A NULL validity timestamp is not later than ts_added_ms.
UPDATE server_signature_keys
SET seen_active = COALESCE(ts_valid_until_ms, 0) > ts_added_ms;
