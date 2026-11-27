/* Copyright 2026 New Vector, Ltd
 *
 * Licensed under the Affero General Public License (AGPL) version 3.
 *
 * Marker for the one-shot rewrite of EventDag locators to carry status.
 *
 * A fresh database has no events at first start, so the startup check marks
 * itself complete. An upgraded database has events and stays incomplete until
 * `synapse_event_status_migrate` has rewritten and verified every locator.
 */

CREATE TABLE IF NOT EXISTS event_status_locator_backfill (
    lock TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    last_event_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (lock)
);

INSERT INTO event_status_locator_backfill (lock, completed, last_event_id)
VALUES ('X', 0, '')
ON CONFLICT (lock) DO NOTHING;
