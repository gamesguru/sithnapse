/* Copyright 2026 New Vector, Ltd
 *
 * Licensed under the Affero General Public License (AGPL) version 3.
 *
 * Marker for the one-shot SQL -> mtxdb event-verdict backfill.
 *
 * A fresh database has no events at first start, so the startup check marks
 * itself complete. An upgraded database has events and stays incomplete until
 * `synapse_event_verdicts_migrate` has rebuilt and verified every verdict.
 */

CREATE TABLE IF NOT EXISTS event_verdict_backfill (
    lock TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    last_event_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (lock)
);

INSERT INTO event_verdict_backfill (lock, completed, last_event_id)
VALUES ('X', 0, '');
