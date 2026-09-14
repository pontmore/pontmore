CREATE TABLE coordination_event_links (
    event_id text PRIMARY KEY REFERENCES raw_events(event_id) ON DELETE CASCADE,
    coordination_id text NOT NULL,
    predecessor_event_id text NOT NULL
);

CREATE INDEX coordination_event_links_coordination_idx
    ON coordination_event_links (coordination_id);

ALTER TABLE connector_operations
    ADD COLUMN coordination_id text,
    ADD COLUMN operation_type text,
    ADD COLUMN trigger_event_id text;

-- Version 0.1 exposed complete connector requests and responses. Remove those durable
-- stream records before the public projection switches to its redacted representation.
DELETE FROM stream_events WHERE event_type = 'connector_operation_updated';
