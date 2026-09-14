-- Private operator controls are implementation state, never canonical protocol state.
INSERT INTO daemon_state (key, value)
VALUES ('economic_execution_enabled', 'true'::jsonb)
ON CONFLICT DO NOTHING;
