-- Operator-added relays are an implementation overlay, not protocol state.
CREATE TABLE operator_relays (
    relay_url text PRIMARY KEY,
    added_at timestamptz NOT NULL DEFAULT now()
);
