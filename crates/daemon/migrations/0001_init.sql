CREATE TABLE raw_events (
    event_id text PRIMARY KEY,
    pubkey text NOT NULL,
    kind bigint NOT NULL,
    created_at bigint NOT NULL,
    document jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX raw_events_kind_created_idx ON raw_events (kind, created_at);

CREATE TABLE coordination_projections (
    coordination_id text PRIMARY KEY REFERENCES raw_events(event_id),
    tip_event_id text NOT NULL,
    state text NOT NULL,
    document jsonb NOT NULL,
    rebuilt_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE connector_operations (
    operation_id uuid PRIMARY KEY,
    connector_id text NOT NULL,
    idempotency_key text NOT NULL UNIQUE,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'ambiguous')),
    request jsonb NOT NULL,
    response jsonb,
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stream_events (
    sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type text NOT NULL,
    data jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
