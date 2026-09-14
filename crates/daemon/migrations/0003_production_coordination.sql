-- Durable work, relay reconciliation, schema pinning, publication, and bounded streams
-- are implementation overlays. Signed Nostr events remain canonical protocol state.

CREATE TABLE work_queue (
    work_id uuid PRIMARY KEY,
    work_type text NOT NULL CHECK (work_type IN ('rebuild', 'connector', 'publish')),
    idempotency_key text NOT NULL UNIQUE,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'leased', 'completed', 'failed')),
    attempts integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 12,
    available_at timestamptz NOT NULL DEFAULT now(),
    deadline_at timestamptz,
    lease_owner uuid,
    lease_expires_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX work_queue_claim_idx
    ON work_queue (available_at, created_at)
    WHERE status IN ('pending', 'leased');

CREATE TABLE relay_checkpoints (
    relay_url text PRIMARY KEY,
    backfill_since bigint NOT NULL DEFAULT 0,
    last_event_created_at bigint NOT NULL DEFAULT 0,
    backfill_complete boolean NOT NULL DEFAULT false,
    last_connected_at timestamptz,
    last_error text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE descriptor_revisions (
    event_id text PRIMARY KEY REFERENCES raw_events(event_id) ON DELETE CASCADE,
    coordinate text NOT NULL,
    pubkey text NOT NULL,
    d_tag text NOT NULL,
    created_at bigint NOT NULL,
    expires_at bigint NOT NULL,
    schema_type text,
    schema_url text,
    schema_sha256 text,
    schema_validated boolean NOT NULL DEFAULT false,
    UNIQUE (coordinate, created_at, event_id)
);

CREATE INDEX descriptor_revisions_current_idx
    ON descriptor_revisions (coordinate, created_at DESC, event_id DESC);

CREATE TABLE schema_artifacts (
    sha256 text PRIMARY KEY,
    media_type text NOT NULL,
    body bytea NOT NULL,
    fetched_from text NOT NULL,
    fetched_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE publication_outbox (
    event_id text PRIMARY KEY,
    coordination_id text NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('secure', 'settle', 'refund', 'resolve_dispute')),
    document jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'publishing', 'published', 'observed', 'failed')),
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error text,
    published_at timestamptz,
    observed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX publication_terminal_outcome_idx
    ON publication_outbox (coordination_id)
    WHERE outcome IN ('settle', 'refund') AND status <> 'failed';

ALTER TABLE connector_operations
    ADD COLUMN connector_version text,
    ADD COLUMN connector_digest text,
    ADD COLUMN deadline_at timestamptz,
    ADD COLUMN failure_class text CHECK (failure_class IN ('retryable', 'permanent'));

CREATE TABLE connector_health (
    connector_id text PRIMARY KEY,
    package_version text NOT NULL,
    entrypoint_sha256 text NOT NULL,
    status text NOT NULL CHECK (status IN ('starting', 'healthy', 'degraded', 'unavailable')),
    consecutive_failures integer NOT NULL DEFAULT 0,
    restart_count integer NOT NULL DEFAULT 0,
    backoff_until timestamptz,
    last_ok_at timestamptz,
    last_error text,
    stderr_tail text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE daemon_state (
    key text PRIMARY KEY,
    value jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO daemon_state (key, value)
VALUES ('stream_schema_version', '1'::jsonb), ('execution_activation_cursor', 'null'::jsonb)
ON CONFLICT DO NOTHING;
