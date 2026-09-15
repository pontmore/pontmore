use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use nostr::event::Event;
use pontmore::{DescriptorBinding, Reconstruction};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
    #[error("database migration: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("stored JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("event id collision with different content")]
    EventCollision,
    #[error("operation idempotency key already names different input")]
    IdempotencyConflict,
    #[error("settlement and refund publication are mutually exclusive")]
    TerminalOutcomeConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InsertOutcome {
    Inserted,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Running,
    Succeeded,
    Failed,
    Ambiguous,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectorOperation {
    pub operation_id: Uuid,
    pub connector_id: String,
    pub idempotency_key: String,
    pub coordination_id: Option<String>,
    pub operation: Option<String>,
    pub trigger_event_id: Option<String>,
    pub status: OperationStatus,
    pub request: Value,
    pub response: Option<Value>,
    pub error: Option<String>,
    pub failure_class: Option<String>,
}

/// Redacted connector progress safe for the client SSE stream.
#[derive(Debug, Clone, Serialize)]
pub struct PublicConnectorOperation {
    pub operation_id: Uuid,
    pub coordination_id: Option<String>,
    pub operation: Option<String>,
    pub status: OperationStatus,
}

/// One durable item in the client-facing SSE event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    pub sequence: i64,
    pub event_type: String,
    pub data: Value,
}

/// Initial client view emitted when an SSE cursor is not supplied.
#[derive(Debug, Clone, Serialize)]
pub struct StreamSnapshot {
    pub schema_version: u64,
    pub cursor: i64,
    pub coordinations: Vec<Value>,
    pub connector_operations: Vec<PublicConnectorOperation>,
}

#[derive(Debug, Clone)]
pub struct WorkItem {
    pub work_id: Uuid,
    pub work_type: String,
    pub payload: Value,
    pub attempts: i32,
    pub max_attempts: i32,
}

/// Authenticated operator view of daemon persistence state.
#[derive(Debug, Clone, Serialize)]
pub struct OperatorDashboard {
    pub generated_at: u64,
    pub event_count: i64,
    pub stream_cursor: i64,
    pub execution_enabled: bool,
    pub coordination_count: i64,
    pub coordinations: Vec<OperatorCoordination>,
    pub work: BTreeMap<String, i64>,
    pub operations: BTreeMap<String, i64>,
    pub publications: BTreeMap<String, i64>,
    pub relays: Vec<OperatorRelay>,
    pub operator_relay_urls: Vec<String>,
    pub connectors: Vec<OperatorConnector>,
    pub recent_events: Vec<OperatorEvent>,
    pub recent_work: Vec<OperatorWork>,
    pub recent_operations: Vec<OperatorOperation>,
    pub recent_publications: Vec<OperatorPublication>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorEvent {
    pub event_id: String,
    pub pubkey: String,
    pub kind: i64,
    pub created_at: i64,
    pub received_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorWork {
    pub work_id: Uuid,
    pub work_type: String,
    pub status: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub updated_at: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorOperation {
    pub operation_id: Uuid,
    pub connector_id: String,
    pub coordination_id: Option<String>,
    pub operation: Option<String>,
    pub status: String,
    pub updated_at: i64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorPublication {
    pub event_id: String,
    pub coordination_id: String,
    pub outcome: String,
    pub status: String,
    pub attempts: i32,
    pub created_at: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorCoordination {
    pub coordination_id: String,
    pub state: String,
    pub tip_event_id: String,
    pub applied_count: i64,
    pub rejection_count: i64,
    pub forked: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorRelay {
    pub relay_url: String,
    pub backfill_complete: bool,
    pub last_event_created_at: i64,
    pub last_connected_at: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperatorConnector {
    pub connector_id: String,
    pub package_version: String,
    pub status: String,
    pub consecutive_failures: i32,
    pub restart_count: i32,
    pub last_ok_at: Option<i64>,
    pub last_error: Option<String>,
}

/// PostgreSQL-backed durable storage with separate protocol, projection,
/// connector-operation, and client-stream state.
#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    /// Connect to the durable database.
    ///
    /// # Errors
    ///
    /// Returns a database connection error.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    /// Apply tracked schema migrations.
    ///
    /// # Errors
    ///
    /// Returns a database migration error.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    /// Check database readiness.
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot execute a probe query.
    pub async fn ready(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Establish the one-time economic execution boundary. Historical events imported
    /// before this daemon version reconstruct state but cannot trigger new value movement.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn activate_execution(&self) -> Result<(), StoreError> {
        sqlx::query("UPDATE daemon_state SET value = to_jsonb(now()), updated_at = now() WHERE key = 'execution_activation_cursor' AND value = 'null'::jsonb")
            .execute(&self.pool).await?;
        Ok(())
    }

    /// Whether automatic economic connector dispatch is enabled by the operator.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn execution_enabled(&self) -> Result<bool, StoreError> {
        Ok(sqlx::query_scalar(
            "SELECT COALESCE((value #>> '{}')::boolean, false) FROM daemon_state WHERE key = 'economic_execution_enabled'",
        )
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or(false))
    }

    /// Persist the operator's economic execution switch.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn set_execution_enabled(&self, enabled: bool) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO daemon_state (key, value) VALUES ('economic_execution_enabled', to_jsonb($1::boolean)) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
        )
        .bind(enabled)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Check whether an event arrived after the activation boundary.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn execution_eligible(&self, event_id: &str) -> Result<bool, StoreError> {
        Ok(sqlx::query_scalar("SELECT raw_events.received_at >= (SELECT (value #>> '{}')::timestamptz FROM daemon_state WHERE key = 'execution_activation_cursor') FROM raw_events WHERE event_id = $1")
            .bind(event_id).fetch_optional(&self.pool).await?.unwrap_or(false))
    }

    /// Mark connector operations interrupted by a prior daemon exit as ambiguous.
    ///
    /// # Errors
    ///
    /// Returns a database update error.
    pub async fn recover_interrupted_operations(&self) -> Result<u64, StoreError> {
        let result = sqlx::query(
            "UPDATE connector_operations SET status = 'ambiguous', \
             error = 'pontmored stopped before recording the connector outcome', \
             updated_at = now() WHERE status = 'running'",
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Atomically insert a verified event and its durable SSE notification.
    ///
    /// # Errors
    ///
    /// Returns a storage, serialization, or event-ID collision error.
    pub async fn put_event(
        &self,
        event: &Event,
    ) -> Result<(InsertOutcome, Option<StreamEvent>), StoreError> {
        let id = event.id.to_hex();
        let document = serde_json::to_value(event)?;
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO raw_events (event_id, pubkey, kind, created_at, document) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(&id)
        .bind(event.pubkey.to_hex())
        .bind(i64::from(event.kind.as_u16()))
        .bind(i64::try_from(event.created_at.as_secs()).unwrap_or(i64::MAX))
        .bind(&document)
        .execute(&mut *transaction)
        .await?;
        if inserted.rows_affected() == 1 {
            if let Some((coordination_id, predecessor_event_id)) = action_links(event) {
                sqlx::query(
                    "INSERT INTO coordination_event_links \
                     (event_id, coordination_id, predecessor_event_id) VALUES ($1, $2, $3)",
                )
                .bind(&id)
                .bind(coordination_id)
                .bind(predecessor_event_id)
                .execute(&mut *transaction)
                .await?;
            }
            if let Some(coordination_id) = coordination_id_for(event) {
                sqlx::query("INSERT INTO work_queue (work_id, work_type, idempotency_key, payload) VALUES ($1, 'rebuild', $2, $3) ON CONFLICT (idempotency_key) DO UPDATE SET status = CASE WHEN work_queue.status = 'completed' THEN 'pending' ELSE work_queue.status END, available_at = now(), updated_at = now()")
                    .bind(Uuid::new_v4())
                    .bind(format!("rebuild:{coordination_id}"))
                    .bind(json!({"coordination_id": coordination_id}))
                    .execute(&mut *transaction).await?;
            }
            let stream_event = append_stream_event(
                &mut transaction,
                "protocol_event",
                &json!({"event": document}),
            )
            .await?;
            transaction.commit().await?;
            return Ok((InsertOutcome::Inserted, Some(stream_event)));
        }
        let existing: Value =
            sqlx::query_scalar("SELECT document FROM raw_events WHERE event_id = $1")
                .bind(id)
                .fetch_one(&mut *transaction)
                .await?;
        transaction.commit().await?;
        if existing == document {
            Ok((InsertOutcome::Duplicate, None))
        } else {
            Err(StoreError::EventCollision)
        }
    }

    /// Retrieve one raw signed event for internal reconstruction.
    ///
    /// # Errors
    ///
    /// Returns a storage or deserialization error.
    pub async fn event(&self, id: &str) -> Result<Option<Event>, StoreError> {
        let value: Option<Value> =
            sqlx::query_scalar("SELECT document FROM raw_events WHERE event_id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        value
            .map(serde_json::from_value)
            .transpose()
            .map_err(Into::into)
    }

    /// Return every stored coordination root ID for startup reconstruction.
    ///
    /// # Errors
    ///
    /// Returns a database error.
    pub async fn coordination_ids(&self) -> Result<Vec<String>, StoreError> {
        Ok(sqlx::query_scalar(
            "SELECT event_id FROM raw_events WHERE kind = 7300 ORDER BY event_id",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    /// Load one root and every action that declares it as its coordination root.
    ///
    /// # Errors
    ///
    /// Returns a storage or deserialization error.
    pub async fn coordination_events(
        &self,
        coordination_id: &str,
    ) -> Result<(Option<Event>, Vec<Event>), StoreError> {
        let root = self.event(coordination_id).await?;
        let documents: Vec<Value> = sqlx::query_scalar(
            "SELECT raw_events.document FROM raw_events \
             JOIN coordination_event_links ON coordination_event_links.event_id = raw_events.event_id \
             WHERE coordination_event_links.coordination_id = $1 ORDER BY raw_events.event_id",
        )
        .bind(coordination_id)
        .fetch_all(&self.pool)
        .await?;
        let actions = documents
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((root, actions))
    }

    /// Atomically replace a derived projection and emit an SSE update.
    ///
    /// # Errors
    ///
    /// Returns a storage or serialization error.
    pub async fn put_projection(
        &self,
        reconstruction: &Reconstruction,
    ) -> Result<Option<StreamEvent>, StoreError> {
        let document = serde_json::to_value(reconstruction)?;
        let mut transaction = self.pool.begin().await?;
        let changed = sqlx::query(
            "INSERT INTO coordination_projections \
             (coordination_id, tip_event_id, state, document, rebuilt_at) \
             VALUES ($1, $2, $3, $4, now()) \
             ON CONFLICT (coordination_id) DO UPDATE SET \
             tip_event_id = EXCLUDED.tip_event_id, state = EXCLUDED.state, \
             document = EXCLUDED.document, rebuilt_at = now() \
             WHERE coordination_projections.document IS DISTINCT FROM EXCLUDED.document \
             RETURNING coordination_id",
        )
        .bind(&reconstruction.coordination_id)
        .bind(&reconstruction.tip)
        .bind(format!("{:?}", reconstruction.state).to_ascii_lowercase())
        .bind(&document)
        .fetch_optional(&mut *transaction)
        .await?;
        let stream_event = if changed.is_some() {
            Some(append_stream_event(&mut transaction, "coordination_updated", &document).await?)
        } else {
            None
        };
        transaction.commit().await?;
        Ok(stream_event)
    }

    /// Create and claim a connector operation exactly once.
    ///
    /// # Errors
    ///
    /// Returns a storage error or an idempotency conflict.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_operation(
        &self,
        connector_id: &str,
        idempotency_key: &str,
        coordination_id: &str,
        operation: &str,
        trigger_event_id: &str,
        request: &Value,
        connector_version: &str,
        connector_digest: &str,
    ) -> Result<(ConnectorOperation, bool, Option<StreamEvent>), StoreError> {
        let operation_id = Uuid::new_v4();
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO connector_operations \
             (operation_id, connector_id, idempotency_key, coordination_id, operation_type, \
             trigger_event_id, status, request, connector_version, connector_digest) \
             VALUES ($1, $2, $3, $4, $5, $6, 'running', $7, $8, $9) \
             ON CONFLICT (idempotency_key) DO NOTHING",
        )
        .bind(operation_id)
        .bind(connector_id)
        .bind(idempotency_key)
        .bind(coordination_id)
        .bind(operation)
        .bind(trigger_event_id)
        .bind(request)
        .bind(connector_version)
        .bind(connector_digest)
        .execute(&mut *transaction)
        .await?;
        let mut created = inserted.rows_affected() == 1;
        let row = sqlx::query(
            "SELECT operation_id, connector_id, idempotency_key, coordination_id, operation_type, \
             trigger_event_id, status, request, response, error, failure_class \
             FROM connector_operations WHERE idempotency_key = $1",
        )
        .bind(idempotency_key)
        .fetch_one(&mut *transaction)
        .await?;
        let mut operation = operation_from_row(&row)?;
        if operation.connector_id != connector_id || operation.request != *request {
            return Err(StoreError::IdempotencyConflict);
        }
        if !created
            && operation.status == OperationStatus::Failed
            && operation.failure_class.as_deref() == Some("retryable")
        {
            sqlx::query("UPDATE connector_operations SET status = 'running', response = NULL, error = NULL, failure_class = NULL, updated_at = now() WHERE operation_id = $1")
                .bind(operation.operation_id).execute(&mut *transaction).await?;
            operation.status = OperationStatus::Running;
            operation.response = None;
            operation.error = None;
            operation.failure_class = None;
            created = true;
        }
        let stream_event = if created {
            Some(
                append_stream_event(
                    &mut transaction,
                    "connector_operation_updated",
                    &serde_json::to_value(public_operation(&operation))?,
                )
                .await?,
            )
        } else {
            None
        };
        transaction.commit().await?;
        Ok((operation, created, stream_event))
    }

    /// Store a connector outcome and emit an SSE update atomically.
    ///
    /// # Errors
    ///
    /// Returns a database or serialization error.
    pub async fn set_operation_status(
        &self,
        operation_id: Uuid,
        status: OperationStatus,
        response: Option<&Value>,
        error: Option<&str>,
        failure_class: Option<&str>,
    ) -> Result<StreamEvent, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "UPDATE connector_operations SET status = $2, response = $3, error = $4, failure_class = $5, \
             updated_at = now() WHERE operation_id = $1 \
             RETURNING operation_id, connector_id, idempotency_key, coordination_id, \
             operation_type, trigger_event_id, status, request, response, error, failure_class",
        )
        .bind(operation_id)
        .bind(status_name(status))
        .bind(response)
        .bind(error)
        .bind(failure_class)
        .fetch_one(&mut *transaction)
        .await?;
        let operation = operation_from_row(&row)?;
        let stream_event = append_stream_event(
            &mut transaction,
            "connector_operation_updated",
            &serde_json::to_value(public_operation(&operation))?,
        )
        .await?;
        transaction.commit().await?;
        Ok(stream_event)
    }

    /// Build a consistent initial view for a new SSE client.
    ///
    /// # Errors
    ///
    /// Returns a database read error.
    pub async fn stream_snapshot(&self) -> Result<StreamSnapshot, StoreError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *transaction)
            .await?;
        let cursor: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(sequence), 0) FROM stream_events")
                .fetch_one(&mut *transaction)
                .await?;
        let coordinations: Vec<Value> = sqlx::query_scalar(
            "SELECT document FROM coordination_projections ORDER BY coordination_id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let rows = sqlx::query(
            "SELECT operation_id, coordination_id, operation_type, status \
             FROM connector_operations ORDER BY created_at, operation_id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let connector_operations = rows
            .iter()
            .map(public_operation_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().await?;
        Ok(StreamSnapshot {
            schema_version: 1,
            cursor,
            coordinations,
            connector_operations,
        })
    }

    /// Return operations that require connector inspection before any retry.
    ///
    /// # Errors
    ///
    /// Returns a database error.
    pub async fn ambiguous_operations(&self) -> Result<Vec<ConnectorOperation>, StoreError> {
        let rows = sqlx::query(
            "SELECT operation_id, connector_id, idempotency_key, coordination_id, operation_type, \
             trigger_event_id, status, request, response, error, failure_class FROM connector_operations \
             WHERE status = 'ambiguous' ORDER BY created_at, operation_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| operation_from_row(row).map_err(StoreError::from))
            .collect()
    }

    /// Retrieve durable SSE events strictly after a client cursor.
    ///
    /// # Errors
    ///
    /// Returns a database read error.
    pub async fn stream_events_after(
        &self,
        cursor: i64,
        limit: i64,
    ) -> Result<Vec<StreamEvent>, StoreError> {
        let rows = sqlx::query(
            "SELECT sequence, event_type, data FROM stream_events \
             WHERE sequence > $1 ORDER BY sequence LIMIT $2",
        )
        .bind(cursor)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(StreamEvent {
                    sequence: row.try_get("sequence")?,
                    event_type: row.try_get("event_type")?,
                    data: row.try_get("data")?,
                })
            })
            .collect()
    }

    /// # Errors
    /// Returns a storage error.
    pub async fn stream_bounds(&self) -> Result<Option<(i64, i64)>, StoreError> {
        let row =
            sqlx::query("SELECT MIN(sequence) AS first, MAX(sequence) AS last FROM stream_events")
                .fetch_one(&self.pool)
                .await?;
        Ok(match (row.try_get("first")?, row.try_get("last")?) {
            (Some(first), Some(last)) => Some((first, last)),
            _ => None,
        })
    }

    /// # Errors
    /// Returns a storage error.
    pub async fn prune_stream(&self, retention: Duration) -> Result<u64, StoreError> {
        let seconds = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
        let result = sqlx::query("DELETE FROM stream_events WHERE created_at < now() - make_interval(secs => $1::double precision)")
            .bind(seconds).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    /// # Errors
    /// Returns a storage error.
    pub async fn record_connector_health(
        &self,
        health: &crate::connector::ConnectorHealth,
    ) -> Result<(), StoreError> {
        sqlx::query("INSERT INTO connector_health (connector_id, package_version, entrypoint_sha256, status, consecutive_failures, last_ok_at, last_error) VALUES ($1,$2,$3,$4,$5,CASE WHEN $6 THEN now() ELSE NULL END,$7) ON CONFLICT (connector_id) DO UPDATE SET package_version = EXCLUDED.package_version, entrypoint_sha256 = EXCLUDED.entrypoint_sha256, status = EXCLUDED.status, consecutive_failures = CASE WHEN $6 THEN 0 ELSE connector_health.consecutive_failures + 1 END, last_ok_at = CASE WHEN $6 THEN now() ELSE connector_health.last_ok_at END, last_error = EXCLUDED.last_error, updated_at = now()")
            .bind(&health.connector_id).bind(&health.package_version).bind(&health.entrypoint_sha256)
            .bind(if health.healthy { "healthy" } else { "degraded" }).bind(i32::from(!health.healthy))
            .bind(health.healthy).bind(&health.error).execute(&self.pool).await?;
        Ok(())
    }

    pub(crate) async fn events_of_kind(&self, kind: u16) -> Result<Vec<Event>, StoreError> {
        let documents: Vec<Value> = sqlx::query_scalar(
            "SELECT document FROM raw_events WHERE kind = $1 ORDER BY created_at, event_id",
        )
        .bind(i64::from(kind))
        .fetch_all(&self.pool)
        .await?;
        documents
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub(crate) async fn put_schema_artifact(
        &self,
        artifact: &crate::schema::SchemaArtifact,
        url: &str,
    ) -> Result<(), StoreError> {
        sqlx::query("INSERT INTO schema_artifacts (sha256, media_type, body, fetched_from) VALUES ($1, $2, $3, $4) ON CONFLICT (sha256) DO NOTHING")
            .bind(&artifact.sha256)
            .bind(&artifact.media_type)
            .bind(&artifact.body)
            .bind(url)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn put_descriptor_revision(
        &self,
        event: &Event,
        binding: &DescriptorBinding,
        d_tag: &str,
        schema_type: Option<&str>,
        schema_url: Option<&str>,
        schema_sha256: Option<&str>,
    ) -> Result<(), StoreError> {
        sqlx::query("INSERT INTO descriptor_revisions (event_id, coordinate, pubkey, d_tag, created_at, expires_at, schema_type, schema_url, schema_sha256, schema_validated) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (event_id) DO UPDATE SET schema_sha256 = EXCLUDED.schema_sha256, schema_validated = EXCLUDED.schema_validated")
            .bind(event.id.to_hex())
            .bind(&binding.coordinate)
            .bind(event.pubkey.to_hex())
            .bind(d_tag)
            .bind(i64::try_from(binding.created_at).unwrap_or(i64::MAX))
            .bind(i64::try_from(binding.expires_at).unwrap_or(i64::MAX))
            .bind(schema_type)
            .bind(schema_url)
            .bind(schema_sha256)
            .bind(schema_sha256.is_some())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn enqueue_publication(
        &self,
        event: &Event,
        coordination_id: &str,
        outcome: &str,
    ) -> Result<(), StoreError> {
        let document = serde_json::to_value(event)?;
        let result = sqlx::query("INSERT INTO publication_outbox (event_id, coordination_id, outcome, document) VALUES ($1,$2,$3,$4) ON CONFLICT (event_id) DO NOTHING")
            .bind(event.id.to_hex())
            .bind(coordination_id)
            .bind(outcome)
            .bind(document)
            .execute(&self.pool)
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(error))
                if error.constraint() == Some("publication_terminal_outcome_idx") =>
            {
                Err(StoreError::TerminalOutcomeConflict)
            }
            Err(error) => Err(StoreError::Database(error)),
        }
    }

    pub(crate) async fn next_publication(&self) -> Result<Option<Event>, StoreError> {
        let document: Option<Value> = sqlx::query_scalar("UPDATE publication_outbox SET status = 'publishing', attempts = attempts + 1 WHERE event_id = (SELECT event_id FROM publication_outbox WHERE (status IN ('pending','published') OR (status = 'publishing' AND next_attempt_at < now() - interval '60 seconds')) AND next_attempt_at <= now() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING document")
            .fetch_optional(&self.pool)
            .await?;
        document
            .map(serde_json::from_value)
            .transpose()
            .map_err(StoreError::from)
    }

    pub(crate) async fn mark_publication_sent(
        &self,
        event_id: &str,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        sqlx::query("UPDATE publication_outbox SET status = CASE WHEN $2::text IS NULL THEN 'published' ELSE CASE WHEN attempts >= 12 THEN 'failed' ELSE 'pending' END END, published_at = CASE WHEN $2::text IS NULL THEN now() ELSE published_at END, last_error = $2, next_attempt_at = now() + make_interval(secs => LEAST(300, power(2, attempts))::double precision) WHERE event_id = $1")
            .bind(event_id)
            .bind(error)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn relay_since(
        &self,
        url: &str,
        lookback: Duration,
    ) -> Result<u64, StoreError> {
        let checkpoint: Option<i64> = sqlx::query_scalar("SELECT last_event_created_at FROM relay_checkpoints WHERE relay_url = $1 AND backfill_complete")
            .bind(url)
            .fetch_optional(&self.pool)
            .await?;
        let fallback = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(lookback.as_secs());
        Ok(checkpoint
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or(fallback))
    }

    pub(crate) async fn mark_relay_backfilled(&self, url: &str) -> Result<(), StoreError> {
        sqlx::query("INSERT INTO relay_checkpoints (relay_url, backfill_complete, last_connected_at) VALUES ($1, true, now()) ON CONFLICT (relay_url) DO UPDATE SET backfill_complete = true, last_connected_at = now(), last_error = NULL, updated_at = now()")
            .bind(url)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn mark_relay_connected(&self, url: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE relay_checkpoints SET last_connected_at = now(), last_error = NULL, updated_at = now() WHERE relay_url = $1")
            .bind(url)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn mark_relay_disconnected(&self, url: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE relay_checkpoints SET last_connected_at = NULL, last_error = 'disconnected', updated_at = now() WHERE relay_url = $1")
            .bind(url)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn mark_relay_error(&self, url: &str, error: &str) -> Result<(), StoreError> {
        sqlx::query("INSERT INTO relay_checkpoints (relay_url, last_error) VALUES ($1, $2) ON CONFLICT (relay_url) DO UPDATE SET last_error = EXCLUDED.last_error, updated_at = now()")
            .bind(url)
            .bind(truncate(error, 4_096))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn advance_relay(&self, url: &str, created_at: u64) -> Result<(), StoreError> {
        let created_at = i64::try_from(created_at).unwrap_or(i64::MAX);
        sqlx::query("INSERT INTO relay_checkpoints (relay_url, last_event_created_at, backfill_complete, last_connected_at) VALUES ($1, $2, true, now()) ON CONFLICT (relay_url) DO UPDATE SET last_event_created_at = GREATEST(relay_checkpoints.last_event_created_at, EXCLUDED.last_event_created_at), last_connected_at = now(), last_error = NULL, updated_at = now()")
            .bind(url)
            .bind(created_at)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Persist an operator-selected relay and make its pending state visible immediately.
    /// Returns false when the URL was already selected.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn add_operator_relay(&self, url: &str) -> Result<bool, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO operator_relays (relay_url) VALUES ($1) ON CONFLICT DO NOTHING",
        )
        .bind(url)
        .execute(&mut *transaction)
        .await?
        .rows_affected()
            == 1;
        if inserted {
            sqlx::query(
                "INSERT INTO relay_checkpoints (relay_url) VALUES ($1) ON CONFLICT DO NOTHING",
            )
            .bind(url)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(inserted)
    }

    /// Load durable relays selected through the operator UI.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn operator_relay_urls(&self) -> Result<Vec<String>, StoreError> {
        Ok(
            sqlx::query_scalar("SELECT relay_url FROM operator_relays ORDER BY relay_url")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    /// Merge restart-managed URLs with durable operator URLs, without duplicates.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn effective_relay_urls(
        &self,
        startup: &[String],
    ) -> Result<Vec<String>, StoreError> {
        let mut urls = startup.to_vec();
        urls.extend(self.operator_relay_urls().await?);
        urls.sort();
        urls.dedup();
        Ok(urls)
    }

    /// Mark an outbox event observed through relay ingestion.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn observe_publication(&self, event_id: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE publication_outbox SET status = 'observed', observed_at = now() WHERE event_id = $1 AND status IN ('pending', 'publishing', 'published')")
            .bind(event_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Check whether all required relays recently completed backfill.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn relay_readiness(&self, startup: &[String]) -> Result<bool, StoreError> {
        let urls = self.effective_relay_urls(startup).await?;
        if urls.is_empty() {
            return Ok(true);
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM relay_checkpoints WHERE relay_url = ANY($1) AND backfill_complete AND last_connected_at > now() - interval '5 minutes'")
            .bind(&urls)
            .fetch_one(&self.pool)
            .await?;
        Ok(count == i64::try_from(urls.len()).unwrap_or(i64::MAX))
    }

    /// # Errors
    /// Returns a storage error.
    pub async fn enqueue_work(
        &self,
        work_type: &str,
        idempotency_key: &str,
        payload: &Value,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query("INSERT INTO work_queue (work_id, work_type, idempotency_key, payload) VALUES ($1, $2, $3, $4) ON CONFLICT (idempotency_key) DO UPDATE SET status = 'pending', attempts = 0, available_at = now(), lease_owner = NULL, lease_expires_at = NULL, last_error = NULL, updated_at = now() WHERE work_queue.status IN ('completed', 'failed')")
            .bind(Uuid::new_v4())
            .bind(work_type)
            .bind(idempotency_key)
            .bind(payload)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() == 1)
    }

    /// # Errors
    /// Returns a storage error.
    pub async fn claim_work(
        &self,
        owner: Uuid,
        lease: Duration,
    ) -> Result<Option<WorkItem>, StoreError> {
        let lease_seconds = i64::try_from(lease.as_secs()).unwrap_or(i64::MAX);
        let row = sqlx::query("WITH candidate AS (SELECT work_id FROM work_queue WHERE ((status = 'pending' AND available_at <= now()) OR (status = 'leased' AND lease_expires_at < now())) AND (deadline_at IS NULL OR deadline_at > now()) ORDER BY available_at, created_at FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE work_queue SET status = 'leased', lease_owner = $1, lease_expires_at = now() + make_interval(secs => $2::double precision), attempts = attempts + 1, updated_at = now() FROM candidate WHERE work_queue.work_id = candidate.work_id RETURNING work_queue.work_id, work_queue.work_type, work_queue.payload, work_queue.attempts, work_queue.max_attempts")
            .bind(owner)
            .bind(lease_seconds)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| -> Result<WorkItem, sqlx::Error> {
            Ok(WorkItem {
                work_id: row.try_get("work_id")?,
                work_type: row.try_get("work_type")?,
                payload: row.try_get("payload")?,
                attempts: row.try_get("attempts")?,
                max_attempts: row.try_get("max_attempts")?,
            })
        })
        .transpose()
        .map_err(StoreError::from)
    }

    /// # Errors
    /// Returns a storage error.
    pub async fn complete_work(&self, work_id: Uuid, owner: Uuid) -> Result<(), StoreError> {
        sqlx::query("UPDATE work_queue SET status = 'completed', lease_owner = NULL, lease_expires_at = NULL, updated_at = now() WHERE work_id = $1 AND lease_owner = $2 AND status = 'leased'")
            .bind(work_id)
            .bind(owner)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// # Errors
    /// Returns a storage error.
    pub async fn retry_work(
        &self,
        item: &WorkItem,
        owner: Uuid,
        error: &str,
    ) -> Result<(), StoreError> {
        let exhausted = item.attempts >= item.max_attempts;
        let exponent = u32::try_from(item.attempts.clamp(1, 10)).unwrap_or(10);
        let delay = 2_i64.pow(exponent).min(300);
        sqlx::query("UPDATE work_queue SET status = $3, last_error = $4, lease_owner = NULL, lease_expires_at = NULL, available_at = now() + make_interval(secs => $5::double precision), updated_at = now() WHERE work_id = $1 AND lease_owner = $2")
            .bind(item.work_id)
            .bind(owner)
            .bind(if exhausted { "failed" } else { "pending" })
            .bind(truncate(error, 4_096))
            .bind(delay)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// # Errors
    /// Returns a storage error.
    pub async fn expire_work(&self) -> Result<u64, StoreError> {
        let result = sqlx::query("UPDATE work_queue SET status = 'failed', last_error = 'work deadline elapsed', lease_owner = NULL, lease_expires_at = NULL, updated_at = now() WHERE status IN ('pending', 'leased') AND deadline_at <= now()")
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Requeue failed work that has not passed its deadline.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn retry_failed_work(&self) -> Result<u64, StoreError> {
        let result = sqlx::query(
            "UPDATE work_queue SET status = 'pending', attempts = 0, available_at = now(), \
             lease_owner = NULL, lease_expires_at = NULL, last_error = NULL, updated_at = now() \
             WHERE status = 'failed' AND (deadline_at IS NULL OR deadline_at > now())",
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Requeue failed signed-event publications.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn retry_failed_publications(&self) -> Result<u64, StoreError> {
        let result = sqlx::query(
            "UPDATE publication_outbox SET status = 'pending', attempts = 0, \
             next_attempt_at = now(), last_error = NULL WHERE status = 'failed'",
        )
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Build the private operational snapshot rendered by the built-in UI.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn operator_dashboard(&self) -> Result<OperatorDashboard, StoreError> {
        let event_count = sqlx::query_scalar("SELECT COUNT(*) FROM raw_events")
            .fetch_one(&self.pool)
            .await?;
        let stream_cursor =
            sqlx::query_scalar("SELECT COALESCE(MAX(sequence), 0) FROM stream_events")
                .fetch_one(&self.pool)
                .await?;
        let coordination_count =
            sqlx::query_scalar("SELECT COUNT(*) FROM coordination_projections")
                .fetch_one(&self.pool)
                .await?;

        let coordination_rows = sqlx::query(
            "SELECT coordination_id, state, tip_event_id, \
             jsonb_array_length(COALESCE(document->'applied', '[]'::jsonb))::bigint AS applied_count, \
             jsonb_array_length(COALESCE(document->'rejections', '[]'::jsonb))::bigint AS rejection_count, \
             COALESCE(document->'fork' <> 'null'::jsonb, false) AS forked \
             FROM coordination_projections ORDER BY rebuilt_at DESC, coordination_id LIMIT 200",
        )
        .fetch_all(&self.pool)
        .await?;
        let coordinations = coordination_rows
            .iter()
            .map(|row| {
                Ok(OperatorCoordination {
                    coordination_id: row.try_get("coordination_id")?,
                    state: row.try_get("state")?,
                    tip_event_id: row.try_get("tip_event_id")?,
                    applied_count: row.try_get("applied_count")?,
                    rejection_count: row.try_get("rejection_count")?,
                    forked: row.try_get("forked")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;

        let relay_rows = sqlx::query(
            "SELECT relay_url, backfill_complete, last_event_created_at, \
             EXTRACT(EPOCH FROM last_connected_at)::bigint AS last_connected_at, last_error \
             FROM relay_checkpoints ORDER BY relay_url LIMIT 200",
        )
        .fetch_all(&self.pool)
        .await?;
        let relays = relay_rows
            .iter()
            .map(|row| {
                Ok(OperatorRelay {
                    relay_url: row.try_get("relay_url")?,
                    backfill_complete: row.try_get("backfill_complete")?,
                    last_event_created_at: row.try_get("last_event_created_at")?,
                    last_connected_at: row.try_get("last_connected_at")?,
                    last_error: row.try_get("last_error")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;

        let connector_rows = sqlx::query(
            "SELECT connector_id, package_version, status, consecutive_failures, restart_count, \
             EXTRACT(EPOCH FROM last_ok_at)::bigint AS last_ok_at, last_error \
             FROM connector_health ORDER BY connector_id LIMIT 200",
        )
        .fetch_all(&self.pool)
        .await?;
        let connectors = connector_rows
            .iter()
            .map(|row| {
                Ok(OperatorConnector {
                    connector_id: row.try_get("connector_id")?,
                    package_version: row.try_get("package_version")?,
                    status: row.try_get("status")?,
                    consecutive_failures: row.try_get("consecutive_failures")?,
                    restart_count: row.try_get("restart_count")?,
                    last_ok_at: row.try_get("last_ok_at")?,
                    last_error: row.try_get("last_error")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;

        Ok(OperatorDashboard {
            generated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_count,
            stream_cursor,
            execution_enabled: self.execution_enabled().await?,
            coordination_count,
            coordinations,
            work: status_counts(&self.pool, "work_queue").await?,
            operations: status_counts(&self.pool, "connector_operations").await?,
            publications: status_counts(&self.pool, "publication_outbox").await?,
            relays,
            operator_relay_urls: self.operator_relay_urls().await?,
            connectors,
            recent_events: operator_events(&self.pool).await?,
            recent_work: operator_work(&self.pool).await?,
            recent_operations: operator_operations(&self.pool).await?,
            recent_publications: operator_publications(&self.pool).await?,
        })
    }
}

async fn status_counts(pool: &PgPool, table: &str) -> Result<BTreeMap<String, i64>, sqlx::Error> {
    let statement = match table {
        "work_queue" => "SELECT status, COUNT(*)::bigint AS count FROM work_queue GROUP BY status",
        "connector_operations" => {
            "SELECT status, COUNT(*)::bigint AS count FROM connector_operations GROUP BY status"
        }
        "publication_outbox" => {
            "SELECT status, COUNT(*)::bigint AS count FROM publication_outbox GROUP BY status"
        }
        _ => unreachable!("status count table is selected internally"),
    };
    sqlx::query(statement)
        .fetch_all(pool)
        .await?
        .iter()
        .map(|row| Ok((row.try_get("status")?, row.try_get("count")?)))
        .collect()
}

async fn operator_events(pool: &PgPool) -> Result<Vec<OperatorEvent>, sqlx::Error> {
    sqlx::query(
        "SELECT event_id, pubkey, kind, created_at, \
         EXTRACT(EPOCH FROM received_at)::bigint AS received_at \
         FROM raw_events ORDER BY received_at DESC, event_id LIMIT 50",
    )
    .fetch_all(pool)
    .await?
    .iter()
    .map(|row| {
        Ok(OperatorEvent {
            event_id: row.try_get("event_id")?,
            pubkey: row.try_get("pubkey")?,
            kind: row.try_get("kind")?,
            created_at: row.try_get("created_at")?,
            received_at: row.try_get("received_at")?,
        })
    })
    .collect()
}

async fn operator_work(pool: &PgPool) -> Result<Vec<OperatorWork>, sqlx::Error> {
    sqlx::query(
        "SELECT work_id, work_type, status, attempts, max_attempts, last_error, \
         EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at \
         FROM work_queue ORDER BY updated_at DESC, work_id LIMIT 50",
    )
    .fetch_all(pool)
    .await?
    .iter()
    .map(|row| {
        Ok(OperatorWork {
            work_id: row.try_get("work_id")?,
            work_type: row.try_get("work_type")?,
            status: row.try_get("status")?,
            attempts: row.try_get("attempts")?,
            max_attempts: row.try_get("max_attempts")?,
            updated_at: row.try_get("updated_at")?,
            last_error: row.try_get("last_error")?,
        })
    })
    .collect()
}

async fn operator_operations(pool: &PgPool) -> Result<Vec<OperatorOperation>, sqlx::Error> {
    sqlx::query(
        "SELECT operation_id, connector_id, coordination_id, operation_type, status, error, \
         EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at \
         FROM connector_operations ORDER BY updated_at DESC, operation_id LIMIT 50",
    )
    .fetch_all(pool)
    .await?
    .iter()
    .map(|row| {
        Ok(OperatorOperation {
            operation_id: row.try_get("operation_id")?,
            connector_id: row.try_get("connector_id")?,
            coordination_id: row.try_get("coordination_id")?,
            operation: row.try_get("operation_type")?,
            status: row.try_get("status")?,
            updated_at: row.try_get("updated_at")?,
            error: row.try_get("error")?,
        })
    })
    .collect()
}

async fn operator_publications(pool: &PgPool) -> Result<Vec<OperatorPublication>, sqlx::Error> {
    sqlx::query(
        "SELECT event_id, coordination_id, outcome, status, attempts, last_error, \
         EXTRACT(EPOCH FROM created_at)::bigint AS created_at \
         FROM publication_outbox ORDER BY created_at DESC, event_id LIMIT 50",
    )
    .fetch_all(pool)
    .await?
    .iter()
    .map(|row| {
        Ok(OperatorPublication {
            event_id: row.try_get("event_id")?,
            coordination_id: row.try_get("coordination_id")?,
            outcome: row.try_get("outcome")?,
            status: row.try_get("status")?,
            attempts: row.try_get("attempts")?,
            created_at: row.try_get("created_at")?,
            last_error: row.try_get("last_error")?,
        })
    })
    .collect()
}

fn truncate(value: &str, limit: usize) -> &str {
    value.get(..limit).unwrap_or(value)
}

async fn append_stream_event(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event_type: &str,
    data: &Value,
) -> Result<StreamEvent, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO stream_events (event_type, data) VALUES ($1, $2) \
         RETURNING sequence, event_type, data",
    )
    .bind(event_type)
    .bind(data)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(StreamEvent {
        sequence: row.try_get("sequence")?,
        event_type: row.try_get("event_type")?,
        data: row.try_get("data")?,
    })
}

fn operation_from_row(row: &sqlx::postgres::PgRow) -> Result<ConnectorOperation, sqlx::Error> {
    Ok(ConnectorOperation {
        operation_id: row.try_get("operation_id")?,
        connector_id: row.try_get("connector_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        coordination_id: row.try_get("coordination_id")?,
        operation: row.try_get("operation_type")?,
        trigger_event_id: row.try_get("trigger_event_id")?,
        status: parse_status(row.try_get::<String, _>("status")?.as_str()),
        request: row.try_get("request")?,
        response: row.try_get("response")?,
        error: row.try_get("error")?,
        failure_class: row.try_get("failure_class")?,
    })
}

fn public_operation(operation: &ConnectorOperation) -> PublicConnectorOperation {
    PublicConnectorOperation {
        operation_id: operation.operation_id,
        coordination_id: operation.coordination_id.clone(),
        operation: operation.operation.clone(),
        status: operation.status,
    }
}

fn public_operation_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<PublicConnectorOperation, sqlx::Error> {
    Ok(PublicConnectorOperation {
        operation_id: row.try_get("operation_id")?,
        coordination_id: row.try_get("coordination_id")?,
        operation: row.try_get("operation_type")?,
        status: parse_status(row.try_get::<String, _>("status")?.as_str()),
    })
}

fn action_links(event: &Event) -> Option<(&str, &str)> {
    if event.kind.as_u16() != 7301 {
        return None;
    }
    Some((
        single_marked_event(event, "root")?,
        single_marked_event(event, "prev")?,
    ))
}

fn coordination_id_for(event: &Event) -> Option<String> {
    match event.kind.as_u16() {
        7300 => Some(event.id.to_hex()),
        7301 => single_marked_event(event, "root").map(str::to_owned),
        _ => None,
    }
}

fn single_marked_event<'a>(event: &'a Event, marker: &str) -> Option<&'a str> {
    let values = event
        .tags
        .iter()
        .filter_map(|tag| match tag.as_slice() {
            [kind, value, _, found_marker, ..] if kind == "e" && found_marker == marker => {
                Some(value.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    match values.as_slice() {
        [value] => Some(value),
        _ => None,
    }
}

const fn status_name(status: OperationStatus) -> &'static str {
    match status {
        OperationStatus::Running => "running",
        OperationStatus::Succeeded => "succeeded",
        OperationStatus::Failed => "failed",
        OperationStatus::Ambiguous => "ambiguous",
    }
}

fn parse_status(value: &str) -> OperationStatus {
    match value {
        "running" => OperationStatus::Running,
        "succeeded" => OperationStatus::Succeeded,
        "failed" => OperationStatus::Failed,
        _ => OperationStatus::Ambiguous,
    }
}
