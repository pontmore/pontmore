use nostr::event::Event;
use pontmore::Reconstruction;
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
    pub status: OperationStatus,
    pub request: Value,
    pub response: Option<Value>,
    pub error: Option<String>,
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
    pub cursor: i64,
    pub coordinations: Vec<Value>,
    pub connector_operations: Vec<ConnectorOperation>,
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

    /// Atomically replace a derived projection and emit an SSE update.
    ///
    /// # Errors
    ///
    /// Returns a storage or serialization error.
    pub async fn put_projection(
        &self,
        reconstruction: &Reconstruction,
    ) -> Result<StreamEvent, StoreError> {
        let document = serde_json::to_value(reconstruction)?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO coordination_projections \
             (coordination_id, tip_event_id, state, document, rebuilt_at) \
             VALUES ($1, $2, $3, $4, now()) \
             ON CONFLICT (coordination_id) DO UPDATE SET \
             tip_event_id = EXCLUDED.tip_event_id, state = EXCLUDED.state, \
             document = EXCLUDED.document, rebuilt_at = now()",
        )
        .bind(&reconstruction.coordination_id)
        .bind(&reconstruction.tip)
        .bind(format!("{:?}", reconstruction.state).to_ascii_lowercase())
        .bind(&document)
        .execute(&mut *transaction)
        .await?;
        let stream_event =
            append_stream_event(&mut transaction, "coordination_updated", &document).await?;
        transaction.commit().await?;
        Ok(stream_event)
    }

    /// Create and claim a connector operation exactly once.
    ///
    /// # Errors
    ///
    /// Returns a storage error or an idempotency conflict.
    pub async fn create_operation(
        &self,
        connector_id: &str,
        idempotency_key: &str,
        request: &Value,
    ) -> Result<(ConnectorOperation, bool, Option<StreamEvent>), StoreError> {
        let operation_id = Uuid::new_v4();
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO connector_operations \
             (operation_id, connector_id, idempotency_key, status, request) \
             VALUES ($1, $2, $3, 'running', $4) \
             ON CONFLICT (idempotency_key) DO NOTHING",
        )
        .bind(operation_id)
        .bind(connector_id)
        .bind(idempotency_key)
        .bind(request)
        .execute(&mut *transaction)
        .await?;
        let created = inserted.rows_affected() == 1;
        let row = sqlx::query(
            "SELECT operation_id, connector_id, idempotency_key, status, request, response, error \
             FROM connector_operations WHERE idempotency_key = $1",
        )
        .bind(idempotency_key)
        .fetch_one(&mut *transaction)
        .await?;
        let operation = operation_from_row(&row)?;
        if operation.connector_id != connector_id || operation.request != *request {
            return Err(StoreError::IdempotencyConflict);
        }
        let stream_event = if created {
            Some(
                append_stream_event(
                    &mut transaction,
                    "connector_operation_updated",
                    &serde_json::to_value(&operation)?,
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
    ) -> Result<StreamEvent, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "UPDATE connector_operations SET status = $2, response = $3, error = $4, \
             updated_at = now() WHERE operation_id = $1 \
             RETURNING operation_id, connector_id, idempotency_key, status, request, response, error",
        )
        .bind(operation_id)
        .bind(status_name(status))
        .bind(response)
        .bind(error)
        .fetch_one(&mut *transaction)
        .await?;
        let operation = operation_from_row(&row)?;
        let stream_event = append_stream_event(
            &mut transaction,
            "connector_operation_updated",
            &serde_json::to_value(operation)?,
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
            "SELECT operation_id, connector_id, idempotency_key, status, request, response, error \
             FROM connector_operations ORDER BY created_at, operation_id",
        )
        .fetch_all(&mut *transaction)
        .await?;
        let connector_operations = rows
            .iter()
            .map(operation_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().await?;
        Ok(StreamSnapshot {
            cursor,
            coordinations,
            connector_operations,
        })
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
        status: parse_status(row.try_get::<String, _>("status")?.as_str()),
        request: row.try_get("request")?,
        response: row.try_get("response")?,
        error: row.try_get("error")?,
    })
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
