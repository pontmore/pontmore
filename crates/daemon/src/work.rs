use std::time::Duration;

use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::store::{Store, StoreError};

#[derive(Debug, Clone)]
pub struct WorkItem {
    pub work_id: Uuid,
    pub work_type: String,
    pub payload: Value,
    pub attempts: i32,
    pub max_attempts: i32,
}

impl Store {
    /// # Errors
    /// Returns a storage error.
    pub async fn enqueue_work(
        &self,
        work_type: &str,
        idempotency_key: &str,
        payload: &Value,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query("INSERT INTO work_queue (work_id, work_type, idempotency_key, payload) VALUES ($1, $2, $3, $4) ON CONFLICT (idempotency_key) DO UPDATE SET status = 'pending', attempts = 0, available_at = now(), lease_owner = NULL, lease_expires_at = NULL, last_error = NULL, updated_at = now() WHERE work_queue.status IN ('completed', 'failed')")
            .bind(Uuid::new_v4()).bind(work_type).bind(idempotency_key).bind(payload)
            .execute(self.pool()).await?;
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
            .bind(owner).bind(lease_seconds).fetch_optional(self.pool()).await?;
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
            .bind(work_id).bind(owner).execute(self.pool()).await?;
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
            .bind(item.work_id).bind(owner).bind(if exhausted { "failed" } else { "pending" }).bind(truncate(error, 4_096)).bind(delay).execute(self.pool()).await?;
        Ok(())
    }

    /// # Errors
    /// Returns a storage error.
    pub async fn expire_work(&self) -> Result<u64, StoreError> {
        let result = sqlx::query("UPDATE work_queue SET status = 'failed', last_error = 'work deadline elapsed', lease_owner = NULL, lease_expires_at = NULL, updated_at = now() WHERE status IN ('pending', 'leased') AND deadline_at <= now()")
            .execute(self.pool()).await?;
        Ok(result.rows_affected())
    }
}

fn truncate(value: &str, limit: usize) -> &str {
    value.get(..limit).unwrap_or(value)
}
