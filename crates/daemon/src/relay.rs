use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use nostr::event::Event;
use nostr_sdk::{
    client::{Client, ClientNotification, ReqTarget},
    prelude::{Filter, Kind, Timestamp},
};
use thiserror::Error;
use tokio::sync::{broadcast, watch};

use crate::{
    coordinator::{Coordinator, CoordinatorError},
    store::{Store, StoreError, StreamEvent},
};

const PONTMORE_KINDS: [Kind; 4] = [
    Kind::Custom(30_360),
    Kind::Custom(30_361),
    Kind::Custom(7_300),
    Kind::Custom(7_301),
];

#[derive(Debug, Error)]
pub enum RelayError {
    #[error("relay client: {0}")]
    Client(#[from] nostr_sdk::error::Error),
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[derive(Clone)]
pub struct RelayService {
    client: Client,
    urls: Vec<String>,
    store: Store,
    coordinator: Coordinator,
    stream_sender: broadcast::Sender<StreamEvent>,
    lookback: Duration,
}

impl RelayService {
    #[must_use]
    pub fn new(
        urls: Vec<String>,
        store: Store,
        coordinator: Coordinator,
        stream_sender: broadcast::Sender<StreamEvent>,
        lookback: Duration,
    ) -> Self {
        Self {
            client: Client::default(),
            urls,
            store,
            coordinator,
            stream_sender,
            lookback,
        }
    }

    /// Backfill, subscribe, and reconcile publication until shutdown.
    ///
    /// # Errors
    /// Returns relay, storage, or coordination errors.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), RelayError> {
        if self.urls.is_empty() {
            return Ok(());
        }
        for url in &self.urls {
            self.client.add_relay(url).await?;
        }
        let mut notifications = self.client.notifications();
        self.client.connect().await;
        let mut subscription_since = u64::MAX;
        for url in &self.urls {
            let since = self.store.relay_since(url, self.lookback).await?;
            subscription_since = subscription_since.min(since);
            let filter = Filter::new()
                .kinds(PONTMORE_KINDS)
                .since(Timestamp::from(since));
            match self
                .client
                .fetch_events(ReqTarget::single(url.as_str(), [filter]))
                .timeout(Duration::from_secs(30))
                .max_events(50_000)
                .await
            {
                Ok(events) => {
                    let mut events = events.into_iter().collect::<Vec<_>>();
                    events.sort_by_key(|event| {
                        (
                            relay_priority(event.kind.as_u16()),
                            event.created_at.as_secs(),
                            event.id,
                        )
                    });
                    for event in events {
                        if let Err(error) = self.ingest(&event).await {
                            tracing::warn!(event_id = %event.id, %error, "relay event rejected");
                        }
                    }
                    self.store.mark_relay_backfilled(url).await?;
                }
                Err(error) => {
                    self.store.mark_relay_error(url, &error.to_string()).await?;
                    tracing::warn!(relay = %url, %error, "relay backfill failed");
                }
            }
        }
        let filter = Filter::new()
            .kinds(PONTMORE_KINDS)
            .since(Timestamp::from(subscription_since));
        self.client.subscribe(filter).await?;
        let mut publish_tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                notification = notifications.next() => match notification {
                    Some(ClientNotification::Event { relay_url, event, .. }) => {
                        if let Err(error) = self.ingest(&event).await { tracing::warn!(event_id = %event.id, %error, "relay event rejected"); }
                        self.store.advance_relay(relay_url.as_str(), event.created_at.as_secs()).await?;
                    }
                    Some(ClientNotification::Shutdown) | None => break,
                    Some(ClientNotification::Message { .. }) => {}
                },
                _ = publish_tick.tick() => self.publish_next().await?,
            }
        }
        self.client.shutdown().await;
        Ok(())
    }

    async fn publish_next(&self) -> Result<(), RelayError> {
        let Some(event) = self.store.next_publication().await? else {
            return Ok(());
        };
        match self.client.send_event(&event).await {
            Ok(_) => {
                self.store
                    .mark_publication_sent(&event.id.to_hex(), None)
                    .await?;
            }
            Err(error) => {
                self.store
                    .mark_publication_sent(&event.id.to_hex(), Some(&error.to_string()))
                    .await?;
                return Ok(());
            }
        }
        let observed = self
            .client
            .fetch_events(Filter::new().id(event.id))
            .timeout(Duration::from_secs(10))
            .max_events(1)
            .await?;
        if let Some(observed) = observed.into_iter().next() {
            self.ingest(&observed).await?;
        }
        Ok(())
    }

    /// Publish a pre-signed event.
    ///
    /// # Errors
    /// Returns a relay delivery error.
    pub async fn publish(&self, event: &Event) -> Result<(), RelayError> {
        self.client.send_event(event).await?;
        Ok(())
    }

    async fn ingest(&self, event: &Event) -> Result<(), RelayError> {
        self.coordinator.validate_for_ingest(event)?;
        let (_, stream_event) = self.store.put_event(event).await?;
        if let Some(stream_event) = stream_event {
            let _ = self.stream_sender.send(stream_event);
        }
        self.coordinator.process_event(event).await?;
        self.store.observe_publication(&event.id.to_hex()).await?;
        Ok(())
    }
}

const fn relay_priority(kind: u16) -> u8 {
    match kind {
        30_360 => 0,
        30_361 => 1,
        7_300 => 2,
        7_301 => 3,
        _ => 4,
    }
}

impl Store {
    async fn next_publication(&self) -> Result<Option<Event>, StoreError> {
        let document: Option<serde_json::Value> = sqlx::query_scalar("UPDATE publication_outbox SET status = 'publishing', attempts = attempts + 1 WHERE event_id = (SELECT event_id FROM publication_outbox WHERE (status IN ('pending','published') OR (status = 'publishing' AND next_attempt_at < now() - interval '60 seconds')) AND next_attempt_at <= now() ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING document")
            .fetch_optional(self.pool()).await?;
        document
            .map(serde_json::from_value)
            .transpose()
            .map_err(StoreError::from)
    }

    async fn mark_publication_sent(
        &self,
        event_id: &str,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        sqlx::query("UPDATE publication_outbox SET status = CASE WHEN $2::text IS NULL THEN 'published' ELSE CASE WHEN attempts >= 12 THEN 'failed' ELSE 'pending' END END, published_at = CASE WHEN $2::text IS NULL THEN now() ELSE published_at END, last_error = $2, next_attempt_at = now() + make_interval(secs => LEAST(300, power(2, attempts))::double precision) WHERE event_id = $1")
            .bind(event_id).bind(error).execute(self.pool()).await?;
        Ok(())
    }

    async fn relay_since(&self, url: &str, lookback: Duration) -> Result<u64, StoreError> {
        let checkpoint: Option<i64> = sqlx::query_scalar("SELECT last_event_created_at FROM relay_checkpoints WHERE relay_url = $1 AND backfill_complete")
        .bind(url).fetch_optional(self.pool())
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

    async fn mark_relay_backfilled(&self, url: &str) -> Result<(), StoreError> {
        sqlx::query("INSERT INTO relay_checkpoints (relay_url, backfill_complete, last_connected_at) VALUES ($1, true, now()) ON CONFLICT (relay_url) DO UPDATE SET backfill_complete = true, last_connected_at = now(), last_error = NULL, updated_at = now()")
            .bind(url).execute(self.pool()).await?;
        Ok(())
    }

    async fn mark_relay_error(&self, url: &str, error: &str) -> Result<(), StoreError> {
        sqlx::query("INSERT INTO relay_checkpoints (relay_url, last_error) VALUES ($1, $2) ON CONFLICT (relay_url) DO UPDATE SET last_error = EXCLUDED.last_error, updated_at = now()")
            .bind(url).bind(error.get(..4_096).unwrap_or(error)).execute(self.pool()).await?;
        Ok(())
    }

    async fn advance_relay(&self, url: &str, created_at: u64) -> Result<(), StoreError> {
        let created_at = i64::try_from(created_at).unwrap_or(i64::MAX);
        sqlx::query("INSERT INTO relay_checkpoints (relay_url, last_event_created_at, backfill_complete, last_connected_at) VALUES ($1, $2, true, now()) ON CONFLICT (relay_url) DO UPDATE SET last_event_created_at = GREATEST(relay_checkpoints.last_event_created_at, EXCLUDED.last_event_created_at), last_connected_at = now(), last_error = NULL, updated_at = now()")
            .bind(url).bind(created_at).execute(self.pool()).await?;
        Ok(())
    }

    /// Mark an outbox event observed through relay ingestion.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn observe_publication(&self, event_id: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE publication_outbox SET status = 'observed', observed_at = now() WHERE event_id = $1 AND status IN ('pending', 'publishing', 'published')")
            .bind(event_id).execute(self.pool()).await?;
        Ok(())
    }

    /// Check whether all required relays recently completed backfill.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn relay_readiness(&self, required: usize) -> Result<bool, StoreError> {
        if required == 0 {
            return Ok(true);
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM relay_checkpoints WHERE backfill_complete AND last_connected_at > now() - interval '5 minutes'")
            .fetch_one(self.pool()).await?;
        Ok(count >= i64::try_from(required).unwrap_or(i64::MAX))
    }
}
