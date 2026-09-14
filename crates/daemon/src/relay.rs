use std::time::Duration;

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
