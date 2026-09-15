use std::{collections::BTreeSet, time::Duration};

use futures_util::StreamExt;
use nostr::event::Event;
use nostr_sdk::{
    client::{Client, ClientNotification, ReqTarget},
    prelude::{Filter, Kind, Timestamp},
};
use thiserror::Error;
use tokio::{
    sync::{broadcast, watch},
    task::JoinSet,
};

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
    #[error("relay subscription: {0}")]
    Subscription(String),
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
        let mut notifications = self.client.notifications();
        let mut publish_tick = tokio::time::interval(Duration::from_secs(1));
        let mut relay_tick = tokio::time::interval(Duration::from_secs(5));
        relay_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut known = BTreeSet::new();
        let mut inflight = BTreeSet::new();
        let mut active: BTreeSet<String> = BTreeSet::new();
        let mut setup_tasks = JoinSet::new();
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
                _ = publish_tick.tick(), if !active.is_empty() => self.publish_next().await?,
                _ = relay_tick.tick() => {
                    match self.store.effective_relay_urls(&self.urls).await {
                        Ok(urls) => {
                            for url in urls {
                                if active.contains(&url) || inflight.contains(&url) { continue; }
                                if !known.contains(&url) {
                                    match self.client.add_relay(url.as_str()).await {
                                        Ok(_) => { known.insert(url.clone()); }
                                        Err(error) => {
                                            self.store.mark_relay_error(&url, &error.to_string()).await?;
                                            tracing::warn!(relay = %url, %error, "relay registration failed");
                                            continue;
                                        }
                                    }
                                }
                                inflight.insert(url.clone());
                                let service = self.clone();
                                setup_tasks.spawn(async move {
                                    let result = service.setup_relay(&url).await;
                                    (url, result)
                                });
                            }
                            for url in &active {
                                if let Some(relay) = self.client.relay(url.as_str()).await? {
                                    if relay.status().is_connected() {
                                        self.store.mark_relay_connected(url).await?;
                                    } else {
                                        self.store.mark_relay_disconnected(url).await?;
                                    }
                                }
                            }
                        }
                        Err(error) => tracing::warn!(%error, "relay discovery failed; retrying"),
                    }
                },
                Some(completed) = setup_tasks.join_next(), if !setup_tasks.is_empty() => {
                    match completed {
                        Ok((url, Ok(()))) => {
                            inflight.remove(&url);
                            active.insert(url);
                        }
                        Ok((url, Err(error))) => {
                            inflight.remove(&url);
                            self.store.mark_relay_error(&url, &error.to_string()).await?;
                            tracing::warn!(relay = %url, %error, "relay setup failed; retrying");
                        }
                        Err(error) => {
                            inflight.clear();
                            tracing::error!(%error, "relay setup task failed");
                        }
                    }
                },
            }
        }
        setup_tasks.abort_all();
        self.client.shutdown().await;
        Ok(())
    }

    async fn setup_relay(&self, url: &str) -> Result<(), RelayError> {
        self.client
            .try_connect_relay(url, Duration::from_secs(10))
            .await?;
        let since = self.store.relay_since(url, self.lookback).await?;
        let filter = Filter::new()
            .kinds(PONTMORE_KINDS)
            .since(Timestamp::from(since));
        let events = self
            .client
            .fetch_events(ReqTarget::single(url, [filter.clone()]))
            .timeout(Duration::from_secs(30))
            .max_events(50_000)
            .await?;
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
        let subscription = self
            .client
            .subscribe(ReqTarget::single(url, [filter]))
            .await?;
        if subscription.success.is_empty() {
            return Err(RelayError::Subscription(
                subscription
                    .failed
                    .values()
                    .next()
                    .cloned()
                    .unwrap_or_else(|| "no relay accepted the subscription".to_owned()),
            ));
        }
        self.store.mark_relay_backfilled(url).await?;
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
