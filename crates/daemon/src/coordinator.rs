use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, RwLock},
};

use nostr::event::Event;
use pontmore::{
    DescriptorBinding, ErrorCode, EscrowDescriptor, ResolutionEffect, ValidationError,
    validate_agent_definition, validate_escrow_descriptor, validate_swap_coordination,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    fs,
    sync::{broadcast, watch},
    time::{Duration, sleep},
};
use uuid::Uuid;

use crate::{
    config::{ConnectorConfig, ExecutionPolicy, SchemaPolicy},
    connector::{
        ConnectorError, ConnectorExecution, ConnectorManager, ConnectorStatus, ConnectorType,
        EconomicOperation,
    },
    schema::{SchemaError, SchemaFetcher},
    store::{OperationStatus, Store, StoreError, StreamEvent},
};

const AGENT_DEFINITION_KIND: u16 = 30_360;
const ESCROW_DESCRIPTOR_KIND: u16 = 30_361;
const COORDINATION_ROOT_KIND: u16 = 7_300;
const COORDINATION_ACTION_KIND: u16 = 7_301;

/// Stable protocol failure returned by signed-event ingestion.
#[derive(Debug, Clone, Serialize)]
pub struct ProtocolFailure {
    pub code: String,
    pub path: String,
    pub message: String,
}

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("protocol validation failed")]
    Protocol(Vec<ProtocolFailure>),
    #[error("descriptor binding configuration: {0}")]
    DescriptorConfiguration(String),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Connector(#[from] ConnectorError),
    #[error("configuration I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("configuration JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Schema(#[from] SchemaError),
}

/// Explicit operator-established PIP-01 facts required by the pure PIP-02 reducer.
#[derive(Clone, Default)]
pub struct DescriptorRegistry {
    bindings: Arc<RwLock<BTreeMap<String, DescriptorBinding>>>,
    records: Arc<RwLock<BTreeMap<String, DescriptorRecord>>>,
    policies: Arc<BTreeMap<String, SchemaPolicy>>,
    store: Option<Store>,
    fetcher: Option<SchemaFetcher>,
    execution_policy: Arc<ExecutionPolicy>,
}

#[derive(Clone)]
struct DescriptorRecord {
    binding: DescriptorBinding,
    allowed_authorities: BTreeSet<String>,
}

impl DescriptorRegistry {
    /// Load descriptor bindings from the static connector configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for unreadable, malformed, or mismatched binding configuration.
    pub async fn load(path: Option<&Path>) -> Result<Self, CoordinatorError> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let config: ConnectorConfig = serde_json::from_slice(&fs::read(path).await?)?;
        for (event_id, binding) in &config.descriptor_bindings {
            if event_id != &binding.event_id {
                return Err(CoordinatorError::DescriptorConfiguration(format!(
                    "binding key `{event_id}` does not match event_id `{}`",
                    binding.event_id
                )));
            }
        }
        Ok(Self {
            bindings: Arc::new(RwLock::new(config.descriptor_bindings)),
            records: Arc::default(),
            policies: Arc::new(config.schema_policies),
            store: None,
            fetcher: None,
            execution_policy: Arc::new(config.execution_policy),
        })
    }

    /// Load policy and hydrate discovered descriptors from storage.
    ///
    /// # Errors
    /// Returns configuration, storage, schema-fetch, or validation errors.
    pub async fn load_with_store(
        path: Option<&Path>,
        store: Store,
        fetcher: SchemaFetcher,
    ) -> Result<Self, CoordinatorError> {
        let mut registry = Self::load(path).await?;
        registry.store = Some(store.clone());
        registry.fetcher = Some(fetcher);
        for event in store.events_of_kind(ESCROW_DESCRIPTOR_KIND).await? {
            if let Err(error) = registry.observe(&event).await {
                tracing::warn!(event_id = %event.id, %error, "stored descriptor is not usable");
            }
        }
        Ok(registry)
    }

    fn for_root(&self, root: &Event) -> Result<DescriptorBinding, CoordinatorError> {
        let event_id = single_marked_event(root, "escrow-version").ok_or_else(|| {
            protocol_failure(
                ErrorCode::InvalidDescriptorBinding,
                "root.tags",
                "root must bind exactly one escrow descriptor event",
            )
        })?;
        if let Some(record) = self
            .records
            .read()
            .expect("descriptor lock poisoned")
            .get(event_id)
        {
            let mut binding = record.binding.clone();
            binding.selectable_at_root = self.is_current_at(event_id, root.created_at.as_secs());
            binding.current_at_acceptance = binding.selectable_at_root;
            for role in ["core/escrow", "core/resolver"] {
                if let Some(pubkey) = single_role_pubkey(root, role)
                    && !record.allowed_authorities.contains(pubkey)
                {
                    binding.authorities_validated = false;
                }
            }
            return Ok(binding);
        }
        self.bindings
            .read()
            .expect("descriptor lock poisoned")
            .get(event_id)
            .cloned()
            .ok_or_else(|| {
                protocol_failure(
                    ErrorCode::InvalidDescriptorBinding,
                    "root.tags",
                    format!("descriptor `{event_id}` is not operator-validated by this node"),
                )
            })
    }

    fn is_current_at(&self, event_id: &str, at: u64) -> bool {
        let records = self.records.read().expect("descriptor lock poisoned");
        let Some(target) = records.get(event_id) else {
            return false;
        };
        records
            .iter()
            .filter(|(_, record)| {
                record.binding.coordinate == target.binding.coordinate
                    && record.binding.created_at <= at
            })
            .max_by(|(left_id, left), (right_id, right)| {
                left.binding
                    .created_at
                    .cmp(&right.binding.created_at)
                    .then_with(|| right_id.cmp(left_id))
            })
            .is_some_and(|(current_id, _)| current_id == event_id)
    }

    fn is_discovered(&self, event_id: &str) -> bool {
        self.records
            .read()
            .expect("descriptor lock poisoned")
            .contains_key(event_id)
    }

    async fn observe(&self, event: &Event) -> Result<(), CoordinatorError> {
        let descriptor: EscrowDescriptor = serde_json::from_str(event.content.as_str())?;
        let d_tag = single_tag(event, "d").ok_or_else(|| {
            CoordinatorError::DescriptorConfiguration("descriptor has no unique d tag".to_owned())
        })?;
        let coordinate = format!("30361:{}:{d_tag}", event.pubkey.to_hex());
        let mut schema_sha256 = None;
        let mut schema_type = None;
        let mut schema_url = None;
        let mut allowed_authorities = BTreeSet::new();
        let mut effects = BTreeSet::new();
        if let Some(service) = descriptor.service {
            let fetcher = self.fetcher.as_ref().ok_or_else(|| {
                CoordinatorError::DescriptorConfiguration(
                    "schema fetcher is unavailable".to_owned(),
                )
            })?;
            let artifact = fetcher
                .fetch(&service.schema.url, &service.schema.schema_type)
                .await?;
            let policy = self.policies.get(&artifact.sha256).ok_or_else(|| {
                CoordinatorError::DescriptorConfiguration(format!(
                    "schema digest {} has no operator-reviewed policy",
                    artifact.sha256
                ))
            })?;
            if policy.sha256 != artifact.sha256 {
                return Err(CoordinatorError::DescriptorConfiguration(
                    "schema policy key and digest differ".to_owned(),
                ));
            }
            allowed_authorities.extend(policy.authority_pubkeys.iter().cloned());
            effects.extend(policy.supported_resolution_effects.iter().copied());
            if let Some(store) = &self.store {
                store
                    .put_schema_artifact(&artifact, &service.schema.url)
                    .await?;
            }
            schema_sha256 = Some(artifact.sha256);
            schema_type = Some(service.schema.schema_type);
            schema_url = Some(service.schema.url);
        }
        let binding = DescriptorBinding {
            event_id: event.id.to_hex(),
            coordinate: coordinate.clone(),
            created_at: event.created_at.as_secs(),
            selectable_at_root: true,
            current_at_acceptance: true,
            expires_at: descriptor.expires_at,
            networks: descriptor.networks.into_iter().collect(),
            authorities_validated: !allowed_authorities.is_empty(),
            supported_resolution_effects: if effects.is_empty() {
                BTreeSet::<ResolutionEffect>::new()
            } else {
                effects
            },
        };
        if let Some(store) = &self.store {
            store
                .put_descriptor_revision(
                    event,
                    &binding,
                    d_tag,
                    schema_type.as_deref(),
                    schema_url.as_deref(),
                    schema_sha256.as_deref(),
                )
                .await?;
        }
        self.records
            .write()
            .expect("descriptor lock poisoned")
            .insert(
                event.id.to_hex(),
                DescriptorRecord {
                    binding,
                    allowed_authorities,
                },
            );
        Ok(())
    }
}

/// Connects signed-event ingestion, deterministic reconstruction, durable operations,
/// and economic connectors without expanding the public client API.
#[derive(Clone)]
pub struct Coordinator {
    store: Store,
    connectors: ConnectorManager,
    descriptors: DescriptorRegistry,
    stream_sender: broadcast::Sender<StreamEvent>,
}

impl Coordinator {
    #[must_use]
    pub const fn new(
        store: Store,
        connectors: ConnectorManager,
        descriptors: DescriptorRegistry,
        stream_sender: broadcast::Sender<StreamEvent>,
    ) -> Self {
        Self {
            store,
            connectors,
            descriptors,
            stream_sender,
        }
    }

    /// Validate all context-free rules before an event enters durable storage.
    ///
    /// Actions may arrive before their root, so contextual action validation occurs
    /// during reconstruction rather than rejecting out-of-order delivery here.
    ///
    /// # Errors
    ///
    /// Returns stable protocol failures for unsupported or malformed events.
    pub fn validate_for_ingest(&self, event: &Event) -> Result<(), CoordinatorError> {
        if event.verify().is_err() {
            return Err(protocol_failure(
                ErrorCode::InvalidEvent,
                "event",
                "invalid Nostr event ID or signature",
            ));
        }
        let tags = event
            .tags
            .iter()
            .map(|tag| tag.as_slice().to_vec())
            .collect::<Vec<_>>();
        match event.kind.as_u16() {
            AGENT_DEFINITION_KIND => {
                validation_result(validate_agent_definition(event.content.as_str(), &tags))
            }
            ESCROW_DESCRIPTOR_KIND => validation_result(validate_escrow_descriptor(
                event.content.as_str(),
                &tags,
                None,
            )),
            COORDINATION_ROOT_KIND => {
                let binding = self.descriptors.for_root(event)?;
                validate_swap_coordination(event, &[], &binding)
                    .map(|_| ())
                    .map_err(|errors| CoordinatorError::Protocol(failures(errors)))
            }
            COORDINATION_ACTION_KIND => validate_action_shape(event),
            kind => Err(CoordinatorError::Protocol(vec![ProtocolFailure {
                code: "UNSUPPORTED_EVENT_KIND".to_owned(),
                path: "event.kind".to_owned(),
                message: format!("event kind {kind} is not in the supported Pontmore profile"),
            }])),
        }
    }

    /// Rebuild any coordination affected by an inserted root or action and execute each
    /// newly eligible economic effect exactly once by durable idempotency key.
    ///
    /// # Errors
    ///
    /// Returns a storage, validation, or connector error.
    pub async fn process_event(&self, event: &Event) -> Result<(), CoordinatorError> {
        if event.kind.as_u16() == ESCROW_DESCRIPTOR_KIND {
            self.descriptors.observe(event).await?;
        }
        let coordination_id = match event.kind.as_u16() {
            COORDINATION_ROOT_KIND => Some(event.id.to_hex()),
            COORDINATION_ACTION_KIND => single_marked_event(event, "root").map(str::to_owned),
            _ => None,
        };
        if let Some(coordination_id) = coordination_id {
            self.rebuild(&coordination_id).await?;
        }
        Ok(())
    }

    /// Run leased durable work until shutdown. Multiple daemon instances may safely
    /// compete for jobs through `PostgreSQL` `SKIP LOCKED` leases.
    pub async fn run_worker(&self, lease: Duration, mut shutdown: watch::Receiver<bool>) {
        let owner = Uuid::new_v4();
        loop {
            if *shutdown.borrow() {
                break;
            }
            if let Err(error) = self.store.expire_work().await {
                tracing::error!(%error, "failed to expire overdue work");
            }
            match self.store.claim_work(owner, lease).await {
                Ok(Some(item)) => {
                    let result = match item.work_type.as_str() {
                        "rebuild" => {
                            match item.payload.get("coordination_id").and_then(Value::as_str) {
                                Some(coordination_id) => self.rebuild(coordination_id).await,
                                None => Err(CoordinatorError::DescriptorConfiguration(
                                    "rebuild work lacks coordination_id".to_owned(),
                                )),
                            }
                        }
                        other => Err(CoordinatorError::DescriptorConfiguration(format!(
                            "unsupported work type `{other}`"
                        ))),
                    };
                    match result {
                        Ok(()) => {
                            if let Err(error) = self.store.complete_work(item.work_id, owner).await
                            {
                                tracing::error!(%error, "failed to complete work");
                            }
                        }
                        Err(error) => {
                            if let Err(store_error) = self
                                .store
                                .retry_work(&item, owner, &error.to_string())
                                .await
                            {
                                tracing::error!(%store_error, "failed to retry work");
                            }
                        }
                    }
                }
                Ok(None) => tokio::select! {
                    () = sleep(Duration::from_millis(200)) => {},
                    _ = shutdown.changed() => {},
                },
                Err(error) => {
                    tracing::error!(%error, "failed to claim work");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Reconstruct all stored roots after startup to repair projections interrupted by
    /// a prior daemon exit.
    ///
    /// # Errors
    ///
    /// Returns the first storage, validation, or connector failure.
    pub async fn rebuild_all(&self) -> Result<(), CoordinatorError> {
        for coordination_id in self.store.coordination_ids().await? {
            self.rebuild(&coordination_id).await?;
        }
        Ok(())
    }

    /// Inspect every ambiguous connector operation before allowing later retries.
    ///
    /// # Errors
    ///
    /// Returns a storage error. Connector inspection failures leave the operation
    /// ambiguous and do not abort reconciliation of other operations.
    pub async fn reconcile_ambiguous(&self) -> Result<(), CoordinatorError> {
        for operation in self.store.ambiguous_operations().await? {
            let result = self
                .connectors
                .inspect(&operation.connector_id, operation.operation_id)
                .await;
            let (status, response, error, failure_class) = match result {
                Ok(result) => (
                    operation_status(result.status),
                    Some(result.result),
                    result.error,
                    (result.status == ConnectorStatus::Failed).then_some(if result.retryable {
                        "retryable"
                    } else {
                        "permanent"
                    }),
                ),
                Err(error) => (
                    OperationStatus::Ambiguous,
                    None,
                    Some(error.to_string()),
                    None,
                ),
            };
            let stream_event = self
                .store
                .set_operation_status(
                    operation.operation_id,
                    status,
                    response.as_ref(),
                    error.as_deref(),
                    failure_class,
                )
                .await?;
            self.publish(stream_event);
        }
        Ok(())
    }

    async fn rebuild(&self, coordination_id: &str) -> Result<(), CoordinatorError> {
        let (Some(root), actions) = self.store.coordination_events(coordination_id).await? else {
            return Ok(());
        };
        let mut binding = self.descriptors.for_root(&root)?;
        if let Some(accepted_at) = actions
            .iter()
            .filter_map(|event| {
                let content: ActionContent = serde_json::from_str(event.content.as_str()).ok()?;
                (content.action == "core/accept").then_some(event.created_at.as_secs())
            })
            .max()
        {
            if let Some(event_id) = single_marked_event(&root, "escrow-version") {
                if self.descriptors.is_discovered(event_id) {
                    binding.current_at_acceptance =
                        self.descriptors.is_current_at(event_id, accepted_at);
                }
            }
        }
        let reconstruction = validate_swap_coordination(&root, &actions, &binding)
            .map_err(|errors| CoordinatorError::Protocol(failures(errors)))?;
        if let Some(stream_event) = self.store.put_projection(&reconstruction).await? {
            self.publish(stream_event);
        }
        self.execute_eligible_effects(&root, &actions, &reconstruction.applied)
            .await
    }

    async fn execute_eligible_effects(
        &self,
        root: &Event,
        actions: &[Event],
        applied: &[String],
    ) -> Result<(), CoordinatorError> {
        let routing: RootRouting = serde_json::from_str(root.content.as_str())?;
        let amount = routing.terms.bitcoin.amount.parse::<u64>().map_err(|_| {
            protocol_failure(
                ErrorCode::InvalidTerms,
                "root.content.terms.bitcoin.amount",
                "bitcoin amount is not an integer",
            )
        })?;
        if self
            .descriptors
            .execution_policy
            .max_automatic_sats
            .is_none_or(|limit| amount > limit)
            && !self
                .descriptors
                .execution_policy
                .approved_coordination_ids
                .contains(&root.id.to_hex())
        {
            tracing::warn!(coordination_id = %root.id, amount, "economic effects await operator approval");
            return Ok(());
        }
        let escrow_authority = single_role_pubkey(root, "core/escrow").ok_or_else(|| {
            protocol_failure(
                ErrorCode::InvalidRoles,
                "root.tags",
                "root must bind exactly one core/escrow authority",
            )
        })?;
        let resolver_authority = single_role_pubkey(root, "core/resolver");
        let actions = actions
            .iter()
            .map(|event| (event.id.to_hex(), event))
            .collect::<BTreeMap<_, _>>();
        for trigger_event_id in applied {
            if !self.store.execution_eligible(trigger_event_id).await? {
                continue;
            }
            let Some(event) = actions.get(trigger_event_id) else {
                continue;
            };
            let action: ActionContent = serde_json::from_str(event.content.as_str())?;
            let operation = match action.action.as_str() {
                "core/accept" => Some(EconomicOperation::EscrowSecure),
                "core/authorize_settlement" => Some(EconomicOperation::EscrowSettle),
                "core/authorize_refund" => Some(EconomicOperation::EscrowRefund),
                "core/open_dispute" => Some(EconomicOperation::ResolveDispute),
                _ => None,
            };
            let Some(operation) = operation else {
                continue;
            };
            let (connector_type, authority) = if operation == EconomicOperation::ResolveDispute {
                let Some(authority) = resolver_authority else {
                    continue;
                };
                (ConnectorType::Resolver, authority)
            } else {
                (ConnectorType::Escrow, escrow_authority)
            };
            let Some(connector_id) = self.connectors.select(
                connector_type,
                &routing.profile,
                &routing.terms.bitcoin.network,
                authority,
            )?
            else {
                continue;
            };
            self.execute_effect(
                &connector_id,
                &root.id.to_hex(),
                trigger_event_id,
                &routing.profile,
                &routing.terms.bitcoin.network,
                operation,
            )
            .await?;
        }
        Ok(())
    }

    /// Enqueue reconstruction for every stored coordination.
    ///
    /// # Errors
    /// Returns a storage error.
    pub async fn enqueue_all(&self) -> Result<(), CoordinatorError> {
        for coordination_id in self.store.coordination_ids().await? {
            self.store
                .enqueue_work(
                    "rebuild",
                    &format!("rebuild:{coordination_id}"),
                    &json!({"coordination_id": coordination_id}),
                )
                .await?;
        }
        Ok(())
    }

    async fn execute_effect(
        &self,
        connector_id: &str,
        coordination_id: &str,
        trigger_event_id: &str,
        profile: &str,
        network: &str,
        operation: EconomicOperation,
    ) -> Result<(), CoordinatorError> {
        let operation_name = economic_operation_name(operation);
        let idempotency_key = format!("{coordination_id}:{trigger_event_id}:{operation_name}");
        let request = json!({
            "coordination_id": coordination_id,
            "trigger_event_id": trigger_event_id,
            "profile": profile,
            "network": network,
            "operation": operation,
        });
        let (connector_version, connector_digest) = self.connectors.audit_identity(connector_id)?;
        let (record, created, stream_event) = self
            .store
            .create_operation(
                connector_id,
                &idempotency_key,
                coordination_id,
                operation_name,
                trigger_event_id,
                &request,
                &connector_version,
                &connector_digest,
            )
            .await?;
        if let Some(stream_event) = stream_event {
            self.publish(stream_event);
        }
        if !created {
            return Ok(());
        }
        let outcome = self
            .connectors
            .execute(
                connector_id,
                ConnectorExecution {
                    operation_id: record.operation_id,
                    idempotency_key,
                    coordination_id: coordination_id.to_owned(),
                    profile: profile.to_owned(),
                    network: network.to_owned(),
                    operation,
                    payload: json!({"trigger_event_id": trigger_event_id}),
                },
            )
            .await;
        let (status, response, error, failure_class, retry) = self
            .interpret_connector_outcome(
                connector_id,
                coordination_id,
                trigger_event_id,
                operation,
                outcome,
            )
            .await?;
        let stream_event = self
            .store
            .set_operation_status(
                record.operation_id,
                status,
                response.as_ref(),
                error.as_deref(),
                failure_class,
            )
            .await?;
        self.publish(stream_event);
        if retry {
            return Err(CoordinatorError::Connector(ConnectorError::Exited(
                "connector requested a retry".to_owned(),
            )));
        }
        Ok(())
    }

    async fn interpret_connector_outcome(
        &self,
        connector_id: &str,
        coordination_id: &str,
        trigger_event_id: &str,
        operation: EconomicOperation,
        outcome: Result<crate::connector::ConnectorResult, ConnectorError>,
    ) -> Result<
        (
            OperationStatus,
            Option<Value>,
            Option<String>,
            Option<&'static str>,
            bool,
        ),
        CoordinatorError,
    > {
        Ok(match outcome {
            Ok(mut result) => {
                if result.status == ConnectorStatus::Succeeded {
                    if let Some(document) = result.result.get("signed_event") {
                        self.queue_signed_outcome(
                            coordination_id,
                            trigger_event_id,
                            operation,
                            document,
                        )
                        .await?;
                    } else if self.connectors.signs_outcomes(connector_id)? {
                        return Err(CoordinatorError::Connector(
                            ConnectorError::InvalidResponse(
                                "successful connector omitted its required signed_event".to_owned(),
                            ),
                        ));
                    }
                }
                redact_secret_echoes(&mut result.result);
                let retry = result.status == ConnectorStatus::Failed && result.retryable;
                (
                    operation_status(result.status),
                    Some(result.result),
                    result.error,
                    (result.status == ConnectorStatus::Failed).then_some(if retry {
                        "retryable"
                    } else {
                        "permanent"
                    }),
                    retry,
                )
            }
            Err(error) => (
                connector_error_status(&error),
                None,
                Some(error.to_string()),
                None,
                false,
            ),
        })
    }

    async fn queue_signed_outcome(
        &self,
        coordination_id: &str,
        trigger_event_id: &str,
        operation: EconomicOperation,
        document: &Value,
    ) -> Result<(), CoordinatorError> {
        let event: Event = serde_json::from_value(document.clone())?;
        if event.verify().is_err() || event.kind.as_u16() != COORDINATION_ACTION_KIND {
            return Err(protocol_failure(
                ErrorCode::InvalidEvent,
                "connector.result.signed_event",
                "connector outcome must be a valid signed kind 7301 event",
            ));
        }
        let expected = match operation {
            EconomicOperation::EscrowSecure => "core/secure",
            EconomicOperation::EscrowSettle => "core/settle",
            EconomicOperation::EscrowRefund => "core/refund",
            EconomicOperation::ResolveDispute => "core/resolve_dispute",
        };
        let content: ActionContent = serde_json::from_str(event.content.as_str())?;
        if content.action != expected
            || single_marked_event(&event, "root") != Some(coordination_id)
            || single_marked_event(&event, "prev") != Some(trigger_event_id)
        {
            return Err(protocol_failure(
                ErrorCode::InvalidActionData,
                "connector.result.signed_event",
                "signed outcome action, root, or predecessor does not match the authorized effect",
            ));
        }
        let (Some(root), mut actions) = self.store.coordination_events(coordination_id).await?
        else {
            return Ok(());
        };
        actions.push(event.clone());
        let binding = self.descriptors.for_root(&root)?;
        let reconstruction = validate_swap_coordination(&root, &actions, &binding)
            .map_err(|errors| CoordinatorError::Protocol(failures(errors)))?;
        if !reconstruction.applied.contains(&event.id.to_hex()) {
            return Err(protocol_failure(
                ErrorCode::InvalidTransition,
                "connector.result.signed_event",
                "signed outcome did not extend the validated chain tip",
            ));
        }
        self.store
            .enqueue_publication(
                &event,
                coordination_id,
                expected.trim_start_matches("core/"),
            )
            .await?;
        Ok(())
    }

    fn publish(&self, event: StreamEvent) {
        let _ = self.stream_sender.send(event);
    }
}

fn redact_secret_echoes(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if matches!(
                    key.to_ascii_lowercase().as_str(),
                    "secret" | "secrets" | "token" | "credential" | "credentials"
                ) {
                    *value = Value::String("[redacted]".to_owned());
                } else {
                    redact_secret_echoes(value);
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(redact_secret_echoes),
        _ => {}
    }
}

#[derive(Debug, Deserialize)]
struct RootRouting {
    profile: String,
    terms: RoutingTerms,
}

#[derive(Debug, Deserialize)]
struct RoutingTerms {
    bitcoin: RoutingBitcoin,
}

#[derive(Debug, Deserialize)]
struct RoutingBitcoin {
    amount: String,
    network: String,
}

#[derive(Debug, Deserialize)]
struct ActionContent {
    version: u64,
    action: String,
    #[serde(default)]
    data: Option<Value>,
}

fn validate_action_shape(event: &Event) -> Result<(), CoordinatorError> {
    let content: ActionContent = serde_json::from_str(event.content.as_str()).map_err(|error| {
        protocol_failure(
            ErrorCode::InvalidContent,
            "action.content",
            format!("invalid action content: {error}"),
        )
    })?;
    if content.version != 2 {
        return Err(protocol_failure(
            ErrorCode::UnsupportedVersion,
            "action.content.version",
            "action content version must be 2",
        ));
    }
    if !matches!(
        content.action.as_str(),
        "core/accept"
            | "core/decline"
            | "core/secure"
            | "core/authorize_settlement"
            | "core/settle"
            | "core/authorize_refund"
            | "core/refund"
            | "core/cancel"
            | "core/expire"
            | "core/open_dispute"
            | "core/resolve_dispute"
            | "swap/fiat_sent"
            | "swap/fiat_confirmed"
    ) {
        return Err(protocol_failure(
            ErrorCode::UnknownAction,
            "action.content.action",
            format!("unsupported action `{}`", content.action),
        ));
    }
    let _ = content.data;
    if single_marked_event(event, "root").is_none() || single_marked_event(event, "prev").is_none()
    {
        return Err(protocol_failure(
            ErrorCode::InvalidActionTags,
            "action.tags",
            "action must contain exactly one root and one predecessor reference",
        ));
    }
    Ok(())
}

fn validation_result(errors: Vec<ValidationError>) -> Result<(), CoordinatorError> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(CoordinatorError::Protocol(failures(errors)))
    }
}

fn failures(errors: Vec<ValidationError>) -> Vec<ProtocolFailure> {
    errors
        .into_iter()
        .map(|error| ProtocolFailure {
            code: error_code(error.code),
            path: error.path,
            message: error.message,
        })
        .collect()
}

fn protocol_failure(
    code: ErrorCode,
    path: impl Into<String>,
    message: impl Into<String>,
) -> CoordinatorError {
    CoordinatorError::Protocol(vec![ProtocolFailure {
        code: error_code(code),
        path: path.into(),
        message: message.into(),
    }])
}

fn error_code(code: ErrorCode) -> String {
    serde_json::to_value(code)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "PROTOCOL_ERROR".to_owned())
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

fn single_role_pubkey<'a>(event: &'a Event, role: &str) -> Option<&'a str> {
    let values = event
        .tags
        .iter()
        .filter_map(|tag| match tag.as_slice() {
            [kind, pubkey, _, found_role, ..] if kind == "p" && found_role == role => {
                Some(pubkey.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    match values.as_slice() {
        [value] => Some(value),
        _ => None,
    }
}

fn single_tag<'a>(event: &'a Event, kind: &str) -> Option<&'a str> {
    let values = event
        .tags
        .iter()
        .filter_map(|tag| match tag.as_slice() {
            [found, value, ..] if found == kind => Some(value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    match values.as_slice() {
        [value] => Some(value),
        _ => None,
    }
}

const fn economic_operation_name(operation: EconomicOperation) -> &'static str {
    match operation {
        EconomicOperation::EscrowSecure => "escrow_secure",
        EconomicOperation::EscrowSettle => "escrow_settle",
        EconomicOperation::EscrowRefund => "escrow_refund",
        EconomicOperation::ResolveDispute => "resolve_dispute",
    }
}

const fn operation_status(status: ConnectorStatus) -> OperationStatus {
    match status {
        ConnectorStatus::Succeeded => OperationStatus::Succeeded,
        ConnectorStatus::Failed => OperationStatus::Failed,
        ConnectorStatus::Ambiguous => OperationStatus::Ambiguous,
    }
}

const fn connector_error_status(error: &ConnectorError) -> OperationStatus {
    match error {
        ConnectorError::Configuration(_)
        | ConnectorError::NotConfigured(_)
        | ConnectorError::Unsupported(_) => OperationStatus::Failed,
        ConnectorError::Io(_)
        | ConnectorError::Timeout
        | ConnectorError::Exited(_)
        | ConnectorError::InvalidResponse(_) => OperationStatus::Ambiguous,
    }
}

#[cfg(test)]
mod tests {
    use nostr::prelude::{EventBuilder, FinalizeEvent, Keys, Kind, Tag, Timestamp};

    use super::*;

    fn descriptor(keys: &Keys, created_at: u64, expires_at: u64) -> Event {
        EventBuilder::new(
            Kind::Custom(ESCROW_DESCRIPTOR_KIND),
            json!({"version":1,"escrow_type":"cashu_escrow","networks":["cashu"],"expires_at":expires_at}).to_string(),
        )
        .tags([Tag::custom("d", ["cashu-main"]), Tag::custom("t", ["pontmore-network:cashu"])])
        .custom_created_at(Timestamp::from_secs(created_at))
        .finalize(keys)
        .expect("sign descriptor")
    }

    #[tokio::test]
    async fn addressable_revision_is_selected_at_the_evaluation_time() {
        let registry = DescriptorRegistry::default();
        let keys = Keys::generate();
        let first = descriptor(&keys, 100, 1_000);
        let replacement = descriptor(&keys, 200, 1_000);
        registry.observe(&first).await.expect("first");
        registry.observe(&replacement).await.expect("replacement");
        assert!(registry.is_current_at(&first.id.to_hex(), 150));
        assert!(!registry.is_current_at(&first.id.to_hex(), 250));
        assert!(registry.is_current_at(&replacement.id.to_hex(), 250));
    }

    #[test]
    fn connector_secret_echoes_are_removed_recursively() {
        let mut value = json!({"nested":{"token":"private","safe":1},"secrets":["private"]});
        redact_secret_echoes(&mut value);
        assert_eq!(value["nested"]["token"], "[redacted]");
        assert_eq!(value["secrets"], "[redacted]");
        assert_eq!(value["nested"]["safe"], 1);
    }
}
