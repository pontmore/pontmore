use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
    time::timeout,
};
use uuid::Uuid;

use crate::config::ConnectorConfig;

pub const CONNECTOR_API_VERSION: u64 = 1;
const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;

/// Economic connector category. Protocol validators are never connectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorType {
    Escrow,
    Resolver,
}

/// Closed set of economic effects supported by connector API version 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EconomicOperation {
    EscrowSecure,
    EscrowSettle,
    EscrowRefund,
    ResolveDispute,
}

/// One daemon-authorized economic effect sent to a connector.
#[derive(Debug, Clone)]
pub struct ConnectorExecution {
    pub operation_id: Uuid,
    pub idempotency_key: String,
    pub coordination_id: String,
    pub profile: String,
    pub network: String,
    pub operation: EconomicOperation,
    pub payload: Value,
}

/// Strict static manifest for a long-lived connector subprocess.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorManifest {
    pub id: String,
    pub package_version: String,
    pub connector_api_version: u64,
    pub connector_type: ConnectorType,
    #[serde(default)]
    pub supported_profiles: Vec<String>,
    #[serde(default)]
    pub supported_networks: Vec<String>,
    #[serde(default)]
    pub authority_pubkeys: Vec<String>,
    pub entrypoint: PathBuf,
    pub entrypoint_sha256: String,
    pub source: String,
    #[serde(default)]
    pub operation_secrets: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub signs_outcomes: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ConnectorCommand {
    Hello {
        daemon_version: String,
    },
    Health,
    Execute {
        operation_id: Uuid,
        idempotency_key: String,
        coordination_id: String,
        profile: String,
        network: String,
        operation: EconomicOperation,
        payload: Value,
        secrets: BTreeMap<String, String>,
    },
    Inspect {
        operation_id: Uuid,
    },
    Shutdown,
}

#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
struct ConnectorRequest {
    version: u64,
    request_id: Uuid,
    command: ConnectorCommand,
}

/// Connector response status. This is operational evidence, not protocol state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorStatus {
    Succeeded,
    Failed,
    Ambiguous,
}

/// Strict response returned by a connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorResult {
    pub version: u64,
    pub request_id: Uuid,
    pub status: ConnectorStatus,
    #[serde(default)]
    pub result: Value,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub retryable: bool,
}

#[derive(Debug, Clone)]
pub struct ConnectorHealth {
    pub connector_id: String,
    pub package_version: String,
    pub entrypoint_sha256: String,
    pub healthy: bool,
    pub error: Option<String>,
}

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("connector configuration: {0}")]
    Configuration(String),
    #[error("connector `{0}` is not configured")]
    NotConfigured(String),
    #[error("connector does not support requested operation: {0}")]
    Unsupported(String),
    #[error("connector I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("connector exchange timed out")]
    Timeout,
    #[error("connector process exited: {0}")]
    Exited(String),
    #[error("invalid connector response: {0}")]
    InvalidResponse(String),
}

struct ConnectorProcess {
    entrypoint: PathBuf,
    expected_digest: String,
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    exchange_timeout: Duration,
}

impl ConnectorProcess {
    async fn spawn(
        entrypoint: PathBuf,
        expected_digest: String,
        exchange_timeout: Duration,
        sandbox: Option<&crate::config::SandboxConfig>,
    ) -> Result<Self, ConnectorError> {
        verify_digest(&entrypoint, &expected_digest).await?;
        let mut command = if let Some(sandbox) = sandbox {
            let mut command = Command::new(&sandbox.launcher);
            command.args(&sandbox.args).arg("--").arg(&entrypoint);
            command
        } else {
            Command::new(&entrypoint)
        };
        let mut child = command
            .env_clear()
            .env(
                "PONTMORE_CONNECTOR_API_VERSION",
                CONNECTOR_API_VERSION.to_string(),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child.stdin.take().ok_or_else(|| {
            ConnectorError::InvalidResponse("connector stdin was unavailable".to_owned())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            ConnectorError::InvalidResponse("connector stdout was unavailable".to_owned())
        })?;
        if let Some(stderr) = child.stderr.take() {
            let label = entrypoint.display().to_string();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let bounded = line.get(..4_096).unwrap_or(&line);
                    tracing::warn!(connector = %label, stderr = %bounded, "connector diagnostic");
                }
            });
        }
        let mut process = Self {
            entrypoint,
            expected_digest,
            child,
            stdin,
            stdout: BufReader::new(stdout),
            exchange_timeout,
        };
        process
            .exchange(ConnectorCommand::Hello {
                daemon_version: env!("CARGO_PKG_VERSION").to_owned(),
            })
            .await?;
        Ok(process)
    }

    async fn exchange(
        &mut self,
        command: ConnectorCommand,
    ) -> Result<ConnectorResult, ConnectorError> {
        verify_digest(&self.entrypoint, &self.expected_digest).await?;
        if let Some(status) = self.child.try_wait()? {
            return Err(ConnectorError::Exited(status.to_string()));
        }
        let request_id = Uuid::new_v4();
        let request = ConnectorRequest {
            version: CONNECTOR_API_VERSION,
            request_id,
            command,
        };
        let bytes = serde_json::to_vec(&request)
            .map_err(|error| ConnectorError::InvalidResponse(error.to_string()))?;
        let exchange = async {
            self.stdin.write_all(&bytes).await?;
            self.stdin.write_all(b"\n").await?;
            self.stdin.flush().await?;
            let mut line = String::new();
            let read = (&mut self.stdout)
                .take(MAX_RESPONSE_BYTES + 1)
                .read_line(&mut line)
                .await?;
            if read == 0 {
                return Err(ConnectorError::InvalidResponse(
                    "connector closed stdout".to_owned(),
                ));
            }
            if u64::try_from(read).unwrap_or(u64::MAX) > MAX_RESPONSE_BYTES {
                return Err(ConnectorError::InvalidResponse(
                    "connector response exceeded one MiB".to_owned(),
                ));
            }
            let response: ConnectorResult = serde_json::from_str(&line)
                .map_err(|error| ConnectorError::InvalidResponse(error.to_string()))?;
            if response.version != CONNECTOR_API_VERSION {
                return Err(ConnectorError::InvalidResponse(
                    "connector API version does not match".to_owned(),
                ));
            }
            if response.request_id != request_id {
                return Err(ConnectorError::InvalidResponse(
                    "response request_id does not match".to_owned(),
                ));
            }
            Ok(response)
        };
        if let Ok(result) = timeout(self.exchange_timeout, exchange).await {
            result
        } else {
            let _ = self.child.kill().await;
            Err(ConnectorError::Timeout)
        }
    }
}

struct InstalledConnector {
    manifest: ConnectorManifest,
    entrypoint: PathBuf,
    expected_digest: String,
    exchange_timeout: Duration,
    process: Mutex<Option<ConnectorProcess>>,
    sandbox: Option<crate::config::SandboxConfig>,
    secrets: BTreeMap<String, PathBuf>,
    failures: Mutex<FailureState>,
}

#[derive(Default)]
struct FailureState {
    consecutive: u32,
    retry_after: Option<tokio::time::Instant>,
}

impl InstalledConnector {
    async fn exchange(&self, command: ConnectorCommand) -> Result<ConnectorResult, ConnectorError> {
        {
            let failures = self.failures.lock().await;
            if failures.consecutive >= 8 {
                return Err(ConnectorError::Exited(
                    "restart budget exhausted".to_owned(),
                ));
            }
            if failures
                .retry_after
                .is_some_and(|deadline| deadline > tokio::time::Instant::now())
            {
                return Err(ConnectorError::Exited(
                    "restart backoff is active".to_owned(),
                ));
            }
        }
        let mut process = self.process.lock().await;
        if process.is_none() {
            *process = Some(
                ConnectorProcess::spawn(
                    self.entrypoint.clone(),
                    self.expected_digest.clone(),
                    self.exchange_timeout,
                    self.sandbox.as_ref(),
                )
                .await?,
            );
        }
        let result = process
            .as_mut()
            .expect("connector process was initialized")
            .exchange(command)
            .await;
        if result.is_err()
            && let Some(mut failed) = process.take()
        {
            let _ = failed.child.kill().await;
        }
        let mut failures = self.failures.lock().await;
        if result.is_ok() {
            failures.consecutive = 0;
            failures.retry_after = None;
        } else {
            failures.consecutive = failures.consecutive.saturating_add(1);
            let delay = 2_u64.pow(failures.consecutive.min(8)).min(300);
            failures.retry_after = Some(tokio::time::Instant::now() + Duration::from_secs(delay));
        }
        result
    }
}

/// Static manager for long-lived economic connector subprocesses.
#[derive(Clone, Default)]
pub struct ConnectorManager {
    connectors: Arc<BTreeMap<String, Arc<InstalledConnector>>>,
}

impl ConnectorManager {
    /// Load, verify, start, and handshake with every enabled connector.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration, a manifest, an entrypoint, or startup fails.
    pub async fn load(
        path: Option<&Path>,
        exchange_timeout: Duration,
    ) -> Result<Self, ConnectorError> {
        Self::load_with_policy(path, exchange_timeout, false).await
    }

    /// Load connectors and optionally require an OS sandbox.
    ///
    /// # Errors
    /// Returns configuration, digest, spawn, or handshake errors.
    pub async fn load_with_policy(
        path: Option<&Path>,
        exchange_timeout: Duration,
        require_sandbox: bool,
    ) -> Result<Self, ConnectorError> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let config: ConnectorConfig = serde_json::from_slice(&fs::read(path).await?)
            .map_err(|error| ConnectorError::Configuration(error.to_string()))?;
        let config_base = path.parent().unwrap_or_else(|| Path::new("."));
        let mut connectors = BTreeMap::new();
        for (expected_id, registration) in config.connectors {
            if !registration.enabled {
                continue;
            }
            if require_sandbox && registration.sandbox.is_none() {
                return Err(ConnectorError::Configuration(format!(
                    "connector `{expected_id}` has no required sandbox launcher"
                )));
            }
            let manifest_path = if registration.manifest.is_absolute() {
                registration.manifest
            } else {
                config_base.join(registration.manifest)
            };
            let manifest: ConnectorManifest =
                serde_json::from_slice(&fs::read(&manifest_path).await?)
                    .map_err(|error| ConnectorError::Configuration(error.to_string()))?;
            validate_manifest(&expected_id, &manifest)?;
            let base = manifest_path.parent().unwrap_or_else(|| Path::new("."));
            let entrypoint = base.join(&manifest.entrypoint);
            let process = ConnectorProcess::spawn(
                entrypoint.clone(),
                manifest.entrypoint_sha256.clone(),
                exchange_timeout,
                registration.sandbox.as_ref(),
            )
            .await?;
            let secrets = registration
                .secrets
                .into_iter()
                .map(|(name, path)| {
                    let path = if path.is_absolute() {
                        path
                    } else {
                        config_base.join(path)
                    };
                    (name, path)
                })
                .collect();
            connectors.insert(
                expected_id,
                Arc::new(InstalledConnector {
                    manifest,
                    entrypoint,
                    expected_digest: process.expected_digest.clone(),
                    exchange_timeout,
                    process: Mutex::new(Some(process)),
                    sandbox: registration.sandbox,
                    secrets,
                    failures: Mutex::new(FailureState::default()),
                }),
            );
        }
        Ok(Self {
            connectors: Arc::new(connectors),
        })
    }

    #[must_use]
    pub fn manifests(&self) -> Vec<ConnectorManifest> {
        self.connectors
            .values()
            .map(|connector| connector.manifest.clone())
            .collect()
    }

    /// Return the pinned package version and executable digest.
    ///
    /// # Errors
    /// Returns an error when the connector is not configured.
    pub fn audit_identity(&self, connector_id: &str) -> Result<(String, String), ConnectorError> {
        let connector = self
            .connectors
            .get(connector_id)
            .ok_or_else(|| ConnectorError::NotConfigured(connector_id.to_owned()))?;
        Ok((
            connector.manifest.package_version.clone(),
            connector.manifest.entrypoint_sha256.clone(),
        ))
    }

    /// Whether the connector contract requires every successful effect to carry a signed outcome.
    ///
    /// # Errors
    /// Returns an error when the connector is not configured.
    pub fn signs_outcomes(&self, connector_id: &str) -> Result<bool, ConnectorError> {
        self.connectors
            .get(connector_id)
            .map(|connector| connector.manifest.signs_outcomes)
            .ok_or_else(|| ConnectorError::NotConfigured(connector_id.to_owned()))
    }

    /// Select the only connector authorized for an exact profile, network, and
    /// protocol authority. Returns `None` when this node does not operate that authority.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration has more than one eligible connector.
    pub fn select(
        &self,
        connector_type: ConnectorType,
        profile: &str,
        network: &str,
        authority_pubkey: &str,
    ) -> Result<Option<String>, ConnectorError> {
        let matches = self
            .connectors
            .values()
            .filter(|connector| {
                connector.manifest.connector_type == connector_type
                    && connector
                        .manifest
                        .supported_profiles
                        .iter()
                        .any(|supported| supported == profile)
                    && connector
                        .manifest
                        .supported_networks
                        .iter()
                        .any(|supported| supported == network)
                    && connector
                        .manifest
                        .authority_pubkeys
                        .iter()
                        .any(|supported| supported == authority_pubkey)
            })
            .map(|connector| connector.manifest.id.clone())
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [] => Ok(None),
            [connector_id] => Ok(Some(connector_id.clone())),
            _ => Err(ConnectorError::Configuration(format!(
                "multiple connectors match {connector_type:?}, `{profile}`, `{network}`, and authority `{authority_pubkey}`"
            ))),
        }
    }

    /// Probe one connector over its existing subprocess session.
    ///
    /// # Errors
    ///
    /// Returns an error when the connector is absent, unhealthy, or returns invalid data.
    pub async fn health(&self, connector_id: &str) -> Result<ConnectorResult, ConnectorError> {
        self.exchange(connector_id, ConnectorCommand::Health).await
    }

    pub async fn health_all(&self) -> Vec<ConnectorHealth> {
        let mut health = Vec::with_capacity(self.connectors.len());
        for connector in self.connectors.values() {
            let outcome = connector.exchange(ConnectorCommand::Health).await;
            health.push(ConnectorHealth {
                connector_id: connector.manifest.id.clone(),
                package_version: connector.manifest.package_version.clone(),
                entrypoint_sha256: connector.manifest.entrypoint_sha256.clone(),
                healthy: outcome
                    .as_ref()
                    .is_ok_and(|result| result.status == ConnectorStatus::Succeeded),
                error: outcome.err().map(|error| error.to_string()),
            });
        }
        health
    }

    /// Execute one daemon-authorized economic operation.
    ///
    /// This method is intentionally not exposed through the client HTTP API.
    ///
    /// # Errors
    ///
    /// Returns an error when the connector is absent, unhealthy, or returns invalid data.
    pub async fn execute(
        &self,
        connector_id: &str,
        execution: ConnectorExecution,
    ) -> Result<ConnectorResult, ConnectorError> {
        let connector = self
            .connectors
            .get(connector_id)
            .ok_or_else(|| ConnectorError::NotConfigured(connector_id.to_owned()))?;
        let required_type = match execution.operation {
            EconomicOperation::EscrowSecure
            | EconomicOperation::EscrowSettle
            | EconomicOperation::EscrowRefund => ConnectorType::Escrow,
            EconomicOperation::ResolveDispute => ConnectorType::Resolver,
        };
        if connector.manifest.connector_type != required_type {
            return Err(ConnectorError::Unsupported(format!(
                "connector `{connector_id}` has the wrong type for `{:?}`",
                execution.operation
            )));
        }
        if !connector
            .manifest
            .supported_profiles
            .contains(&execution.profile)
        {
            return Err(ConnectorError::Unsupported(format!(
                "connector `{connector_id}` does not support profile `{}`",
                execution.profile
            )));
        }
        if !connector
            .manifest
            .supported_networks
            .contains(&execution.network)
        {
            return Err(ConnectorError::Unsupported(format!(
                "connector `{connector_id}` does not support network `{}`",
                execution.network
            )));
        }
        self.exchange(
            connector_id,
            ConnectorCommand::Execute {
                operation_id: execution.operation_id,
                idempotency_key: execution.idempotency_key,
                coordination_id: execution.coordination_id,
                profile: execution.profile,
                network: execution.network,
                operation: execution.operation,
                payload: execution.payload,
                secrets: connector.load_secrets(execution.operation).await?,
            },
        )
        .await
    }

    /// Inspect an existing operation before deciding whether it can be retried.
    ///
    /// # Errors
    ///
    /// Returns an error when the connector is absent, unhealthy, or returns invalid data.
    pub async fn inspect(
        &self,
        connector_id: &str,
        operation_id: Uuid,
    ) -> Result<ConnectorResult, ConnectorError> {
        self.exchange(connector_id, ConnectorCommand::Inspect { operation_id })
            .await
    }

    async fn exchange(
        &self,
        connector_id: &str,
        command: ConnectorCommand,
    ) -> Result<ConnectorResult, ConnectorError> {
        let connector = self
            .connectors
            .get(connector_id)
            .ok_or_else(|| ConnectorError::NotConfigured(connector_id.to_owned()))?;
        connector.exchange(command).await
    }

    /// Ask every connector to stop cleanly.
    pub async fn shutdown(&self) {
        for connector in self.connectors.values() {
            let mut process = connector.process.lock().await;
            if let Some(mut running) = process.take() {
                let _ = running.exchange(ConnectorCommand::Shutdown).await;
                let _ = running.child.wait().await;
            }
        }
    }
}

impl InstalledConnector {
    async fn load_secrets(
        &self,
        operation: EconomicOperation,
    ) -> Result<BTreeMap<String, String>, ConnectorError> {
        let operation = economic_operation_name(operation);
        let required = self
            .manifest
            .operation_secrets
            .get(operation)
            .cloned()
            .unwrap_or_default();
        let mut values = BTreeMap::new();
        for name in required {
            let path = self.secrets.get(&name).ok_or_else(|| {
                ConnectorError::Configuration(format!("secret `{name}` is not configured"))
            })?;
            let metadata = fs::metadata(path).await?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o077 != 0 {
                    return Err(ConnectorError::Configuration(format!(
                        "secret file {} must not be group/world accessible",
                        path.display()
                    )));
                }
            }
            let value = fs::read_to_string(path).await?;
            values.insert(name, value.trim_end().to_owned());
        }
        Ok(values)
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

fn validate_manifest(
    expected_id: &str,
    manifest: &ConnectorManifest,
) -> Result<(), ConnectorError> {
    if manifest.id != expected_id || !valid_id(&manifest.id) {
        return Err(ConnectorError::Configuration(format!(
            "invalid or mismatched connector id `{}`",
            manifest.id
        )));
    }
    if manifest.connector_api_version != CONNECTOR_API_VERSION {
        return Err(ConnectorError::Configuration(format!(
            "connector `{expected_id}` uses unsupported API version {}",
            manifest.connector_api_version
        )));
    }
    if manifest.entrypoint.is_absolute()
        || manifest
            .entrypoint
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(ConnectorError::Configuration(format!(
            "connector `{expected_id}` entrypoint must remain below its manifest directory"
        )));
    }
    if manifest.source.trim().is_empty() {
        return Err(ConnectorError::Configuration(format!(
            "connector `{expected_id}` requires source provenance"
        )));
    }
    if manifest.authority_pubkeys.is_empty()
        || manifest.authority_pubkeys.iter().any(|pubkey| {
            pubkey.len() != 64
                || !pubkey
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    {
        return Err(ConnectorError::Configuration(format!(
            "connector `{expected_id}` requires lowercase 32-byte authority pubkeys"
        )));
    }
    Ok(())
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-._/".contains(&byte)
        })
}

async fn verify_digest(path: &Path, expected: &str) -> Result<(), ConnectorError> {
    if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ConnectorError::Configuration(
            "entrypoint_sha256 must contain 64 hexadecimal characters".to_owned(),
        ));
    }
    let actual = hex::encode(Sha256::digest(fs::read(path).await?));
    if !actual.eq_ignore_ascii_case(expected) {
        return Err(ConnectorError::Configuration(format!(
            "entrypoint digest mismatch for {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, json};

    #[test]
    fn manifest_rejects_parent_entrypoint() {
        let manifest = ConnectorManifest {
            id: "example/escrow".to_owned(),
            package_version: "1.0.0".to_owned(),
            connector_api_version: 1,
            connector_type: ConnectorType::Escrow,
            supported_profiles: vec![],
            supported_networks: vec![],
            authority_pubkeys: vec!["0".repeat(64)],
            entrypoint: PathBuf::from("../escape"),
            entrypoint_sha256: "0".repeat(64),
            source: "https://example.test/source".to_owned(),
            operation_secrets: BTreeMap::new(),
            signs_outcomes: false,
        };
        assert!(validate_manifest("example/escrow", &manifest).is_err());
    }

    #[tokio::test]
    async fn simulated_connector_reuses_one_correlated_process() {
        let config =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/connectors.example.json");
        let manager = ConnectorManager::load(Some(&config), Duration::from_secs(2))
            .await
            .expect("example connector should load");
        let health = manager
            .health("pontmore/simulated-escrow")
            .await
            .expect("health should succeed");
        let operation_id = Uuid::new_v4();
        let execution = manager
            .execute(
                "pontmore/simulated-escrow",
                ConnectorExecution {
                    operation_id,
                    idempotency_key: "coordination:accept:escrow_secure".to_owned(),
                    coordination_id: "coordination".to_owned(),
                    profile: "pontmore/swap@1".to_owned(),
                    network: "regtest".to_owned(),
                    operation: EconomicOperation::EscrowSecure,
                    payload: Value::Object(Map::default()),
                },
            )
            .await
            .expect("execution should succeed");
        assert_eq!(execution.status, ConnectorStatus::Succeeded);
        assert_eq!(health.result["pid"], execution.result["pid"]);
        let inspection = manager
            .inspect("pontmore/simulated-escrow", operation_id)
            .await
            .expect("inspection should succeed");
        assert_eq!(inspection.result["status"], json!("succeeded"));
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn failed_connector_is_restarted_before_the_next_exchange() {
        let config =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/connectors.example.json");
        let manager = ConnectorManager::load(Some(&config), Duration::from_secs(2))
            .await
            .expect("example connector should load");
        let failed = manager
            .execute(
                "pontmore/simulated-escrow",
                ConnectorExecution {
                    operation_id: Uuid::new_v4(),
                    idempotency_key: "coordination:failure:escrow_secure".to_owned(),
                    coordination_id: "coordination".to_owned(),
                    profile: "pontmore/swap@1".to_owned(),
                    network: "regtest".to_owned(),
                    operation: EconomicOperation::EscrowSecure,
                    payload: json!({"inject_failure": "exit"}),
                },
            )
            .await;
        assert!(failed.is_err());
        tokio::time::sleep(Duration::from_millis(2_100)).await;
        let health = manager
            .health("pontmore/simulated-escrow")
            .await
            .expect("next exchange should start and handshake with a replacement process");
        assert_eq!(health.status, ConnectorStatus::Succeeded);
        manager.shutdown().await;
    }

    #[tokio::test]
    async fn malformed_and_timed_out_connectors_are_contained() {
        let config =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/connectors.example.json");
        for (failure, expected) in [
            ("malformed", "invalid connector response"),
            ("timeout", "timed out"),
        ] {
            let manager = ConnectorManager::load(Some(&config), Duration::from_millis(100))
                .await
                .expect("load connector");
            let error = manager
                .execute(
                    "pontmore/simulated-escrow",
                    ConnectorExecution {
                        operation_id: Uuid::new_v4(),
                        idempotency_key: format!("test:{failure}"),
                        coordination_id: "coordination".to_owned(),
                        profile: "pontmore/swap@1".to_owned(),
                        network: "regtest".to_owned(),
                        operation: EconomicOperation::EscrowSecure,
                        payload: json!({"inject_failure": failure}),
                    },
                )
                .await
                .expect_err("fault must be contained");
            assert!(error.to_string().contains(expected), "{error}");
            manager.shutdown().await;
        }
    }
}
