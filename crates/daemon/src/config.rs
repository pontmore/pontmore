use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, time::Duration};

use clap::Parser;
use pontmore::{DescriptorBinding, ResolutionEffect};
use serde::{Deserialize, Serialize};

/// Command-line and environment configuration.
#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub struct Config {
    /// `PostgreSQL` connection URL.
    #[arg(long, env = "PONTMORE_DATABASE_URL")]
    pub database_url: String,

    /// HTTP listener address.
    #[arg(long, env = "PONTMORE_LISTEN", default_value = "127.0.0.1:7300")]
    pub listen: SocketAddr,

    /// JSON file containing the static economic-connector configuration.
    #[arg(long, env = "PONTMORE_CONNECTOR_CONFIG")]
    pub connector_config: Option<PathBuf>,

    /// Maximum time allowed for one connector exchange.
    #[arg(long, env = "PONTMORE_CONNECTOR_TIMEOUT_MS", default_value_t = 30_000)]
    pub connector_timeout_ms: u64,

    /// Comma-separated read/write relay URLs. Empty disables relay synchronization.
    #[arg(long, env = "PONTMORE_RELAYS", value_delimiter = ',')]
    pub relays: Vec<String>,

    /// Backfill horizon used when a relay has no durable checkpoint.
    #[arg(long, env = "PONTMORE_RELAY_LOOKBACK_SECS", default_value_t = 604_800)]
    pub relay_lookback_secs: u64,

    /// Maximum service-schema response size.
    #[arg(long, env = "PONTMORE_SCHEMA_MAX_BYTES", default_value_t = 2_097_152)]
    pub schema_max_bytes: usize,

    /// Work-item lease duration.
    #[arg(long, env = "PONTMORE_WORK_LEASE_SECS", default_value_t = 60)]
    pub work_lease_secs: u64,

    /// Maximum number of SSE records replayed for one reconnect.
    #[arg(long, env = "PONTMORE_STREAM_MAX_REPLAY", default_value_t = 10_000)]
    pub stream_max_replay: i64,

    /// Durable SSE retention window.
    #[arg(
        long,
        env = "PONTMORE_STREAM_RETENTION_SECS",
        default_value_t = 604_800
    )]
    pub stream_retention_secs: u64,

    /// Refuse economic connectors without an external OS sandbox launcher.
    #[arg(
        long,
        env = "PONTMORE_REQUIRE_CONNECTOR_SANDBOX",
        default_value_t = false
    )]
    pub require_connector_sandbox: bool,
}

impl Config {
    #[must_use]
    pub const fn connector_timeout(&self) -> Duration {
        Duration::from_millis(self.connector_timeout_ms)
    }
}

/// Static operator-selected connector configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorConfig {
    #[serde(default)]
    pub connectors: BTreeMap<String, ConnectorRegistration>,
    /// Operator-established PIP-01 facts that cannot be inferred from local event order.
    #[serde(default)]
    pub descriptor_bindings: BTreeMap<String, DescriptorBinding>,
    /// Operator-reviewed semantics for an exact immutable service-schema digest.
    #[serde(default)]
    pub schema_policies: BTreeMap<String, SchemaPolicy>,
    #[serde(default)]
    pub execution_policy: ExecutionPolicy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPolicy {
    pub max_automatic_sats: Option<u64>,
    #[serde(default)]
    pub approved_coordination_ids: std::collections::BTreeSet<String>,
}

/// Local trust policy for schema facts that PIP-01 deliberately does not standardize.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPolicy {
    pub sha256: String,
    #[serde(default)]
    pub authority_pubkeys: Vec<String>,
    #[serde(default)]
    pub supported_resolution_effects: Vec<ResolutionEffect>,
}

/// One explicitly configured connector manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorRegistration {
    pub manifest: PathBuf,
    #[serde(default)]
    pub enabled: bool,
    /// Optional external sandbox launcher (for example bubblewrap or systemd-run).
    #[serde(default)]
    pub sandbox: Option<SandboxConfig>,
    /// Secret files made available only to manifest-declared operations over stdin.
    #[serde(default)]
    pub secrets: BTreeMap<String, PathBuf>,
}

/// OS-enforced connector launcher. Pontmore never interpolates these arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxConfig {
    pub launcher: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
}
