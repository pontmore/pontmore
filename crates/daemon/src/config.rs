use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, time::Duration};

use clap::Parser;
use pontmore::DescriptorBinding;
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
}

/// One explicitly configured connector manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorRegistration {
    pub manifest: PathBuf,
    #[serde(default)]
    pub enabled: bool,
}
