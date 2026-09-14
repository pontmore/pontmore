//! Pontmore node, durable event store, SSE API, and economic-connector manager.

pub mod api;
pub mod config;
pub mod connector;
pub mod coordinator;
pub mod relay;
pub mod schema;
pub mod store;
pub mod work;

pub use api::{AppState, router};
pub use config::{Config, ConnectorConfig, ConnectorRegistration};
pub use connector::{
    ConnectorExecution, ConnectorHealth, ConnectorManager, ConnectorManifest, ConnectorResult,
    ConnectorStatus, ConnectorType, EconomicOperation,
};
pub use coordinator::{Coordinator, CoordinatorError, DescriptorRegistry, ProtocolFailure};
pub use store::{InsertOutcome, OperationStatus, Store};
