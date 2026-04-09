// SPDX-License-Identifier: Apache-2.0
// Fuse Core — connector traits, config, error types

pub mod alerting;
pub mod config;
pub mod connector;
pub mod error;
pub mod registry;
pub mod security;
pub mod version;

// Re-export commonly used types at crate root for convenience
pub use connector::{
    ConnectorCapabilities, ConnectorHealth, ConnectorType, FederatedConnector, HealthStatus,
    SchemaInfo, SubQuery,
};
pub use error::ConnectorError;
pub use registry::ConnectorRegistry;

/// Alias used by the DataFusion-based planner.
pub use FederatedConnector as FuseConnector;
