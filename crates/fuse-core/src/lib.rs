// SPDX-License-Identifier: Apache-2.0
// Fuse Core — connector traits, config, error types

pub mod alerting;
pub mod config;
pub mod config_validator;
pub mod connector;
pub mod cost_model;
pub mod dependency_graph;
pub mod error;
pub mod expr;
pub mod factory;
pub mod health_history;
pub mod materialized_view;
pub mod metadata_cache;
pub mod registry;
pub mod optimizer;
pub mod plan_serde;
pub mod planner;
pub mod predicate;
pub mod pushdown;
pub mod query_stats;
pub mod schema_compat;
pub mod secrets;
pub mod security;
pub mod sql;
pub mod tls;
pub mod url;
pub mod type_map;
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
