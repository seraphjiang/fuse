use serde::Deserialize;
use std::collections::HashMap;

/// Top-level configuration for the Fuse engine.
#[derive(Debug, Clone, Deserialize)]
pub struct FuseConfig {
    /// Registered datasource connector configurations, keyed by datasource name.
    #[serde(default)]
    pub datasources: HashMap<String, ConnectorConfig>,
}

/// Configuration for a single datasource connector.
#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorConfig {
    /// Connector type identifier (e.g. "opensearch", "s3", "prometheus").
    pub connector_type: String,
    /// Connector-specific settings as arbitrary key-value pairs.
    #[serde(default)]
    pub settings: HashMap<String, String>,
}
