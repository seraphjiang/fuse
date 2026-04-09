// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use serde::Deserialize;

/// Top-level config loaded from fuse.toml.
#[derive(Debug, Deserialize)]
pub struct FuseConfig {
    pub engine: EngineConfig,
    #[serde(default)]
    pub connector: Vec<ConnectorConfig>,
}

#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_queries: usize,
    #[serde(default = "default_timeout")]
    pub default_timeout: String,
}

fn default_bind() -> String {
    "0.0.0.0:9400".to_string()
}
fn default_max_concurrent() -> usize {
    64
}
fn default_timeout() -> String {
    "30s".to_string()
}

/// Configuration for a single connector instance, loaded from [[connector]] in fuse.toml.
#[derive(Debug, Deserialize)]
pub struct ConnectorConfig {
    pub id: String,
    #[serde(rename = "type")]
    pub connector_type: String,
    /// All other fields are captured as a flat map for connector-specific parsing.
    #[serde(flatten)]
    pub properties: HashMap<String, toml::Value>,
}

impl FuseConfig {
    pub fn from_file(path: &str) -> Result<Self, crate::error::FuseError> {
        let content =
            std::fs::read_to_string(path).map_err(|e| crate::error::FuseError::Config(e.to_string()))?;
        toml::from_str(&content).map_err(|e| crate::error::FuseError::Config(e.to_string()))
    }
}
