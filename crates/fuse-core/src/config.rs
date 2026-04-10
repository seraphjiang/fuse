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
    #[serde(default = "default_rate_limit_global")]
    pub rate_limit_global: u32,
    #[serde(default = "default_rate_limit_per_ip")]
    pub rate_limit_per_ip: u32,
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
fn default_rate_limit_global() -> u32 {
    1000
}
fn default_rate_limit_per_ip() -> u32 {
    100
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

impl ConnectorConfig {
    /// Read `max_connections` from properties, defaulting to `default`.
    pub fn max_connections(&self, default: u32) -> u32 {
        self.properties
            .get("max_connections")
            .and_then(|v| v.as_integer())
            .map(|n| n as u32)
            .unwrap_or(default)
    }

    /// Read `connection_timeout_secs` from properties, defaulting to `default`.
    pub fn connection_timeout_secs(&self, default: u64) -> u64 {
        self.properties
            .get("connection_timeout_secs")
            .and_then(|v| v.as_integer())
            .map(|n| n as u64)
            .unwrap_or(default)
    }
}

impl FuseConfig {
    pub fn from_file(path: &str) -> Result<Self, crate::error::FuseError> {
        let content =
            std::fs::read_to_string(path).map_err(|e| crate::error::FuseError::Config(e.to_string()))?;
        toml::from_str(&content).map_err(|e| crate::error::FuseError::Config(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
[engine]
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.engine.bind, "0.0.0.0:9400");
        assert_eq!(cfg.engine.max_concurrent_queries, 64);
        assert_eq!(cfg.engine.default_timeout, "30s");
        assert!(cfg.connector.is_empty());
    }

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
[engine]
bind = "127.0.0.1:8080"
max_concurrent_queries = 16
default_timeout = "10s"

[[connector]]
id = "test_cluster"
type = "opensearch"
url = "https://localhost:9200"
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.engine.bind, "127.0.0.1:8080");
        assert_eq!(cfg.engine.max_concurrent_queries, 16);
        assert_eq!(cfg.connector.len(), 1);
        assert_eq!(cfg.connector[0].id, "test_cluster");
        assert_eq!(cfg.connector[0].connector_type, "opensearch");
        assert_eq!(cfg.connector[0].properties["url"].as_str(), Some("https://localhost:9200"));
    }

    #[test]
    fn test_parse_multiple_connectors() {
        let toml = r#"
[engine]

[[connector]]
id = "a"
type = "opensearch"

[[connector]]
id = "b"
type = "s3"
bucket = "my-bucket"
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.connector.len(), 2);
        assert_eq!(cfg.connector[1].properties["bucket"].as_str(), Some("my-bucket"));
    }

    #[test]
    fn test_from_file_missing() {
        let result = FuseConfig::from_file("/nonexistent/path.toml");
        assert!(result.is_err());
    }

    #[test]
    fn test_rate_limit_defaults() {
        let toml = r#"[engine]"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.engine.rate_limit_global, 1000);
        assert_eq!(cfg.engine.rate_limit_per_ip, 100);
    }

    #[test]
    fn test_rate_limit_configurable() {
        let toml = r#"
[engine]
rate_limit_global = 500
rate_limit_per_ip = 50
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.engine.rate_limit_global, 500);
        assert_eq!(cfg.engine.rate_limit_per_ip, 50);
    }

    #[test]
    fn test_connector_config_max_connections_default() {
        let config = ConnectorConfig { id: "x".into(), connector_type: "pg".into(), properties: Default::default() };
        assert_eq!(config.max_connections(10), 10);
    }

    #[test]
    fn test_connector_config_max_connections_from_properties() {
        let mut props = HashMap::new();
        props.insert("max_connections".into(), toml::Value::Integer(25));
        let config = ConnectorConfig { id: "x".into(), connector_type: "pg".into(), properties: props };
        assert_eq!(config.max_connections(10), 25);
    }

    #[test]
    fn test_connector_config_timeout_default() {
        let config = ConnectorConfig { id: "x".into(), connector_type: "es".into(), properties: Default::default() };
        assert_eq!(config.connection_timeout_secs(30), 30);
    }
}
