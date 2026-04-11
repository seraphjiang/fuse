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
    /// Allowed CORS origins. Empty = same-origin only. Use ["*"] for any origin.
    #[serde(default)]
    pub cors_origins: Vec<String>,
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
    /// Resolve any `secret://` prefixed property values via AWS Secrets Manager.
    pub async fn resolve_secrets(&mut self) -> Result<(), crate::error::FuseError> {
        self.properties = crate::secrets::resolve_secrets(&self.properties).await?;
        Ok(())
    }

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

    /// Extract optional TLS configuration from `[connector.tls]`.
    pub fn tls_config(&self) -> Option<crate::tls::TlsConfig> {
        crate::tls::TlsConfig::from_properties(&self.properties)
    }
}

impl FuseConfig {
    pub fn from_file(path: &str) -> Result<Self, crate::error::FuseError> {
        let content =
            std::fs::read_to_string(path).map_err(|e| crate::error::FuseError::config(e.to_string()))?;
        toml::from_str(&content).map_err(|e| crate::error::FuseError::config(e.to_string()))
    }

    /// Validate the configuration, returning all errors found (not just the first).
    /// Call at startup to fail fast with clear diagnostics.
    pub fn validate(&self, known_types: &[&str]) -> Result<(), crate::error::FuseError> {
        let mut errors: Vec<String> = Vec::new();

        // Engine validation
        if let Err(e) = parse_bind_addr(&self.engine.bind) {
            errors.push(format!("engine.bind: {e}"));
        }
        if self.engine.max_concurrent_queries == 0 {
            errors.push("engine.max_concurrent_queries must be > 0".into());
        }

        // Connector validation
        let mut seen_ids = std::collections::HashSet::new();
        for (i, cc) in self.connector.iter().enumerate() {
            let label = if cc.id.is_empty() {
                format!("connector[{i}]")
            } else {
                format!("connector '{}'", cc.id)
            };

            if cc.id.is_empty() {
                errors.push(format!("{label}: id is required"));
            } else if !seen_ids.insert(&cc.id) {
                errors.push(format!("{label}: duplicate connector id"));
            }

            if cc.connector_type.is_empty() {
                errors.push(format!("{label}: type is required"));
            } else if !known_types.contains(&cc.connector_type.as_str()) {
                errors.push(format!(
                    "{label}: unknown type '{}' (known: {})",
                    cc.connector_type,
                    known_types.join(", ")
                ));
            }

            // Validate TLS config if present
            if let Some(tls) = cc.tls_config() {
                if let Err(e) = tls.validate() {
                    errors.push(format!("{label}: tls: {e}"));
                }
            }

            // Validate secret:// references are well-formed
            errors.extend(crate::secrets::validate_secret_refs(&cc.id, &cc.properties));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(crate::error::FuseError::Config {
                code: crate::error::ErrorCode::CONFIG_INVALID,
                msg: format!(
                    "configuration has {} error(s):\n  - {}",
                    errors.len(),
                    errors.join("\n  - ")
                ),
            })
        }
    }
}

fn parse_bind_addr(s: &str) -> Result<(), String> {
    use std::net::ToSocketAddrs;
    s.to_socket_addrs()
        .map(|_| ())
        .map_err(|e| format!("invalid bind address '{}': {}", s, e))
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

    #[test]
    fn test_connector_config_tls_none() {
        let config = ConnectorConfig { id: "x".into(), connector_type: "es".into(), properties: Default::default() };
        assert!(config.tls_config().is_none());
    }

    #[test]
    fn test_connector_config_tls_from_toml() {
        let toml_str = r#"
[engine]

[[connector]]
id = "secure"
type = "opensearch"
url = "https://localhost:9200"

[connector.tls]
ca_cert = "/tmp/ca.pem"
"#;
        let cfg: FuseConfig = toml::from_str(toml_str).unwrap();
        let tls = cfg.connector[0].tls_config().unwrap();
        assert_eq!(tls.ca_cert.unwrap().to_str().unwrap(), "/tmp/ca.pem");
        assert!(tls.client_cert.is_none());
    }

    const KNOWN: &[&str] = &["opensearch", "elasticsearch", "postgres", "mysql", "dynamodb", "s3", "s3-o11y", "prometheus", "cloudwatch", "redis", "csv-json", "mongodb", "influxdb", "clickhouse", "kafka", "redshift", "duckdb", "sqlite"];

    #[test]
    fn test_validate_valid_config() {
        let toml = r#"
[engine]
bind = "0.0.0.0:9400"

[[connector]]
id = "a"
type = "opensearch"
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        assert!(cfg.validate(KNOWN).is_ok());
    }

    #[test]
    fn test_validate_empty_connectors_ok() {
        let toml = r#"[engine]"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        assert!(cfg.validate(KNOWN).is_ok());
    }

    #[test]
    fn test_validate_duplicate_ids() {
        let toml = r#"
[engine]

[[connector]]
id = "dup"
type = "opensearch"

[[connector]]
id = "dup"
type = "postgres"
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        let err = cfg.validate(KNOWN).unwrap_err();
        assert!(err.to_string().contains("duplicate connector id"));
    }

    #[test]
    fn test_validate_unknown_type() {
        let toml = r#"
[engine]

[[connector]]
id = "x"
type = "oracle"
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        let err = cfg.validate(KNOWN).unwrap_err();
        assert!(err.to_string().contains("unknown type 'oracle'"));
    }

    #[test]
    fn test_validate_empty_id() {
        let toml = r#"
[engine]

[[connector]]
id = ""
type = "opensearch"
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        let err = cfg.validate(KNOWN).unwrap_err();
        assert!(err.to_string().contains("id is required"));
    }

    #[test]
    fn test_validate_empty_type() {
        let toml = r#"
[engine]

[[connector]]
id = "x"
type = ""
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        let err = cfg.validate(KNOWN).unwrap_err();
        assert!(err.to_string().contains("type is required"));
    }

    #[test]
    fn test_validate_zero_concurrency() {
        let toml = r#"
[engine]
max_concurrent_queries = 0
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        let err = cfg.validate(KNOWN).unwrap_err();
        assert!(err.to_string().contains("max_concurrent_queries must be > 0"));
    }

    #[test]
    fn test_validate_multiple_errors() {
        let toml = r#"
[engine]
max_concurrent_queries = 0

[[connector]]
id = ""
type = "bogus"
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        let err = cfg.validate(KNOWN).unwrap_err();
        let msg = err.to_string();
        // Should report all errors, not just the first
        assert!(msg.contains("3 error(s)"));
    }

    #[test]
    fn test_validate_empty_secret_ref() {
        let toml = r#"
[engine]
bind = "0.0.0.0:9400"
max_concurrent_queries = 64

[[connector]]
id = "pg"
type = "postgres"
url = "secret://"
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        let err = cfg.validate(KNOWN).unwrap_err();
        assert!(err.to_string().contains("empty secret://"));
    }

    #[test]
    fn test_validate_valid_secret_ref() {
        let toml = r#"
[engine]
bind = "0.0.0.0:9400"
max_concurrent_queries = 64

[[connector]]
id = "pg"
type = "postgres"
url = "secret://fuse/prod/pg-url"
"#;
        let cfg: FuseConfig = toml::from_str(toml).unwrap();
        assert!(cfg.validate(KNOWN).is_ok());
    }
}
