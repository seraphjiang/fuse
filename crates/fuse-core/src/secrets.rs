// SPDX-License-Identifier: Apache-2.0

//! Resolve `secret://<secret-name>` references in connector config properties
//! via AWS Secrets Manager. Plain values pass through unchanged.
//!
//! # Secret URI Format
//!
//! Any connector property value — including nested tables like `[connector.auth]` —
//! can reference a secret using the `secret://` prefix:
//!
//! ```toml
//! [[connector]]
//! id = "my_pg"
//! type = "postgres"
//! url = "secret://fuse/prod/postgres-url"
//!
//! [connector.auth]
//! type = "basic"
//! username = "admin"
//! password = "secret://fuse/prod/pg-password"
//! ```
//!
//! ## Supported patterns
//!
//! - `secret://my-secret` — simple secret name
//! - `secret://prod/fuse/db-password` — hierarchical path
//! - `secret://arn:aws:secretsmanager:us-west-2:123456:secret:my-secret` — full ARN

use std::collections::HashMap;
use tracing::info;

const SECRET_PREFIX: &str = "secret://";

/// Check if a string is a secret reference.
pub fn is_secret_ref(value: &str) -> bool {
    value.starts_with(SECRET_PREFIX)
}

/// Extract the secret name from a `secret://` URI.
pub fn secret_name(value: &str) -> Option<&str> {
    value.strip_prefix(SECRET_PREFIX)
}

// ── Validation ──

/// Validate all `secret://` references in a properties map, recursing into nested tables.
pub fn validate_secret_refs(
    connector_id: &str,
    properties: &HashMap<String, toml::Value>,
) -> Vec<String> {
    let mut errors = Vec::new();
    validate_recursive(connector_id, "", properties.iter(), &mut errors);
    errors
}

fn validate_recursive<'a>(
    connector_id: &str,
    prefix: &str,
    iter: impl Iterator<Item = (&'a String, &'a toml::Value)>,
    errors: &mut Vec<String>,
) {
    for (key, value) in iter {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            toml::Value::String(s) if s.starts_with(SECRET_PREFIX) => {
                if s[SECRET_PREFIX.len()..].trim().is_empty() {
                    errors.push(format!(
                        "connector '{}': property '{}' has empty secret:// reference",
                        connector_id, full_key
                    ));
                }
            }
            toml::Value::Table(table) => {
                validate_recursive(connector_id, &full_key, table.iter(), errors);
            }
            _ => {}
        }
    }
}

// ── Collection ──

/// Collect all secret names referenced in a properties map, recursing into nested tables.
pub fn collect_secret_refs(properties: &HashMap<String, toml::Value>) -> Vec<(String, String)> {
    let mut refs = Vec::new();
    collect_recursive("", properties.iter(), &mut refs);
    refs
}

fn collect_recursive<'a>(
    prefix: &str,
    iter: impl Iterator<Item = (&'a String, &'a toml::Value)>,
    refs: &mut Vec<(String, String)>,
) {
    for (key, value) in iter {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            toml::Value::String(s) => {
                if let Some(name) = s.strip_prefix(SECRET_PREFIX) {
                    if !name.trim().is_empty() {
                        refs.push((full_key, name.to_string()));
                    }
                }
            }
            toml::Value::Table(table) => {
                collect_recursive(&full_key, table.iter(), refs);
            }
            _ => {}
        }
    }
}

// ── Resolution ──

/// Trait for secret resolution — enables mock testing without AWS.
#[async_trait::async_trait]
pub trait SecretResolver: Send + Sync {
    async fn get_secret(&self, secret_name: &str) -> Result<String, String>;
}

/// AWS Secrets Manager resolver.
pub struct AwsSecretResolver {
    client: aws_sdk_secretsmanager::Client,
}

impl AwsSecretResolver {
    pub async fn new() -> Self {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Self {
            client: aws_sdk_secretsmanager::Client::new(&config),
        }
    }
}

#[async_trait::async_trait]
impl SecretResolver for AwsSecretResolver {
    async fn get_secret(&self, secret_name: &str) -> Result<String, String> {
        let resp = self
            .client
            .get_secret_value()
            .secret_id(secret_name)
            .send()
            .await
            .map_err(|e| format!("failed to resolve secret '{}': {}", secret_name, e))?;
        resp.secret_string()
            .map(|s| s.to_string())
            .ok_or_else(|| format!("secret '{}' has no string value", secret_name))
    }
}

/// Resolve all `secret://` prefixed values using the provided resolver.
/// Recurses into nested TOML tables (e.g. `[connector.auth]`).
pub async fn resolve_secrets_with(
    properties: &HashMap<String, toml::Value>,
    resolver: &dyn SecretResolver,
) -> Result<HashMap<String, toml::Value>, crate::error::FuseError> {
    let mut resolved = HashMap::with_capacity(properties.len());
    for (key, value) in properties {
        resolved.insert(key.clone(), resolve_value(key, value, resolver).await?);
    }
    Ok(resolved)
}

fn resolve_value<'a>(
    key: &'a str,
    value: &'a toml::Value,
    resolver: &'a dyn SecretResolver,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<toml::Value, crate::error::FuseError>> + Send + 'a>,
> {
    Box::pin(async move {
        match value {
            toml::Value::String(s) if s.starts_with(SECRET_PREFIX) => {
                let name = &s[SECRET_PREFIX.len()..];
                let secret_string = resolver.get_secret(name).await.map_err(|e| {
                    crate::error::FuseError::Config {
                        code: crate::error::ErrorCode::CONFIG_SECRET_RESOLVE,
                        msg: format!("key '{}': {}", key, e),
                    }
                })?;
                info!(key = %key, secret = %name, "resolved secret");
                Ok(toml::Value::String(secret_string))
            }
            toml::Value::Table(table) => {
                let mut inner = toml::map::Map::new();
                for (k, v) in table {
                    let child_key = format!("{key}.{k}");
                    inner.insert(k.clone(), resolve_value(&child_key, v, resolver).await?);
                }
                Ok(toml::Value::Table(inner))
            }
            other => Ok(other.clone()),
        }
    })
}

/// Resolve all `secret://` prefixed values via AWS Secrets Manager.
pub async fn resolve_secrets(
    properties: &HashMap<String, toml::Value>,
) -> Result<HashMap<String, toml::Value>, crate::error::FuseError> {
    if !has_secret_refs_in_values(properties.values()) {
        return Ok(properties.clone());
    }
    let resolver = AwsSecretResolver::new().await;
    resolve_secrets_with(properties, &resolver).await
}

fn has_secret_refs_in_values<'a>(values: impl Iterator<Item = &'a toml::Value>) -> bool {
    values.into_iter().any(|v| match v {
        toml::Value::String(s) => s.starts_with(SECRET_PREFIX),
        toml::Value::Table(t) => has_secret_refs_in_values(t.values()),
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_secret_ref() {
        assert!(is_secret_ref("secret://my-secret"));
        assert!(!is_secret_ref("http://localhost"));
        assert!(!is_secret_ref(""));
    }

    #[test]
    fn test_secret_name() {
        assert_eq!(secret_name("secret://fuse/db"), Some("fuse/db"));
        assert_eq!(secret_name("http://x"), None);
    }

    #[test]
    fn test_validate_secret_refs_valid() {
        let mut props = HashMap::new();
        props.insert(
            "url".into(),
            toml::Value::String("secret://fuse/db-url".into()),
        );
        props.insert("port".into(), toml::Value::Integer(5432));
        assert!(validate_secret_refs("pg", &props).is_empty());
    }

    #[test]
    fn test_validate_secret_refs_empty_name() {
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("secret://".into()));
        let errors = validate_secret_refs("pg", &props);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("empty secret://"));
    }

    #[test]
    fn test_validate_secret_refs_whitespace_name() {
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("secret://  ".into()));
        assert_eq!(validate_secret_refs("pg", &props).len(), 1);
    }

    #[test]
    fn test_validate_nested_empty_secret() {
        let mut auth = toml::map::Map::new();
        auth.insert("password".into(), toml::Value::String("secret://".into()));
        let mut props = HashMap::new();
        props.insert("auth".into(), toml::Value::Table(auth));
        let errors = validate_secret_refs("os", &props);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("auth.password"));
    }

    #[test]
    fn test_validate_nested_valid_secret() {
        let mut auth = toml::map::Map::new();
        auth.insert(
            "password".into(),
            toml::Value::String("secret://fuse/pass".into()),
        );
        let mut props = HashMap::new();
        props.insert("auth".into(), toml::Value::Table(auth));
        assert!(validate_secret_refs("os", &props).is_empty());
    }

    #[test]
    fn test_collect_secret_refs_flat() {
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("secret://fuse/db".into()));
        props.insert("port".into(), toml::Value::Integer(5432));
        props.insert(
            "pass".into(),
            toml::Value::String("secret://fuse/pass".into()),
        );
        assert_eq!(collect_secret_refs(&props).len(), 2);
    }

    #[test]
    fn test_collect_secret_refs_skips_empty() {
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("secret://".into()));
        assert!(collect_secret_refs(&props).is_empty());
    }

    #[test]
    fn test_collect_nested_secret_refs() {
        let mut auth = toml::map::Map::new();
        auth.insert(
            "token".into(),
            toml::Value::String("secret://fuse/token".into()),
        );
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("http://localhost".into()));
        props.insert("auth".into(), toml::Value::Table(auth));
        let refs = collect_secret_refs(&props);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].0, "auth.token");
        assert_eq!(refs[0].1, "fuse/token");
    }

    #[test]
    fn test_no_secrets_passthrough() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut props = HashMap::new();
            props.insert("url".into(), toml::Value::String("http://localhost".into()));
            props.insert("port".into(), toml::Value::Integer(5432));
            let resolved = resolve_secrets(&props).await.unwrap();
            assert_eq!(resolved["url"].as_str(), Some("http://localhost"));
            assert_eq!(resolved["port"].as_integer(), Some(5432));
        });
    }

    struct MockResolver {
        secrets: HashMap<String, String>,
    }

    impl MockResolver {
        fn new(pairs: &[(&str, &str)]) -> Self {
            Self {
                secrets: pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            }
        }
    }

    #[async_trait::async_trait]
    impl SecretResolver for MockResolver {
        async fn get_secret(&self, name: &str) -> Result<String, String> {
            self.secrets
                .get(name)
                .cloned()
                .ok_or_else(|| format!("secret '{}' not found", name))
        }
    }

    #[tokio::test]
    async fn test_resolve_with_mock() {
        let resolver = MockResolver::new(&[("fuse/db-url", "postgresql://user:pass@host/db")]);
        let mut props = HashMap::new();
        props.insert(
            "url".into(),
            toml::Value::String("secret://fuse/db-url".into()),
        );
        props.insert("port".into(), toml::Value::Integer(5432));
        let resolved = resolve_secrets_with(&props, &resolver).await.unwrap();
        assert_eq!(
            resolved["url"].as_str(),
            Some("postgresql://user:pass@host/db")
        );
        assert_eq!(resolved["port"].as_integer(), Some(5432));
    }

    #[tokio::test]
    async fn test_resolve_missing_secret_errors() {
        let resolver = MockResolver::new(&[]);
        let mut props = HashMap::new();
        props.insert(
            "url".into(),
            toml::Value::String("secret://nonexistent".into()),
        );
        let result = resolve_secrets_with(&props, &resolver).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nonexistent"));
    }

    #[tokio::test]
    async fn test_resolve_multiple_secrets() {
        let resolver = MockResolver::new(&[("fuse/url", "pg://host/db"), ("fuse/pass", "s3cret")]);
        let mut props = HashMap::new();
        props.insert(
            "url".into(),
            toml::Value::String("secret://fuse/url".into()),
        );
        props.insert(
            "password".into(),
            toml::Value::String("secret://fuse/pass".into()),
        );
        props.insert("port".into(), toml::Value::Integer(5432));
        let resolved = resolve_secrets_with(&props, &resolver).await.unwrap();
        assert_eq!(resolved["url"].as_str(), Some("pg://host/db"));
        assert_eq!(resolved["password"].as_str(), Some("s3cret"));
        assert_eq!(resolved["port"].as_integer(), Some(5432));
    }

    #[tokio::test]
    async fn test_resolve_no_secrets_skips_client() {
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("http://localhost".into()));
        let resolved = resolve_secrets(&props).await.unwrap();
        assert_eq!(resolved["url"].as_str(), Some("http://localhost"));
    }

    #[tokio::test]
    async fn test_resolve_nested_auth_secrets() {
        let resolver = MockResolver::new(&[("fuse/pass", "hunter2")]);
        let mut auth = toml::map::Map::new();
        auth.insert("type".into(), toml::Value::String("basic".into()));
        auth.insert("username".into(), toml::Value::String("admin".into()));
        auth.insert(
            "password".into(),
            toml::Value::String("secret://fuse/pass".into()),
        );
        let mut props = HashMap::new();
        props.insert(
            "url".into(),
            toml::Value::String("https://localhost:9200".into()),
        );
        props.insert("auth".into(), toml::Value::Table(auth));
        let resolved = resolve_secrets_with(&props, &resolver).await.unwrap();
        assert_eq!(resolved["url"].as_str(), Some("https://localhost:9200"));
        let auth_resolved = resolved["auth"].as_table().unwrap();
        assert_eq!(auth_resolved["type"].as_str(), Some("basic"));
        assert_eq!(auth_resolved["username"].as_str(), Some("admin"));
        assert_eq!(auth_resolved["password"].as_str(), Some("hunter2"));
    }

    #[tokio::test]
    async fn test_resolve_nested_missing_secret_errors() {
        let resolver = MockResolver::new(&[]);
        let mut auth = toml::map::Map::new();
        auth.insert(
            "token".into(),
            toml::Value::String("secret://missing".into()),
        );
        let mut props = HashMap::new();
        props.insert("auth".into(), toml::Value::Table(auth));
        let result = resolve_secrets_with(&props, &resolver).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing"));
    }

    #[test]
    fn test_has_secret_refs_nested() {
        let mut auth = toml::map::Map::new();
        auth.insert("token".into(), toml::Value::String("secret://x".into()));
        let mut props: HashMap<String, toml::Value> = HashMap::new();
        props.insert("auth".into(), toml::Value::Table(auth));
        assert!(has_secret_refs_in_values(props.values()));
    }

    #[test]
    fn test_has_secret_refs_none() {
        let mut props: HashMap<String, toml::Value> = HashMap::new();
        props.insert("url".into(), toml::Value::String("http://localhost".into()));
        assert!(!has_secret_refs_in_values(props.values()));
    }
}
