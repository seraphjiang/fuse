// SPDX-License-Identifier: Apache-2.0

//! Resolve `secret://<secret-name>` references in connector config properties
//! via AWS Secrets Manager. Plain values pass through unchanged.
//!
//! # Secret URI Format
//!
//! Any connector property value can reference a secret using the `secret://` prefix:
//!
//! ```toml
//! [[connector]]
//! id = "my_pg"
//! type = "postgres"
//! url = "secret://fuse/prod/postgres-url"
//! ```
//!
//! The secret name after `secret://` is the AWS Secrets Manager secret ID.
//! At startup, all `secret://` references are resolved and replaced with the
//! actual secret string value. If resolution fails, the server refuses to start.
//!
//! ## Supported patterns
//!
//! - `secret://my-secret` — simple secret name
//! - `secret://prod/fuse/db-password` — hierarchical path
//! - `secret://arn:aws:secretsmanager:us-west-2:123456:secret:my-secret` — full ARN
//!
//! ## Validation
//!
//! Use [`validate_secret_refs`] at startup to check that all `secret://` URIs
//! are well-formed before attempting resolution. Use [`resolve_secrets`] (or
//! the mock-friendly [`resolve_secrets_with`]) to fetch actual values.

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

/// Validate all `secret://` references in a properties map.
/// Returns errors for empty or whitespace-only secret names.
pub fn validate_secret_refs(
    connector_id: &str,
    properties: &HashMap<String, toml::Value>,
) -> Vec<String> {
    let mut errors = Vec::new();
    for (key, value) in properties {
        if let Some(s) = value.as_str() {
            if let Some(name) = s.strip_prefix(SECRET_PREFIX) {
                if name.trim().is_empty() {
                    errors.push(format!(
                        "connector '{}': property '{}' has empty secret:// reference",
                        connector_id, key
                    ));
                }
            }
        }
    }
    errors
}

/// Collect all secret names referenced in a properties map.
pub fn collect_secret_refs(properties: &HashMap<String, toml::Value>) -> Vec<(String, String)> {
    properties
        .iter()
        .filter_map(|(key, value)| {
            value
                .as_str()
                .and_then(|s| s.strip_prefix(SECRET_PREFIX))
                .filter(|name| !name.trim().is_empty())
                .map(|name| (key.clone(), name.to_string()))
        })
        .collect()
}

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
pub async fn resolve_secrets_with(
    properties: &HashMap<String, toml::Value>,
    resolver: &dyn SecretResolver,
) -> Result<HashMap<String, toml::Value>, crate::error::FuseError> {
    let mut resolved = HashMap::with_capacity(properties.len());
    for (key, value) in properties {
        let resolved_value = match value.as_str() {
            Some(s) if s.starts_with(SECRET_PREFIX) => {
                let name = &s[SECRET_PREFIX.len()..];
                let secret_string = resolver.get_secret(name).await.map_err(|e| {
                    crate::error::FuseError::Config {
                        code: crate::error::ErrorCode::CONFIG_SECRET_RESOLVE,
                        msg: format!("key '{}': {}", key, e),
                    }
                })?;
                info!(key = %key, secret = %name, "resolved secret");
                toml::Value::String(secret_string)
            }
            _ => value.clone(),
        };
        resolved.insert(key.clone(), resolved_value);
    }
    Ok(resolved)
}

/// Resolve all `secret://` prefixed values via AWS Secrets Manager.
pub async fn resolve_secrets(
    properties: &HashMap<String, toml::Value>,
) -> Result<HashMap<String, toml::Value>, crate::error::FuseError> {
    // Only create AWS client if there are actual secret refs
    let has_secrets = properties
        .values()
        .any(|v| v.as_str().map_or(false, |s| s.starts_with(SECRET_PREFIX)));
    if !has_secrets {
        return Ok(properties.clone());
    }
    let resolver = AwsSecretResolver::new().await;
    resolve_secrets_with(properties, &resolver).await
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
        props.insert("url".into(), toml::Value::String("secret://fuse/db-url".into()));
        props.insert("port".into(), toml::Value::Integer(5432));
        let errors = validate_secret_refs("pg", &props);
        assert!(errors.is_empty());
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
        let errors = validate_secret_refs("pg", &props);
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn test_collect_secret_refs() {
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("secret://fuse/db".into()));
        props.insert("port".into(), toml::Value::Integer(5432));
        props.insert("pass".into(), toml::Value::String("secret://fuse/pass".into()));
        let refs = collect_secret_refs(&props);
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn test_collect_secret_refs_skips_empty() {
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("secret://".into()));
        let refs = collect_secret_refs(&props);
        assert!(refs.is_empty());
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

    /// Mock resolver for testing without AWS.
    struct MockResolver {
        secrets: HashMap<String, String>,
    }

    impl MockResolver {
        fn new(pairs: &[(&str, &str)]) -> Self {
            Self {
                secrets: pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
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
        props.insert("url".into(), toml::Value::String("secret://fuse/db-url".into()));
        props.insert("port".into(), toml::Value::Integer(5432));
        let resolved = resolve_secrets_with(&props, &resolver).await.unwrap();
        assert_eq!(resolved["url"].as_str(), Some("postgresql://user:pass@host/db"));
        assert_eq!(resolved["port"].as_integer(), Some(5432));
    }

    #[tokio::test]
    async fn test_resolve_missing_secret_errors() {
        let resolver = MockResolver::new(&[]);
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("secret://nonexistent".into()));
        let result = resolve_secrets_with(&props, &resolver).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent"));
    }

    #[tokio::test]
    async fn test_resolve_multiple_secrets() {
        let resolver = MockResolver::new(&[
            ("fuse/url", "pg://host/db"),
            ("fuse/pass", "s3cret"),
        ]);
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("secret://fuse/url".into()));
        props.insert("password".into(), toml::Value::String("secret://fuse/pass".into()));
        props.insert("port".into(), toml::Value::Integer(5432));
        let resolved = resolve_secrets_with(&props, &resolver).await.unwrap();
        assert_eq!(resolved["url"].as_str(), Some("pg://host/db"));
        assert_eq!(resolved["password"].as_str(), Some("s3cret"));
        assert_eq!(resolved["port"].as_integer(), Some(5432));
    }

    #[tokio::test]
    async fn test_resolve_no_secrets_skips_client() {
        // No secret:// refs → should succeed without any resolver call
        let mut props = HashMap::new();
        props.insert("url".into(), toml::Value::String("http://localhost".into()));
        let resolved = resolve_secrets(&props).await.unwrap();
        assert_eq!(resolved["url"].as_str(), Some("http://localhost"));
    }
}
