// SPDX-License-Identifier: Apache-2.0

//! Resolve `secret://<secret-name>` references in connector config properties
//! via AWS Secrets Manager. Plain values pass through unchanged.

use std::collections::HashMap;
use tracing::info;

const SECRET_PREFIX: &str = "secret://";

/// Resolve all `secret://` prefixed string values in a properties map.
/// Non-string and non-prefixed values are returned as-is.
pub async fn resolve_secrets(
    properties: &HashMap<String, toml::Value>,
) -> Result<HashMap<String, toml::Value>, crate::error::FuseError> {
    let mut resolved = HashMap::with_capacity(properties.len());
    let mut client = None;

    for (key, value) in properties {
        let resolved_value = match value.as_str() {
            Some(s) if s.starts_with(SECRET_PREFIX) => {
                let secret_name = &s[SECRET_PREFIX.len()..];
                let sm = match &client {
                    Some(c) => c,
                    None => {
                        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                        client = Some(aws_sdk_secretsmanager::Client::new(&config));
                        client.as_ref().unwrap()
                    }
                };
                let resp = sm
                    .get_secret_value()
                    .secret_id(secret_name)
                    .send()
                    .await
                    .map_err(|e| {
                        crate::error::FuseError::Config(format!(
                            "failed to resolve secret '{secret_name}' for key '{key}': {e}"
                        ))
                    })?;
                let secret_string = resp.secret_string().ok_or_else(|| {
                    crate::error::FuseError::Config(format!(
                        "secret '{secret_name}' has no string value"
                    ))
                })?;
                info!(key = %key, secret = %secret_name, "resolved secret");
                toml::Value::String(secret_string.to_string())
            }
            _ => value.clone(),
        };
        resolved.insert(key.clone(), resolved_value);
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_detects_secret_prefix() {
        let val = "secret://fuse/postgres/url";
        assert!(val.starts_with(SECRET_PREFIX));
        assert_eq!(&val[SECRET_PREFIX.len()..], "fuse/postgres/url");
    }
}
