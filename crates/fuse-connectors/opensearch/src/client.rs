use std::time::Duration;

use opensearch::auth::Credentials;
use opensearch::cert::CertificateValidation;
use opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use opensearch::http::Url;
use opensearch::OpenSearch;

use fuse_core::config::ConnectorConfig;
use fuse_core::ConnectorError;

/// Wrapper around the opensearch-rs client with auth configuration.
pub struct OpenSearchClient {
    inner: OpenSearch,
    pub url: String,
    pub request_timeout: Duration,
    pub scroll_size: usize,
}

impl std::fmt::Debug for OpenSearchClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenSearchClient")
            .field("url", &self.url)
            .field("request_timeout", &self.request_timeout)
            .field("scroll_size", &self.scroll_size)
            .finish()
    }
}

impl OpenSearchClient {
    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config
            .properties
            .get("url")
            .and_then(|v: &toml::Value| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("missing 'url' in connector config".into()))?;

        let request_timeout = config
            .properties
            .get("request_timeout")
            .and_then(|v: &toml::Value| v.as_str())
            .and_then(|s| s.strip_suffix('s'))
            .and_then(|n| n.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(30));

        let scroll_size = config
            .properties
            .get("scroll_size")
            .and_then(|v: &toml::Value| v.as_integer())
            .unwrap_or(1000) as usize;

        let parsed_url =
            Url::parse(url).map_err(|e| ConnectorError::Connection(format!("invalid url: {e}")))?;
        let pool = SingleNodeConnectionPool::new(parsed_url);
        let mut builder = TransportBuilder::new(pool)
            .timeout(request_timeout)
            .cert_validation(CertificateValidation::None);

        // Auth
        if let Some(auth_table) = config.properties.get("auth").and_then(|v: &toml::Value| v.as_table()) {
            let auth_type = auth_table
                .get("type")
                .and_then(|v: &toml::Value| v.as_str())
                .unwrap_or("none");

            match auth_type {
                "basic" => {
                    let username = auth_table
                        .get("username")
                        .and_then(|v: &toml::Value| v.as_str())
                        .unwrap_or_default();
                    let password = resolve_secret(auth_table, "password", "password_env");
                    builder = builder.auth(Credentials::Basic(
                        username.to_string(),
                        password,
                    ));
                }
                "sigv4" => {
                    let region = auth_table
                        .get("region")
                        .and_then(|v: &toml::Value| v.as_str())
                        .unwrap_or("us-west-2");
                    let service = auth_table
                        .get("service")
                        .and_then(|v: &toml::Value| v.as_str())
                        .unwrap_or("aoss");
                    let sdk_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                    let creds = Credentials::try_from(&sdk_config)
                        .map_err(|e| ConnectorError::Connection(format!("SigV4 credentials: {e}")))?;
                    builder = builder
                        .auth(creds)
                        .service_name(service);
                    tracing::info!(region, service, "SigV4 auth configured for OpenSearch connector");
                }
                _ => {} // none, bearer — bearer needs additional support
            }
        }

        let transport = builder
            .build()
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        Ok(Self {
            inner: OpenSearch::new(transport),
            url: url.to_string(),
            request_timeout,
            scroll_size,
        })
    }

    pub fn client(&self) -> &OpenSearch {
        &self.inner
    }
}

/// Resolve a secret value: use the direct key first, fall back to env var key.
fn resolve_secret(table: &toml::Table, direct_key: &str, env_key: &str) -> String {
    if let Some(val) = table.get(direct_key).and_then(|v: &toml::Value| v.as_str()) {
        if let Some(env_name) = table.get(env_key).and_then(|v: &toml::Value| v.as_str()) {
            return std::env::var(env_name).unwrap_or_else(|_| val.to_string());
        }
        return val.to_string();
    }
    if let Some(env_name) = table.get(env_key).and_then(|v: &toml::Value| v.as_str()) {
        return std::env::var(env_name).unwrap_or_default();
    }
    String::new()
}
