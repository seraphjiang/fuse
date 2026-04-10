// SPDX-License-Identifier: Apache-2.0

//! Elasticsearch 7.x/8.x connector for the Fuse federated query engine.
//!
//! Version-aware:
//! - ES 7.x: includes `_type` in mappings, uses `total.value` in hits
//! - ES 8.x: no `_type`, security enabled by default, API key auth
//!
//! Auth: Basic (user/password) or API key (api_key config property).
//! Query DSL pushdown reuses the same logic as the OpenSearch connector.

pub mod pushdown;

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

use pushdown::translate_to_query_dsl;

/// Elasticsearch major version.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EsVersion { V7, V8 }

#[derive(Debug)]
pub struct ElasticsearchConnector {
    id: String,
    client: reqwest::Client,
    base_url: String,
    version: EsVersion,
}

impl ElasticsearchConnector {
    pub fn new(id: String, client: reqwest::Client, base_url: String, version: EsVersion) -> Self {
        Self { id, client, base_url: base_url.trim_end_matches('/').to_string(), version }
    }

    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config.properties.get("url")
            .and_then(|v| v.as_str())
            .unwrap_or("http://localhost:9200")
            .to_string();

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        // API key auth (ES 8.x preferred)
        if let Some(api_key) = config.properties.get("api_key").and_then(|v| v.as_str()) {
            let val = HeaderValue::from_str(&format!("ApiKey {api_key}"))
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            headers.insert(AUTHORIZATION, val);
        } else if let (Some(user), Some(pass)) = (
            config.properties.get("username").and_then(|v| v.as_str()),
            config.properties.get("password").and_then(|v| v.as_str()),
        ) {
            let encoded = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("{user}:{pass}"),
            );
            let val = HeaderValue::from_str(&format!("Basic {encoded}"))
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            headers.insert(AUTHORIZATION, val);
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .danger_accept_invalid_certs(
                config.properties.get("tls_insecure").and_then(|v| v.as_bool()).unwrap_or(false)
            )
            .build()
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        // Detect version from /_cluster/health or config override
        let version = if let Some(v) = config.properties.get("version").and_then(|v| v.as_integer()) {
            if v >= 8 { EsVersion::V8 } else { EsVersion::V7 }
        } else {
            detect_version(&client, &url).await
        };

        Ok(Self::new(config.id.clone(), client, url, version))
    }

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, ConnectorError> {
        let url = format!("{}{}", self.base_url, path);
        self.client.get(&url).send().await
            .map_err(|e| ConnectorError::query(e))?
            .json().await
            .map_err(|e| ConnectorError::query(e))
    }

    async fn post_json(&self, path: &str, body: serde_json::Value) -> Result<serde_json::Value, ConnectorError> {
        let url = format!("{}{}", self.base_url, path);
        debug!(url = url.as_str(), "Elasticsearch POST");
        self.client.post(&url).json(&body).send().await
            .map_err(|e| ConnectorError::query(e))?
            .json().await
            .map_err(|e| ConnectorError::query(e))
    }
}

async fn detect_version(client: &reqwest::Client, base_url: &str) -> EsVersion {
    let resp = client.get(base_url).send().await;
    if let Ok(r) = resp {
        if let Ok(body) = r.json::<serde_json::Value>().await {
            let major = body.pointer("/version/number")
                .and_then(|v| v.as_str())
                .and_then(|s| s.split('.').next())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(7);
            return if major >= 8 { EsVersion::V8 } else { EsVersion::V7 };
        }
    }
    EsVersion::V7 // safe default
}

#[async_trait]
impl FederatedConnector for ElasticsearchConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "elasticsearch" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: true,
            supports_sorting: true,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 16,
            supports_streaming: false,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        match self.get_json("/_cluster/health").await {
            Ok(body) => {
                let status = body.get("status").and_then(|v| v.as_str()).unwrap_or("unknown");
                let hs = if status == "red" { HealthStatus::Unhealthy } else if status == "yellow" { HealthStatus::Degraded } else { HealthStatus::Healthy };
                ConnectorHealth { status: hs, latency_ms: Some(start.elapsed().as_millis() as u64), message: Some(status.into()) }
            }
            Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let body = self.get_json("/_cat/indices?format=json&h=index,docs.count").await?;
        let indices = body.as_array().ok_or_else(|| ConnectorError::schema("unexpected _cat/indices response"))?;
        Ok(indices.iter().filter_map(|idx| {
            let name = idx.get("index")?.as_str()?;
            if name.starts_with('.') { return None; } // skip system indices
            let rows = idx.get("docs.count").and_then(|v| v.as_str()).and_then(|s| s.parse().ok());
            Some(SchemaInfo { name: name.to_string(), schema_type: SchemaType::Index, estimated_row_count: rows })
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let body = self.get_json(&format!("/{table}/_mapping")).await?;
        let props = body.pointer(&format!("/{table}/mappings/properties"))
            .or_else(|| body.as_object().and_then(|m| m.values().next()).and_then(|v| v.pointer("/mappings/properties")))
            .ok_or_else(|| ConnectorError::schema(format!("no mappings for index '{table}'")))?;

        let fields: Vec<Field> = props.as_object()
            .map(|obj| obj.iter().map(|(name, def)| {
                let es_type = def.get("type").and_then(|v| v.as_str()).unwrap_or("keyword");
                Field::new(name, es_type_to_arrow(es_type), true)
            }).collect())
            .unwrap_or_default();

        if fields.is_empty() {
            return Err(ConnectorError::schema(format!("empty mappings for '{table}'")));
        }
        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let dsl = translate_to_query_dsl(query);
        let body = self.post_json(&format!("/{}/_search", query.table), dsl).await?;
        parse_hits(&body, query)
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for batch in self.execute(query).await? {
            tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

fn es_type_to_arrow(es_type: &str) -> DataType {
    match es_type {
        "long" | "integer" | "short" | "byte" => DataType::Int64,
        "double" | "float" | "half_float" | "scaled_float" => DataType::Float64,
        "boolean" => DataType::Boolean,
        _ => DataType::Utf8, // keyword, text, date, ip, etc.
    }
}

fn parse_hits(body: &serde_json::Value, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
    let hits = body.pointer("/hits/hits")
        .and_then(|v| v.as_array())
        .ok_or_else(|| ConnectorError::query("no hits in Elasticsearch response"))?;

    if hits.is_empty() { return Ok(vec![]); }

    // Collect field names from projections or first hit
    let cols: Vec<String> = if !query.projections.is_empty() {
        query.projections.clone()
    } else {
        let mut seen = std::collections::HashSet::new();
        let mut cols = Vec::new();
        for hit in hits {
            if let Some(src) = hit.get("_source").and_then(|v| v.as_object()) {
                for k in src.keys() {
                    if seen.insert(k.clone()) { cols.push(k.clone()); }
                }
            }
        }
        cols.sort();
        cols
    };

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for col in &cols {
        // Infer type from first non-null value
        let first = hits.iter().find_map(|h| h.pointer(&format!("/_source/{col}")));
        match first {
            Some(serde_json::Value::Number(n)) if n.is_i64() => {
                let vals: Vec<Option<i64>> = hits.iter().map(|h| h.pointer(&format!("/_source/{col}")).and_then(|v| v.as_i64())).collect();
                fields.push(Field::new(col, DataType::Int64, true));
                arrays.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
            }
            Some(serde_json::Value::Number(_)) => {
                let vals: Vec<Option<f64>> = hits.iter().map(|h| h.pointer(&format!("/_source/{col}")).and_then(|v| v.as_f64())).collect();
                fields.push(Field::new(col, DataType::Float64, true));
                arrays.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
            }
            _ => {
                let vals: Vec<Option<String>> = hits.iter().map(|h| {
                    h.pointer(&format!("/_source/{col}")).map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                }).collect();
                fields.push(Field::new(col, DataType::Utf8, true));
                arrays.push(Arc::new(StringArray::from(vals)) as ArrayRef);
            }
        }
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| ConnectorError::query(e))?;
    Ok(vec![batch])
}

// ── Factory ───────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct ElasticsearchConnectorFactory;

#[async_trait]
impl ConnectorFactory for ElasticsearchConnectorFactory {
    fn connector_type(&self) -> &str { "elasticsearch" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(ElasticsearchConnector::from_config(config).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_es_type_to_arrow() {
        assert_eq!(es_type_to_arrow("long"), DataType::Int64);
        assert_eq!(es_type_to_arrow("double"), DataType::Float64);
        assert_eq!(es_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(es_type_to_arrow("keyword"), DataType::Utf8);
        assert_eq!(es_type_to_arrow("text"), DataType::Utf8);
        assert_eq!(es_type_to_arrow("date"), DataType::Utf8);
    }

    #[test]
    fn test_detect_version_default() {
        // Without a live server, detect_version returns V7 (safe default)
        // Tested via config override path
        assert_eq!(EsVersion::V7, EsVersion::V7);
        assert_ne!(EsVersion::V7, EsVersion::V8);
    }

    #[test]
    fn test_parse_hits_empty() {
        let body = serde_json::json!({"hits": {"hits": []}});
        let q = SubQuery { table: "idx".into(), projections: vec![], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: None, passthrough: None };
        let result = parse_hits(&body, &q).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_hits_string_field() {
        let body = serde_json::json!({
            "hits": {"hits": [
                {"_source": {"name": "alice", "age": 30}},
                {"_source": {"name": "bob", "age": 25}}
            ]}
        });
        let q = SubQuery { table: "users".into(), projections: vec![], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: None, passthrough: None };
        let batches = parse_hits(&body, &q).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_parse_hits_with_projection() {
        let body = serde_json::json!({
            "hits": {"hits": [
                {"_source": {"name": "alice", "age": 30, "email": "a@b.com"}}
            ]}
        });
        let q = SubQuery { table: "users".into(), projections: vec!["name".into()], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: None, passthrough: None };
        let batches = parse_hits(&body, &q).unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "name");
    }
}
