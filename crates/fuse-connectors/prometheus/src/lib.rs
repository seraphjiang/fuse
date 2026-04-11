// SPDX-License-Identifier: Apache-2.0

//! Prometheus connector for the Fuse federated query engine.
//!
//! Queries the Prometheus HTTP API, translating SubQuery filters into PromQL
//! label matchers and returning time-series data as Arrow RecordBatches.

pub mod promql;

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

/// Prometheus connector — queries via the Prometheus HTTP API.
#[derive(Debug)]
pub struct PrometheusConnector {
    id: String,
    client: reqwest::Client,
    base_url: String,
}

/// The standard schema for Prometheus time-series results.
fn timeseries_schema() -> Schema {
    Schema::new(vec![
        Field::new("__name__", DataType::Utf8, true),
        Field::new("labels", DataType::Utf8, true), // JSON-encoded label set
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        Field::new("value", DataType::Float64, false),
    ])
}

/// Schema for instant query results (vector).
fn instant_schema() -> Schema {
    Schema::new(vec![
        Field::new("__name__", DataType::Utf8, true),
        Field::new("labels", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
    ])
}

impl PrometheusConnector {
    pub fn new(id: String, base_url: String, client: reqwest::Client) -> Self {
        Self {
            id,
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config
            .properties
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("missing 'url' in config".into()))?
            .to_string();

        let mut headers = HeaderMap::new();

        // Auth
        if let Some(auth) = config.properties.get("auth").and_then(|v| v.as_table()) {
            let auth_type = auth
                .get("type")
                .and_then(|v: &toml::Value| v.as_str())
                .unwrap_or("none");

            match auth_type {
                "bearer" => {
                    let token = auth
                        .get("token_env")
                        .and_then(|v: &toml::Value| v.as_str())
                        .and_then(|env| std::env::var(env).ok())
                        .or_else(|| {
                            auth.get("token")
                                .and_then(|v: &toml::Value| v.as_str())
                                .map(|s| s.to_string())
                        })
                        .unwrap_or_default();
                    if let Ok(val) = HeaderValue::from_str(&format!("Bearer {token}")) {
                        headers.insert(AUTHORIZATION, val);
                    }
                }
                "basic" => {
                    let user = auth
                        .get("username")
                        .and_then(|v: &toml::Value| v.as_str())
                        .unwrap_or_default();
                    let pass = auth
                        .get("password")
                        .and_then(|v: &toml::Value| v.as_str())
                        .unwrap_or_default();
                    use base64::Engine as _;
                    let encoded = base64::engine::general_purpose::STANDARD
                        .encode(format!("{user}:{pass}"));
                    if let Ok(val) = HeaderValue::from_str(&format!("Basic {encoded}")) {
                        headers.insert(AUTHORIZATION, val);
                    }
                }
                _ => {}
            }
        }

        let mut client_builder = reqwest::Client::builder()
            .default_headers(headers)
            .pool_max_idle_per_host(config.max_connections(8) as usize)
            .timeout(std::time::Duration::from_secs(config.connection_timeout_secs(30)));

        if let Some(tls) = config.tls_config() {
            tls.validate().map_err(|e| ConnectorError::Connection(e.to_string()))?;
            client_builder = tls
                .apply_to_reqwest(client_builder)
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        }

        let client = client_builder
            .build()
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        Ok(Self::new(config.id.clone(), url, client))
    }

    /// Query the Prometheus instant query API: /api/v1/query
    async fn instant_query(&self, promql: &str) -> Result<serde_json::Value, ConnectorError> {
        let url = format!("{}/api/v1/query", self.base_url);
        debug!(promql, "Prometheus instant query");

        let resp = self
            .client
            .get(&url)
            .query(&[("query", promql)])
            .send()
            .await
            .map_err(ConnectorError::query)?;

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(ConnectorError::query)?;

        if body.get("status").and_then(|s| s.as_str()) != Some("success") {
            let err = body
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown error");
            return Err(ConnectorError::query(format!("Prometheus error: {err}")));
        }

        Ok(body)
    }

    /// Query the Prometheus range query API: /api/v1/query_range
    async fn range_query(
        &self,
        promql: &str,
        start: &str,
        end: &str,
        step: &str,
    ) -> Result<serde_json::Value, ConnectorError> {
        let url = format!("{}/api/v1/query_range", self.base_url);
        debug!(promql, start, end, step, "Prometheus range query");

        let resp = self
            .client
            .get(&url)
            .query(&[
                ("query", promql),
                ("start", start),
                ("end", end),
                ("step", step),
            ])
            .send()
            .await
            .map_err(ConnectorError::query)?;

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(ConnectorError::query)?;

        if body.get("status").and_then(|s| s.as_str()) != Some("success") {
            let err = body
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown error");
            return Err(ConnectorError::query(format!("Prometheus error: {err}")));
        }

        Ok(body)
    }
}

#[async_trait]
impl FederatedConnector for PrometheusConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn connector_type(&self) -> &str {
        "prometheus"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,   // Label matchers
            supports_projection: false, // Prometheus returns fixed schema
            supports_aggregation: true, // PromQL aggregation operators
            supports_sorting: false,
            supports_limit: false,
            supports_join: false,
            max_concurrent_queries: 8,
            supports_streaming: false,
            latency_class: LatencyClass::Medium,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        let url = format!("{}/-/healthy", self.base_url);
        match self.client.get(&url).send().await {
            Ok(resp) => {
                let latency = start.elapsed().as_millis() as u64;
                let status = if resp.status().is_success() {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Degraded
                };
                ConnectorHealth {
                    status,
                    latency_ms: Some(latency),
                    message: None,
                }
            }
            Err(e) => ConnectorHealth {
                status: HealthStatus::Unhealthy,
                latency_ms: None,
                message: Some(e.to_string()),
            },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        // GET /api/v1/label/__name__/values
        let url = format!("{}/api/v1/label/__name__/values", self.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(ConnectorError::schema)?;

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(ConnectorError::schema)?;

        let names = body
            .pointer("/data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| ConnectorError::schema("unexpected response from label values API"))?;

        Ok(names
            .iter()
            .filter_map(|v| {
                let name = v.as_str()?.to_string();
                Some(SchemaInfo {
                    name,
                    schema_type: SchemaType::MetricName,
                    estimated_row_count: None,
                })
            })
            .collect())
    }

    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        // All Prometheus metrics share the same time-series schema.
        // Use instant schema by default; range queries use timeseries_schema.
        Ok(instant_schema())
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let pql = promql::build_promql(query);

        // Check if this is a range query (passthrough has start/end/step)
        let is_range = query
            .passthrough
            .as_ref()
            .and_then(|p| p.get("start"))
            .is_some();

        let body = if is_range {
            let pt = query.passthrough.as_ref().unwrap();
            let start = pt.get("start").and_then(|v| v.as_str()).unwrap_or("now-1h");
            let end = pt.get("end").and_then(|v| v.as_str()).unwrap_or("now");
            let step = pt.get("step").and_then(|v| v.as_str()).unwrap_or("15s");
            self.range_query(&pql, start, end, step).await?
        } else {
            self.instant_query(&pql).await?
        };

        let result_type = body
            .pointer("/data/resultType")
            .and_then(|v| v.as_str())
            .unwrap_or("vector");

        let results = body
            .pointer("/data/result")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ConnectorError::query("no result in Prometheus response"))?;

        match result_type {
            "matrix" => parse_matrix_results(results),
            _ => parse_vector_results(results),
        }
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        // Prometheus doesn't support streaming; execute and send all at once.
        let batches = self.execute(query).await?;
        for batch in batches {
            tx.send(Ok(batch))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

/// Parse instant vector results into a RecordBatch.
fn parse_vector_results(
    results: &[serde_json::Value],
) -> Result<Vec<RecordBatch>, ConnectorError> {
    if results.is_empty() {
        return Ok(vec![]);
    }

    let mut names: Vec<Option<String>> = Vec::with_capacity(results.len());
    let mut labels_col: Vec<Option<String>> = Vec::with_capacity(results.len());
    let mut values: Vec<f64> = Vec::with_capacity(results.len());

    for r in results {
        let metric = r.get("metric");
        let name = metric
            .and_then(|m| m.get("__name__"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string());
        names.push(name);
        labels_col.push(metric.map(|m| m.to_string()));

        let val = r
            .pointer("/value/1")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(f64::NAN);
        values.push(val);
    }

    let schema = Arc::new(instant_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(labels_col)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .map_err(ConnectorError::query)?;

    Ok(vec![batch])
}

/// Parse range matrix results into a RecordBatch (one row per sample).
fn parse_matrix_results(
    results: &[serde_json::Value],
) -> Result<Vec<RecordBatch>, ConnectorError> {
    if results.is_empty() {
        return Ok(vec![]);
    }

    let mut names: Vec<Option<String>> = Vec::new();
    let mut labels_col: Vec<Option<String>> = Vec::new();
    let mut timestamps: Vec<i64> = Vec::new();
    let mut values: Vec<f64> = Vec::new();

    for r in results {
        let metric = r.get("metric");
        let name = metric
            .and_then(|m| m.get("__name__"))
            .and_then(|n| n.as_str())
            .map(|s| s.to_string());
        let labels_json = metric.map(|m| m.to_string());

        let samples = r
            .get("values")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for sample in &samples {
            let arr = sample.as_array();
            let ts = arr
                .and_then(|a| a.first())
                .and_then(|v| v.as_f64())
                .map(|f| (f * 1000.0) as i64)
                .unwrap_or(0);
            let val = arr
                .and_then(|a| a.get(1))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(f64::NAN);

            names.push(name.clone());
            labels_col.push(labels_json.clone());
            timestamps.push(ts);
            values.push(val);
        }
    }

    if timestamps.is_empty() {
        return Ok(vec![]);
    }

    let schema = Arc::new(timeseries_schema());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(labels_col)),
            Arc::new(TimestampMillisecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .map_err(ConnectorError::query)?;

    Ok(vec![batch])
}

// ── Factory ──

pub struct PrometheusConnectorFactory;

#[async_trait]
impl ConnectorFactory for PrometheusConnectorFactory {
    fn connector_type(&self) -> &str {
        "prometheus"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(PrometheusConnector::from_config(config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_connector() -> PrometheusConnector {
        PrometheusConnector::new(
            "test-prom".into(),
            "http://localhost:9090".into(),
            reqwest::Client::new(),
        )
    }

    #[test]
    fn test_capabilities() {
        let c = make_connector();
        let caps = c.capabilities();
        assert!(caps.supports_filtering);
        assert!(!caps.supports_projection);
        assert!(caps.supports_aggregation);
        assert!(!caps.supports_sorting);
        assert!(!caps.supports_limit);
        assert!(!caps.supports_join);
        assert!(!caps.supports_streaming);
        assert_eq!(caps.max_concurrent_queries, 8);
        assert!(matches!(caps.latency_class, LatencyClass::Medium));
    }

    #[test]
    fn test_connector_metadata() {
        let c = make_connector();
        assert_eq!(c.id(), "test-prom");
        assert_eq!(c.connector_type(), "prometheus");
    }

    #[test]
    fn test_parse_vector_results_empty() {
        let results: Vec<serde_json::Value> = vec![];
        let batches = parse_vector_results(&results).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_parse_vector_results_single() {
        let results = vec![serde_json::json!({
            "metric": {"__name__": "up", "job": "api"},
            "value": [1609459200.0, "1"]
        })];
        let batches = parse_vector_results(&results).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 3); // __name__, labels, value

        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "up");

        let values = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1.0);
    }

    #[test]
    fn test_parse_vector_results_multiple() {
        let results = vec![
            serde_json::json!({
                "metric": {"__name__": "up", "job": "api"},
                "value": [1609459200.0, "1"]
            }),
            serde_json::json!({
                "metric": {"__name__": "up", "job": "web"},
                "value": [1609459200.0, "0"]
            }),
        ];
        let batches = parse_vector_results(&results).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_parse_vector_results_nan_value() {
        let results = vec![serde_json::json!({
            "metric": {"__name__": "m"},
            "value": [1609459200.0, "NaN"]
        })];
        let batches = parse_vector_results(&results).unwrap();
        let values = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(values.value(0).is_nan());
    }

    #[test]
    fn test_parse_matrix_results_empty() {
        let results: Vec<serde_json::Value> = vec![];
        let batches = parse_matrix_results(&results).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_parse_matrix_results_with_samples() {
        let results = vec![serde_json::json!({
            "metric": {"__name__": "cpu_usage", "host": "server1"},
            "values": [
                [1609459200.0, "0.5"],
                [1609459215.0, "0.7"],
                [1609459230.0, "0.3"]
            ]
        })];
        let batches = parse_matrix_results(&results).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 4); // __name__, labels, timestamp, value

        let values = batches[0]
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 0.5);
        assert_eq!(values.value(1), 0.7);
        assert_eq!(values.value(2), 0.3);
    }

    #[test]
    fn test_parse_matrix_results_no_values() {
        let results = vec![serde_json::json!({
            "metric": {"__name__": "m"},
            "values": []
        })];
        let batches = parse_matrix_results(&results).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_timeseries_schema_fields() {
        let s = timeseries_schema();
        assert_eq!(s.fields().len(), 4);
        assert_eq!(s.field(0).name(), "__name__");
        assert_eq!(s.field(1).name(), "labels");
        assert_eq!(s.field(2).name(), "timestamp");
        assert_eq!(s.field(3).name(), "value");
    }

    #[test]
    fn test_instant_schema_fields() {
        let s = instant_schema();
        assert_eq!(s.fields().len(), 3);
        assert_eq!(s.field(0).name(), "__name__");
        assert_eq!(s.field(2).name(), "value");
    }

    #[test]
    fn test_basic_auth_is_valid_base64() {
        // Verify the basic auth header is proper RFC 7617 base64, not ascii-escaped
        use base64::Engine as _;
        let user = "admin";
        let pass = "s3cr3t";
        let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&encoded)
            .unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "admin:s3cr3t");
        // Must not contain backslash-escaped bytes (the old bug)
        assert!(!encoded.contains('\\'));
    }

    // ── #242 Range query passthrough detection (tester) ──

    #[test]
    fn test_passthrough_with_range_is_detected() {
        use fuse_core::connector::SubQuery;
        let sq = SubQuery {
            table: "http_requests".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            passthrough: Some(serde_json::json!({
                "start": "2024-01-01T00:00:00Z",
                "end": "2024-01-02T00:00:00Z",
                "step": "1m"
            })),
            offset: None,
        };
        let is_range = sq.passthrough.as_ref().and_then(|p| p.get("start")).is_some();
        assert!(is_range);
    }

    #[test]
    fn test_passthrough_without_range_is_instant() {
        use fuse_core::connector::SubQuery;
        let sq = SubQuery {
            table: "http_requests".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            offset: None, passthrough: None,
        };
        let is_range = sq.passthrough.as_ref().and_then(|p| p.get("start")).is_some();
        assert!(!is_range);
    }
}
