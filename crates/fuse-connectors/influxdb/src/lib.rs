// SPDX-License-Identifier: Apache-2.0

//! InfluxDB connector for the Fuse federated query engine.
//!
//! Supports InfluxDB 1.x (InfluxQL via /query) and 2.x (Flux via /api/v2/query).
//! Version detected from config or via GET /.
//! Filter pushdown to InfluxQL WHERE clause.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InfluxVersion { V1, V2 }

#[derive(Debug)]
pub struct InfluxDbConnector {
    id: String,
    client: reqwest::Client,
    base_url: String,
    version: InfluxVersion,
    /// v1: database name; v2: bucket name
    bucket: String,
    /// v2: org name
    org: String,
}

impl InfluxDbConnector {
    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config.properties.get("url")
            .and_then(|v| v.as_str())
            .unwrap_or("http://localhost:8086")
            .trim_end_matches('/')
            .to_string();

        let bucket = config.properties.get("bucket")
            .or_else(|| config.properties.get("database"))
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        let org = config.properties.get("org")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut headers = HeaderMap::new();
        if let Some(token) = config.properties.get("token").and_then(|v| v.as_str()) {
            let val = HeaderValue::from_str(&format!("Token {token}"))
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            headers.insert(AUTHORIZATION, val);
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

        let version = if let Some(v) = config.properties.get("version").and_then(|v| v.as_integer()) {
            if v >= 2 { InfluxVersion::V2 } else { InfluxVersion::V1 }
        } else {
            // Detect: v2 has /health endpoint returning {"status":"pass"}
            let is_v2 = client.get(format!("{url}/health")).send().await
                .ok()
                .and_then(|r| if r.status().is_success() { Some(()) } else { None })
                .is_some();
            if is_v2 { InfluxVersion::V2 } else { InfluxVersion::V1 }
        };

        Ok(Self { id: config.id.clone(), client, base_url: url, version, bucket, org })
    }

    async fn query_v1(&self, influxql: &str) -> Result<serde_json::Value, ConnectorError> {
        debug!(influxql, "InfluxDB v1 query");
        self.client
            .get(format!("{}/query", self.base_url))
            .query(&[("db", &self.bucket), ("q", &influxql.to_string())])
            .send().await.map_err(ConnectorError::query)?
            .json().await.map_err(ConnectorError::query)
    }

    async fn query_v2(&self, flux: &str) -> Result<String, ConnectorError> {
        debug!(flux, "InfluxDB v2 Flux query");
        let body = serde_json::json!({"query": flux, "type": "flux"});
        let resp = self.client
            .post(format!("{}/api/v2/query", self.base_url))
            .query(&[("org", &self.org)])
            .header("Accept", "application/csv")
            .json(&body)
            .send().await.map_err(ConnectorError::query)?;
        resp.text().await.map_err(ConnectorError::query)
    }
}

#[async_trait]
impl FederatedConnector for InfluxDbConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "influxdb" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: false, // InfluxQL selects all fields
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 4,
            supports_streaming: false,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        let url = match self.version {
            InfluxVersion::V1 => format!("{}/ping", self.base_url),
            InfluxVersion::V2 => format!("{}/health", self.base_url),
        };
        match self.client.get(&url).send().await {
            Ok(r) if r.status().is_success() || r.status().as_u16() == 204 =>
                ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(start.elapsed().as_millis() as u64), message: None },
            Ok(r) =>
                ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(format!("HTTP {}", r.status())) },
            Err(e) =>
                ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        match self.version {
            InfluxVersion::V1 => {
                let body = self.query_v1("SHOW MEASUREMENTS").await?;
                let names: Vec<String> = body
                    .pointer("/results/0/series/0/values")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|row| row.get(0)?.as_str().map(|s| s.to_string())).collect())
                    .unwrap_or_default();
                Ok(names.into_iter().map(|name| SchemaInfo { name, schema_type: SchemaType::MetricName, estimated_row_count: None }).collect())
            }
            InfluxVersion::V2 => {
                let _flux = format!("buckets() |> filter(fn: (r) => r.name == \"{}\") |> yield()", self.bucket);
                // For schema discovery, list measurements via Flux schema package
                let flux = format!("import \"influxdata/influxdb/schema\"\nschema.measurements(bucket: \"{}\")", self.bucket);
                let csv = self.query_v2(&flux).await?;
                let names = parse_flux_csv_column(&csv, "_value");
                Ok(names.into_iter().map(|name| SchemaInfo { name, schema_type: SchemaType::MetricName, estimated_row_count: None }).collect())
            }
        }
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        // InfluxDB schema: time (always), tags (string), fields (numeric)
        // Sample one row to infer field types
        let fields = match self.version {
            InfluxVersion::V1 => {
                let body = self.query_v1(&format!("SHOW FIELD KEYS FROM \"{table}\"")).await?;
                let rows = body.pointer("/results/0/series/0/values")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let mut fields = vec![Field::new("time", DataType::Utf8, false)];
                for row in &rows {
                    let name = row.get(0).and_then(|v| v.as_str()).unwrap_or("field");
                    let ftype = row.get(1).and_then(|v| v.as_str()).unwrap_or("string");
                    let dt = match ftype { "float" | "integer" => DataType::Float64, _ => DataType::Utf8 };
                    fields.push(Field::new(name, dt, true));
                }
                fields
            }
            InfluxVersion::V2 => {
                vec![
                    Field::new("_time", DataType::Utf8, false),
                    Field::new("_measurement", DataType::Utf8, false),
                    Field::new("_field", DataType::Utf8, false),
                    Field::new("_value", DataType::Float64, true),
                ]
            }
        };
        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        match self.version {
            InfluxVersion::V1 => {
                let influxql = subquery_to_influxql(query);
                let body = self.query_v1(&influxql).await?;
                parse_v1_response(&body)
            }
            InfluxVersion::V2 => {
                let flux = subquery_to_flux(query, &self.bucket);
                let csv = self.query_v2(&flux).await?;
                parse_flux_csv(&csv)
            }
        }
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

// ── Query translation ─────────────────────────────────────────────────────────

fn subquery_to_influxql(q: &SubQuery) -> String {
    let select = if q.projections.is_empty() { "*".to_string() } else { q.projections.join(", ") };
    let mut sql = format!("SELECT {select} FROM \"{}\"", q.table);
    if let Some(f) = &q.filter {
        sql.push_str(&format!(" WHERE {}", filter_to_influxql(f)));
    }
    if let Some(limit) = q.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    sql
}

/// Sanitize a string value for InfluxQL single-quoted literals.
fn sanitize_influxql(s: &str) -> String {
    s.replace('\\', "\\\\")
     .replace('\'', "\\'")
     .replace('\n', "\\n")
     .replace('\r', "\\r")
     .replace('\0', "")
}

/// Sanitize an identifier (field/measurement name) — allow only alphanumeric + underscore + dot.
fn sanitize_identifier(s: &str) -> String {
    let clean: String = s.chars().filter(|c| c.is_alphanumeric() || *c == '_' || *c == '.').collect();
    if clean.is_empty() { "_".to_string() } else { clean }
}

fn filter_to_influxql(f: &FilterExpr) -> String {
    match f {
        FilterExpr::And(l, r) => format!("({} AND {})", filter_to_influxql(l), filter_to_influxql(r)),
        FilterExpr::Or(l, r) => format!("({} OR {})", filter_to_influxql(l), filter_to_influxql(r)),
        FilterExpr::Not(inner) => format!("NOT ({})", filter_to_influxql(inner)),
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=", ComparisonOp::Neq => "!=",
                ComparisonOp::Lt => "<", ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">", ComparisonOp::Gte => ">=",
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => "=~",
            };
            let val = match value {
                ScalarValue::Utf8(s) => format!("'{}'", sanitize_influxql(s)),
                ScalarValue::Int64(n) => n.to_string(),
                ScalarValue::Float64(f) => f.to_string(),
                ScalarValue::Boolean(b) => b.to_string(),
                ScalarValue::Null => "null".to_string(),
            };
            format!("{} {op_str} {val}", sanitize_identifier(field))
        }
        FilterExpr::In { field, values } => {
            let parts: Vec<String> = values.iter().map(|v| {
                filter_to_influxql(&FilterExpr::Comparison { field: field.clone(), op: ComparisonOp::Eq, value: v.clone() })
            }).collect();
            format!("({})", parts.join(" OR "))
        }
        FilterExpr::IsNull(field) => format!("{} = ''", sanitize_identifier(field)),
        FilterExpr::IsNotNull(field) => format!("{} != ''", sanitize_identifier(field)),
    }
}

fn subquery_to_flux(q: &SubQuery, bucket: &str) -> String {
    let mut flux = format!("from(bucket: \"{bucket}\") |> range(start: -30d) |> filter(fn: (r) => r._measurement == \"{}\")", q.table);
    if let Some(f) = &q.filter {
        flux.push_str(&format!(" |> filter(fn: (r) => {})", filter_to_flux(f)));
    }
    if let Some(limit) = q.limit {
        flux.push_str(&format!(" |> limit(n: {limit})"));
    }
    flux
}

fn filter_to_flux(f: &FilterExpr) -> String {
    match f {
        FilterExpr::And(l, r) => format!("({} and {})", filter_to_flux(l), filter_to_flux(r)),
        FilterExpr::Or(l, r) => format!("({} or {})", filter_to_flux(l), filter_to_flux(r)),
        FilterExpr::Not(inner) => format!("not ({})", filter_to_flux(inner)),
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "==", ComparisonOp::Neq => "!=",
                ComparisonOp::Lt => "<", ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">", ComparisonOp::Gte => ">=",
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => "=~",
            };
            let val = match value {
                ScalarValue::Utf8(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
                ScalarValue::Int64(n) => n.to_string(),
                ScalarValue::Float64(f) => f.to_string(),
                ScalarValue::Boolean(b) => b.to_string(),
                ScalarValue::Null => "null".to_string(),
            };
            format!("r.{} {op_str} {val}", sanitize_identifier(field))
        }
        FilterExpr::In { field, values } => {
            let parts: Vec<String> = values.iter().map(|v| {
                filter_to_flux(&FilterExpr::Comparison { field: field.clone(), op: ComparisonOp::Eq, value: v.clone() })
            }).collect();
            format!("({})", parts.join(" or "))
        }
        FilterExpr::IsNull(field) => format!("not exists r.{}", sanitize_identifier(field)),
        FilterExpr::IsNotNull(field) => format!("exists r.{}", sanitize_identifier(field)),
    }
}

// ── Response parsing ──────────────────────────────────────────────────────────

fn parse_v1_response(body: &serde_json::Value) -> Result<Vec<RecordBatch>, ConnectorError> {
    let series = body.pointer("/results/0/series/0");
    let Some(series) = series else { return Ok(vec![]); };

    let cols: Vec<String> = series.get("columns")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default();

    let rows = series.get("values").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    if rows.is_empty() { return Ok(vec![]); }

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (i, col) in cols.iter().enumerate() {
        let first = rows.iter().find_map(|r| r.get(i)).and_then(|v| if v.is_null() { None } else { Some(v) });
        match first {
            Some(serde_json::Value::Number(n)) if n.is_i64() => {
                let vals: Vec<Option<i64>> = rows.iter().map(|r| r.get(i).and_then(|v| v.as_i64())).collect();
                fields.push(Field::new(col, DataType::Int64, true));
                arrays.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
            }
            Some(serde_json::Value::Number(_)) => {
                let vals: Vec<Option<f64>> = rows.iter().map(|r| r.get(i).and_then(|v| v.as_f64())).collect();
                fields.push(Field::new(col, DataType::Float64, true));
                arrays.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
            }
            _ => {
                let vals: Vec<Option<String>> = rows.iter().map(|r| r.get(i).and_then(|v| v.as_str()).map(|s| s.to_string())).collect();
                fields.push(Field::new(col, DataType::Utf8, true));
                arrays.push(Arc::new(StringArray::from(vals)) as ArrayRef);
            }
        }
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(ConnectorError::query)?;
    Ok(vec![batch])
}

fn parse_flux_csv(csv: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
    // Flux CSV has annotation rows starting with #; skip them
    let data_rows: Vec<&str> = csv.lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .collect();
    if data_rows.len() < 2 { return Ok(vec![]); }

    let headers: Vec<&str> = data_rows[0].split(',').collect();
    let mut col_data: Vec<Vec<Option<String>>> = vec![vec![]; headers.len()];

    for row in &data_rows[1..] {
        let vals: Vec<&str> = row.split(',').collect();
        for (i, h) in headers.iter().enumerate() {
            let _ = h; // used via index
            let v = vals.get(i).copied().unwrap_or("");
            col_data[i].push(if v.is_empty() { None } else { Some(v.to_string()) });
        }
    }

    let fields: Vec<Field> = headers.iter().map(|h| Field::new(*h, DataType::Utf8, true)).collect();
    let arrays: Vec<ArrayRef> = col_data.into_iter()
        .map(|vals| Arc::new(StringArray::from(vals)) as ArrayRef)
        .collect();

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(ConnectorError::query)?;
    Ok(vec![batch])
}

fn parse_flux_csv_column(csv: &str, col: &str) -> Vec<String> {
    let data_rows: Vec<&str> = csv.lines().filter(|l| !l.starts_with('#') && !l.is_empty()).collect();
    if data_rows.len() < 2 { return vec![]; }
    let headers: Vec<&str> = data_rows[0].split(',').collect();
    let idx = headers.iter().position(|h| *h == col);
    let Some(idx) = idx else { return vec![]; };
    data_rows[1..].iter().filter_map(|row| {
        row.split(',').nth(idx).filter(|v| !v.is_empty()).map(|s| s.to_string())
    }).collect()
}

#[derive(Debug, Default)]
pub struct InfluxDbConnectorFactory;

#[async_trait]
impl ConnectorFactory for InfluxDbConnectorFactory {
    fn connector_type(&self) -> &str { "influxdb" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(InfluxDbConnector::from_config(config).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subquery_to_influxql_basic() {
        let q = SubQuery { table: "cpu".into(), projections: vec![], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: None, passthrough: None, offset: None };
        assert_eq!(subquery_to_influxql(&q), "SELECT * FROM \"cpu\"");
    }

    #[test]
    fn test_subquery_to_influxql_with_filter_and_limit() {
        let q = SubQuery {
            table: "cpu".into(),
            projections: vec!["usage".into()],
            filter: Some(FilterExpr::Comparison { field: "host".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("web01".into()) }),
            aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: Some(10), passthrough: None, offset: None,
        };
        let sql = subquery_to_influxql(&q);
        assert!(sql.contains("WHERE host = 'web01'"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_subquery_to_flux_basic() {
        let q = SubQuery { table: "cpu".into(), projections: vec![], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: None, passthrough: None, offset: None };
        let flux = subquery_to_flux(&q, "mybucket");
        assert!(flux.contains("from(bucket: \"mybucket\")"));
        assert!(flux.contains("r._measurement == \"cpu\""));
    }

    #[test]
    fn test_filter_to_flux_eq() {
        let f = FilterExpr::Comparison { field: "host".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("web01".into()) };
        assert_eq!(filter_to_flux(&f), "r.host == \"web01\"");
    }

    #[test]
    fn test_filter_to_flux_and() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "a".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(1) }),
            Box::new(FilterExpr::Comparison { field: "b".into(), op: ComparisonOp::Gt, value: ScalarValue::Int64(0) }),
        );
        let flux = filter_to_flux(&f);
        assert!(flux.contains(" and "));
    }

    #[test]
    fn test_parse_flux_csv_empty() {
        assert!(parse_flux_csv("").unwrap().is_empty());
        assert!(parse_flux_csv("# annotation\n").unwrap().is_empty());
    }

    #[test]
    fn test_parse_flux_csv_column() {
        let csv = "_value,_measurement\n42,cpu\n55,mem\n";
        let vals = parse_flux_csv_column(csv, "_value");
        assert_eq!(vals, vec!["42", "55"]);
    }

    #[test]
    fn test_parse_v1_response_empty() {
        let body = serde_json::json!({"results": [{}]});
        assert!(parse_v1_response(&body).unwrap().is_empty());
    }

    #[test]
    fn test_influx_version_enum() {
        assert_ne!(InfluxVersion::V1, InfluxVersion::V2);
    }

    // ── #451 Verification tests (tester) ──

    #[test]
    fn test_influxql_projections() {
        let q = SubQuery { table: "cpu".into(), projections: vec!["usage".into(), "host".into()], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: None, passthrough: None, offset: None };
        let sql = subquery_to_influxql(&q);
        assert!(sql.contains("SELECT usage, host FROM"));
    }

    #[test]
    fn test_flux_with_filter() {
        let q = SubQuery {
            table: "cpu".into(), projections: vec![],
            filter: Some(FilterExpr::Comparison { field: "host".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("web01".into()) }),
            aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: None, passthrough: None, offset: None,
        };
        let flux = subquery_to_flux(&q, "metrics");
        assert!(flux.contains("|> filter(fn: (r) =>"));
        assert!(flux.contains("r.host == \"web01\""));
    }

    #[test]
    fn test_flux_with_limit() {
        let q = SubQuery { table: "cpu".into(), projections: vec![], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: Some(5), passthrough: None, offset: None };
        let flux = subquery_to_flux(&q, "b");
        assert!(flux.contains("|> limit(n: 5)"));
    }

    #[test]
    fn test_filter_to_flux_or() {
        let f = FilterExpr::Or(
            Box::new(FilterExpr::Comparison { field: "a".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(1) }),
            Box::new(FilterExpr::Comparison { field: "b".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(2) }),
        );
        assert!(filter_to_flux(&f).contains(" or "));
    }

    #[test]
    fn test_parse_v1_response_with_data() {
        let body = serde_json::json!({
            "results": [{"series": [{"name": "cpu", "columns": ["time", "usage"], "values": [["2026-01-01T00:00:00Z", 42.5]]}]}]
        });
        let batches = parse_v1_response(&body).unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
    }

    // ── #600 InfluxDB injection fix verification (tester) ──

    #[test]
    fn test_influxql_escapes_single_quotes() {
        let q = SubQuery {
            table: "cpu".into(), projections: vec![],
            filter: Some(FilterExpr::Comparison {
                field: "host".into(), op: ComparisonOp::Eq,
                value: ScalarValue::Utf8("test'inject".into()),
            }),
            aggregations: vec![], group_by: vec![], having: None,
            sort: vec![], limit: None, passthrough: None, offset: None,
        };
        let sql = subquery_to_influxql(&q);
        // The raw unescaped pattern "test'inject" should NOT appear — it should be "test\'inject"
        assert!(sql.contains("test\\'inject"), "InfluxQL should escape quote: {}", sql);
    }

    #[test]
    fn test_flux_escapes_double_quotes() {
        let f = FilterExpr::Comparison {
            field: "host".into(), op: ComparisonOp::Eq,
            value: ScalarValue::Utf8("test\"inject".into()),
        };
        let flux = filter_to_flux(&f);
        assert!(flux.contains("test\\\"inject"), "Flux should escape double quote: {}", flux);
    }

    #[test]
    fn test_sanitize_influxql_injection() {
        // Single quote injection attempt
        assert_eq!(sanitize_influxql("val'; DROP MEASUREMENT m; --"), "val\\'; DROP MEASUREMENT m; --");
        // Newline injection
        assert_eq!(sanitize_influxql("val\ninjected"), "val\\ninjected");
        // Null byte
        assert!(!sanitize_influxql("val\0bad").contains('\0'));
    }

    #[test]
    fn test_sanitize_identifier_strips_special() {
        assert_eq!(sanitize_identifier("host"), "host");
        assert_eq!(sanitize_identifier("host; DROP"), "hostDROP");
        assert_eq!(sanitize_identifier(""), "_");
        assert_eq!(sanitize_identifier("a.b_c"), "a.b_c");
    }

    #[test]
    fn test_influxql_filter_uses_sanitized_field() {
        let f = FilterExpr::Comparison {
            field: "host; DROP".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8("web01".into()),
        };
        let sql = filter_to_influxql(&f);
        assert!(!sql.contains(';'), "should strip semicolons from field: {}", sql);
        assert!(sql.contains("hostDROP"), "field should be sanitized: {}", sql);
    }
}
