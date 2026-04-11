// SPDX-License-Identifier: Apache-2.0

//! ClickHouse connector for the Fuse federated query engine.
//!
//! Uses the ClickHouse HTTP interface (port 8123).
//! SQL passthrough with full pushdown — ClickHouse is SQL-native.
//! Response format: JSONEachRow for efficient streaming parsing.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
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

// Reuse SQL generation from postgres connector pattern
pub mod sql;
pub use sql::subquery_to_sql;

#[derive(Debug)]
pub struct ClickHouseConnector {
    id: String,
    client: reqwest::Client,
    base_url: String,
    database: String,
}

impl ClickHouseConnector {
    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config.properties.get("url")
            .and_then(|v| v.as_str())
            .unwrap_or("http://localhost:8123")
            .trim_end_matches('/')
            .to_string();

        let database = config.properties.get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        let mut headers = HeaderMap::new();
        if let (Some(user), Some(pass)) = (
            config.properties.get("username").and_then(|v| v.as_str()),
            config.properties.get("password").and_then(|v| v.as_str()),
        ) {
            let encoded = base64_encode(&format!("{user}:{pass}"));
            let val = HeaderValue::from_str(&format!("Basic {encoded}"))
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            headers.insert(AUTHORIZATION, val);
        }

        let mut client_builder = reqwest::Client::builder()
            .default_headers(headers)
            .pool_max_idle_per_host(config.max_connections(16) as usize)
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

        Ok(Self { id: config.id.clone(), client, base_url: url, database })
    }

    async fn run_query(&self, sql: &str) -> Result<String, ConnectorError> {
        debug!(sql, "ClickHouse query");
        let query = format!("{sql} FORMAT JSONEachRow");
        self.client
            .post(&self.base_url)
            .query(&[("database", &self.database)])
            .body(query)
            .send().await.map_err(|e| ConnectorError::query(e))?
            .text().await.map_err(|e| ConnectorError::query(e))
    }
}

fn base64_encode(s: &str) -> String {
    use std::fmt::Write;
    let bytes = s.as_bytes();
    let mut out = String::new();
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = if chunk.len() > 1 { chunk[1] as usize } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as usize } else { 0 };
        let _ = write!(out, "{}{}{}{}", CHARS[b0 >> 2] as char, CHARS[((b0 & 3) << 4) | (b1 >> 4)] as char,
            if chunk.len() > 1 { CHARS[((b1 & 0xf) << 2) | (b2 >> 6)] as char } else { '=' },
            if chunk.len() > 2 { CHARS[b2 & 0x3f] as char } else { '=' });
    }
    out
}

#[async_trait]
impl FederatedConnector for ClickHouseConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "clickhouse" }

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
        match self.client.get(format!("{}/ping", self.base_url)).send().await {
            Ok(r) if r.status().is_success() =>
                ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(start.elapsed().as_millis() as u64), message: None },
            Ok(r) =>
                ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(format!("HTTP {}", r.status())) },
            Err(e) =>
                ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let text = self.run_query(&format!("SELECT name FROM system.tables WHERE database = '{}'", self.database)).await?;
        let names: Vec<String> = text.lines()
            .filter(|l| !l.is_empty())
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter_map(|v| v.get("name")?.as_str().map(|s| s.to_string()))
            .collect();
        Ok(names.into_iter().map(|name| SchemaInfo { name, schema_type: SchemaType::Table, estimated_row_count: None }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let text = self.run_query(&format!(
            "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{table}'",
            self.database
        )).await?;

        let fields: Vec<Field> = text.lines()
            .filter(|l| !l.is_empty())
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter_map(|v| {
                let name = v.get("name")?.as_str()?.to_string();
                let ch_type = v.get("type")?.as_str().unwrap_or("String");
                Some(Field::new(&name, ch_type_to_arrow(ch_type), true))
            })
            .collect();

        if fields.is_empty() {
            return Err(ConnectorError::schema(format!("table '{table}' not found in database '{}'", self.database)));
        }
        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let sql = subquery_to_sql(query);
        let text = self.run_query(&sql).await?;
        parse_json_each_row(&text)
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

/// Traverse a dot-separated path through a JSON object.
fn get_nested_json<'a>(mut val: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    for key in path.split('.') {
        val = val.get(key)?;
    }
    Some(val)
}

fn ch_type_to_arrow(ch_type: &str) -> DataType {
    // Strip Nullable() wrapper
    let inner = ch_type.trim_start_matches("Nullable(").trim_end_matches(')');
    match inner {
        t if t.starts_with("Int") || t.starts_with("UInt") => DataType::Int64,
        t if t.starts_with("Float") || t.starts_with("Decimal") => DataType::Float64,
        "Bool" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

fn parse_json_each_row(text: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
    let rows: Vec<serde_json::Value> = text.lines()
        .filter(|l| !l.is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();

    if rows.is_empty() { return Ok(vec![]); }

    // Collect columns from first row
    let cols: Vec<String> = rows[0].as_object()
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for col in &cols {
        let first = rows.iter().find_map(|r| get_nested_json(r, col)).filter(|v| !v.is_null());
        match first {
            Some(serde_json::Value::Number(n)) if n.is_i64() => {
                let vals: Vec<Option<i64>> = rows.iter().map(|r| get_nested_json(r, col).and_then(|v| v.as_i64())).collect();
                fields.push(Field::new(col, DataType::Int64, true));
                arrays.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
            }
            Some(serde_json::Value::Number(_)) => {
                let vals: Vec<Option<f64>> = rows.iter().map(|r| get_nested_json(r, col).and_then(|v| v.as_f64())).collect();
                fields.push(Field::new(col, DataType::Float64, true));
                arrays.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
            }
            Some(serde_json::Value::Bool(_)) => {
                let vals: Vec<Option<bool>> = rows.iter().map(|r| get_nested_json(r, col).and_then(|v| v.as_bool())).collect();
                fields.push(Field::new(col, DataType::Boolean, true));
                arrays.push(Arc::new(BooleanArray::from(vals)) as ArrayRef);
            }
            _ => {
                let vals: Vec<Option<String>> = rows.iter().map(|r| get_nested_json(r, col).map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })).collect();
                fields.push(Field::new(col, DataType::Utf8, true));
                arrays.push(Arc::new(StringArray::from(vals)) as ArrayRef);
            }
        }
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| ConnectorError::query(e))?;
    Ok(vec![batch])
}

#[derive(Debug, Default)]
pub struct ClickHouseConnectorFactory;

#[async_trait]
impl ConnectorFactory for ClickHouseConnectorFactory {
    fn connector_type(&self) -> &str { "clickhouse" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(ClickHouseConnector::from_config(config).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ch_type_to_arrow_int() {
        assert_eq!(ch_type_to_arrow("Int64"), DataType::Int64);
        assert_eq!(ch_type_to_arrow("UInt32"), DataType::Int64);
        assert_eq!(ch_type_to_arrow("Nullable(Int32)"), DataType::Int64);
    }

    #[test]
    fn test_ch_type_to_arrow_float() {
        assert_eq!(ch_type_to_arrow("Float64"), DataType::Float64);
        assert_eq!(ch_type_to_arrow("Decimal(10,2)"), DataType::Float64);
    }

    #[test]
    fn test_ch_type_to_arrow_bool() {
        assert_eq!(ch_type_to_arrow("Bool"), DataType::Boolean);
    }

    #[test]
    fn test_ch_type_to_arrow_string() {
        assert_eq!(ch_type_to_arrow("String"), DataType::Utf8);
        assert_eq!(ch_type_to_arrow("DateTime"), DataType::Utf8);
        assert_eq!(ch_type_to_arrow("FixedString(10)"), DataType::Utf8);
    }

    #[test]
    fn test_parse_json_each_row_empty() {
        assert!(parse_json_each_row("").unwrap().is_empty());
    }

    #[test]
    fn test_parse_json_each_row_single() {
        let text = r#"{"name":"alice","age":30}"#;
        let batches = parse_json_each_row(text).unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_parse_json_each_row_multiple() {
        let text = "{\"id\":1,\"val\":10.5}\n{\"id\":2,\"val\":20.0}\n";
        let batches = parse_json_each_row(text).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_subquery_to_sql_basic() {
        let q = SubQuery { table: "events".into(), projections: vec![], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: Some(5), passthrough: None, offset: None };
        let sql = subquery_to_sql(&q);
        assert_eq!(sql, "SELECT * FROM events LIMIT 5");
    }

    #[test]
    fn test_get_nested_json_top_level() {
        let v = serde_json::json!({"name": "alice"});
        assert_eq!(get_nested_json(&v, "name").unwrap(), "alice");
    }

    #[test]
    fn test_get_nested_json_dot_path() {
        let v = serde_json::json!({"meta": {"region": "us-east-1"}});
        assert_eq!(get_nested_json(&v, "meta.region").unwrap(), "us-east-1");
    }

    #[test]
    fn test_get_nested_json_missing() {
        let v = serde_json::json!({"name": "alice"});
        assert!(get_nested_json(&v, "meta.region").is_none());
    }

    #[test]
    fn test_parse_json_each_row_nested() {
        let text = r#"{"user":{"name":"alice"},"score":100}"#;
        let batches = parse_json_each_row(text).unwrap();
        // Top-level keys only in auto-discovery; nested via projection
        assert_eq!(batches[0].num_rows(), 1);
    }
}
