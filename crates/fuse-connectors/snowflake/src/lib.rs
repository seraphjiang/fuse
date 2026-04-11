// SPDX-License-Identifier: Apache-2.0

//! Snowflake connector for the Fuse federated query engine.
//!
//! Full SQL pushdown via Snowflake's SQL API (REST). Queries are submitted
//! as SQL statements, results fetched as JSON and converted to Arrow.
//!
//! # Configuration
//!
//! ```toml
//! [[connector]]
//! id = "my_snowflake"
//! type = "snowflake"
//! account = "myorg-myaccount"
//! database = "MY_DB"
//! schema = "PUBLIC"
//! warehouse = "COMPUTE_WH"
//! # Auth: key-pair JWT or username/password
//! token = "secret://fuse/snowflake-jwt"
//! # Or: username = "user", password = "secret://fuse/snowflake-pass"
//! ```

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

#[derive(Debug)]
pub struct SnowflakeConnector {
    id: String,
    client: reqwest::Client,
    api_url: String,
    database: String,
    sf_schema: String,
    warehouse: String,
}

impl SnowflakeConnector {
    pub fn new(
        id: String,
        client: reqwest::Client,
        account: &str,
        database: String,
        sf_schema: String,
        warehouse: String,
    ) -> Self {
        let api_url = format!("https://{}.snowflakecomputing.com/api/v2/statements", account);
        Self { id, client, api_url, database, sf_schema, warehouse }
    }

    async fn submit_sql(&self, sql: &str) -> Result<serde_json::Value, ConnectorError> {
        debug!(sql = %sql, "submitting to Snowflake SQL API");

        let body = serde_json::json!({
            "statement": sql,
            "database": self.database,
            "schema": self.sf_schema,
            "warehouse": self.warehouse,
            "timeout": 60,
        });

        let resp = self.client
            .post(&self.api_url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::query(format!("Snowflake API returned {}: {}", status, text)));
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| ConnectorError::query(e.to_string()))?;

        // Handle async execution — poll for results
        if let Some(handle) = json["statementHandle"].as_str() {
            if json["statementStatusUrl"].is_string() {
                return self.poll_results(handle).await;
            }
        }

        Ok(json)
    }

    async fn poll_results(&self, handle: &str) -> Result<serde_json::Value, ConnectorError> {
        let status_url = format!("{}/{}", self.api_url, handle);
        for _ in 0..120 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let resp = self.client.get(&status_url)
                .header("Accept", "application/json")
                .send().await
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            let json: serde_json::Value = resp.json().await
                .map_err(|e| ConnectorError::query(e.to_string()))?;

            match json["statementStatus"]["status"].as_str() {
                Some("SUCCEEDED") | Some("succeeded") => return Ok(json),
                Some("FAILED") | Some("failed") => {
                    let msg = json["message"].as_str().unwrap_or("unknown error");
                    return Err(ConnectorError::query(format!("Snowflake query failed: {}", msg)));
                }
                _ => continue,
            }
        }
        Err(ConnectorError::query("Snowflake query timed out"))
    }

    fn parse_response(json: &serde_json::Value) -> Result<Vec<RecordBatch>, ConnectorError> {
        let columns: Vec<String> = json["resultSetMetaData"]["rowType"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|c| c["name"].as_str().map(|s| s.to_string()))
            .collect();

        let rows: Vec<Vec<serde_json::Value>> = json["data"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|r| r.as_array().cloned())
            .collect();

        json_to_batches(&columns, &rows)
    }
}

fn json_to_batches(
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let fields: Vec<Field> = columns.iter().map(|c| Field::new(c, DataType::Utf8, true)).collect();
    let schema = Arc::new(Schema::new(fields));

    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..columns.len())
        .map(|col_idx| {
            let values: Vec<Option<String>> = rows.iter().map(|row| {
                row.get(col_idx).and_then(|v| match v {
                    serde_json::Value::Null => None,
                    serde_json::Value::String(s) => Some(s.clone()),
                    other => Some(other.to_string()),
                })
            }).collect();
            Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>
        })
        .collect();

    let batch = RecordBatch::try_new(schema, arrays).map_err(ConnectorError::query)?;
    Ok(vec![batch])
}

fn build_snowflake_sql(query: &SubQuery) -> String {
    let cols = if query.projections.is_empty() { "*".into() } else { query.projections.join(", ") };
    let mut sql = format!("SELECT {} FROM {}", cols, query.table);

    if let Some(ref f) = query.filter { sql.push_str(&format!(" WHERE {}", filter_to_sql(f))); }
    if !query.group_by.is_empty() { sql.push_str(&format!(" GROUP BY {}", query.group_by.join(", "))); }
    if let Some(ref h) = query.having { sql.push_str(&format!(" HAVING {}", filter_to_sql(h))); }
    if !query.sort.is_empty() {
        let s: Vec<String> = query.sort.iter().map(|s| if s.descending { format!("{} DESC", s.field) } else { s.field.clone() }).collect();
        sql.push_str(&format!(" ORDER BY {}", s.join(", ")));
    }
    if let Some(l) = query.limit { sql.push_str(&format!(" LIMIT {}", l)); }
    if let Some(o) = query.offset { sql.push_str(&format!(" OFFSET {}", o)); }
    sql
}

fn filter_to_sql(f: &FilterExpr) -> String {
    match f {
        FilterExpr::And(l, r) => format!("({} AND {})", filter_to_sql(l), filter_to_sql(r)),
        FilterExpr::Or(l, r) => format!("({} OR {})", filter_to_sql(l), filter_to_sql(r)),
        FilterExpr::Not(inner) => format!("NOT ({})", filter_to_sql(inner)),
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=", ComparisonOp::Neq => "!=",
                ComparisonOp::Lt => "<", ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">", ComparisonOp::Gte => ">=",
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => "LIKE",
            };
            format!("{} {} {}", field, op_str, scalar_to_sql(value))
        }
        FilterExpr::In { field, values } => {
            let v: Vec<String> = values.iter().map(scalar_to_sql).collect();
            format!("{} IN ({})", field, v.join(", "))
        }
        FilterExpr::IsNull(f) => format!("{} IS NULL", f),
        FilterExpr::IsNotNull(f) => format!("{} IS NOT NULL", f),
    }
}

fn scalar_to_sql(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Null => "NULL".into(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(n) => n.to_string(),
        ScalarValue::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

#[async_trait]
impl FederatedConnector for SnowflakeConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "snowflake" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }

    async fn health_check(&self) -> ConnectorHealth {
        match self.submit_sql("SELECT 1").await {
            Ok(_) => ConnectorHealth { status: HealthStatus::Healthy, latency_ms: None, message: None },
            Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let json = self.submit_sql("SHOW TABLES").await?;
        let empty = vec![]; let rows = json["data"].as_array().unwrap_or(&empty);
        Ok(rows.iter().filter_map(|r| {
            r.as_array().and_then(|a| a.get(1)).and_then(|v| v.as_str()).map(|name| SchemaInfo {
                name: name.to_string(), schema_type: SchemaType::Table, estimated_row_count: None,
            })
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let json = self.submit_sql(&format!("DESCRIBE TABLE {}", table)).await?;
        let empty = vec![]; let rows = json["data"].as_array().unwrap_or(&empty);
        let fields: Vec<Field> = rows.iter().filter_map(|r| {
            r.as_array().and_then(|a| a.first()).and_then(|v| v.as_str()).map(|name| {
                Field::new(name, DataType::Utf8, true)
            })
        }).collect();
        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let sql = build_snowflake_sql(query);
        let json = self.submit_sql(&sql).await?;
        Self::parse_response(&json)
    }

    async fn execute_streaming(
        &self, query: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for batch in self.execute(query).await? {
            tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

pub struct SnowflakeConnectorFactory;

#[async_trait]
impl ConnectorFactory for SnowflakeConnectorFactory {
    fn connector_type(&self) -> &str { "snowflake" }

    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let account = config.properties.get("account").and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'account' is required".into()))?;
        let database = config.properties.get("database").and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'database' is required".into()))?.to_string();
        let sf_schema = config.properties.get("schema").and_then(|v| v.as_str()).unwrap_or("PUBLIC").to_string();
        let warehouse = config.properties.get("warehouse").and_then(|v| v.as_str()).unwrap_or("COMPUTE_WH").to_string();

        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = config.properties.get("token").and_then(|v| v.as_str()) {
            headers.insert(reqwest::header::AUTHORIZATION, format!("Bearer {}", token).parse()
                .map_err(|e: reqwest::header::InvalidHeaderValue| ConnectorError::Connection(e.to_string()))?);
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .pool_max_idle_per_host(config.max_connections(8) as usize)
            .timeout(std::time::Duration::from_secs(config.connection_timeout_secs(120)))
            .build().map_err(|e| ConnectorError::Connection(e.to_string()))?;

        Ok(Arc::new(SnowflakeConnector::new(config.id.clone(), client, account, database, sf_schema, warehouse)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sq(table: &str) -> SubQuery {
        SubQuery { table: table.into(), projections: vec![], filter: None, aggregations: vec![],
            group_by: vec![], sort: vec![], limit: None, having: None, passthrough: None, offset: None }
    }

    #[test]
    fn test_build_sql_simple() {
        assert_eq!(build_snowflake_sql(&sq("events")), "SELECT * FROM events");
    }

    #[test]
    fn test_build_sql_full() {
        let mut q = sq("logs");
        q.projections = vec!["host".into(), "count(*)".into()];
        q.filter = Some(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Gte, value: ScalarValue::Int64(500) });
        q.group_by = vec!["host".into()];
        q.sort = vec![SortExpr { field: "host".into(), descending: false }];
        q.limit = Some(50);
        assert_eq!(
            build_snowflake_sql(&q),
            "SELECT host, count(*) FROM logs WHERE status >= 500 GROUP BY host ORDER BY host LIMIT 50"
        );
    }

    #[test]
    fn test_json_to_batches_empty() {
        let batches = json_to_batches(&["a".into()], &[]).unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_json_to_batches_with_data() {
        let cols = vec!["name".into(), "val".into()];
        let rows = vec![
            vec![serde_json::json!("alice"), serde_json::json!("10")],
            vec![serde_json::json!("bob"), serde_json::Value::Null],
        ];
        let batches = json_to_batches(&cols, &rows).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_parse_response() {
        let json = serde_json::json!({
            "resultSetMetaData": { "rowType": [{"name": "ID"}, {"name": "NAME"}] },
            "data": [["1", "alice"], ["2", "bob"]]
        });
        let batches = SnowflakeConnector::parse_response(&json).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_parse_response_empty() {
        let json = serde_json::json!({
            "resultSetMetaData": { "rowType": [{"name": "ID"}] },
            "data": []
        });
        let batches = SnowflakeConnector::parse_response(&json).unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_filter_not() {
        let f = FilterExpr::Not(Box::new(FilterExpr::IsNull("x".into())));
        assert_eq!(filter_to_sql(&f), "NOT (x IS NULL)");
    }

    #[test]
    fn test_capabilities_full() {
        let c = SnowflakeConnector::new("t".into(), reqwest::Client::new(), "acct", "db".into(), "PUBLIC".into(), "WH".into());
        let caps = c.capabilities();
        assert!(caps.supports_filtering);
        assert!(!caps.supports_join); // join handled by Fuse engine, not connector
    }

    #[test]
    fn test_api_url() {
        let c = SnowflakeConnector::new("t".into(), reqwest::Client::new(), "myorg-myacct", "db".into(), "s".into(), "w".into());
        assert_eq!(c.api_url, "https://myorg-myacct.snowflakecomputing.com/api/v2/statements");
    }
}
