// SPDX-License-Identifier: Apache-2.0

//! Google BigQuery connector for the Fuse federated query engine.
//!
//! Full SQL pushdown via BigQuery Jobs API. Queries are submitted as SQL,
//! results fetched via getQueryResults and converted to Arrow RecordBatches.
//!
//! # Configuration
//!
//! ```toml
//! [[connector]]
//! id = "my_bq"
//! type = "bigquery"
//! project_id = "my-gcp-project"
//! dataset = "my_dataset"
//! # Auth: OAuth2 bearer token or service account
//! token = "secret://fuse/bigquery-token"
//! # Optional
//! # location = "US"
//! # max_results = 10000
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
use fuse_core::sql::quote_ident_backtick as qi;

const BQ_API_BASE: &str = "https://bigquery.googleapis.com/bigquery/v2";

#[derive(Debug)]
pub struct BigQueryConnector {
    id: String,
    client: reqwest::Client,
    project_id: String,
    dataset: String,
    location: Option<String>,
    max_results: u32,
}

impl BigQueryConnector {
    pub fn new(
        id: String, client: reqwest::Client, project_id: String,
        dataset: String, location: Option<String>, max_results: u32,
    ) -> Self {
        Self { id, client, project_id, dataset, location, max_results }
    }

    #[allow(dead_code)]
    fn jobs_url(&self) -> String {
        format!("{}/projects/{}/jobs", BQ_API_BASE, self.project_id)
    }

    fn query_url(&self) -> String {
        format!("{}/projects/{}/queries", BQ_API_BASE, self.project_id)
    }

    fn tables_url(&self) -> String {
        format!("{}/projects/{}/datasets/{}/tables", BQ_API_BASE, self.project_id, self.dataset)
    }

    fn table_url(&self, table: &str) -> String {
        format!("{}/{}", self.tables_url(), table)
    }

    async fn run_query(&self, sql: &str) -> Result<serde_json::Value, ConnectorError> {
        debug!(project = %self.project_id, sql = %sql, "querying BigQuery");

        let mut body = serde_json::json!({
            "query": sql,
            "useLegacySql": false,
            "maxResults": self.max_results,
            "defaultDataset": {
                "projectId": self.project_id,
                "datasetId": self.dataset,
            }
        });
        if let Some(ref loc) = self.location {
            body["location"] = serde_json::Value::String(loc.clone());
        }

        let resp = self.client.post(self.query_url())
            .json(&body).send().await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::query(format!("BigQuery returned {}: {}", status, text)));
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| ConnectorError::query(e.to_string()))?;

        // If job not complete, poll
        if json["jobComplete"].as_bool() != Some(true) {
            if let Some(job_id) = json["jobReference"]["jobId"].as_str() {
                return self.poll_job(job_id).await;
            }
        }

        Ok(json)
    }

    async fn poll_job(&self, job_id: &str) -> Result<serde_json::Value, ConnectorError> {
        let url = format!("{}/projects/{}/queries/{}", BQ_API_BASE, self.project_id, job_id);
        for _ in 0..120 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let resp = self.client.get(&url).send().await
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            let json: serde_json::Value = resp.json().await
                .map_err(|e| ConnectorError::query(e.to_string()))?;
            if json["jobComplete"].as_bool() == Some(true) {
                return Ok(json);
            }
        }
        Err(ConnectorError::query("BigQuery job timed out"))
    }
}

fn parse_bq_response(json: &serde_json::Value) -> Result<Vec<RecordBatch>, ConnectorError> {
    let empty_fields = vec![];
    let fields_json = json["schema"]["fields"].as_array().unwrap_or(&empty_fields);
    let columns: Vec<String> = fields_json.iter()
        .filter_map(|f| f["name"].as_str().map(|s| s.to_string()))
        .collect();

    let empty_rows = vec![];
    let rows_json = json["rows"].as_array().unwrap_or(&empty_rows);
    let rows: Vec<Vec<Option<String>>> = rows_json.iter().map(|row| {
        let empty_f = vec![];
        row["f"].as_array().unwrap_or(&empty_f).iter()
            .map(|cell| cell["v"].as_str().map(|s| s.to_string()))
            .collect()
    }).collect();

    let fields: Vec<Field> = columns.iter().map(|c| Field::new(c, DataType::Utf8, true)).collect();
    let schema = Arc::new(Schema::new(fields));

    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..columns.len()).map(|i| {
        let vals: Vec<Option<&str>> = rows.iter().map(|r| r.get(i).and_then(|v| v.as_deref())).collect();
        Arc::new(StringArray::from(vals)) as Arc<dyn arrow::array::Array>
    }).collect();

    let batch = RecordBatch::try_new(schema, arrays).map_err(ConnectorError::query)?;
    Ok(vec![batch])
}

fn build_bq_sql(query: &SubQuery, dataset: &str) -> String {
    let cols = if query.projections.is_empty() { "*".into() } else {
        query.projections.iter().map(|p| if p == "*" { "*".to_string() } else { qi(p) }).collect::<Vec<_>>().join(", ")
    };
    let table_ref = format!("`{}.{}`", dataset, query.table);
    let mut sql = format!("SELECT {} FROM {}", cols, table_ref);

    if let Some(ref f) = query.filter { sql.push_str(&format!(" WHERE {}", filter_to_sql(f))); }
    if !query.group_by.is_empty() { sql.push_str(&format!(" GROUP BY {}", query.group_by.iter().map(|g| qi(g)).collect::<Vec<_>>().join(", "))); }
    if let Some(ref h) = query.having { sql.push_str(&format!(" HAVING {}", filter_to_sql(h))); }
    if !query.sort.is_empty() {
        let s: Vec<String> = query.sort.iter().map(|s| if s.descending { format!("{} DESC", qi(&s.field)) } else { qi(&s.field) }).collect();
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
            format!("{} {} {}", qi(field), op_str, scalar_to_sql(value))
        }
        FilterExpr::In { field, values } => {
            let v: Vec<String> = values.iter().map(scalar_to_sql).collect();
            format!("{} IN ({})", qi(field), v.join(", "))
        }
        FilterExpr::IsNull(f) => format!("{} IS NULL", qi(f)),
        FilterExpr::IsNotNull(f) => format!("{} IS NOT NULL", qi(f)),
    }
}

fn scalar_to_sql(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Null => "NULL".into(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(n) => n.to_string(),
        ScalarValue::Utf8(s) => format!("'{}'", s.replace('\'', "\\'")),
    }
}

#[async_trait]
impl FederatedConnector for BigQueryConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "bigquery" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }

    async fn health_check(&self) -> ConnectorHealth {
        match self.client.get(self.tables_url()).query(&[("maxResults", "1")]).send().await {
            Ok(r) if r.status().is_success() => ConnectorHealth { status: HealthStatus::Healthy, latency_ms: None, message: None },
            Ok(r) => ConnectorHealth { status: HealthStatus::Degraded, latency_ms: None, message: Some(format!("HTTP {}", r.status())) },
            Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let resp = self.client.get(self.tables_url()).send().await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        let json: serde_json::Value = resp.json().await
            .map_err(|e| ConnectorError::query(e.to_string()))?;
        let empty = vec![];
        let tables = json["tables"].as_array().unwrap_or(&empty);
        Ok(tables.iter().filter_map(|t| {
            t["tableReference"]["tableId"].as_str().map(|name| SchemaInfo {
                name: name.to_string(), schema_type: SchemaType::Table, estimated_row_count: None,
            })
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let resp = self.client.get(self.table_url(table)).send().await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        let json: serde_json::Value = resp.json().await
            .map_err(|e| ConnectorError::query(e.to_string()))?;
        let empty = vec![];
        let fields_json = json["schema"]["fields"].as_array().unwrap_or(&empty);
        let fields: Vec<Field> = fields_json.iter().filter_map(|f| {
            f["name"].as_str().map(|name| Field::new(name, DataType::Utf8, true))
        }).collect();
        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let sql = build_bq_sql(query, &self.dataset);
        let json = self.run_query(&sql).await?;
        parse_bq_response(&json)
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

pub struct BigQueryConnectorFactory;

#[async_trait]
impl ConnectorFactory for BigQueryConnectorFactory {
    fn connector_type(&self) -> &str { "bigquery" }

    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let project_id = config.properties.get("project_id").and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'project_id' is required".into()))?.to_string();
        let dataset = config.properties.get("dataset").and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'dataset' is required".into()))?.to_string();
        let location = config.properties.get("location").and_then(|v| v.as_str()).map(|s| s.to_string());
        let max_results = config.properties.get("max_results").and_then(|v| v.as_integer()).unwrap_or(10000) as u32;

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

        Ok(Arc::new(BigQueryConnector::new(config.id.clone(), client, project_id, dataset, location, max_results)))
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
    fn test_build_bq_sql_simple() {
        assert_eq!(build_bq_sql(&sq("events"), "ds"), "SELECT * FROM `ds.events`");
    }

    #[test]
    fn test_build_bq_sql_full() {
        let mut q = sq("logs");
        q.projections = vec!["host".into(), "count(*)".into()];
        q.filter = Some(FilterExpr::Comparison { field: "level".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("ERROR".into()) });
        q.group_by = vec!["host".into()];
        q.sort = vec![SortExpr { field: "host".into(), descending: true }];
        q.limit = Some(100);
        assert_eq!(
            build_bq_sql(&q, "myds"),
            "SELECT `host`, `count(*)` FROM `myds.logs` WHERE `level` = 'ERROR' GROUP BY `host` ORDER BY `host` DESC LIMIT 100"
        );
    }

    #[test]
    fn test_parse_bq_response_with_data() {
        let json = serde_json::json!({
            "schema": { "fields": [{"name": "id"}, {"name": "name"}] },
            "rows": [
                {"f": [{"v": "1"}, {"v": "alice"}]},
                {"f": [{"v": "2"}, {"v": "bob"}]}
            ]
        });
        let batches = parse_bq_response(&json).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_parse_bq_response_empty() {
        let json = serde_json::json!({
            "schema": { "fields": [{"name": "id"}] },
            "rows": []
        });
        let batches = parse_bq_response(&json).unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_parse_bq_response_null_values() {
        let json = serde_json::json!({
            "schema": { "fields": [{"name": "a"}] },
            "rows": [{"f": [{"v": null}]}, {"f": [{"v": "val"}]}]
        });
        let batches = parse_bq_response(&json).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_tables_url() {
        let c = BigQueryConnector::new("t".into(), reqwest::Client::new(), "proj".into(), "ds".into(), None, 1000);
        assert!(c.tables_url().contains("proj"));
        assert!(c.tables_url().contains("ds"));
    }

    #[test]
    fn test_bq_backtick_table_ref() {
        // BigQuery uses backtick-quoted dataset.table
        let sql = build_bq_sql(&sq("my_table"), "my_dataset");
        assert!(sql.contains("`my_dataset.my_table`"));
    }

    #[test]
    fn test_filter_in_clause() {
        let f = FilterExpr::In {
            field: "region".into(),
            values: vec![ScalarValue::Utf8("US".into()), ScalarValue::Utf8("EU".into())],
        };
        assert_eq!(filter_to_sql(&f), "`region` IN ('US', 'EU')");
    }
}
