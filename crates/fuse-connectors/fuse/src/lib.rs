// SPDX-License-Identifier: Apache-2.0

//! Fuse-to-Fuse connector — federate queries across Fuse instances.
//!
//! Connects to a remote Fuse server via its REST API, enabling cross-cluster
//! federation. Queries are forwarded as SQL to the remote instance and results
//! are converted from JSON rows back to Arrow RecordBatches.
//!
//! # Configuration
//!
//! ```toml
//! [[connector]]
//! id = "remote_fuse"
//! type = "fuse"
//! url = "https://remote-fuse.example.com:9400"
//! # Optional: bearer token for auth
//! # token = "secret://fuse/remote-token"
//! ```

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::{
    ConnectorCapabilities, ConnectorHealth, FederatedConnector, HealthStatus,
    SchemaInfo, SchemaType, SubQuery,
};
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

#[derive(Debug)]
pub struct FuseConnector {
    id: String,
    base_url: String,
    client: reqwest::Client,
}

impl FuseConnector {
    pub fn new(id: String, base_url: String, client: reqwest::Client) -> Self {
        Self { id, base_url: base_url.trim_end_matches('/').to_string(), client }
    }

    /// Build the query URL for the remote Fuse instance.
    fn query_url(&self) -> String {
        format!("{}/api/fuse/query", self.base_url)
    }

    fn health_url(&self) -> String {
        format!("{}/api/fuse/health", self.base_url)
    }

    fn datasources_url(&self) -> String {
        format!("{}/api/fuse/datasources", self.base_url)
    }

    fn schemas_url(&self, datasource: &str) -> String {
        format!("{}/api/fuse/datasources/{}/schemas", self.base_url, datasource)
    }

    fn fields_url(&self, datasource: &str, table: &str) -> String {
        format!(
            "{}/api/fuse/datasources/{}/schemas/{}/fields",
            self.base_url, datasource, table
        )
    }

    /// Convert JSON query response rows + columns into Arrow RecordBatches.
    fn json_to_batches(
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<Vec<RecordBatch>, ConnectorError> {
        if columns.is_empty() || rows.is_empty() {
            let schema = Arc::new(Schema::new(
                columns.iter().map(|c| Field::new(c, DataType::Utf8, true)).collect::<Vec<_>>(),
            ));
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        let fields: Vec<Field> = columns
            .iter()
            .map(|c| Field::new(c, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..columns.len())
            .map(|col_idx| {
                let values: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| {
                        row.get(col_idx).and_then(|v| match v {
                            serde_json::Value::Null => None,
                            serde_json::Value::String(s) => Some(s.clone()),
                            other => Some(other.to_string()),
                        })
                    })
                    .collect();
                Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>
            })
            .collect();

        let batch = RecordBatch::try_new(schema, arrays).map_err(ConnectorError::query)?;
        Ok(vec![batch])
    }
}

#[async_trait]
impl FederatedConnector for FuseConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn connector_type(&self) -> &str {
        "fuse"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        // The remote Fuse handles all pushdown internally
        ConnectorCapabilities::full()
    }

    async fn health_check(&self) -> ConnectorHealth {
        match self.client.get(self.health_url()).send().await {
            Ok(resp) if resp.status().is_success() => ConnectorHealth {
                status: HealthStatus::Healthy,
                latency_ms: None,
                message: None,
            },
            Ok(resp) => ConnectorHealth {
                status: HealthStatus::Degraded,
                latency_ms: None,
                message: Some(format!("remote returned {}", resp.status())),
            },
            Err(e) => ConnectorHealth {
                status: HealthStatus::Unhealthy,
                latency_ms: None,
                message: Some(e.to_string()),
            },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let resp = self
            .client
            .get(self.datasources_url())
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        let mut schemas = Vec::new();
        if let Some(datasources) = json.as_array() {
            for ds in datasources {
                if let Some(id) = ds["id"].as_str() {
                    // Fetch tables for each remote datasource
                    let tables_resp = self
                        .client
                        .get(self.schemas_url(id))
                        .send()
                        .await
                        .map_err(|e| ConnectorError::Connection(e.to_string()))?;
                    let tables_json: serde_json::Value = tables_resp
                        .json()
                        .await
                        .map_err(|e| ConnectorError::Connection(e.to_string()))?;
                    if let Some(tables) = tables_json.as_array() {
                        for t in tables {
                            if let Some(name) = t.as_str().or_else(|| t["name"].as_str()) {
                                schemas.push(SchemaInfo {
                                    name: format!("{}.{}", id, name),
                                    schema_type: SchemaType::Table,
                                    estimated_row_count: None,
                                });
                            }
                        }
                    }
                }
            }
        }
        Ok(schemas)
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        // table may be "datasource.table" or just "table"
        let (ds, tbl) = if let Some(dot) = table.find('.') {
            (&table[..dot], &table[dot + 1..])
        } else {
            (self.id.as_str(), table)
        };

        let resp = self
            .client
            .get(self.fields_url(ds, tbl))
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        let fields: Vec<Field> = json
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .map(|f| {
                let name = f["name"].as_str().unwrap_or("unknown").to_string();
                Field::new(name, DataType::Utf8, true)
            })
            .collect();

        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let sql = build_remote_sql(query);
        debug!(remote = %self.base_url, sql = %sql, "forwarding query to remote Fuse");

        let resp = self
            .client
            .post(self.query_url())
            .json(&serde_json::json!({
                "query": sql,
                "format": "sql"
            }))
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::query(format!(
                "remote Fuse returned {}: {}",
                status, body
            )));
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ConnectorError::query(e.to_string()))?;

        let columns: Vec<String> = json["columns"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        let rows: Vec<Vec<serde_json::Value>> = json["rows"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_array().cloned())
            .collect();

        Self::json_to_batches(&columns, &rows)
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for batch in self.execute(query).await? {
            tx.send(Ok(batch))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

/// Build a SQL query string from a SubQuery to forward to the remote Fuse.
fn build_remote_sql(query: &SubQuery) -> String {
    let cols = if query.projections.is_empty() {
        "*".to_string()
    } else {
        query.projections.join(", ")
    };

    let mut sql = format!("SELECT {} FROM {}", cols, query.table);

    if let Some(ref filter) = query.filter {
        sql.push_str(&format!(" WHERE {}", filter_to_sql(filter)));
    }

    if !query.group_by.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", query.group_by.join(", ")));
    }

    if let Some(ref having) = query.having {
        sql.push_str(&format!(" HAVING {}", filter_to_sql(having)));
    }

    if !query.sort.is_empty() {
        let sorts: Vec<String> = query.sort.iter().map(|s| {
            if s.descending { format!("{} DESC", s.field) } else { s.field.clone() }
        }).collect();
        sql.push_str(&format!(" ORDER BY {}", sorts.join(", ")));
    }

    if let Some(limit) = query.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }

    if let Some(offset) = query.offset {
        sql.push_str(&format!(" OFFSET {}", offset));
    }

    sql
}

fn filter_to_sql(f: &fuse_core::connector::FilterExpr) -> String {
    use fuse_core::connector::{ComparisonOp, FilterExpr};
    match f {
        FilterExpr::And(l, r) => format!("({} AND {})", filter_to_sql(l), filter_to_sql(r)),
        FilterExpr::Or(l, r) => format!("({} OR {})", filter_to_sql(l), filter_to_sql(r)),
        FilterExpr::Not(inner) => format!("NOT ({})", filter_to_sql(inner)),
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=",
                ComparisonOp::Neq => "!=",
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => "LIKE",
            };
            format!("{} {} {}", field, op_str, scalar_to_sql(value))
        }
        FilterExpr::In { field, values } => {
            let vals: Vec<String> = values.iter().map(scalar_to_sql).collect();
            format!("{} IN ({})", field, vals.join(", "))
        }
        FilterExpr::IsNull(field) => format!("{} IS NULL", field),
        FilterExpr::IsNotNull(field) => format!("{} IS NOT NULL", field),
    }
}

fn scalar_to_sql(v: &fuse_core::connector::ScalarValue) -> String {
    use fuse_core::connector::ScalarValue;
    match v {
        ScalarValue::Null => "NULL".into(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(n) => n.to_string(),
        ScalarValue::Utf8(s) => format!("\'{}\'", s.replace('\'', "''")),
    }
}

/// Factory for creating FuseConnector instances from config.
pub struct FuseConnectorFactory;

#[async_trait]
impl ConnectorFactory for FuseConnectorFactory {
    fn connector_type(&self) -> &str {
        "fuse"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let url = config
            .properties
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'url' is required for fuse connector".into()))?
            .to_string();

        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = config.properties.get("token").and_then(|v| v.as_str()) {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))
                    .map_err(|e| ConnectorError::Connection(e.to_string()))?,
            );
        }

        let mut client_builder = reqwest::Client::builder()
            .default_headers(headers)
            .pool_max_idle_per_host(config.max_connections(8) as usize)
            .timeout(std::time::Duration::from_secs(
                config.connection_timeout_secs(60),
            ));

        if let Some(tls) = config.tls_config() {
            tls.validate().map_err(|e| ConnectorError::Connection(e.to_string()))?;
            client_builder = tls
                .apply_to_reqwest(client_builder)
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        }

        let client = client_builder
            .build()
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        Ok(Arc::new(FuseConnector::new(config.id.clone(), url, client)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub_query(table: &str) -> SubQuery {
        SubQuery {
            table: table.into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None,
            passthrough: None,
            offset: None,
        }
    }

    #[test]
    fn test_build_remote_sql_simple() {
        let sq = sub_query("logs");
        assert_eq!(build_remote_sql(&sq), "SELECT * FROM logs");
    }

    #[test]
    fn test_build_remote_sql_with_filter_and_limit() {
        use fuse_core::connector::{ComparisonOp, FilterExpr, ScalarValue};
        let mut sq = sub_query("logs");
        sq.filter = Some(FilterExpr::Comparison {
            field: "status".into(),
            op: ComparisonOp::Gte,
            value: ScalarValue::Int64(500),
        });
        sq.limit = Some(10);
        assert_eq!(
            build_remote_sql(&sq),
            "SELECT * FROM logs WHERE status >= 500 LIMIT 10"
        );
    }

    #[test]
    fn test_build_remote_sql_with_projections() {
        let mut sq = sub_query("logs");
        sq.projections = vec!["host".into(), "status".into()];
        assert_eq!(build_remote_sql(&sq), "SELECT host, status FROM logs");
    }

    #[test]
    fn test_build_remote_sql_with_group_by() {
        let mut sq = sub_query("logs");
        sq.projections = vec!["service".into(), "count(*)".into()];
        sq.group_by = vec!["service".into()];
        assert_eq!(
            build_remote_sql(&sq),
            "SELECT service, count(*) FROM logs GROUP BY service"
        );
    }

    #[test]
    fn test_build_remote_sql_with_sort_and_offset() {
        use fuse_core::connector::SortExpr;
        let mut sq = sub_query("logs");
        sq.sort = vec![SortExpr { field: "timestamp".into(), descending: true }];
        sq.limit = Some(20);
        sq.offset = Some(40);
        assert_eq!(
            build_remote_sql(&sq),
            "SELECT * FROM logs ORDER BY timestamp DESC LIMIT 20 OFFSET 40"
        );
    }

    #[test]
    fn test_json_to_batches_empty() {
        let cols = vec!["a".into(), "b".into()];
        let rows: Vec<Vec<serde_json::Value>> = vec![];
        let batches = FuseConnector::json_to_batches(&cols, &rows).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_json_to_batches_with_data() {
        let cols = vec!["host".into(), "status".into()];
        let rows = vec![
            vec![serde_json::json!("web-01"), serde_json::json!(200)],
            vec![serde_json::json!("web-02"), serde_json::json!(500)],
        ];
        let batches = FuseConnector::json_to_batches(&cols, &rows).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_json_to_batches_null_handling() {
        use arrow::array::Array;
        let cols = vec!["a".into()];
        let rows = vec![
            vec![serde_json::Value::Null],
            vec![serde_json::json!("val")],
        ];
        let batches = FuseConnector::json_to_batches(&cols, &rows).unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(col.is_null(0));
        assert_eq!(col.value(1), "val");
    }

    #[test]
    fn test_capabilities_full() {
        let c = FuseConnector::new("test".into(), "http://localhost:9400".into(), reqwest::Client::new());
        let caps = c.capabilities();
        assert!(caps.supports_filtering);
        assert!(caps.supports_projection);
        assert!(caps.supports_limit);
    }

    #[test]
    fn test_query_url() {
        let c = FuseConnector::new("test".into(), "http://localhost:9400/".into(), reqwest::Client::new());
        assert_eq!(c.query_url(), "http://localhost:9400/api/fuse/query");
    }

    #[test]
    fn test_url_trailing_slash_stripped() {
        let c = FuseConnector::new("test".into(), "http://host:9400///".into(), reqwest::Client::new());
        assert_eq!(c.health_url(), "http://host:9400/api/fuse/health");
    }
}
