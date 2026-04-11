// SPDX-License-Identifier: Apache-2.0

//! Amazon Athena connector for the Fuse federated query engine.
//!
//! Full SQL pushdown — queries are forwarded as-is to Athena, which executes
//! them against data in S3 (via Glue catalog). Results are fetched via
//! GetQueryResults and converted to Arrow RecordBatches.
//!
//! # Configuration
//!
//! ```toml
//! [[connector]]
//! id = "my_athena"
//! type = "athena"
//! database = "my_glue_database"
//! output_location = "s3://my-bucket/athena-results/"
//! # Optional
//! # workgroup = "primary"
//! # region = "us-west-2"
//! # poll_interval_ms = 500
//! # max_poll_attempts = 120
//! ```

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

#[derive(Debug)]
pub struct AthenaConnector {
    id: String,
    client: aws_sdk_athena::Client,
    database: String,
    output_location: String,
    workgroup: Option<String>,
    poll_interval_ms: u64,
    max_poll_attempts: u32,
}

impl AthenaConnector {
    pub fn new(
        id: String,
        client: aws_sdk_athena::Client,
        database: String,
        output_location: String,
        workgroup: Option<String>,
        poll_interval_ms: u64,
        max_poll_attempts: u32,
    ) -> Self {
        Self {
            id,
            client,
            database,
            output_location,
            workgroup,
            poll_interval_ms,
            max_poll_attempts,
        }
    }

    /// Execute a SQL query on Athena and wait for results.
    async fn run_query(&self, sql: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
        debug!(database = %self.database, sql = %sql, "starting Athena query");

        // Start query execution
        let mut req = self
            .client
            .start_query_execution()
            .query_string(sql)
            .query_execution_context(
                aws_sdk_athena::types::QueryExecutionContext::builder()
                    .database(&self.database)
                    .build(),
            )
            .result_configuration(
                aws_sdk_athena::types::ResultConfiguration::builder()
                    .output_location(&self.output_location)
                    .build(),
            );

        if let Some(ref wg) = self.workgroup {
            req = req.work_group(wg);
        }

        let start_resp = req
            .send()
            .await
            .map_err(|e| ConnectorError::query(format!("StartQueryExecution failed: {e}")))?;

        let query_id = start_resp
            .query_execution_id()
            .ok_or_else(|| ConnectorError::query("no query execution ID returned"))?
            .to_string();

        debug!(query_id = %query_id, "Athena query started, polling for completion");

        // Poll for completion
        for attempt in 0..self.max_poll_attempts {
            tokio::time::sleep(std::time::Duration::from_millis(self.poll_interval_ms)).await;

            let status_resp = self
                .client
                .get_query_execution()
                .query_execution_id(&query_id)
                .send()
                .await
                .map_err(|e| ConnectorError::query(format!("GetQueryExecution failed: {e}")))?;

            let state = status_resp
                .query_execution()
                .and_then(|qe| qe.status())
                .and_then(|s| s.state())
                .cloned();

            match state {
                Some(aws_sdk_athena::types::QueryExecutionState::Succeeded) => {
                    return self.fetch_results(&query_id).await;
                }
                Some(aws_sdk_athena::types::QueryExecutionState::Failed) => {
                    let reason = status_resp
                        .query_execution()
                        .and_then(|qe| qe.status())
                        .and_then(|s| s.state_change_reason())
                        .unwrap_or("unknown");
                    return Err(ConnectorError::query(format!(
                        "Athena query failed: {reason}"
                    )));
                }
                Some(aws_sdk_athena::types::QueryExecutionState::Cancelled) => {
                    return Err(ConnectorError::query("Athena query was cancelled"));
                }
                _ => {
                    if attempt % 10 == 9 {
                        debug!(query_id = %query_id, attempt, "still waiting for Athena query");
                    }
                }
            }
        }

        Err(ConnectorError::query(format!(
            "Athena query timed out after {} attempts",
            self.max_poll_attempts
        )))
    }

    /// Fetch results from a completed Athena query.
    async fn fetch_results(&self, query_id: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
        let mut all_rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut columns: Vec<String> = Vec::new();
        let mut next_token: Option<String> = None;
        let mut first_page = true;

        loop {
            let mut req = self
                .client
                .get_query_results()
                .query_execution_id(query_id);

            if let Some(ref token) = next_token {
                req = req.next_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| ConnectorError::query(format!("GetQueryResults failed: {e}")))?;

            if let Some(result_set) = resp.result_set() {
                // Extract column names from metadata on first page
                if first_page {
                    if let Some(meta) = result_set.result_set_metadata() {
                        columns = meta
                            .column_info()
                            .iter()
                            .map(|ci| ci.name().to_string())
                            .collect();
                    }
                }

                for (i, row) in result_set.rows().iter().enumerate() {
                    // Skip header row on first page
                    if first_page && i == 0 {
                        continue;
                    }
                    let values: Vec<Option<String>> = row
                        .data()
                        .iter()
                        .map(|datum| datum.var_char_value().map(|s| s.to_string()))
                        .collect();
                    all_rows.push(values);
                }
                first_page = false;
            }

            next_token = resp.next_token().map(|s| s.to_string());
            if next_token.is_none() {
                break;
            }
        }

        rows_to_batches(&columns, &all_rows)
    }
}

/// Convert string rows to Arrow RecordBatches (all Utf8 columns).
fn rows_to_batches(
    columns: &[String],
    rows: &[Vec<Option<String>>],
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(c, DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..columns.len())
        .map(|col_idx| {
            let values: Vec<Option<&str>> = rows
                .iter()
                .map(|row| row.get(col_idx).and_then(|v| v.as_deref()))
                .collect();
            Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>
        })
        .collect();

    let batch = RecordBatch::try_new(schema, arrays).map_err(ConnectorError::query)?;
    Ok(vec![batch])
}

/// Build a SQL string from a SubQuery for Athena (full SQL pushdown).
fn build_athena_sql(query: &SubQuery) -> String {
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
        let sorts: Vec<String> = query
            .sort
            .iter()
            .map(|s| {
                if s.descending {
                    format!("{} DESC", s.field)
                } else {
                    s.field.clone()
                }
            })
            .collect();
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

fn filter_to_sql(f: &FilterExpr) -> String {
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
impl FederatedConnector for AthenaConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn connector_type(&self) -> &str {
        "athena"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::full()
    }

    async fn health_check(&self) -> ConnectorHealth {
        // List workgroups as a lightweight health check
        match self.client.list_work_groups().max_results(1).send().await {
            Ok(_) => ConnectorHealth {
                status: HealthStatus::Healthy,
                latency_ms: None,
                message: None,
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
            .list_table_metadata()
            .catalog_name("AwsDataCatalog")
            .database_name(&self.database)
            .send()
            .await
            .map_err(|e| ConnectorError::query(format!("ListTableMetadata failed: {e}")))?;

        let schemas = resp
            .table_metadata_list()
            .iter()
            .map(|t| SchemaInfo {
                name: t.name().to_string(),
                schema_type: SchemaType::Table,
                estimated_row_count: None,
            })
            .collect();

        Ok(schemas)
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let resp = self
            .client
            .get_table_metadata()
            .catalog_name("AwsDataCatalog")
            .database_name(&self.database)
            .table_name(table)
            .send()
            .await
            .map_err(|e| ConnectorError::query(format!("GetTableMetadata failed: {e}")))?;

        let fields: Vec<Field> = resp
            .table_metadata()
            .map(|t| {
                t.columns()
                    .iter()
                    .map(|c| Field::new(c.name(), DataType::Utf8, true))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let sql = build_athena_sql(query);
        self.run_query(&sql).await
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

/// Factory for creating AthenaConnector instances from config.
pub struct AthenaConnectorFactory;

#[async_trait]
impl ConnectorFactory for AthenaConnectorFactory {
    fn connector_type(&self) -> &str {
        "athena"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let database = config
            .properties
            .get("database")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'database' is required".into()))?
            .to_string();

        let output_location = config
            .properties
            .get("output_location")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'output_location' is required".into()))?
            .to_string();

        let workgroup = config
            .properties
            .get("workgroup")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let poll_interval_ms = config
            .properties
            .get("poll_interval_ms")
            .and_then(|v| v.as_integer())
            .unwrap_or(500) as u64;

        let max_poll_attempts = config
            .properties
            .get("max_poll_attempts")
            .and_then(|v| v.as_integer())
            .unwrap_or(120) as u32;

        let sdk_config = aws_config::load_from_env().await;
        let mut builder = aws_sdk_athena::config::Builder::from(&sdk_config);

        if let Some(region) = config.properties.get("region").and_then(|v| v.as_str()) {
            builder = builder.region(aws_sdk_athena::config::Region::new(region.to_string()));
        }

        let client = aws_sdk_athena::Client::from_conf(builder.build());

        Ok(Arc::new(AthenaConnector::new(
            config.id.clone(),
            client,
            database,
            output_location,
            workgroup,
            poll_interval_ms,
            max_poll_attempts,
        )))
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
    fn test_build_athena_sql_simple() {
        assert_eq!(build_athena_sql(&sub_query("logs")), "SELECT * FROM logs");
    }

    #[test]
    fn test_build_athena_sql_with_projections() {
        let mut sq = sub_query("events");
        sq.projections = vec!["id".into(), "name".into()];
        assert_eq!(build_athena_sql(&sq), "SELECT id, name FROM events");
    }

    #[test]
    fn test_build_athena_sql_with_filter() {
        let mut sq = sub_query("logs");
        sq.filter = Some(FilterExpr::Comparison {
            field: "status".into(),
            op: ComparisonOp::Gte,
            value: ScalarValue::Int64(500),
        });
        sq.limit = Some(100);
        assert_eq!(
            build_athena_sql(&sq),
            "SELECT * FROM logs WHERE status >= 500 LIMIT 100"
        );
    }

    #[test]
    fn test_build_athena_sql_with_group_by() {
        let mut sq = sub_query("logs");
        sq.projections = vec!["service".into(), "count(*)".into()];
        sq.group_by = vec!["service".into()];
        assert_eq!(
            build_athena_sql(&sq),
            "SELECT service, count(*) FROM logs GROUP BY service"
        );
    }

    #[test]
    fn test_build_athena_sql_with_sort() {
        let mut sq = sub_query("logs");
        sq.sort = vec![SortExpr { field: "ts".into(), descending: true }];
        sq.limit = Some(10);
        assert_eq!(
            build_athena_sql(&sq),
            "SELECT * FROM logs ORDER BY ts DESC LIMIT 10"
        );
    }

    #[test]
    fn test_filter_to_sql_and() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison {
                field: "a".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Int64(1),
            }),
            Box::new(FilterExpr::Comparison {
                field: "b".into(),
                op: ComparisonOp::Lt,
                value: ScalarValue::Utf8("x".into()),
            }),
        );
        assert_eq!(filter_to_sql(&f), "(a = 1 AND b < 'x')");
    }

    #[test]
    fn test_filter_to_sql_in() {
        let f = FilterExpr::In {
            field: "status".into(),
            values: vec![ScalarValue::Int64(200), ScalarValue::Int64(404)],
        };
        assert_eq!(filter_to_sql(&f), "status IN (200, 404)");
    }

    #[test]
    fn test_filter_to_sql_is_null() {
        assert_eq!(filter_to_sql(&FilterExpr::IsNull("x".into())), "x IS NULL");
    }

    #[test]
    fn test_scalar_to_sql_escapes_quotes() {
        assert_eq!(scalar_to_sql(&ScalarValue::Utf8("O'Brien".into())), "'O''Brien'");
    }

    #[test]
    fn test_rows_to_batches_empty() {
        let cols = vec!["a".into()];
        let batches = rows_to_batches(&cols, &[]).unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_rows_to_batches_with_data() {
        let cols = vec!["name".into(), "val".into()];
        let rows = vec![
            vec![Some("alice".into()), Some("10".into())],
            vec![Some("bob".into()), None],
        ];
        let batches = rows_to_batches(&cols, &rows).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }
}
