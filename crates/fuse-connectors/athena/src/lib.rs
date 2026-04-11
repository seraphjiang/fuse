// SPDX-License-Identifier: Apache-2.0

//! Amazon Athena connector for the Fuse federated query engine.
//!
//! Full SQL pushdown — Athena is SQL-native (Presto/Trino).
//! Uses AWS SDK to start query execution, poll for results.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;
use fuse_core::sql::{quote_ident, quote_table};

/// SQL generation from SubQuery for Athena (Presto-compatible SQL).
pub fn subquery_to_sql(sq: &SubQuery) -> String {
    let cols = if sq.projections.is_empty() {
        "*".to_string()
    } else {
        sq.projections.iter().map(|p| if p == "*" { "*".to_string() } else { quote_ident(p) }).collect::<Vec<_>>().join(", ")
    };
    let mut sql = format!("SELECT {} FROM {}", cols, quote_table(&sq.table));
    if let Some(ref f) = sq.filter {
        sql.push_str(&format!(" WHERE {}", filter_to_sql(f)));
    }
    if !sq.group_by.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", sq.group_by.iter().map(|g| quote_ident(g)).collect::<Vec<_>>().join(", ")));
    }
    if !sq.sort.is_empty() {
        let clauses: Vec<String> = sq.sort.iter()
            .map(|s| format!("{} {}", quote_ident(&s.field), if s.descending { "DESC" } else { "ASC" }))
            .collect();
        sql.push_str(&format!(" ORDER BY {}", clauses.join(", ")));
    }
    if let Some(limit) = sq.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }
    if let Some(offset) = sq.offset {
        sql.push_str(&format!(" OFFSET {}", offset));
    }
    sql
}

fn filter_to_sql(f: &FilterExpr) -> String {
    match f {
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=",
                ComparisonOp::Neq => "!=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Like | ComparisonOp::Contains => "LIKE",
                ComparisonOp::ILike => "LIKE",
            };
            format!("{} {} {}", quote_ident(field), op_str, scalar_to_sql(value))
        }
        FilterExpr::And(left, right) => format!("({} AND {})", filter_to_sql(left), filter_to_sql(right)),
        FilterExpr::Or(left, right) => format!("({} OR {})", filter_to_sql(left), filter_to_sql(right)),
        FilterExpr::Not(inner) => format!("NOT ({})", filter_to_sql(inner)),
        FilterExpr::In { field, values } => {
            let vals: Vec<String> = values.iter().map(scalar_to_sql).collect();
            format!("{} IN ({})", quote_ident(field), vals.join(", "))
        }
        FilterExpr::IsNull(field) => format!("{} IS NULL", quote_ident(field)),
        FilterExpr::IsNotNull(field) => format!("{} IS NOT NULL", quote_ident(field)),
    }
}

fn scalar_to_sql(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(f) => f.to_string(),
        ScalarValue::Boolean(b) => b.to_string().to_uppercase(),
        ScalarValue::Null => "NULL".to_string(),
    }
}

#[derive(Debug)]
pub struct AthenaConnector {
    id: String,
    client: aws_sdk_athena::Client,
    database: String,
    output_location: String,
    workgroup: String,
}

impl AthenaConnector {
    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let region = config.properties.get("region")
            .and_then(|v| v.as_str())
            .unwrap_or("us-east-1");

        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load()
            .await;

        let client = aws_sdk_athena::Client::new(&aws_config);

        let database = config.properties.get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        let output_location = config.properties.get("output_location")
            .and_then(|v| v.as_str())
            .unwrap_or("s3://aws-athena-query-results/")
            .to_string();

        let workgroup = config.properties.get("workgroup")
            .and_then(|v| v.as_str())
            .unwrap_or("primary")
            .to_string();

        Ok(Self { id: config.id.clone(), client, database, output_location, workgroup })
    }

    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
        let t0 = Instant::now();
        debug!(connector = %self.id, sql = sql, "Starting Athena query");

        let start = self.client.start_query_execution()
            .query_string(sql)
            .query_execution_context(
                aws_sdk_athena::types::QueryExecutionContext::builder()
                    .database(&self.database)
                    .build()
            )
            .result_configuration(
                aws_sdk_athena::types::ResultConfiguration::builder()
                    .output_location(&self.output_location)
                    .build()
            )
            .work_group(&self.workgroup)
            .send()
            .await
            .map_err(|e| ConnectorError::query(e.to_string()))?;

        let exec_id = start.query_execution_id()
            .ok_or_else(|| ConnectorError::query("no execution ID returned"))?
            .to_string();

        // Poll for completion
        loop {
            let status = self.client.get_query_execution()
                .query_execution_id(&exec_id)
                .send()
                .await
                .map_err(|e| ConnectorError::query(e.to_string()))?;

            let state = status.query_execution()
                .and_then(|qe| qe.status())
                .and_then(|s| s.state())
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();

            match state.as_str() {
                "SUCCEEDED" => break,
                "FAILED" | "CANCELLED" => {
                    let reason = status.query_execution()
                        .and_then(|qe| qe.status())
                        .and_then(|s| s.state_change_reason())
                        .unwrap_or("unknown error");
                    return Err(ConnectorError::query(format!("Athena query {}: {}", state, reason)));
                }
                _ => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
            }
        }

        // Fetch results
        let results = self.client.get_query_results()
            .query_execution_id(&exec_id)
            .send()
            .await
            .map_err(|e| ConnectorError::query(e.to_string()))?;

        let result_set = results.result_set();
        let elapsed = t0.elapsed().as_millis() as u64;

        // Build schema from column info
        let columns = result_set
            .and_then(|rs| rs.result_set_metadata())
            .map(|m| m.column_info())
            .unwrap_or_default();

        if columns.is_empty() {
            debug!(connector = %self.id, elapsed_ms = elapsed, "Empty result");
            return Ok(vec![]);
        }

        let fields: Vec<Field> = columns.iter()
            .map(|c| Field::new(c.name(), DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Parse rows (skip header row)
        let rows = result_set
            .map(|rs| rs.rows())
            .unwrap_or_default();

        let data_rows: Vec<_> = if rows.len() > 1 { &rows[1..] } else { &[] }.to_vec();

        if data_rows.is_empty() {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        let num_cols = columns.len();
        let mut col_builders: Vec<Vec<Option<String>>> = (0..num_cols).map(|_| Vec::new()).collect();

        for row in &data_rows {
            let data = row.data();
            for col_idx in 0..num_cols {
                let val = data.get(col_idx).and_then(|d| d.var_char_value().map(|s| s.to_string()));
                col_builders[col_idx].push(val);
            }
        }

        let arrays: Vec<ArrayRef> = col_builders.into_iter()
            .map(|vals| {
                let arr: StringArray = vals.into_iter().collect();
                Arc::new(arr) as ArrayRef
            })
            .collect();

        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| ConnectorError::query(e.to_string()))?;

        debug!(connector = %self.id, rows = batch.num_rows(), elapsed_ms = elapsed, "Athena query complete");
        Ok(vec![batch])
    }
}

#[async_trait::async_trait]
impl fuse_core::connector::FederatedConnector for AthenaConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "athena" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: true,
            supports_sorting: true,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 4,
            supports_streaming: true,
            latency_class: LatencyClass::High,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        match self.client.list_work_groups().max_results(1).send().await {
            Ok(_) => ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None },
            Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let resp = self.client.list_table_metadata()
            .catalog_name("AwsDataCatalog")
            .database_name(&self.database)
            .send()
            .await
            .map_err(|e| ConnectorError::schema(e.to_string()))?;

        Ok(resp.table_metadata_list()
            .iter()
            .map(|t| SchemaInfo {
                name: t.name().to_string(),
                schema_type: SchemaType::Table,
                estimated_row_count: None,
            })
            .collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let resp = self.client.get_table_metadata()
            .catalog_name("AwsDataCatalog")
            .database_name(&self.database)
            .table_name(table)
            .send()
            .await
            .map_err(|e| ConnectorError::schema(e.to_string()))?;

        let columns = resp.table_metadata()
            .map(|t| t.columns())
            .unwrap_or_default();

        let fields: Vec<Field> = columns.iter()
            .map(|c| {
                let dt = match c.r#type().unwrap_or("string").to_lowercase().as_str() {
                    "int" | "integer" | "bigint" => DataType::Int64,
                    "double" | "float" | "decimal" => DataType::Float64,
                    "boolean" => DataType::Boolean,
                    _ => DataType::Utf8,
                };
                Field::new(c.name(), dt, true)
            })
            .collect();

        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let sql = subquery_to_sql(query);
        self.execute_sql(&sql).await
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let batches = self.execute(query).await?;
        for b in batches {
            tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

/// Factory for creating Athena connectors from config.
pub struct AthenaConnectorFactory;

#[async_trait::async_trait]
impl ConnectorFactory for AthenaConnectorFactory {
    fn connector_type(&self) -> &str { "athena" }

    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(AthenaConnector::from_config(config).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subquery_to_sql_simple() {
        let sq = SubQuery {
            table: "logs".into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec![], sort: vec![],
            limit: None, having: None, offset: None, passthrough: None,
        };
        assert_eq!(subquery_to_sql(&sq), "SELECT * FROM \"logs\"");
    }

    #[test]
    fn test_subquery_to_sql_with_projections() {
        let sq = SubQuery {
            table: "logs".into(), projections: vec!["id".into(), "name".into()],
            filter: None, aggregations: vec![], group_by: vec![], sort: vec![],
            limit: None, having: None, offset: None, passthrough: None,
        };
        assert_eq!(subquery_to_sql(&sq), "SELECT \"id\", \"name\" FROM \"logs\"");
    }

    #[test]
    fn test_subquery_to_sql_with_limit() {
        let sq = SubQuery {
            table: "t".into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec![], sort: vec![],
            limit: Some(10), having: None, offset: None, passthrough: None,
        };
        assert_eq!(subquery_to_sql(&sq), "SELECT * FROM \"t\" LIMIT 10");
    }

    #[test]
    fn test_subquery_to_sql_with_filter() {
        let sq = SubQuery {
            table: "t".into(), projections: vec![],
            filter: Some(FilterExpr::Comparison {
                field: "status".into(),
                op: ComparisonOp::Gte,
                value: ScalarValue::Int64(500),
            }),
            aggregations: vec![], group_by: vec![], sort: vec![],
            limit: None, having: None, offset: None, passthrough: None,
        };
        assert_eq!(subquery_to_sql(&sq), "SELECT * FROM \"t\" WHERE \"status\" >= 500");
    }

    #[test]
    fn test_subquery_to_sql_full() {
        let sq = SubQuery {
            table: "events".into(),
            projections: vec!["region".into(), "count".into()],
            filter: Some(FilterExpr::Comparison {
                field: "type".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("error".into()),
            }),
            aggregations: vec![], group_by: vec!["region".into()],
            sort: vec![SortExpr { field: "count".into(), descending: true }],
            limit: Some(5), having: None, offset: None, passthrough: None,
        };
        let sql = subquery_to_sql(&sq);
        assert!(sql.contains("SELECT \"region\", \"count\" FROM \"events\""));
        assert!(sql.contains("WHERE \"type\" = 'error'"));
        assert!(sql.contains("GROUP BY \"region\""));
        assert!(sql.contains("ORDER BY \"count\" DESC"));
        assert!(sql.contains("LIMIT 5"));
    }

    #[test]
    fn test_scalar_to_sql_types() {
        assert_eq!(scalar_to_sql(&ScalarValue::Utf8("hello".into())), "'hello'");
        assert_eq!(scalar_to_sql(&ScalarValue::Int64(42)), "42");
        assert_eq!(scalar_to_sql(&ScalarValue::Float64(3.14)), "3.14");
        assert_eq!(scalar_to_sql(&ScalarValue::Boolean(true)), "TRUE");
        assert_eq!(scalar_to_sql(&ScalarValue::Null), "NULL");
    }

    #[test]
    fn test_scalar_to_sql_escapes_quotes() {
        assert_eq!(scalar_to_sql(&ScalarValue::Utf8("it's".into())), "'it''s'");
    }

    #[test]
    fn test_filter_and_or() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "a".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(1) }),
            Box::new(FilterExpr::Or(
                Box::new(FilterExpr::Comparison { field: "b".into(), op: ComparisonOp::Gt, value: ScalarValue::Int64(2) }),
                Box::new(FilterExpr::IsNull("c".into())),
            )),
        );
        let sql = filter_to_sql(&f);
        assert!(sql.contains("\"a\" = 1"));
        assert!(sql.contains("\"b\" > 2"));
        assert!(sql.contains("\"c\" IS NULL"));
    }
}
