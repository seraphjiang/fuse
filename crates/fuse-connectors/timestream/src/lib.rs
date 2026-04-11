// SPDX-License-Identifier: Apache-2.0

//! Amazon Timestream connector for the Fuse federated query engine.
//!
//! Full SQL pushdown — Timestream supports SQL with time-series extensions.
//! Uses AWS SDK for query execution and schema discovery.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;
use fuse_core::sql::quote_ident;

pub fn subquery_to_sql(sq: &SubQuery, database: &str, table: &str) -> String {
    let cols = if sq.projections.is_empty() { "*".into() } else {
        sq.projections.iter().map(|p| if p == "*" { "*".to_string() } else { quote_ident(p) }).collect::<Vec<_>>().join(", ")
    };
    let fqn = format!("{}.{}", quote_ident(database), quote_ident(table));
    let mut sql = format!("SELECT {} FROM {}", cols, fqn);
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
    sql
}

fn filter_to_sql(f: &FilterExpr) -> String {
    match f {
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=", ComparisonOp::Neq => "!=",
                ComparisonOp::Gt => ">", ComparisonOp::Gte => ">=",
                ComparisonOp::Lt => "<", ComparisonOp::Lte => "<=",
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => "LIKE",
            };
            format!("{} {} {}", quote_ident(field), op_str, scalar_to_sql(value))
        }
        FilterExpr::And(l, r) => format!("({} AND {})", filter_to_sql(l), filter_to_sql(r)),
        FilterExpr::Or(l, r) => format!("({} OR {})", filter_to_sql(l), filter_to_sql(r)),
        FilterExpr::Not(inner) => format!("NOT ({})", filter_to_sql(inner)),
        FilterExpr::In { field, values } => {
            let vals: Vec<String> = values.iter().map(scalar_to_sql).collect();
            format!("{} IN ({})", quote_ident(field), vals.join(", "))
        }
        FilterExpr::IsNull(f) => format!("{} IS NULL", quote_ident(f)),
        FilterExpr::IsNotNull(f) => format!("{} IS NOT NULL", quote_ident(f)),
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

fn ts_type_to_arrow(ts_type: &str) -> DataType {
    match ts_type.to_uppercase().as_str() {
        "BIGINT" => DataType::Int64,
        "DOUBLE" => DataType::Float64,
        "BOOLEAN" => DataType::Boolean,
        "TIMESTAMP" | "DATE" | "TIME" | "VARCHAR" => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

#[derive(Debug)]
pub struct TimestreamConnector {
    id: String,
    query_client: aws_sdk_timestreamquery::Client,
    write_client: aws_sdk_timestreamwrite::Client,
    database: String,
}

impl TimestreamConnector {
    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let region = config.properties.get("region")
            .and_then(|v| v.as_str()).unwrap_or("us-east-1");
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load().await;
        let query_client = aws_sdk_timestreamquery::Client::new(&aws_config);
        let write_client = aws_sdk_timestreamwrite::Client::new(&aws_config);
        let database = config.properties.get("database")
            .and_then(|v| v.as_str()).unwrap_or("default").to_string();
        Ok(Self { id: config.id.clone(), query_client, write_client, database })
    }

    async fn run_query(&self, sql: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
        let t0 = Instant::now();
        debug!(connector = %self.id, sql = sql, "Timestream query");

        let resp = self.query_client.query()
            .query_string(sql)
            .send().await
            .map_err(|e| ConnectorError::query(e.to_string()))?;

        let col_info = resp.column_info();
        if col_info.is_empty() {
            return Ok(vec![]);
        }

        let fields: Vec<Field> = col_info.iter()
            .map(|c| {
                let dt = c.r#type().and_then(|t| t.scalar_type())
                    .map(|s| ts_type_to_arrow(s.as_str()))
                    .unwrap_or(DataType::Utf8);
                Field::new(c.name().unwrap_or("unknown"), dt, true)
            })
            .collect();
        let schema = Arc::new(Schema::new(fields.clone()));

        let rows = resp.rows();
        if rows.is_empty() {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        let num_cols = col_info.len();
        let mut col_values: Vec<Vec<Option<String>>> = (0..num_cols).map(|_| Vec::new()).collect();

        for row in rows {
            let data = row.data();
            for (i, datum) in data.iter().enumerate() {
                if i < num_cols {
                    col_values[i].push(datum.scalar_value().map(String::from));
                }
            }
        }

        let arrays: Vec<ArrayRef> = col_values.into_iter().enumerate()
            .map(|(i, vals)| -> ArrayRef {
                match &fields[i].data_type() {
                    DataType::Int64 => {
                        let arr: Int64Array = vals.iter()
                            .map(|v| v.as_deref().and_then(|s| s.parse().ok()))
                            .collect();
                        Arc::new(arr)
                    }
                    DataType::Float64 => {
                        let arr: Float64Array = vals.iter()
                            .map(|v| v.as_deref().and_then(|s| s.parse().ok()))
                            .collect();
                        Arc::new(arr)
                    }
                    _ => {
                        let arr: StringArray = vals.into_iter().collect();
                        Arc::new(arr)
                    }
                }
            })
            .collect();

        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| ConnectorError::query(e.to_string()))?;
        debug!(connector = %self.id, rows = batch.num_rows(), elapsed_ms = t0.elapsed().as_millis() as u64, "Timestream query complete");
        Ok(vec![batch])
    }
}

#[async_trait::async_trait]
impl FederatedConnector for TimestreamConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "timestream" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true, supports_projection: true,
            supports_aggregation: true, supports_sorting: true,
            supports_limit: true, supports_join: false,
            max_concurrent_queries: 4, supports_streaming: true,
            latency_class: LatencyClass::Medium,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        match self.write_client.list_databases().max_results(1).send().await {
            Ok(_) => ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None },
            Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let resp = self.write_client.list_tables()
            .database_name(&self.database)
            .send().await
            .map_err(|e| ConnectorError::schema(e.to_string()))?;
        Ok(resp.tables().iter()
            .map(|t| SchemaInfo {
                name: t.table_name().unwrap_or_default().to_string(),
                schema_type: SchemaType::Table,
                estimated_row_count: None,
            })
            .collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let resp = self.write_client.describe_table()
            .database_name(&self.database)
            .table_name(table)
            .send().await
            .map_err(|e| ConnectorError::schema(e.to_string()))?;
        let fields: Vec<Field> = resp.table()
            .map(|t| t.schema().map(|s| s.composite_partition_key()).unwrap_or_default())
            .unwrap_or_default()
            .iter()
            .map(|_| Field::new("measure_value", DataType::Float64, true))
            .collect();
        // Fallback: run a LIMIT 0 query to get column info
        if fields.is_empty() {
            let sql = format!("SELECT * FROM {}.{} LIMIT 0", quote_ident(&self.database), quote_ident(table));
            let qr = self.query_client.query().query_string(&sql).send().await
                .map_err(|e| ConnectorError::schema(e.to_string()))?;
            let cols: Vec<Field> = qr.column_info().iter()
                .map(|c| {
                    let dt = c.r#type().and_then(|t| t.scalar_type())
                        .map(|s| ts_type_to_arrow(s.as_str()))
                        .unwrap_or(DataType::Utf8);
                    Field::new(c.name().unwrap_or("unknown"), dt, true)
                })
                .collect();
            return Ok(Schema::new(cols));
        }
        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let sql = subquery_to_sql(query, &self.database, &query.table);
        self.run_query(&sql).await
    }

    async fn execute_streaming(&self, query: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for b in self.execute(query).await? {
            tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

pub struct TimestreamConnectorFactory;

#[async_trait::async_trait]
impl ConnectorFactory for TimestreamConnectorFactory {
    fn connector_type(&self) -> &str { "timestream" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(TimestreamConnector::from_config(config).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_sq(table: &str) -> SubQuery {
        SubQuery { table: table.into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec![], sort: vec![],
            limit: None, having: None, offset: None, passthrough: None }
    }

    #[test]
    fn test_subquery_to_sql_simple() {
        let sql = subquery_to_sql(&simple_sq("metrics"), "mydb", "metrics");
        assert_eq!(sql, "SELECT * FROM \"mydb\".\"metrics\"");
    }

    #[test]
    fn test_subquery_to_sql_with_projections() {
        let mut sq = simple_sq("t");
        sq.projections = vec!["time".into(), "value".into()];
        let sql = subquery_to_sql(&sq, "db", "t");
        assert_eq!(sql, "SELECT \"time\", \"value\" FROM \"db\".\"t\"");
    }

    #[test]
    fn test_subquery_to_sql_with_limit() {
        let mut sq = simple_sq("t");
        sq.limit = Some(100);
        let sql = subquery_to_sql(&sq, "db", "t");
        assert!(sql.ends_with("LIMIT 100"));
    }

    #[test]
    fn test_subquery_to_sql_with_filter() {
        let mut sq = simple_sq("t");
        sq.filter = Some(FilterExpr::Comparison {
            field: "measure_value".into(), op: ComparisonOp::Gt, value: ScalarValue::Float64(0.5),
        });
        let sql = subquery_to_sql(&sq, "db", "t");
        assert!(sql.contains("WHERE \"measure_value\" > 0.5"));
    }

    #[test]
    fn test_subquery_to_sql_group_by_and_sort() {
        let mut sq = simple_sq("t");
        sq.group_by = vec!["region".into()];
        sq.sort = vec![SortExpr { field: "time".into(), descending: true }];
        let sql = subquery_to_sql(&sq, "db", "t");
        assert!(sql.contains("GROUP BY \"region\""));
        assert!(sql.contains("ORDER BY \"time\" DESC"));
    }

    #[test]
    fn test_ts_type_to_arrow() {
        assert_eq!(ts_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(ts_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(ts_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(ts_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(ts_type_to_arrow("TIMESTAMP"), DataType::Utf8);
    }

    #[test]
    fn test_scalar_to_sql() {
        assert_eq!(scalar_to_sql(&ScalarValue::Utf8("hello".into())), "'hello'");
        assert_eq!(scalar_to_sql(&ScalarValue::Int64(42)), "42");
        assert_eq!(scalar_to_sql(&ScalarValue::Float64(3.14)), "3.14");
        assert_eq!(scalar_to_sql(&ScalarValue::Boolean(true)), "TRUE");
        assert_eq!(scalar_to_sql(&ScalarValue::Null), "NULL");
    }

    #[test]
    fn test_filter_compound() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "a".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(1) }),
            Box::new(FilterExpr::IsNotNull("b".into())),
        );
        let sql = filter_to_sql(&f);
        assert!(sql.contains("\"a\" = 1") && sql.contains("\"b\" IS NOT NULL"));
    }

    #[test]
    fn test_subquery_to_sql_with_and_filter() {
        let mut sq = simple_sq("t");
        sq.filter = Some(FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "region".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("us-east-1".into()) }),
            Box::new(FilterExpr::Comparison { field: "value".into(), op: ComparisonOp::Gt, value: ScalarValue::Float64(0.0) }),
        ));
        let sql = subquery_to_sql(&sq, "db", "t");
        assert!(sql.contains("'us-east-1'") && sql.contains("AND") && sql.contains("> 0"));
    }

    #[test]
    fn test_subquery_to_sql_with_in_filter() {
        let mut sq = simple_sq("t");
        sq.filter = Some(FilterExpr::In {
            field: "status".into(),
            values: vec![ScalarValue::Utf8("ok".into()), ScalarValue::Utf8("warn".into())],
        });
        let sql = subquery_to_sql(&sq, "db", "t");
        assert!(sql.contains("IN") && sql.contains("'ok'") && sql.contains("'warn'"));
    }

    #[test]
    fn test_subquery_to_sql_full_query() {
        let sq = SubQuery {
            table: "metrics".into(),
            projections: vec!["time".into(), "avg(value)".into()],
            filter: Some(FilterExpr::Comparison { field: "region".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("us-west-2".into()) }),
            aggregations: vec![], group_by: vec!["time".into()],
            sort: vec![SortExpr { field: "time".into(), descending: false }],
            limit: Some(1000),
            having: None, offset: None, passthrough: None,
        };
        let sql = subquery_to_sql(&sq, "mydb", "metrics");
        assert!(sql.contains("time") && sql.contains("avg(value)"));
        assert!(sql.contains("mydb") && sql.contains("metrics"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("ORDER BY") && sql.contains("ASC"));
        assert!(sql.ends_with("LIMIT 1000"));
    }
}
