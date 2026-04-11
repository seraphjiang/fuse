// SPDX-License-Identifier: Apache-2.0

//! Amazon Timestream connector for the Fuse federated query engine.
//!
//! Queries Timestream time-series databases using the Timestream Query SDK.
//! Full SQL pushdown — Timestream supports a SQL-like query language natively.
//!
//! # Configuration
//!
//! ```toml
//! [[connector]]
//! id = "my_ts"
//! type = "timestream"
//! database = "my_timestream_db"
//! # Optional
//! # region = "us-east-1"
//! # max_rows = 10000
//! ```

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

#[derive(Debug)]
pub struct TimestreamConnector {
    id: String,
    query_client: aws_sdk_timestreamquery::Client,
    write_client: aws_sdk_timestreamwrite::Client,
    database: String,
    max_rows: i32,
}

impl TimestreamConnector {
    pub fn new(
        id: String,
        query_client: aws_sdk_timestreamquery::Client,
        write_client: aws_sdk_timestreamwrite::Client,
        database: String,
        max_rows: i32,
    ) -> Self {
        Self { id, query_client, write_client, database, max_rows }
    }

    async fn run_query(&self, sql: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
        debug!(database = %self.database, sql = %sql, "querying Timestream");

        let mut all_rows: Vec<aws_sdk_timestreamquery::types::Row> = Vec::new();
        let mut column_info: Vec<aws_sdk_timestreamquery::types::ColumnInfo> = Vec::new();
        let mut next_token: Option<String> = None;

        loop {
            let mut req = self
                .query_client
                .query()
                .query_string(sql)
                .max_rows(self.max_rows);

            if let Some(ref token) = next_token {
                req = req.next_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| ConnectorError::query(format!("Timestream query failed: {e}")))?;

            if column_info.is_empty() {
                column_info = resp.column_info().to_vec();
            }

            all_rows.extend(resp.rows().iter().cloned());

            next_token = resp.next_token().map(|s| s.to_string());
            if next_token.is_none() {
                break;
            }
        }

        convert_rows(&column_info, &all_rows)
    }
}

/// Convert Timestream rows to Arrow RecordBatches using column type info.
fn convert_rows(
    column_info: &[aws_sdk_timestreamquery::types::ColumnInfo],
    rows: &[aws_sdk_timestreamquery::types::Row],
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let fields: Vec<Field> = column_info
        .iter()
        .map(|ci| {
            let name = ci.name().unwrap_or("unknown").to_string();
            let dt = match ci.r#type().and_then(|t| t.scalar_type()).map(|s| s.as_str()) {
                Some("BIGINT" | "INTEGER") => DataType::Int64,
                Some("DOUBLE") => DataType::Float64,
                Some("TIMESTAMP") => DataType::Timestamp(TimeUnit::Millisecond, None),
                _ => DataType::Utf8,
            };
            Field::new(name, dt, true)
        })
        .collect();

    let schema = Arc::new(Schema::new(fields.clone()));

    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let arrays: Vec<Arc<dyn arrow::array::Array>> = fields
        .iter()
        .enumerate()
        .map(|(col_idx, field)| {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| {
                    row.data()
                        .get(col_idx)
                        .and_then(|d| d.scalar_value().map(|s| s.to_string()))
                })
                .collect();

            match field.data_type() {
                DataType::Int64 => {
                    let arr: Vec<Option<i64>> = values
                        .iter()
                        .map(|v| v.as_deref().and_then(|s| s.parse().ok()))
                        .collect();
                    Arc::new(Int64Array::from(arr)) as Arc<dyn arrow::array::Array>
                }
                DataType::Float64 => {
                    let arr: Vec<Option<f64>> = values
                        .iter()
                        .map(|v| v.as_deref().and_then(|s| s.parse().ok()))
                        .collect();
                    Arc::new(Float64Array::from(arr)) as Arc<dyn arrow::array::Array>
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    let arr: Vec<Option<i64>> = values
                        .iter()
                        .map(|v| v.as_deref().and_then(|s| parse_timestamp_ms(s)))
                        .collect();
                    Arc::new(TimestampMillisecondArray::from(arr)) as Arc<dyn arrow::array::Array>
                }
                _ => {
                    let arr: Vec<Option<&str>> =
                        values.iter().map(|v| v.as_deref()).collect();
                    Arc::new(StringArray::from(arr)) as Arc<dyn arrow::array::Array>
                }
            }
        })
        .collect();

    let batch = RecordBatch::try_new(schema, arrays).map_err(ConnectorError::query)?;
    Ok(vec![batch])
}

/// Parse Timestream timestamp string to epoch milliseconds.
fn parse_timestamp_ms(s: &str) -> Option<i64> {
    // Timestream returns timestamps like "2024-01-15 10:30:00.000000000"
    // Try parsing as epoch millis first, then as datetime string
    if let Ok(n) = s.parse::<i64>() {
        return Some(n);
    }
    // Simple parse: strip fractional seconds beyond millis
    let trimmed = s.trim();
    if trimmed.len() >= 23 {
        // "2024-01-15 10:30:00.000"
        let date_part = &trimmed[..10];
        let time_part = &trimmed[11..23.min(trimmed.len())];
        let datetime_str = format!("{}T{}Z", date_part, time_part);
        if let Ok(dt) = chrono_parse_simple(&datetime_str) {
            return Some(dt);
        }
    }
    None
}

/// Minimal datetime parsing without chrono dependency.
fn chrono_parse_simple(s: &str) -> Result<i64, ()> {
    // Parse "YYYY-MM-DDTHH:MM:SS.mmmZ" to epoch millis
    if s.len() < 20 { return Err(()); }
    let year: i64 = s[0..4].parse().map_err(|_| ())?;
    let month: i64 = s[5..7].parse().map_err(|_| ())?;
    let day: i64 = s[8..10].parse().map_err(|_| ())?;
    let hour: i64 = s[11..13].parse().map_err(|_| ())?;
    let min: i64 = s[14..16].parse().map_err(|_| ())?;
    let sec: i64 = s[17..19].parse().map_err(|_| ())?;
    let millis: i64 = if s.len() > 20 && s.as_bytes()[19] == b'.' {
        let end = 23.min(s.len() - if s.ends_with('Z') { 1 } else { 0 });
        s[20..end].parse().unwrap_or(0)
    } else {
        0
    };

    // Simplified days-from-epoch (not accounting for leap seconds, good enough)
    let days = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100 + (year - 1601) / 400
        + month_day_offset(month, is_leap(year)) + day - 1;
    Ok(days * 86_400_000 + hour * 3_600_000 + min * 60_000 + sec * 1_000 + millis)
}

fn month_day_offset(month: i64, leap: bool) -> i64 {
    const OFFSETS: [i64; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let base = OFFSETS.get(month as usize).copied().unwrap_or(0);
    if leap && month > 2 { base + 1 } else { base }
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Build SQL for Timestream from a SubQuery.
fn build_timestream_sql(query: &SubQuery, database: &str) -> String {
    let cols = if query.projections.is_empty() {
        "*".to_string()
    } else {
        query.projections.join(", ")
    };

    // Timestream uses "database"."table" syntax
    let table_ref = format!("\"{}\".\"{}\"", database, query.table);
    let mut sql = format!("SELECT {} FROM {}", cols, table_ref);

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
impl FederatedConnector for TimestreamConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "timestream" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: true,
            supports_sorting: true,
            supports_limit: true,
            supports_join: false, // Timestream doesn't support cross-table joins
            max_concurrent_queries: 4,
            supports_streaming: true,
            latency_class: LatencyClass::Medium,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        match self.write_client.list_databases().max_results(1).send().await {
            Ok(_) => ConnectorHealth { status: HealthStatus::Healthy, latency_ms: None, message: None },
            Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let resp = self.write_client
            .list_tables()
            .database_name(&self.database)
            .send()
            .await
            .map_err(|e| ConnectorError::query(format!("ListTables failed: {e}")))?;

        Ok(resp.tables().iter().map(|t| SchemaInfo {
            name: t.table_name().unwrap_or("unknown").to_string(),
            schema_type: SchemaType::Table,
            estimated_row_count: None,
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let resp = self.write_client
            .describe_table()
            .database_name(&self.database)
            .table_name(table)
            .send()
            .await
            .map_err(|e| ConnectorError::query(format!("DescribeTable failed: {e}")))?;

        let fields: Vec<Field> = resp.table()
            .map(|t| t.schema().map(|s| {
                s.composite_partition_key().iter().map(|pk| {
                    Field::new(pk.name().unwrap_or("key"), DataType::Utf8, false)
                }).collect::<Vec<_>>()
            }).unwrap_or_default())
            .unwrap_or_default();

        // Timestream schemas are dynamic; return measure_name + time + measure_value as base
        let mut base = vec![
            Field::new("measure_name", DataType::Utf8, false),
            Field::new("time", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("measure_value::double", DataType::Float64, true),
            Field::new("measure_value::bigint", DataType::Int64, true),
            Field::new("measure_value::varchar", DataType::Utf8, true),
        ];
        base.extend(fields);
        Ok(Schema::new(base))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let sql = build_timestream_sql(query, &self.database);
        self.run_query(&sql).await
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

pub struct TimestreamConnectorFactory;

#[async_trait]
impl ConnectorFactory for TimestreamConnectorFactory {
    fn connector_type(&self) -> &str { "timestream" }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let database = config.properties.get("database")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'database' is required".into()))?
            .to_string();

        let max_rows = config.properties.get("max_rows")
            .and_then(|v| v.as_integer())
            .unwrap_or(10000) as i32;

        let sdk_config = aws_config::load_from_env().await;

        let mut qb = aws_sdk_timestreamquery::config::Builder::from(&sdk_config);
        let mut wb = aws_sdk_timestreamwrite::config::Builder::from(&sdk_config);

        if let Some(region) = config.properties.get("region").and_then(|v| v.as_str()) {
            let r = aws_sdk_timestreamquery::config::Region::new(region.to_string());
            qb = qb.region(r.clone());
            wb = wb.region(aws_sdk_timestreamwrite::config::Region::new(region.to_string()));
        }

        let query_client = aws_sdk_timestreamquery::Client::from_conf(qb.build());
        let write_client = aws_sdk_timestreamwrite::Client::from_conf(wb.build());

        Ok(Arc::new(TimestreamConnector::new(
            config.id.clone(), query_client, write_client, database, max_rows,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub_query(table: &str) -> SubQuery {
        SubQuery {
            table: table.into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec![], sort: vec![],
            limit: None, having: None, passthrough: None, offset: None,
        }
    }

    #[test]
    fn test_build_sql_simple() {
        assert_eq!(
            build_timestream_sql(&sub_query("metrics"), "mydb"),
            "SELECT * FROM \"mydb\".\"metrics\""
        );
    }

    #[test]
    fn test_build_sql_with_filter_and_limit() {
        let mut sq = sub_query("metrics");
        sq.filter = Some(FilterExpr::Comparison {
            field: "measure_name".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8("cpu".into()),
        });
        sq.limit = Some(100);
        assert_eq!(
            build_timestream_sql(&sq, "db"),
            "SELECT * FROM \"db\".\"metrics\" WHERE measure_name = 'cpu' LIMIT 100"
        );
    }

    #[test]
    fn test_build_sql_with_projections_and_sort() {
        let mut sq = sub_query("metrics");
        sq.projections = vec!["time".into(), "measure_value::double".into()];
        sq.sort = vec![SortExpr { field: "time".into(), descending: true }];
        assert_eq!(
            build_timestream_sql(&sq, "db"),
            "SELECT time, measure_value::double FROM \"db\".\"metrics\" ORDER BY time DESC"
        );
    }

    #[test]
    fn test_build_sql_with_group_by() {
        let mut sq = sub_query("metrics");
        sq.projections = vec!["measure_name".into(), "avg(measure_value::double)".into()];
        sq.group_by = vec!["measure_name".into()];
        assert_eq!(
            build_timestream_sql(&sq, "db"),
            "SELECT measure_name, avg(measure_value::double) FROM \"db\".\"metrics\" GROUP BY measure_name"
        );
    }

    #[test]
    fn test_filter_to_sql_and_or() {
        let f = FilterExpr::Or(
            Box::new(FilterExpr::Comparison {
                field: "region".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("us-east-1".into()),
            }),
            Box::new(FilterExpr::Comparison {
                field: "region".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("eu-west-1".into()),
            }),
        );
        assert_eq!(filter_to_sql(&f), "(region = 'us-east-1' OR region = 'eu-west-1')");
    }

    #[test]
    fn test_parse_timestamp_ms_epoch() {
        assert_eq!(parse_timestamp_ms("1705312200000"), Some(1705312200000));
    }

    #[test]
    fn test_parse_timestamp_ms_datetime() {
        let ms = parse_timestamp_ms("2024-01-15 10:30:00.000000000");
        assert!(ms.is_some());
        // Should be roughly 2024-01-15 10:30 UTC in millis
        let val = ms.unwrap();
        assert!(val > 1705300000000 && val < 1705400000000);
    }

    #[test]
    fn test_parse_timestamp_ms_invalid() {
        assert!(parse_timestamp_ms("not-a-date").is_none());
    }

    #[test]
    fn test_is_leap() {
        assert!(is_leap(2024));
        assert!(!is_leap(2023));
        assert!(is_leap(2000));
        assert!(!is_leap(1900));
    }

    #[test]
    fn test_scalar_to_sql() {
        assert_eq!(scalar_to_sql(&ScalarValue::Null), "NULL");
        assert_eq!(scalar_to_sql(&ScalarValue::Int64(42)), "42");
        assert_eq!(scalar_to_sql(&ScalarValue::Utf8("it's".into())), "'it''s'");
    }
}
