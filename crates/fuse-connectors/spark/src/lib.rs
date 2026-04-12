// SPDX-License-Identifier: Apache-2.0

//! Apache Spark connector for the Fuse federated query engine.
//!
//! Full SQL pushdown via Spark Thrift Server (HiveServer2 HTTP).
//! Submits Spark SQL queries, polls for results, converts to Arrow.
//!
//! # Configuration
//!
//! ```toml
//! [[connector]]
//! id = "my_spark"
//! type = "spark"
//! url = "http://spark-thrift:10001"
//! # Optional
//! # database = "default"
//! # token = "secret://fuse/spark-token"
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
pub struct SparkConnector {
    id: String,
    client: reqwest::Client,
    url: String,
    database: String,
}

impl SparkConnector {
    pub fn new(id: String, client: reqwest::Client, url: String, database: String) -> Self {
        Self {
            id,
            client,
            url,
            database,
        }
    }

    async fn run_query(&self, sql: &str) -> Result<serde_json::Value, ConnectorError> {
        debug!(url = %self.url, sql = %sql, "querying Spark");
        let body = serde_json::json!({ "statement": sql, "database": self.database });
        let resp = self
            .client
            .post(format!("{}/api/v1/statements", self.url))
            .json(&body)
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::query(format!(
                "Spark returned error: {}",
                text
            )));
        }
        resp.json()
            .await
            .map_err(|e| ConnectorError::query(e.to_string()))
    }
}

fn parse_spark_response(json: &serde_json::Value) -> Result<Vec<RecordBatch>, ConnectorError> {
    let empty = vec![];
    let columns: Vec<String> = json["schema"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .filter_map(|c| c["name"].as_str().map(|s| s.to_string()))
        .collect();
    let rows: Vec<Vec<Option<String>>> = json["data"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .map(|row| {
            row.as_array()
                .unwrap_or(&empty)
                .iter()
                .map(|v| {
                    if v.is_null() {
                        None
                    } else {
                        Some(v.as_str().unwrap_or(&v.to_string()).to_string())
                    }
                })
                .collect()
        })
        .collect();

    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(c, DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    let arrays: Vec<Arc<dyn arrow::array::Array>> = (0..columns.len())
        .map(|i| {
            let vals: Vec<Option<&str>> = rows
                .iter()
                .map(|r| r.get(i).and_then(|v| v.as_deref()))
                .collect();
            Arc::new(StringArray::from(vals)) as _
        })
        .collect();

    Ok(vec![
        RecordBatch::try_new(schema, arrays).map_err(ConnectorError::query)?
    ])
}

fn build_spark_sql(query: &SubQuery, database: &str) -> String {
    let cols = if query.projections.is_empty() {
        "*".into()
    } else {
        query.projections.join(", ")
    };
    let table = if database.is_empty() {
        query.table.clone()
    } else {
        format!("{}.{}", database, query.table)
    };
    let mut sql = format!("SELECT {} FROM {}", cols, table);
    if let Some(ref f) = query.filter {
        sql.push_str(&format!(" WHERE {}", filter_to_sql(f)));
    }
    if !query.group_by.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", query.group_by.join(", ")));
    }
    if let Some(ref h) = query.having {
        sql.push_str(&format!(" HAVING {}", filter_to_sql(h)));
    }
    if !query.sort.is_empty() {
        let s: Vec<String> = query
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
        sql.push_str(&format!(" ORDER BY {}", s.join(", ")));
    }
    if let Some(l) = query.limit {
        sql.push_str(&format!(" LIMIT {}", l));
    }
    if let Some(o) = query.offset {
        sql.push_str(&format!(" OFFSET {}", o));
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
impl FederatedConnector for SparkConnector {
    fn id(&self) -> &str {
        &self.id
    }
    fn connector_type(&self) -> &str {
        "spark"
    }
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::full()
    }

    async fn health_check(&self) -> ConnectorHealth {
        match self
            .client
            .get(format!("{}/api/v1/status", self.url))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => ConnectorHealth {
                status: HealthStatus::Healthy,
                latency_ms: None,
                message: None,
            },
            Ok(r) => ConnectorHealth {
                status: HealthStatus::Degraded,
                latency_ms: None,
                message: Some(format!("HTTP {}", r.status())),
            },
            Err(e) => ConnectorHealth {
                status: HealthStatus::Unhealthy,
                latency_ms: None,
                message: Some(e.to_string()),
            },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let json = self.run_query("SHOW TABLES").await?;
        let empty = vec![];
        Ok(json["data"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|row| {
                row.as_array()
                    .and_then(|r| r.get(1))
                    .and_then(|v| v.as_str())
                    .map(|name| SchemaInfo {
                        name: name.to_string(),
                        schema_type: SchemaType::Table,
                        estimated_row_count: None,
                    })
            })
            .collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let json = self.run_query(&format!("DESCRIBE {}", table)).await?;
        let empty = vec![];
        let fields: Vec<Field> = json["data"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|row| {
                row.as_array()
                    .and_then(|r| r.first())
                    .and_then(|v| v.as_str())
                    .map(|name| Field::new(name, DataType::Utf8, true))
            })
            .collect();
        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let sql = build_spark_sql(query, &self.database);
        let json = self.run_query(&sql).await?;
        parse_spark_response(&json)
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

pub struct SparkConnectorFactory;

#[async_trait]
impl ConnectorFactory for SparkConnectorFactory {
    fn connector_type(&self) -> &str {
        "spark"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let url = config
            .properties
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'url' is required".into()))?
            .to_string();
        let database = config
            .properties
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = config.properties.get("token").and_then(|v| v.as_str()) {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", token).parse().map_err(
                    |e: reqwest::header::InvalidHeaderValue| {
                        ConnectorError::Connection(e.to_string())
                    },
                )?,
            );
        }

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(
                config.connection_timeout_secs(300),
            ))
            .build()
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        Ok(Arc::new(SparkConnector::new(
            config.id.clone(),
            client,
            url,
            database,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sq(table: &str) -> SubQuery {
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
    fn test_build_spark_sql_simple() {
        assert_eq!(
            build_spark_sql(&sq("events"), "default"),
            "SELECT * FROM default.events"
        );
    }

    #[test]
    fn test_build_spark_sql_no_database() {
        assert_eq!(build_spark_sql(&sq("events"), ""), "SELECT * FROM events");
    }

    #[test]
    fn test_build_spark_sql_full() {
        let mut q = sq("logs");
        q.projections = vec!["host".into(), "count(*)".into()];
        q.filter = Some(FilterExpr::Comparison {
            field: "level".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8("ERROR".into()),
        });
        q.group_by = vec!["host".into()];
        q.sort = vec![SortExpr {
            field: "host".into(),
            descending: true,
        }];
        q.limit = Some(100);
        assert_eq!(
            build_spark_sql(&q, "db"),
            "SELECT host, count(*) FROM db.logs WHERE level = 'ERROR' GROUP BY host ORDER BY host DESC LIMIT 100"
        );
    }

    #[test]
    fn test_parse_spark_response_with_data() {
        let json = serde_json::json!({
            "schema": [{"name": "id"}, {"name": "name"}],
            "data": [["1", "alice"], ["2", "bob"]]
        });
        let batches = parse_spark_response(&json).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_parse_spark_response_empty() {
        let json = serde_json::json!({ "schema": [{"name": "id"}], "data": [] });
        let batches = parse_spark_response(&json).unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_parse_spark_response_nulls() {
        let json = serde_json::json!({ "schema": [{"name": "x"}], "data": [[null], ["val"]] });
        let batches = parse_spark_response(&json).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_connector_type() {
        let c = SparkConnector::new(
            "t".into(),
            reqwest::Client::new(),
            "http://x".into(),
            "db".into(),
        );
        assert_eq!(c.connector_type(), "spark");
    }

    #[test]
    fn test_filter_or() {
        let f = FilterExpr::Or(
            Box::new(FilterExpr::Comparison {
                field: "a".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Int64(1),
            }),
            Box::new(FilterExpr::Comparison {
                field: "b".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Int64(2),
            }),
        );
        assert_eq!(filter_to_sql(&f), "(a = 1 OR b = 2)");
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;

    fn sq(table: &str) -> SubQuery {
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
    fn test_nested_and_or_filter() {
        let mut q = sq("t");
        q.filter = Some(FilterExpr::And(
            Box::new(FilterExpr::Or(
                Box::new(FilterExpr::Comparison {
                    field: "a".into(),
                    op: ComparisonOp::Eq,
                    value: ScalarValue::Int64(1),
                }),
                Box::new(FilterExpr::Comparison {
                    field: "b".into(),
                    op: ComparisonOp::Eq,
                    value: ScalarValue::Int64(2),
                }),
            )),
            Box::new(FilterExpr::IsNotNull("c".into())),
        ));
        q.limit = Some(10);
        let sql = build_spark_sql(&q, "db");
        assert!(sql.contains("((a = 1 OR b = 2) AND c IS NOT NULL)"));
    }

    #[test]
    fn test_having_clause() {
        let mut q = sq("events");
        q.projections = vec!["host".into(), "count(*)".into()];
        q.group_by = vec!["host".into()];
        q.having = Some(FilterExpr::Comparison {
            field: "count(*)".into(),
            op: ComparisonOp::Gt,
            value: ScalarValue::Int64(10),
        });
        q.limit = Some(50);
        let sql = build_spark_sql(&q, "db");
        assert!(sql.contains("HAVING count(*) > 10"));
    }

    #[test]
    fn test_offset() {
        let mut q = sq("t");
        q.limit = Some(10);
        q.offset = Some(20);
        let sql = build_spark_sql(&q, "db");
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 20"));
    }

    #[test]
    fn test_special_chars_in_string() {
        let mut q = sq("t");
        q.filter = Some(FilterExpr::Comparison {
            field: "name".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8("O'Brien".into()),
        });
        q.limit = Some(1);
        let sql = build_spark_sql(&q, "db");
        assert!(
            sql.contains("O''Brien"),
            "should escape single quotes: {}",
            sql
        );
    }

    #[test]
    fn test_in_clause() {
        let mut q = sq("t");
        q.filter = Some(FilterExpr::In {
            field: "status".into(),
            values: vec![
                ScalarValue::Utf8("active".into()),
                ScalarValue::Utf8("pending".into()),
            ],
        });
        q.limit = Some(100);
        let sql = build_spark_sql(&q, "db");
        assert!(sql.contains("status IN ('active', 'pending')"));
    }

    #[test]
    fn test_multiple_sort_columns() {
        let mut q = sq("t");
        q.sort = vec![
            SortExpr {
                field: "created_at".into(),
                descending: true,
            },
            SortExpr {
                field: "id".into(),
                descending: false,
            },
        ];
        q.limit = Some(10);
        let sql = build_spark_sql(&q, "db");
        assert!(sql.contains("ORDER BY created_at DESC, id"));
    }

    #[test]
    fn test_null_scalar() {
        assert_eq!(scalar_to_sql(&ScalarValue::Null), "NULL");
    }

    #[test]
    fn test_float_scalar() {
        assert_eq!(scalar_to_sql(&ScalarValue::Float64(2.72)), "2.72");
    }
}
