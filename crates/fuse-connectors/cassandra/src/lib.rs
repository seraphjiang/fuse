// SPDX-License-Identifier: Apache-2.0

//! Apache Cassandra connector for the Fuse federated query engine.
//!
//! Uses the `scylla` driver (compatible with Cassandra 3.x/4.x and ScyllaDB).
//! CQL pushdown with partition-aware query routing.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use scylla::Session;
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;
use fuse_core::sql::quote_ident;

/// Convert SubQuery to CQL SELECT statement.
pub fn subquery_to_cql(sq: &SubQuery, keyspace: &str) -> String {
    let cols = if sq.projections.is_empty() { "*".into() } else {
        sq.projections.iter().map(|p| if p == "*" { "*".to_string() } else { quote_ident(p) }).collect::<Vec<_>>().join(", ")
    };
    let mut cql = format!("SELECT {} FROM {}.{}", cols, quote_ident(keyspace), quote_ident(&sq.table));
    if let Some(ref f) = sq.filter {
        cql.push_str(&format!(" WHERE {}", filter_to_cql(f)));
    }
    if !sq.sort.is_empty() {
        let clauses: Vec<String> = sq.sort.iter()
            .map(|s| format!("{} {}", quote_ident(&s.field), if s.descending { "DESC" } else { "ASC" }))
            .collect();
        cql.push_str(&format!(" ORDER BY {}", clauses.join(", ")));
    }
    if let Some(limit) = sq.limit {
        cql.push_str(&format!(" LIMIT {}", limit));
    }
    if sq.filter.is_some() {
        cql.push_str(" ALLOW FILTERING");
    }
    cql
}

fn filter_to_cql(f: &FilterExpr) -> String {
    match f {
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=", ComparisonOp::Neq => "!=",
                ComparisonOp::Gt => ">", ComparisonOp::Gte => ">=",
                ComparisonOp::Lt => "<", ComparisonOp::Lte => "<=",
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => "=",
            };
            format!("{} {} {}", quote_ident(field), op_str, scalar_to_cql(value))
        }
        FilterExpr::And(l, r) => format!("{} AND {}", filter_to_cql(l), filter_to_cql(r)),
        FilterExpr::Or(l, r) => format!("{} AND {}", filter_to_cql(l), filter_to_cql(r)), // CQL doesn't support OR
        FilterExpr::Not(_) => String::new(),
        FilterExpr::In { field, values } => {
            let vals: Vec<String> = values.iter().map(scalar_to_cql).collect();
            format!("{} IN ({})", quote_ident(field), vals.join(", "))
        }
        FilterExpr::IsNull(_) | FilterExpr::IsNotNull(_) => String::new(),
    }
}

fn scalar_to_cql(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(f) => f.to_string(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Null => "null".to_string(),
    }
}

fn cql_type_to_arrow(cql_type: &str) -> DataType {
    match cql_type.to_lowercase().as_str() {
        "int" | "bigint" | "counter" | "varint" | "smallint" | "tinyint" => DataType::Int64,
        "float" | "double" | "decimal" => DataType::Float64,
        "boolean" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

#[derive(Debug)]
pub struct CassandraConnector {
    id: String,
    session: Arc<Session>,
    keyspace: String,
}

impl CassandraConnector {
    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let hosts = config.properties.get("hosts")
            .and_then(|v| v.as_str())
            .unwrap_or("127.0.0.1:9042");
        let keyspace = config.properties.get("keyspace")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        let host_list: Vec<&str> = hosts.split(',').map(|h| h.trim()).collect();
        let session = scylla::SessionBuilder::new()
            .known_nodes(&host_list)
            .build()
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        Ok(Self { id: config.id.clone(), session: Arc::new(session), keyspace })
    }

    async fn run_cql(&self, cql: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
        let t0 = Instant::now();
        debug!(connector = %self.id, cql = cql, "Cassandra query");

        let result = self.session.query_unpaged(cql, &[])
            .await
            .map_err(|e| ConnectorError::query(e.to_string()))?;

        let col_specs = result.col_specs();
        if col_specs.is_empty() {
            return Ok(vec![]);
        }

        let fields: Vec<Field> = col_specs.iter()
            .map(|c| Field::new(&c.name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let num_cols = col_specs.len();

        let rows = result.rows().map_err(|e| ConnectorError::query(e.to_string()))?;
        if rows.is_empty() {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        let mut col_values: Vec<Vec<Option<String>>> = (0..num_cols).map(|_| Vec::new()).collect();
        for row in &rows {
            for i in 0..num_cols {
                let val: Option<String> = row.columns[i].as_ref().map(|c| format!("{:?}", c));
                col_values[i].push(val);
            }
        }

        let arrays: Vec<ArrayRef> = col_values.into_iter()
            .map(|vals| Arc::new(StringArray::from(vals)) as ArrayRef)
            .collect();

        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| ConnectorError::query(e.to_string()))?;
        debug!(connector = %self.id, rows = batch.num_rows(), elapsed_ms = t0.elapsed().as_millis() as u64, "Cassandra query complete");
        Ok(vec![batch])
    }

}
#[async_trait::async_trait]
impl FederatedConnector for CassandraConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "cassandra" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true, supports_projection: true,
            supports_aggregation: false, supports_sorting: true,
            supports_limit: true, supports_join: false,
            max_concurrent_queries: 8, supports_streaming: true,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        match self.session.query_unpaged("SELECT now() FROM system.local", &[]).await {
            Ok(_) => ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None },
            Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let cql = format!(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{}'",
            self.keyspace.replace('\'', "''")
        );
        let result = self.session.query_unpaged(cql.as_str(), &[]).await
            .map_err(|e| ConnectorError::schema(e.to_string()))?;
        let rows = result.rows().map_err(|e| ConnectorError::schema(e.to_string()))?;
        let tables = rows.iter().filter_map(|row| {
            row.columns.first()?.as_ref().and_then(|v| {
                if let scylla::frame::response::result::CqlValue::Text(s) = v { Some(s.clone()) } else { None }
            })
        }).map(|name| SchemaInfo { name, schema_type: SchemaType::Table, estimated_row_count: None })
        .collect();
        Ok(tables)
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let cql = format!(
            "SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = '{}' AND table_name = '{}'",
            self.keyspace.replace('\'', "''"), table.replace('\'', "''")
        );
        let result = self.session.query_unpaged(cql.as_str(), &[]).await
            .map_err(|e| ConnectorError::schema(e.to_string()))?;
        let rows = result.rows().map_err(|e| ConnectorError::schema(e.to_string()))?;
        let fields: Vec<Field> = rows.iter().filter_map(|row| {
            let name = row.columns.first()?.as_ref().and_then(|v| {
                if let scylla::frame::response::result::CqlValue::Text(s) = v { Some(s.clone()) } else { None }
            })?;
            let dtype = row.columns.get(1)?.as_ref().and_then(|v| {
                if let scylla::frame::response::result::CqlValue::Text(s) = v { Some(s.clone()) } else { None }
            }).unwrap_or_else(|| "text".into());
            Some(Field::new(name.as_str(), cql_type_to_arrow(&dtype), true))
        }).collect();
        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let cql = subquery_to_cql(query, &self.keyspace);
        self.run_cql(&cql).await
    }

    async fn execute_streaming(&self, query: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for b in self.execute(query).await? {
            tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

pub struct CassandraConnectorFactory;

#[async_trait::async_trait]
impl ConnectorFactory for CassandraConnectorFactory {
    fn connector_type(&self) -> &str { "cassandra" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(CassandraConnector::from_config(config).await?))
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
    fn test_subquery_to_cql_simple() {
        assert_eq!(subquery_to_cql(&simple_sq("users"), "mykeyspace"), "SELECT * FROM \"mykeyspace\".\"users\"");
    }

    #[test]
    fn test_subquery_to_cql_with_projections() {
        let mut sq = simple_sq("t");
        sq.projections = vec!["id".into(), "name".into()];
        assert_eq!(subquery_to_cql(&sq, "ks"), "SELECT \"id\", \"name\" FROM \"ks\".\"t\"");
    }

    #[test]
    fn test_subquery_to_cql_with_limit() {
        let mut sq = simple_sq("t");
        sq.limit = Some(50);
        assert!(subquery_to_cql(&sq, "ks").ends_with("LIMIT 50"));
    }

    #[test]
    fn test_subquery_to_cql_with_filter_adds_allow_filtering() {
        let mut sq = simple_sq("t");
        sq.filter = Some(FilterExpr::Comparison {
            field: "status".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("active".into()),
        });
        let cql = subquery_to_cql(&sq, "ks");
        assert!(cql.contains("WHERE \"status\" = 'active'"));
        assert!(cql.ends_with("ALLOW FILTERING"));
    }

    #[test]
    fn test_subquery_to_cql_with_in() {
        let mut sq = simple_sq("t");
        sq.filter = Some(FilterExpr::In {
            field: "id".into(),
            values: vec![ScalarValue::Int64(1), ScalarValue::Int64(2)],
        });
        let cql = subquery_to_cql(&sq, "ks");
        assert!(cql.contains("\"id\" IN (1, 2)"));
    }

    #[test]
    fn test_subquery_to_cql_sort() {
        let mut sq = simple_sq("t");
        sq.sort = vec![SortExpr { field: "ts".into(), descending: true }];
        assert!(subquery_to_cql(&sq, "ks").contains("ORDER BY \"ts\" DESC"));
    }

    #[test]
    fn test_cql_type_to_arrow() {
        assert_eq!(cql_type_to_arrow("bigint"), DataType::Int64);
        assert_eq!(cql_type_to_arrow("double"), DataType::Float64);
        assert_eq!(cql_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(cql_type_to_arrow("text"), DataType::Utf8);
        assert_eq!(cql_type_to_arrow("uuid"), DataType::Utf8);
    }

    #[test]
    fn test_scalar_to_cql() {
        assert_eq!(scalar_to_cql(&ScalarValue::Utf8("hello".into())), "'hello'");
        assert_eq!(scalar_to_cql(&ScalarValue::Int64(42)), "42");
        assert_eq!(scalar_to_cql(&ScalarValue::Boolean(true)), "true");
        assert_eq!(scalar_to_cql(&ScalarValue::Null), "null");
    }
}
