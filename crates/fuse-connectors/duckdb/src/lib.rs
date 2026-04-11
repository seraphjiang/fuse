// SPDX-License-Identifier: Apache-2.0

//! DuckDB connector for the Fuse federated query engine.
//!
//! DuckDB is an embedded OLAP database. This connector:
//! - Opens a DuckDB file (or :memory: for in-memory)
//! - Translates SubQuery to SQL (same as ClickHouse/Postgres)
//! - Executes via spawn_blocking (DuckDB is synchronous)
//! - Returns Arrow RecordBatches

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

mod sql;
pub use sql::subquery_to_sql;

#[derive(Debug, Clone)]
pub struct DuckDbConnector {
    id: String,
    path: String, // file path or ":memory:"
}

impl DuckDbConnector {
    pub fn new(id: String, path: String) -> Self {
        Self { id, path }
    }

    pub fn from_config(config: &ConnectorConfig) -> Self {
        let path = config.properties.get("path")
            .and_then(|v| v.as_str())
            .unwrap_or(":memory:")
            .to_string();
        Self::new(config.id.clone(), path)
    }

    fn open(&self) -> Result<duckdb::Connection, ConnectorError> {
        duckdb::Connection::open(&self.path)
            .map_err(|e| ConnectorError::Connection(e.to_string()))
    }
}

#[async_trait]
impl FederatedConnector for DuckDbConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "duckdb" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: true,
            supports_sorting: true,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 4, // DuckDB has internal concurrency limits
            supports_streaming: false,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        let path = self.path.clone();
        let ok = tokio::task::spawn_blocking(move || {
            duckdb::Connection::open(&path)
                .and_then(|conn| conn.execute("SELECT 1", []).map(|_| ()))
                .is_ok()
        }).await.unwrap_or(false);

        if ok {
            ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(start.elapsed().as_millis() as u64), message: None }
        } else {
            ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some("DuckDB open failed".into()) }
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let path = self.path.clone();
        let names = tokio::task::spawn_blocking(move || -> Result<Vec<String>, ConnectorError> {
            let conn = duckdb::Connection::open(&path)
                .map_err(|e| ConnectorError::schema(e))?;
            let mut stmt = conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name")
                .map_err(|e| ConnectorError::schema(e))?;
            let names: Vec<String> = stmt.query_map([], |row| row.get(0))
                .map_err(|e| ConnectorError::schema(e))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(names)
        }).await.map_err(|e| ConnectorError::schema(e))??;

        Ok(names.into_iter().map(|name| SchemaInfo {
            name,
            schema_type: SchemaType::Table,
            estimated_row_count: None,
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let path = self.path.clone();
        let table = table.to_string();
        let table2 = table.clone();
        let fields = tokio::task::spawn_blocking(move || -> Result<Vec<(String, String)>, ConnectorError> {
            let conn = duckdb::Connection::open(&path)
                .map_err(|e| ConnectorError::schema(e))?;
            let sql = format!("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position", table.replace('\'', "''"));
            let mut stmt = conn.prepare(&sql).map_err(|e| ConnectorError::schema(e))?;
            let rows: Vec<(String, String)> = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| ConnectorError::schema(e))?
                .filter_map(|r| r.ok())
                .collect();
            Ok(rows)
        }).await.map_err(|e| ConnectorError::schema(e))??;

        if fields.is_empty() {
            return Err(ConnectorError::schema(format!("table '{table2}' not found")));
        }
        Ok(Schema::new(fields.iter().map(|(col, dt)| Field::new(col, duckdb_type_to_arrow(dt), true)).collect::<Vec<_>>()))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let path = self.path.clone();
        let sql = subquery_to_sql(query);
        debug!(sql = sql.as_str(), "DuckDB execute");

        tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, ConnectorError> {
            let conn = duckdb::Connection::open(&path)
                .map_err(|e| ConnectorError::query(e))?;
            let mut stmt = conn.prepare(&sql).map_err(|e| ConnectorError::query(e))?;

            let mut rows = stmt.query([]).map_err(|e| ConnectorError::query(e))?;

            // Get column info from statement after query() is called
            let (col_count, col_names) = match rows.as_ref() {
                Some(stmt) => (stmt.column_count(), stmt.column_names()),
                None => (0, vec![]),
            };

            let mut col_data: Vec<Vec<Option<String>>> = vec![vec![]; col_count];
            while let Some(row) = rows.next().map_err(|e| ConnectorError::query(e))? {
                for i in 0..col_count {
                    let val: Option<String> = row.get::<_, Option<String>>(i)
                        .or_else(|_| row.get::<_, Option<i64>>(i).map(|v| v.map(|n| n.to_string())))
                        .or_else(|_| row.get::<_, Option<f64>>(i).map(|v| v.map(|f| f.to_string())))
                        .unwrap_or(None);
                    col_data[i].push(val);
                }
            }

            if col_count == 0 || col_data.is_empty() || col_data[0].is_empty() { return Ok(vec![]); }

            let fields: Vec<Field> = col_names.iter().map(|n| Field::new(n, DataType::Utf8, true)).collect();
            let arrays: Vec<ArrayRef> = col_data.into_iter()
                .map(|vals| Arc::new(StringArray::from(vals)) as ArrayRef)
                .collect();

            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
                .map_err(|e| ConnectorError::query(e))?;
            Ok(vec![batch])
        }).await.map_err(|e| ConnectorError::query(e))?
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

    async fn write_batches(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<u64, ConnectorError> {
        if batches.is_empty() { return Ok(0); }
        let path = self.path.clone();
        let table = table.to_string();

        tokio::task::spawn_blocking(move || -> Result<u64, ConnectorError> {
            let conn = duckdb::Connection::open(&path)
                .map_err(|e| ConnectorError::query(e))?;
            let mut total = 0u64;

            for batch in &batches {
                if batch.num_rows() == 0 { continue; }
                let schema = batch.schema();
                let cols: Vec<String> = schema.fields().iter()
                    .map(|f| format!("\"{}\"", f.name()))
                    .collect();
                let col_list = cols.join(", ");

                let mut value_rows = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    let vals: Vec<String> = (0..batch.num_columns())
                        .map(|c| cell_to_sql(batch, c, row))
                        .collect();
                    value_rows.push(format!("({})", vals.join(", ")));
                }

                let sql = format!("INSERT INTO {} ({}) VALUES {}", table, col_list, value_rows.join(", "));
                conn.execute_batch(&sql).map_err(|e| ConnectorError::query(e))?;
                total += batch.num_rows() as u64;
            }
            Ok(total)
        }).await.map_err(|e| ConnectorError::query(e))?
    }
}

/// Convert an Arrow cell to a SQL literal for INSERT.
fn cell_to_sql(batch: &RecordBatch, col: usize, row: usize) -> String {
    use arrow::array as aa;
    let arr = batch.column(col);
    if arr.is_null(row) { return "NULL".into(); }
    match arr.data_type() {
        DataType::Int64 => arr.as_any().downcast_ref::<aa::Int64Array>().map(|a| a.value(row).to_string()).unwrap_or_else(|| "NULL".into()),
        DataType::Int32 => arr.as_any().downcast_ref::<aa::Int32Array>().map(|a| a.value(row).to_string()).unwrap_or_else(|| "NULL".into()),
        DataType::Float64 => arr.as_any().downcast_ref::<aa::Float64Array>().map(|a| a.value(row).to_string()).unwrap_or_else(|| "NULL".into()),
        DataType::Boolean => arr.as_any().downcast_ref::<aa::BooleanArray>().map(|a| a.value(row).to_string()).unwrap_or_else(|| "NULL".into()),
        _ => {
            let s = arr.as_any().downcast_ref::<aa::StringArray>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_default();
            format!("'{}'", s.replace('\'', "''"))
        }
    }
}

fn duckdb_type_to_arrow(t: &str) -> DataType {
    match t.to_uppercase().as_str() {
        "BIGINT" | "INTEGER" | "SMALLINT" | "TINYINT" | "HUGEINT" | "INT" | "INT4" | "INT8" => DataType::Int64,
        "DOUBLE" | "FLOAT" | "REAL" | "DECIMAL" | "NUMERIC" => DataType::Float64,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

#[derive(Debug, Default)]
pub struct DuckDbConnectorFactory;

#[async_trait]
impl ConnectorFactory for DuckDbConnectorFactory {
    fn connector_type(&self) -> &str { "duckdb" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(DuckDbConnector::from_config(config)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckdb_type_to_arrow() {
        assert_eq!(duckdb_type_to_arrow("BIGINT"), DataType::Int64);
        assert_eq!(duckdb_type_to_arrow("INTEGER"), DataType::Int64);
        assert_eq!(duckdb_type_to_arrow("DOUBLE"), DataType::Float64);
        assert_eq!(duckdb_type_to_arrow("BOOLEAN"), DataType::Boolean);
        assert_eq!(duckdb_type_to_arrow("VARCHAR"), DataType::Utf8);
        assert_eq!(duckdb_type_to_arrow("TIMESTAMP"), DataType::Utf8);
    }

    #[test]
    fn test_connector_metadata() {
        let c = DuckDbConnector::new("duck1".into(), ":memory:".into());
        assert_eq!(c.id(), "duck1");
        assert_eq!(c.connector_type(), "duckdb");
    }

    #[test]
    fn test_capabilities() {
        let c = DuckDbConnector::new("d".into(), ":memory:".into());
        let caps = c.capabilities();
        assert!(caps.supports_aggregation);
        assert!(caps.supports_sorting);
        assert!(caps.supports_limit);
    }

    #[tokio::test]
    async fn test_health_check_memory() {
        let c = DuckDbConnector::new("d".into(), ":memory:".into());
        let h = c.health_check().await;
        assert_eq!(h.status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_execute_memory_query() {
        let c = DuckDbConnector::new("d".into(), ":memory:".into());
        let q = SubQuery {
            table: "(SELECT 1 AS id, 'alice' AS name) t".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            passthrough: None, offset: None,
        };
        let batches = c.execute(&q).await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn test_cell_to_sql_types() {
        use arrow::array::{Int64Array, Float64Array, StringArray, BooleanArray};
        use arrow::datatypes::Field;

        // Int64
        let s = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let b = RecordBatch::try_new(s, vec![Arc::new(Int64Array::from(vec![Some(42), None])) as ArrayRef]).unwrap();
        assert_eq!(cell_to_sql(&b, 0, 0), "42");
        assert_eq!(cell_to_sql(&b, 0, 1), "NULL");

        // String with quote
        let s = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        let b = RecordBatch::try_new(s, vec![Arc::new(StringArray::from(vec!["it's"])) as ArrayRef]).unwrap();
        assert_eq!(cell_to_sql(&b, 0, 0), "'it''s'");

        // Boolean
        let s = Arc::new(Schema::new(vec![Field::new("x", DataType::Boolean, false)]));
        let b = RecordBatch::try_new(s, vec![Arc::new(BooleanArray::from(vec![true])) as ArrayRef]).unwrap();
        assert_eq!(cell_to_sql(&b, 0, 0), "true");
    }

    #[tokio::test]
    async fn test_write_batches_file() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::Field;

        let tmp = std::env::temp_dir().join("fuse_duckdb_write_test.db");
        let path = tmp.to_str().unwrap().to_string();
        // Clean up from previous runs
        let _ = std::fs::remove_file(&path);

        let connector = DuckDbConnector::new("test".into(), path.clone());

        // Create table
        let conn = connector.open().unwrap();
        conn.execute_batch("CREATE TABLE test_write (id BIGINT, name VARCHAR)").unwrap();
        drop(conn);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
            ],
        ).unwrap();

        let rows = connector.write_batches("test_write", vec![batch]).await.unwrap();
        assert_eq!(rows, 2);

        // Verify data was written
        let conn = connector.open().unwrap();
        let mut stmt = conn.prepare("SELECT count(*) FROM test_write").unwrap();
        let count: i64 = stmt.query_row([], |r| r.get(0)).unwrap();
        assert_eq!(count, 2);

        let _ = std::fs::remove_file(&path);
    }
}
