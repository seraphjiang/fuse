// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL/MySQL connector for the Fuse federated query engine.
//!
//! - SQL passthrough to remote DB (queries sent as-is after table rewriting)
//! - Schema discovery via information_schema
//! - Connection pooling via sqlx PgPool / MySqlPool
//! - Filter/projection/limit pushdown (native SQL)

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use sqlx::Column;
use sqlx::Row;
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

mod sql;
pub use sql::subquery_to_sql;

// ── Connector variants ────────────────────────────────────────────────────────

#[derive(Debug)]
enum Pool {
    Postgres(sqlx::PgPool),
    Mysql(sqlx::MySqlPool),
}

#[derive(Debug)]
pub struct SqlConnector {
    id: String,
    pool: Pool,
    db_type: &'static str,
}

impl SqlConnector {
    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let url = config
            .properties
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("missing 'url' in connector config".into()))?;

        let max_conns = config
            .properties
            .get("max_connections")
            .and_then(|v| v.as_integer())
            .unwrap_or(10) as u32;

        let (pool, db_type) = if url.starts_with("postgres") {
            let p = sqlx::postgres::PgPoolOptions::new()
                .max_connections(max_conns)
                .connect(url)
                .await
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            (Pool::Postgres(p), "postgres")
        } else {
            let p = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(max_conns)
                .connect(url)
                .await
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            (Pool::Mysql(p), "mysql")
        };

        Ok(Self { id: config.id.clone(), pool, db_type })
    }
}

#[async_trait]
impl FederatedConnector for SqlConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { self.db_type }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: true,
            supports_sorting: true,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 10,
            supports_streaming: false,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        let ok = match &self.pool {
            Pool::Postgres(p) => sqlx::query("SELECT 1").execute(p).await.is_ok(),
            Pool::Mysql(p) => sqlx::query("SELECT 1").execute(p).await.is_ok(),
        };
        if ok {
            ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(start.elapsed().as_millis() as u64), message: None }
        } else {
            ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some("ping failed".into()) }
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let sql = "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog','performance_schema','sys') ORDER BY table_name";
        let names: Vec<String> = match &self.pool {
            Pool::Postgres(p) => sqlx::query_scalar(sql).fetch_all(p).await
                .map_err(|e| ConnectorError::schema(e))?,
            Pool::Mysql(p) => sqlx::query_scalar(sql).fetch_all(p).await
                .map_err(|e| ConnectorError::schema(e))?,
        };
        Ok(names.into_iter().map(|name| SchemaInfo {
            name,
            schema_type: SchemaType::Table,
            estimated_row_count: None,
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position";
        let rows: Vec<(String, String)> = match &self.pool {
            Pool::Postgres(p) => sqlx::query_as(sql).bind(table).fetch_all(p).await
                .map_err(|e| ConnectorError::schema(e))?,
            Pool::Mysql(p) => {
                let sql_my = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position";
                sqlx::query_as(sql_my).bind(table).fetch_all(p).await
                    .map_err(|e| ConnectorError::schema(e))?
            }
        };
        let fields: Vec<Field> = rows.iter().map(|(col, dt)| {
            Field::new(col, sql_type_to_arrow(dt), true)
        }).collect();
        if fields.is_empty() {
            return Err(ConnectorError::schema(format!("table '{table}' not found")));
        }
        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let sql = subquery_to_sql(query);
        debug!(sql = sql.as_str(), "SQL connector execute");
        match &self.pool {
            Pool::Postgres(p) => execute_pg(p, &sql).await,
            Pool::Mysql(p) => execute_my(p, &sql).await,
        }
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

// ── Query execution ───────────────────────────────────────────────────────────

async fn execute_pg(pool: &sqlx::PgPool, sql: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
    let rows = sqlx::query(sql).fetch_all(pool).await
        .map_err(|e| ConnectorError::query(e))?;
    if rows.is_empty() { return Ok(vec![]); }
    pg_rows_to_batch(&rows)
}

async fn execute_my(pool: &sqlx::MySqlPool, sql: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
    let rows = sqlx::query(sql).fetch_all(pool).await
        .map_err(|e| ConnectorError::query(e))?;
    if rows.is_empty() { return Ok(vec![]); }
    my_rows_to_batch(&rows)
}

fn pg_rows_to_batch(rows: &[sqlx::postgres::PgRow]) -> Result<Vec<RecordBatch>, ConnectorError> {
    let cols: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (i, col) in cols.iter().enumerate() {
        // Try i64 first, then f64, then bool, then string
        let as_i64: Vec<Option<i64>> = rows.iter().map(|r| r.try_get::<i64, _>(i).ok()).collect();
        if as_i64.iter().any(|v| v.is_some()) {
            fields.push(Field::new(*col, DataType::Int64, true));
            arrays.push(Arc::new(Int64Array::from(as_i64)) as ArrayRef);
            continue;
        }
        let as_f64: Vec<Option<f64>> = rows.iter().map(|r| r.try_get::<f64, _>(i).ok()).collect();
        if as_f64.iter().any(|v| v.is_some()) {
            fields.push(Field::new(*col, DataType::Float64, true));
            arrays.push(Arc::new(Float64Array::from(as_f64)) as ArrayRef);
            continue;
        }
        let as_bool: Vec<Option<bool>> = rows.iter().map(|r| r.try_get::<bool, _>(i).ok()).collect();
        if as_bool.iter().any(|v| v.is_some()) {
            fields.push(Field::new(*col, DataType::Boolean, true));
            arrays.push(Arc::new(BooleanArray::from(as_bool)) as ArrayRef);
            continue;
        }
        let as_str: Vec<Option<String>> = rows.iter().map(|r| r.try_get::<String, _>(i).ok()).collect();
        fields.push(Field::new(*col, DataType::Utf8, true));
        arrays.push(Arc::new(StringArray::from(as_str)) as ArrayRef);
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| ConnectorError::query(e))?;
    Ok(vec![batch])
}

fn my_rows_to_batch(rows: &[sqlx::mysql::MySqlRow]) -> Result<Vec<RecordBatch>, ConnectorError> {
    let cols: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for (i, col) in cols.iter().enumerate() {
        let as_i64: Vec<Option<i64>> = rows.iter().map(|r| r.try_get::<i64, _>(i).ok()).collect();
        if as_i64.iter().any(|v| v.is_some()) {
            fields.push(Field::new(*col, DataType::Int64, true));
            arrays.push(Arc::new(Int64Array::from(as_i64)) as ArrayRef);
            continue;
        }
        let as_f64: Vec<Option<f64>> = rows.iter().map(|r| r.try_get::<f64, _>(i).ok()).collect();
        if as_f64.iter().any(|v| v.is_some()) {
            fields.push(Field::new(*col, DataType::Float64, true));
            arrays.push(Arc::new(Float64Array::from(as_f64)) as ArrayRef);
            continue;
        }
        let as_str: Vec<Option<String>> = rows.iter().map(|r| r.try_get::<String, _>(i).ok()).collect();
        fields.push(Field::new(*col, DataType::Utf8, true));
        arrays.push(Arc::new(StringArray::from(as_str)) as ArrayRef);
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| ConnectorError::query(e))?;
    Ok(vec![batch])
}

fn sql_type_to_arrow(sql_type: &str) -> DataType {
    match sql_type.to_lowercase().as_str() {
        "integer" | "int" | "int4" | "int8" | "bigint" | "smallint" | "tinyint" => DataType::Int64,
        "float" | "double" | "real" | "numeric" | "decimal" | "float4" | "float8" => DataType::Float64,
        "boolean" | "bool" | "tinyint(1)" => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

// ── Factory ───────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct PostgresConnectorFactory;

#[async_trait]
impl ConnectorFactory for PostgresConnectorFactory {
    fn connector_type(&self) -> &str { "postgres" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(SqlConnector::from_config(config).await?))
    }
}

#[derive(Debug, Default)]
pub struct MysqlConnectorFactory;

#[async_trait]
impl ConnectorFactory for MysqlConnectorFactory {
    fn connector_type(&self) -> &str { "mysql" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(SqlConnector::from_config(config).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_type_to_arrow_int() {
        assert_eq!(sql_type_to_arrow("integer"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("bigint"), DataType::Int64);
        assert_eq!(sql_type_to_arrow("int4"), DataType::Int64);
    }

    #[test]
    fn test_sql_type_to_arrow_float() {
        assert_eq!(sql_type_to_arrow("float"), DataType::Float64);
        assert_eq!(sql_type_to_arrow("numeric"), DataType::Float64);
    }

    #[test]
    fn test_sql_type_to_arrow_bool() {
        assert_eq!(sql_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(sql_type_to_arrow("bool"), DataType::Boolean);
    }

    #[test]
    fn test_sql_type_to_arrow_text() {
        assert_eq!(sql_type_to_arrow("text"), DataType::Utf8);
        assert_eq!(sql_type_to_arrow("varchar"), DataType::Utf8);
        assert_eq!(sql_type_to_arrow("timestamp"), DataType::Utf8);
    }
}
