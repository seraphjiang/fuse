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

pub mod sql;
pub use sql::subquery_to_sql;

// ── Connector variants ────────────────────────────────────────────────────────

#[derive(Debug)]
enum Pool {
    Postgres(sqlx::PgPool),
    Mysql(sqlx::MySqlPool),
    Sqlite(sqlx::SqlitePool),
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

        let max_conns = config.max_connections(10);

        let (pool, db_type) = if url.starts_with("postgres") {
            let p = sqlx::postgres::PgPoolOptions::new()
                .max_connections(max_conns)
                .connect(url)
                .await
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            (Pool::Postgres(p), "postgres")
        } else if url.starts_with("sqlite") {
            let p = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(max_conns)
                .connect(url)
                .await
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            (Pool::Sqlite(p), "sqlite")
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
            Pool::Sqlite(p) => sqlx::query("SELECT 1").execute(p).await.is_ok(),
        };
        if ok {
            ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(start.elapsed().as_millis() as u64), message: None }
        } else {
            ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some("ping failed".into()) }
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let names: Vec<String> = match &self.pool {
            Pool::Postgres(p) => {
                let sql = "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog') ORDER BY table_name";
                sqlx::query_scalar(sql).fetch_all(p).await.map_err(|e| ConnectorError::schema(e))?
            }
            Pool::Mysql(p) => {
                let sql = "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','performance_schema','sys') ORDER BY table_name";
                sqlx::query_scalar(sql).fetch_all(p).await.map_err(|e| ConnectorError::schema(e))?
            }
            Pool::Sqlite(p) => {
                let sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name";
                sqlx::query_scalar(sql).fetch_all(p).await.map_err(|e| ConnectorError::schema(e))?
            }
        };
        Ok(names.into_iter().map(|name| SchemaInfo {
            name,
            schema_type: SchemaType::Table,
            estimated_row_count: None,
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let rows: Vec<(String, String)> = match &self.pool {
            Pool::Postgres(p) => {
                let sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position";
                sqlx::query_as(sql).bind(table).fetch_all(p).await.map_err(|e| ConnectorError::schema(e))?
            }
            Pool::Mysql(p) => {
                let sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position";
                sqlx::query_as(sql).bind(table).fetch_all(p).await.map_err(|e| ConnectorError::schema(e))?
            }
            Pool::Sqlite(p) => {
                // SQLite PRAGMA returns (cid, name, type, notnull, dflt_value, pk)
                let sql = format!("PRAGMA table_info({table})");
                let rows: Vec<(i64, String, String, i64, Option<String>, i64)> =
                    sqlx::query_as(&sql).fetch_all(p).await.map_err(|e| ConnectorError::schema(e))?;
                rows.into_iter().map(|(_, name, typ, _, _, _)| (name, typ)).collect()
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
            Pool::Sqlite(p) => execute_sqlite(p, &sql).await,
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

    async fn write_batches(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<u64, ConnectorError> {
        let mut total = 0u64;
        for batch in &batches {
            if batch.num_rows() == 0 { continue; }
            let schema = batch.schema();
            let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            let col_list = cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");

            match &self.pool {
                Pool::Postgres(pool) => {
                    total += write_pg(pool, table, &col_list, batch).await?;
                }
                Pool::Mysql(pool) => {
                    total += write_my(pool, table, &col_list, batch).await?;
                }
                Pool::Sqlite(pool) => {
                    total += write_sqlite(pool, table, &col_list, batch).await?;
                }
            }
        }
        Ok(total)
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

async fn execute_sqlite(pool: &sqlx::SqlitePool, sql: &str) -> Result<Vec<RecordBatch>, ConnectorError> {
    let rows = sqlx::query(sql).fetch_all(pool).await
        .map_err(|e| ConnectorError::query(e))?;
    if rows.is_empty() { return Ok(vec![]); }
    sqlite_rows_to_batch(&rows)
}

fn sqlite_rows_to_batch(rows: &[sqlx::sqlite::SqliteRow]) -> Result<Vec<RecordBatch>, ConnectorError> {
    use sqlx::Column;
    use sqlx::Row;
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

/// Extract a cell value as a SQL literal string for INSERT.
fn cell_to_sql_literal(batch: &RecordBatch, col: usize, row: usize) -> String {
    use arrow::array::{self as aa};
    let arr = batch.column(col);
    if arr.is_null(row) { return "NULL".into(); }
    match arr.data_type() {
        DataType::Int64 => arr.as_any().downcast_ref::<aa::Int64Array>().map(|a| a.value(row).to_string()).unwrap_or_else(|| "NULL".into()),
        DataType::Int32 => arr.as_any().downcast_ref::<aa::Int32Array>().map(|a| a.value(row).to_string()).unwrap_or_else(|| "NULL".into()),
        DataType::Float64 => arr.as_any().downcast_ref::<aa::Float64Array>().map(|a| a.value(row).to_string()).unwrap_or_else(|| "NULL".into()),
        DataType::Float32 => arr.as_any().downcast_ref::<aa::Float32Array>().map(|a| a.value(row).to_string()).unwrap_or_else(|| "NULL".into()),
        DataType::Boolean => arr.as_any().downcast_ref::<aa::BooleanArray>().map(|a| a.value(row).to_string()).unwrap_or_else(|| "NULL".into()),
        _ => {
            let s = arr.as_any().downcast_ref::<aa::StringArray>()
                .map(|a| a.value(row).to_string())
                .unwrap_or_else(|| format!("{:?}", arr));
            format!("'{}'", s.replace('\'', "''"))
        }
    }
}

async fn write_pg(pool: &sqlx::PgPool, table: &str, col_list: &str, batch: &RecordBatch) -> Result<u64, ConnectorError> {
    let num_cols = batch.num_columns();
    // Build batch INSERT: INSERT INTO table (cols) VALUES (row1), (row2), ...
    // Use literal values (safe for internal use; table/col names already quoted)
    let mut values_clauses = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let vals: Vec<String> = (0..num_cols).map(|c| cell_to_sql_literal(batch, c, row)).collect();
        values_clauses.push(format!("({})", vals.join(", ")));
    }
    let sql = format!("INSERT INTO {} ({}) VALUES {}", table, col_list, values_clauses.join(", "));
    let result = sqlx::query(&sql).execute(pool).await
        .map_err(|e| ConnectorError::query(e.to_string()))?;
    Ok(result.rows_affected())
}

async fn write_my(pool: &sqlx::MySqlPool, table: &str, col_list: &str, batch: &RecordBatch) -> Result<u64, ConnectorError> {
    let num_cols = batch.num_columns();
    let mut values_clauses = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let vals: Vec<String> = (0..num_cols).map(|c| cell_to_sql_literal(batch, c, row)).collect();
        values_clauses.push(format!("({})", vals.join(", ")));
    }
    let sql = format!("INSERT INTO {} ({}) VALUES {}", table, col_list, values_clauses.join(", "));
    let result = sqlx::query(&sql).execute(pool).await
        .map_err(|e| ConnectorError::query(e.to_string()))?;
    Ok(result.rows_affected())
}

async fn write_sqlite(pool: &sqlx::SqlitePool, table: &str, col_list: &str, batch: &RecordBatch) -> Result<u64, ConnectorError> {
    let num_cols = batch.num_columns();
    let mut total = 0u64;
    // SQLite has a limit on compound INSERT size, so insert row-by-row
    for row in 0..batch.num_rows() {
        let vals: Vec<String> = (0..num_cols).map(|c| cell_to_sql_literal(batch, c, row)).collect();
        let sql = format!("INSERT INTO {} ({}) VALUES ({})", table, col_list, vals.join(", "));
        let result = sqlx::query(&sql).execute(pool).await
            .map_err(|e| ConnectorError::query(e.to_string()))?;
        total += result.rows_affected();
    }
    Ok(total)
}

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

/// Redshift is PostgreSQL-compatible — reuses SqlConnector with redshift connector_type.
/// Supports IAM auth via GetClusterCredentials when `cluster_id` + `db_user` are set.
///
/// Config:
/// ```toml
/// [[connector]]
/// id = "warehouse"
/// connector_type = "redshift"
/// # Option 1: direct URL
/// url = "postgres://user:pass@cluster.region.redshift.amazonaws.com:5439/db"
/// # Option 2: IAM auth
/// cluster_id = "my-cluster"
/// db_name = "mydb"
/// db_user = "admin"
/// region = "us-west-2"
/// ```
#[derive(Debug, Default)]
pub struct RedshiftConnectorFactory;

#[async_trait]
impl ConnectorFactory for RedshiftConnectorFactory {
    fn connector_type(&self) -> &str { "redshift" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        // If cluster_id is set, use IAM auth to get temporary credentials
        if let Some(cluster_id) = config.properties.get("cluster_id").and_then(|v| v.as_str()) {
            let db_name = config.properties.get("db_name").and_then(|v| v.as_str()).unwrap_or("dev");
            let db_user = config.properties.get("db_user").and_then(|v| v.as_str()).unwrap_or("admin");
            let region = config.properties.get("region").and_then(|v| v.as_str()).unwrap_or("us-east-1");

            let url = get_redshift_iam_url(cluster_id, db_name, db_user, region).await?;
            let mut props = config.properties.clone();
            props.insert("url".into(), toml::Value::String(url));
            let iam_config = fuse_core::config::ConnectorConfig {
                id: config.id.clone(),
                connector_type: config.connector_type.clone(),
                properties: props,
            };
            return Ok(Arc::new(SqlConnector::from_config(&iam_config).await?));
        }
        // Fallback: direct URL
        Ok(Arc::new(SqlConnector::from_config(config).await?))
    }
}

/// Fetch temporary credentials via IAM GetClusterCredentials.
async fn get_redshift_iam_url(
    cluster_id: &str,
    db_name: &str,
    db_user: &str,
    region: &str,
) -> Result<String, ConnectorError> {
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()))
        .load()
        .await;
    let client = aws_sdk_redshift::Client::new(&aws_config);

    let resp = client.get_cluster_credentials()
        .cluster_identifier(cluster_id)
        .db_name(db_name)
        .db_user(db_user)
        .send()
        .await
        .map_err(|e| ConnectorError::Auth(format!("Redshift IAM auth failed: {e}")))?;

    let temp_user = resp.db_user().unwrap_or(db_user);
    let temp_pass = resp.db_password().unwrap_or_default();

    // Describe cluster to get endpoint
    let desc = client.describe_clusters()
        .cluster_identifier(cluster_id)
        .send()
        .await
        .map_err(|e| ConnectorError::Connection(format!("Redshift describe failed: {e}")))?;

    let cluster = desc.clusters().first()
        .ok_or_else(|| ConnectorError::Connection(format!("cluster '{}' not found", cluster_id)))?;
    let endpoint = cluster.endpoint()
        .ok_or_else(|| ConnectorError::Connection("cluster has no endpoint".into()))?;
    let host = endpoint.address().unwrap_or("localhost");
    let port = endpoint.port().unwrap_or(5439);

    // URL-encode password
    let encoded_pass: String = temp_pass.bytes().map(|b| {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            format!("{}", b as char)
        } else {
            format!("%{:02X}", b)
        }
    }).collect();

    Ok(format!("postgres://{}:{}@{}:{}/{}", temp_user, encoded_pass, host, port, db_name))
}

#[derive(Debug, Default)]
pub struct SqliteConnectorFactory;

#[async_trait]
impl ConnectorFactory for SqliteConnectorFactory {
    fn connector_type(&self) -> &str { "sqlite" }
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

    #[test]
    fn test_cell_to_sql_literal_int() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![Some(42), None]))]).unwrap();
        assert_eq!(cell_to_sql_literal(&batch, 0, 0), "42");
        assert_eq!(cell_to_sql_literal(&batch, 0, 1), "NULL");
    }

    #[test]
    fn test_cell_to_sql_literal_float() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![3.14]))]).unwrap();
        assert_eq!(cell_to_sql_literal(&batch, 0, 0), "3.14");
    }

    #[test]
    fn test_cell_to_sql_literal_string() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap();
        assert_eq!(cell_to_sql_literal(&batch, 0, 0), "'hello'");
    }

    #[test]
    fn test_cell_to_sql_literal_string_escapes_quotes() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["it's"]))]).unwrap();
        assert_eq!(cell_to_sql_literal(&batch, 0, 0), "'it''s'");
    }

    #[test]
    fn test_cell_to_sql_literal_bool() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Boolean, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(BooleanArray::from(vec![true]))]).unwrap();
        assert_eq!(cell_to_sql_literal(&batch, 0, 0), "true");
    }
}
