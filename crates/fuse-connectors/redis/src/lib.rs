// SPDX-License-Identifier: Apache-2.0

//! Redis connector for Fuse.
//!
//! Scans Redis keys matching a pattern, reads hash fields or string values,
//! infers schema from a sample, and returns Arrow RecordBatches.
//!
//! Config:
//! ```toml
//! [[connector]]
//! id = "cache"
//! connector_type = "redis"
//! url = "redis://localhost:6379"
//! key_pattern = "user:*"
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bb8_redis::RedisConnectionManager;
use redis::AsyncCommands;
use tokio::sync::mpsc;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

type RedisPool = bb8::Pool<RedisConnectionManager>;

pub struct RedisConnector {
    id: String,
    pool: RedisPool,
    key_pattern: String,
}

impl std::fmt::Debug for RedisConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisConnector")
            .field("id", &self.id)
            .field("key_pattern", &self.key_pattern)
            .finish()
    }
}

impl RedisConnector {
    pub async fn new(
        id: String,
        url: &str,
        key_pattern: String,
        max_connections: u32,
    ) -> Result<Self, ConnectorError> {
        let manager = RedisConnectionManager::new(url)
            .map_err(|e| ConnectorError::Connection(format!("Redis connect: {e}")))?;
        let pool = bb8::Pool::builder()
            .max_size(max_connections)
            .build(manager)
            .await
            .map_err(|e| ConnectorError::Connection(format!("Redis pool: {e}")))?;
        Ok(Self {
            id,
            pool,
            key_pattern,
        })
    }

    async fn get_connection(
        &self,
    ) -> Result<bb8::PooledConnection<'_, RedisConnectionManager>, ConnectorError> {
        self.pool
            .get()
            .await
            .map_err(|e| ConnectorError::Connection(format!("Redis pool get: {e}")))
    }

    /// Scan keys matching the pattern.
    async fn scan_keys(&self, limit: Option<u64>) -> Result<Vec<String>, ConnectorError> {
        let mut conn = self.get_connection().await?;
        let inner: &mut redis::aio::MultiplexedConnection = &mut conn;
        let mut keys = Vec::new();
        let max = limit.unwrap_or(10_000) as usize;

        let mut iter: redis::AsyncIter<String> = inner
            .scan_match(&self.key_pattern)
            .await
            .map_err(|e| ConnectorError::QueryFailed(format!("SCAN: {e}")))?;

        while let Some(key) = iter.next_item().await {
            keys.push(key);
            if keys.len() >= max {
                break;
            }
        }
        Ok(keys)
    }

    async fn read_hash(
        conn: &mut redis::aio::MultiplexedConnection,
        key: &str,
    ) -> Result<BTreeMap<String, String>, ConnectorError> {
        let fields: BTreeMap<String, String> = conn
            .hgetall(key)
            .await
            .map_err(|e| ConnectorError::QueryFailed(format!("HGETALL {key}: {e}")))?;
        Ok(fields)
    }

    async fn read_string(
        conn: &mut redis::aio::MultiplexedConnection,
        key: &str,
    ) -> Result<String, ConnectorError> {
        let val: String = conn
            .get(key)
            .await
            .map_err(|e| ConnectorError::QueryFailed(format!("GET {key}: {e}")))?;
        Ok(val)
    }

    /// Infer schema from a sample of keys.
    async fn infer_schema_from_sample(&self) -> Result<(Schema, String), ConnectorError> {
        let mut conn = self.get_connection().await?;
        let inner = &mut *conn;
        let keys = self.scan_keys(Some(10)).await?;
        let key = keys
            .first()
            .ok_or_else(|| ConnectorError::SchemaDiscovery("no keys match pattern".into()))?;

        let key_type: String = redis::cmd("TYPE")
            .arg(key)
            .query_async(inner)
            .await
            .map_err(|e| ConnectorError::SchemaDiscovery(format!("TYPE: {e}")))?;

        match key_type.as_str() {
            "hash" => {
                // Collect all field names from sample keys
                let mut field_names = BTreeSet::new();
                for k in keys.iter().take(10) {
                    if let Ok(fields) = Self::read_hash(inner, k).await {
                        field_names.extend(fields.keys().cloned());
                    }
                }
                let mut fields = vec![Field::new("_key", DataType::Utf8, false)];
                for name in &field_names {
                    fields.push(Field::new(name, DataType::Utf8, true));
                }
                Ok((Schema::new(fields), "hash".into()))
            }
            "string" => Ok((
                Schema::new(vec![
                    Field::new("_key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, false),
                ]),
                "string".into(),
            )),
            other => Err(ConnectorError::SchemaDiscovery(format!(
                "unsupported key type: {other}"
            ))),
        }
    }

    /// Build RecordBatches from hash keys.
    fn hashes_to_batch(
        keys: &[String],
        rows: &[BTreeMap<String, String>],
        schema: &Arc<Schema>,
    ) -> Result<RecordBatch, ConnectorError> {
        let num_cols = schema.fields().len();
        let mut columns: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(rows.len()); num_cols];

        for (i, row) in rows.iter().enumerate() {
            // First column is _key
            columns[0].push(Some(keys[i].clone()));
            #[allow(clippy::needless_range_loop)]
            for col_idx in 1..num_cols {
                let field_name = schema.field(col_idx).name();
                columns[col_idx].push(row.get(field_name).cloned());
            }
        }

        let arrays: Vec<Arc<dyn arrow::array::Array>> = columns
            .into_iter()
            .map(|col| Arc::new(StringArray::from(col)) as Arc<dyn arrow::array::Array>)
            .collect();

        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| ConnectorError::QueryFailed(format!("build batch: {e}")))
    }

    /// Build RecordBatch from string keys.
    fn strings_to_batch(
        keys: &[String],
        values: &[String],
        schema: &Arc<Schema>,
    ) -> Result<RecordBatch, ConnectorError> {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(StringArray::from(values.to_vec())),
            ],
        )
        .map_err(|e| ConnectorError::QueryFailed(format!("build batch: {e}")))
    }
}

#[async_trait]
impl FederatedConnector for RedisConnector {
    fn id(&self) -> &str {
        &self.id
    }
    fn connector_type(&self) -> &str {
        "redis"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: false,
            supports_projection: false,
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 10,
            supports_streaming: false,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        match self.get_connection().await {
            Ok(mut conn) => {
                let pong: Result<String, _> = redis::cmd("PING").query_async(&mut *conn).await;
                match pong {
                    Ok(_) => ConnectorHealth {
                        status: HealthStatus::Healthy,
                        latency_ms: Some(start.elapsed().as_millis() as u64),
                        message: None,
                    },
                    Err(e) => ConnectorHealth {
                        status: HealthStatus::Unhealthy,
                        latency_ms: Some(start.elapsed().as_millis() as u64),
                        message: Some(format!("{e}")),
                    },
                }
            }
            Err(e) => ConnectorHealth {
                status: HealthStatus::Unhealthy,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                message: Some(format!("{e}")),
            },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let table_name = self
            .key_pattern
            .replace('*', "")
            .replace(':', "_")
            .trim_matches('_')
            .to_string();
        let name = if table_name.is_empty() {
            "keys".to_string()
        } else {
            table_name
        };
        Ok(vec![SchemaInfo {
            name,
            schema_type: SchemaType::Table,
            estimated_row_count: None,
        }])
    }

    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        let (schema, _) = self.infer_schema_from_sample().await?;
        Ok(schema)
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let mut conn = self.get_connection().await?;
        let (schema, key_type) = self.infer_schema_from_sample().await?;
        let schema = Arc::new(schema);
        let keys = self.scan_keys(query.limit).await?;

        if keys.is_empty() {
            return Ok(vec![]);
        }

        match key_type.as_str() {
            "hash" => {
                let mut rows = Vec::with_capacity(keys.len());
                for key in &keys {
                    rows.push(Self::read_hash(&mut conn, key).await.unwrap_or_default());
                }
                Ok(vec![Self::hashes_to_batch(&keys, &rows, &schema)?])
            }
            "string" => {
                let mut values = Vec::with_capacity(keys.len());
                for key in &keys {
                    values.push(Self::read_string(&mut conn, key).await.unwrap_or_default());
                }
                Ok(vec![Self::strings_to_batch(&keys, &values, &schema)?])
            }
            _ => Err(ConnectorError::QueryFailed("unsupported key type".into())),
        }
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let batches = self.execute(query).await?;
        for batch in batches {
            let _ = tx.send(Ok(batch)).await;
        }
        Ok(())
    }
}

// ── Factory ──

pub struct RedisConnectorFactory;

#[async_trait]
impl ConnectorFactory for RedisConnectorFactory {
    fn connector_type(&self) -> &str {
        "redis"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let url = config
            .properties
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or("redis://127.0.0.1:6379");
        let key_pattern = config
            .properties
            .get("key_pattern")
            .and_then(|v| v.as_str())
            .unwrap_or("*")
            .to_string();
        Ok(Arc::new(
            RedisConnector::new(
                config.id.clone(),
                url,
                key_pattern,
                config.max_connections(8),
            )
            .await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_connector() -> RedisConnector {
        RedisConnector::new("r1".into(), "redis://localhost:6379", "user:*".into(), 2)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_metadata() {
        let c = make_connector().await;
        assert_eq!(c.id(), "r1");
        assert_eq!(c.connector_type(), "redis");
    }

    #[tokio::test]
    async fn test_capabilities() {
        let caps = make_connector().await.capabilities();
        assert!(caps.supports_limit);
        assert!(!caps.supports_filtering);
        assert!(!caps.supports_aggregation);
        assert!(matches!(caps.latency_class, LatencyClass::Low));
    }

    #[test]
    fn test_factory_type() {
        assert_eq!(RedisConnectorFactory.connector_type(), "redis");
    }

    #[test]
    fn test_hashes_to_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_key", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Utf8, true),
        ]));
        let keys = vec!["user:1".to_string(), "user:2".to_string()];
        let rows = vec![
            BTreeMap::from([("name".into(), "Alice".into()), ("age".into(), "30".into())]),
            BTreeMap::from([("name".into(), "Bob".into())]), // missing age
        ];
        let batch = RedisConnector::hashes_to_batch(&keys, &rows, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_strings_to_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let keys = vec!["k1".into(), "k2".into()];
        let values = vec!["v1".into(), "v2".into()];
        let batch = RedisConnector::strings_to_batch(&keys, &values, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_discover_schemas_pattern() {
        let c = make_connector().await;
        let schemas = c.discover_schemas().await.unwrap();
        assert_eq!(schemas[0].name, "user");
    }

    #[tokio::test]
    async fn test_discover_schemas_wildcard_only() {
        let c = RedisConnector::new("r".into(), "redis://localhost:6379", "*".into(), 2)
            .await
            .unwrap();
        let schemas = c.discover_schemas().await.unwrap();
        assert_eq!(schemas[0].name, "keys");
    }

    #[tokio::test]
    async fn test_new_invalid_url() {
        let result = RedisConnector::new("r".into(), "not-a-url", "*".into(), 2).await;
        assert!(result.is_err());
    }

    // ── #303 Redis verification (tester) ──

    #[test]
    fn test_hashes_to_batch_missing_fields_are_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_key", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ]));
        let keys = vec!["user:1".to_string()];
        let rows = vec![BTreeMap::from([("name".into(), "Alice".into())])]; // no email
        let batch = RedisConnector::hashes_to_batch(&keys, &rows, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        // email column should exist but have null
        assert!(batch.column(2).is_null(0));
    }

    #[test]
    fn test_hashes_to_batch_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_key", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        let batch = RedisConnector::hashes_to_batch(&[], &[], &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_strings_to_batch_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RedisConnector::strings_to_batch(&[], &[], &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_new_valid_url_formats() {
        assert!(
            RedisConnector::new("r".into(), "redis://localhost", "*".into(), 2)
                .await
                .is_ok()
        );
        assert!(
            RedisConnector::new("r".into(), "redis://localhost:6379", "*".into(), 2)
                .await
                .is_ok()
        );
    }
}
