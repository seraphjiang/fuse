// SPDX-License-Identifier: Apache-2.0

//! S3/Parquet connector for the Fuse federated query engine.

pub mod reader;
pub mod select;

use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

const DEFAULT_BATCH_SIZE: usize = 8192;

/// S3/Parquet connector — reads Parquet files from S3.
#[derive(Debug)]
pub struct S3ParquetConnector {
    id: String,
    client: S3Client,
    bucket: String,
    prefix: String,
}

impl S3ParquetConnector {
    pub fn new(id: String, client: S3Client, bucket: String, prefix: String) -> Self {
        Self {
            id,
            client,
            bucket,
            prefix,
        }
    }

    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let bucket = config
            .properties
            .get("bucket")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("missing 'bucket' in config".into()))?
            .to_string();

        let prefix = config
            .properties
            .get("prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let region = config
            .properties
            .get("region")
            .and_then(|v| v.as_str())
            .unwrap_or("us-east-1");

        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region.to_string()))
            .load()
            .await;

        let client = S3Client::new(&sdk_config);

        Ok(Self::new(config.id.clone(), client, bucket, prefix))
    }

    /// List Parquet files under the configured prefix.
    async fn list_parquet_keys(&self) -> Result<Vec<String>, ConnectorError> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&self.prefix);

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| ConnectorError::schema(e))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    if key.ends_with(".parquet") || key.ends_with(".parq") {
                        keys.push(key.to_string());
                    }
                }
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(keys)
    }

    /// Download an S3 object as bytes.
    async fn get_object_bytes(&self, key: &str) -> Result<bytes::Bytes, ConnectorError> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| ConnectorError::query(e))?;

        resp.body
            .collect()
            .await
            .map(|agg| agg.into_bytes())
            .map_err(|e| ConnectorError::query(e))
    }

    /// Derive a "table name" from an S3 key by stripping prefix and extension.
    fn key_to_table_name(key: &str, prefix: &str) -> String {
        let stripped = key.strip_prefix(prefix).unwrap_or(key);
        let stripped = stripped.strip_prefix('/').unwrap_or(stripped);
        // Group by first path segment (directory = table)
        stripped
            .split('/')
            .next()
            .unwrap_or(stripped)
            .trim_end_matches(".parquet")
            .trim_end_matches(".parq")
            .to_string()
    }

    /// Find the first Parquet key matching a table name.
    async fn find_key_for_table(&self, table: &str) -> Result<String, ConnectorError> {
        let keys = self.list_parquet_keys().await?;
        keys.into_iter()
            .find(|k| Self::key_to_table_name(k, &self.prefix) == table)
            .ok_or_else(|| ConnectorError::schema(format!("no Parquet file found for table '{table}'")))
    }
}

#[async_trait]
impl FederatedConnector for S3ParquetConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn connector_type(&self) -> &str {
        "s3"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,   // Partial via S3 Select
            supports_projection: true,  // Full — column pruning in Parquet
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,       // Partial via S3 Select
            supports_join: false,
            max_concurrent_queries: 8,
            supports_streaming: true,
            latency_class: LatencyClass::High,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        match self
            .client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
        {
            Ok(_) => ConnectorHealth {
                status: HealthStatus::Healthy,
                latency_ms: Some(start.elapsed().as_millis() as u64),
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
        let keys = self.list_parquet_keys().await?;

        // Group keys by table name, count files per table
        let mut tables: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for key in &keys {
            let name = Self::key_to_table_name(key, &self.prefix);
            *tables.entry(name).or_default() += 1;
        }

        Ok(tables
            .into_iter()
            .map(|(name, _count)| SchemaInfo {
                name,
                schema_type: SchemaType::Bucket,
                estimated_row_count: None, // would need to read footers
            })
            .collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let key = self.find_key_for_table(table).await?;
        debug!(key = key.as_str(), "Reading Parquet schema from S3");
        let data = self.get_object_bytes(&key).await?;
        reader::read_schema(&data)
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let key = self.find_key_for_table(&query.table).await?;
        debug!(key = key.as_str(), table = query.table.as_str(), "Executing S3 Parquet query");

        let data = self.get_object_bytes(&key).await?;
        let mut batches = reader::read_batches(&data, &query.projections, DEFAULT_BATCH_SIZE)?;

        // Client-side filter if we have one (S3 Select not used in download path)
        // For Phase 2, we'll add S3 Select support here.
        // For now, filters and sort are handled by the engine's merge layer.

        // Apply limit client-side
        if let Some(limit) = query.limit {
            let limit = limit as usize;
            let mut total = 0;
            batches = batches
                .into_iter()
                .take_while(|b| {
                    let take = total < limit;
                    total += b.num_rows();
                    take
                })
                .collect();
            // Slice last batch if needed
            if let Some(last) = batches.last() {
                let excess = total.saturating_sub(limit);
                if excess > 0 {
                    let trimmed = last.slice(0, last.num_rows() - excess);
                    let len = batches.len();
                    batches[len - 1] = trimmed;
                }
            }
        }

        Ok(batches)
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let key = self.find_key_for_table(&query.table).await?;
        let data = self.get_object_bytes(&key).await?;
        let batches = reader::read_batches(&data, &query.projections, DEFAULT_BATCH_SIZE)?;

        let limit = query.limit.map(|n| n as usize);
        let mut sent = 0usize;

        for batch in batches {
            if let Some(lim) = limit {
                if sent >= lim {
                    break;
                }
                let remaining = lim - sent;
                let batch = if batch.num_rows() > remaining {
                    batch.slice(0, remaining)
                } else {
                    batch
                };
                sent += batch.num_rows();
                tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;
            } else {
                tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;
            }
        }

        Ok(())
    }
}

// ── Factory ──

pub struct S3ConnectorFactory;

#[async_trait::async_trait]
impl ConnectorFactory for S3ConnectorFactory {
    fn connector_type(&self) -> &str {
        "s3"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let connector = S3ParquetConnector::from_config(config)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        Ok(Arc::new(connector))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capabilities() {
        // Create a dummy connector to test capabilities
        let sdk_config = tokio::runtime::Runtime::new().unwrap().block_on(async {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .load()
                .await
        });
        let client = S3Client::new(&sdk_config);
        let connector = S3ParquetConnector::new(
            "test".into(),
            client,
            "test-bucket".into(),
            "prefix/".into(),
        );

        let caps = connector.capabilities();
        assert!(caps.supports_filtering);
        assert!(caps.supports_projection);
        assert!(!caps.supports_aggregation);
        assert!(!caps.supports_sorting);
        assert!(caps.supports_limit);
        assert!(!caps.supports_join);
        assert!(caps.supports_streaming);
        assert_eq!(caps.max_concurrent_queries, 8);
        assert!(matches!(caps.latency_class, LatencyClass::High));
    }

    #[test]
    fn test_key_to_table_name_simple() {
        assert_eq!(
            S3ParquetConnector::key_to_table_name("prefix/logs/file.parquet", "prefix/"),
            "logs"
        );
    }

    #[test]
    fn test_key_to_table_name_no_prefix() {
        assert_eq!(
            S3ParquetConnector::key_to_table_name("data.parquet", ""),
            "data"
        );
    }

    #[test]
    fn test_key_to_table_name_nested() {
        assert_eq!(
            S3ParquetConnector::key_to_table_name("prefix/metrics/2024/01.parquet", "prefix/"),
            "metrics"
        );
    }

    #[test]
    fn test_key_to_table_name_parq_extension() {
        assert_eq!(
            S3ParquetConnector::key_to_table_name("events.parq", ""),
            "events"
        );
    }

    #[test]
    fn test_connector_type() {
        let sdk_config = tokio::runtime::Runtime::new().unwrap().block_on(async {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .load()
                .await
        });
        let client = S3Client::new(&sdk_config);
        let connector = S3ParquetConnector::new(
            "my-s3".into(),
            client,
            "bucket".into(),
            "".into(),
        );
        assert_eq!(connector.id(), "my-s3");
        assert_eq!(connector.connector_type(), "s3");
    }
}
