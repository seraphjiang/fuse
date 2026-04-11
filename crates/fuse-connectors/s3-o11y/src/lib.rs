// SPDX-License-Identifier: Apache-2.0

//! S3 O11y connector — reads gzipped NDJSON logs from S3.
//!
//! Designed for the S3 O11y log format: gzipped NDJSON files stored under
//! an S3 prefix, with fields like timestamp, level, service, message, trace_id.

pub mod ndjson;

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

/// S3 O11y connector for gzipped NDJSON logs.
#[derive(Debug)]
pub struct S3O11yConnector {
    id: String,
    client: S3Client,
    bucket: String,
    prefix: String,
}

impl S3O11yConnector {
    pub fn new(id: String, client: S3Client, bucket: String, prefix: String) -> Self {
        Self { id, client, bucket, prefix }
    }

    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let bucket = config.properties.get("bucket")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("missing 'bucket'".into()))?
            .to_string();
        let prefix = config.properties.get("prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let region = config.properties.get("region")
            .and_then(|v| v.as_str())
            .unwrap_or("us-west-1");

        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region.to_string()))
            .load()
            .await;
        Ok(Self::new(config.id.clone(), S3Client::new(&sdk_config), bucket, prefix))
    }

    async fn list_keys(&self) -> Result<Vec<String>, ConnectorError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = self.client.list_objects_v2()
                .bucket(&self.bucket).prefix(&self.prefix);
            if let Some(t) = &token { req = req.continuation_token(t); }
            let resp = req.send().await.map_err(ConnectorError::schema)?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    if key.ends_with(".json.gz") || key.ends_with(".ndjson.gz")
                        || key.ends_with(".ndjson") || key.ends_with(".jsonl")
                        || key.ends_with(".jsonl.gz")
                    {
                        keys.push(key.to_string());
                    }
                }
            }
            if resp.is_truncated() == Some(true) {
                token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        Ok(keys)
    }

    fn key_to_table(key: &str, prefix: &str) -> String {
        let stripped = key.strip_prefix(prefix).unwrap_or(key);
        let stripped = stripped.strip_prefix('/').unwrap_or(stripped);
        stripped.split('/').next().unwrap_or(stripped)
            .trim_end_matches(".json.gz")
            .trim_end_matches(".ndjson.gz")
            .trim_end_matches(".ndjson")
            .trim_end_matches(".jsonl.gz")
            .trim_end_matches(".jsonl")
            .to_string()
    }

    async fn keys_for_table(&self, table: &str) -> Result<Vec<String>, ConnectorError> {
        let matching: Vec<String> = self.list_keys().await?.into_iter()
            .filter(|k| Self::key_to_table(k, &self.prefix) == table)
            .collect();
        if matching.is_empty() {
            return Err(ConnectorError::schema(format!("no files for table '{table}'")));
        }
        Ok(matching)
    }

    async fn read_object(&self, key: &str) -> Result<Vec<u8>, ConnectorError> {
        debug!(key, "Downloading S3 object");
        let resp = self.client.get_object()
            .bucket(&self.bucket).key(key)
            .send().await.map_err(ConnectorError::query)?;
        let bytes = resp.body.collect().await
            .map(|agg| agg.into_bytes())
            .map_err(ConnectorError::query)?;
        ndjson::decompress(&bytes)
    }
}

#[async_trait]
impl FederatedConnector for S3O11yConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "s3-o11y" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: false,
            supports_projection: true,
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 4,
            supports_streaming: true,
            latency_class: LatencyClass::High,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        match self.client.head_bucket().bucket(&self.bucket).send().await {
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
        let keys = self.list_keys().await?;
        let mut tables: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
        for key in &keys {
            *tables.entry(Self::key_to_table(key, &self.prefix)).or_default() += 1;
        }
        Ok(tables.into_keys().map(|name| SchemaInfo {
            name, schema_type: SchemaType::Bucket, estimated_row_count: None,
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let keys = self.keys_for_table(table).await?;
        let data = self.read_object(&keys[0]).await?;
        let records = ndjson::parse_lines(&data);
        if records.is_empty() {
            return Err(ConnectorError::schema("no records for schema discovery"));
        }
        Ok(ndjson::discover_schema(&records))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let keys = self.keys_for_table(&query.table).await?;
        let mut all_records = Vec::new();
        for key in &keys {
            let data = self.read_object(key).await?;
            all_records.append(&mut ndjson::parse_lines(&data));
            if let Some(limit) = query.limit {
                if all_records.len() >= limit as usize { break; }
            }
        }
        let records = ndjson::apply_limit(all_records, query.limit);
        if records.is_empty() { return Ok(vec![]); }
        let schema = ndjson::discover_schema(&records);
        Ok(vec![ndjson::records_to_batch(&records, &schema, &query.projections)?])
    }

    async fn execute_streaming(
        &self, query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let keys = self.keys_for_table(&query.table).await?;
        let mut sent = 0usize;
        let limit = query.limit.map(|n| n as usize);
        for key in &keys {
            if limit.is_some_and(|l| sent >= l) { break; }
            let data = self.read_object(key).await?;
            let mut records = ndjson::parse_lines(&data);
            if let Some(l) = limit { records.truncate(l - sent); }
            if records.is_empty() { continue; }
            let schema = ndjson::discover_schema(&records);
            sent += records.len();
            let batch = ndjson::records_to_batch(&records, &schema, &query.projections)?;
            tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

pub struct S3O11yConnectorFactory;

#[async_trait::async_trait]
impl ConnectorFactory for S3O11yConnectorFactory {
    fn connector_type(&self) -> &str { "s3-o11y" }

    async fn create(
        &self, config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(S3O11yConnector::from_config(config).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_to_table_json_gz() {
        assert_eq!(S3O11yConnector::key_to_table("fuse/logs/2025-01.json.gz", "fuse/"), "logs");
    }

    #[test]
    fn test_key_to_table_ndjson() {
        assert_eq!(S3O11yConnector::key_to_table("prefix/events.ndjson", "prefix/"), "events");
    }

    #[test]
    fn test_key_to_table_no_prefix() {
        assert_eq!(S3O11yConnector::key_to_table("data.jsonl.gz", ""), "data");
    }

    #[test]
    fn test_key_to_table_nested() {
        assert_eq!(S3O11yConnector::key_to_table("fuse/metrics/2025/01/data.ndjson.gz", "fuse/"), "metrics");
    }

    #[test]
    fn test_factory_type() {
        assert_eq!(S3O11yConnectorFactory.connector_type(), "s3-o11y");
    }
}
