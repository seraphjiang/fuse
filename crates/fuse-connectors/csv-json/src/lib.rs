// SPDX-License-Identifier: Apache-2.0

//! CSV/JSON file connector for Fuse.
//!
//! Reads CSV and JSON files from S3 with auto-detection by file extension
//! and schema inference from the first batch of rows.
//!
//! Config:
//! ```toml
//! [[connector]]
//! id = "data_files"
//! connector_type = "csv-json"
//! bucket = "my-bucket"
//! prefix = "data/"
//! region = "us-west-2"
//! ```

use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use tokio::sync::mpsc;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FileFormat {
    Csv,
    Json,
}

impl FileFormat {
    pub fn from_key(key: &str) -> Option<Self> {
        let lower = key.to_lowercase();
        if lower.ends_with(".csv") || lower.ends_with(".csv.gz") {
            Some(Self::Csv)
        } else if lower.ends_with(".json") || lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
            Some(Self::Json)
        } else {
            None
        }
    }
}

pub struct CsvJsonConnector {
    id: String,
    client: S3Client,
    bucket: String,
    prefix: String,
}

impl std::fmt::Debug for CsvJsonConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvJsonConnector")
            .field("id", &self.id)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl CsvJsonConnector {
    pub async fn new(id: String, region: &str, bucket: String, prefix: String) -> Self {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load()
            .await;
        Self { id, client: S3Client::new(&config), bucket, prefix }
    }

    /// List files under the prefix.
    async fn list_files(&self) -> Result<Vec<String>, ConnectorError> {
        let resp = self.client.list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .max_keys(1000)
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(format!("S3 list: {e}")))?;

        Ok(resp.contents()
            .iter()
            .filter_map(|obj| obj.key().map(String::from))
            .filter(|k| FileFormat::from_key(k).is_some())
            .collect())
    }

    /// Fetch a file from S3.
    async fn get_file(&self, key: &str) -> Result<Vec<u8>, ConnectorError> {
        let resp = self.client.get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(format!("S3 get {key}: {e}")))?;

        resp.body.collect().await
            .map(|b| b.to_vec())
            .map_err(|e| ConnectorError::QueryFailed(format!("read body: {e}")))
    }

    /// Parse a file into RecordBatches based on format.
    fn parse_file(&self, data: &[u8], format: FileFormat) -> Result<Vec<RecordBatch>, ConnectorError> {
        match format {
            FileFormat::Csv => self.parse_csv(data),
            FileFormat::Json => self.parse_json(data),
        }
    }

    fn parse_csv(&self, data: &[u8]) -> Result<Vec<RecordBatch>, ConnectorError> {
        let format = arrow_csv::reader::Format::default().with_header(true);
        let (schema, _) = format.infer_schema(Cursor::new(data), Some(100))
            .map_err(|e| ConnectorError::QueryFailed(format!("CSV schema infer: {e}")))?;

        let reader = arrow_csv::ReaderBuilder::new(Arc::new(schema))
            .with_header(true)
            .with_batch_size(8192)
            .build(Cursor::new(data))
            .map_err(|e| ConnectorError::QueryFailed(format!("CSV parse: {e}")))?;

        let batches: Result<Vec<_>, _> = reader.collect();
        batches.map_err(|e| ConnectorError::QueryFailed(format!("CSV read: {e}")))
    }

    fn parse_json(&self, data: &[u8]) -> Result<Vec<RecordBatch>, ConnectorError> {
        use std::io::BufReader;
        let (schema, _) = arrow_json::reader::infer_json_schema_from_seekable(
            &mut BufReader::new(Cursor::new(data)), None,
        ).map_err(|e| ConnectorError::QueryFailed(format!("JSON schema infer: {e}")))?;

        let mut reader = arrow_json::ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(8192)
            .build(BufReader::new(Cursor::new(data)))
            .map_err(|e| ConnectorError::QueryFailed(format!("JSON build: {e}")))?;

        let mut batches = Vec::new();
        while let Some(batch) = reader.next() {
            batches.push(batch.map_err(|e| ConnectorError::QueryFailed(format!("JSON read: {e}")))?);
        }
        Ok(batches)
    }

    /// Infer schema from the first file found.
    async fn infer_schema(&self) -> Result<Schema, ConnectorError> {
        let files = self.list_files().await?;
        let key = files.first()
            .ok_or_else(|| ConnectorError::SchemaDiscovery("no files found".into()))?;
        let data = self.get_file(key).await?;
        let format = FileFormat::from_key(key)
            .ok_or_else(|| ConnectorError::SchemaDiscovery("unknown format".into()))?;

        let batches = self.parse_file(&data, format)?;
        batches.first()
            .map(|b| b.schema().as_ref().clone())
            .ok_or_else(|| ConnectorError::SchemaDiscovery("empty file".into()))
    }

    /// Apply limit to batches.
    fn apply_limit(batches: Vec<RecordBatch>, limit: Option<u64>) -> Vec<RecordBatch> {
        let Some(limit) = limit else { return batches };
        let limit = limit as usize;
        let mut result = Vec::new();
        let mut remaining = limit;
        for batch in batches {
            if remaining == 0 { break; }
            if batch.num_rows() <= remaining {
                remaining -= batch.num_rows();
                result.push(batch);
            } else {
                result.push(batch.slice(0, remaining));
                break;
            }
        }
        result
    }

    /// Apply column projection to batches.
    fn apply_projection(batches: Vec<RecordBatch>, projections: &[String]) -> Vec<RecordBatch> {
        if projections.is_empty() { return batches; }
        batches.into_iter().filter_map(|batch| {
            let schema = batch.schema();
            let indices: Vec<usize> = projections.iter()
                .filter_map(|p| schema.index_of(p).ok())
                .collect();
            if indices.is_empty() { return None; }
            batch.project(&indices).ok()
        }).collect()
    }
}

#[async_trait]
impl FederatedConnector for CsvJsonConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "csv-json" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: false, // post-fetch filtering via Arrow compute
            supports_projection: true,
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 10,
            supports_streaming: false,
            latency_class: LatencyClass::Medium,
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
                latency_ms: Some(start.elapsed().as_millis() as u64),
                message: Some(format!("{e}")),
            },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        // Each unique "directory" under prefix is a table
        let files = self.list_files().await?;
        let table_name = self.prefix.trim_end_matches('/').rsplit('/').next()
            .unwrap_or("data").to_string();
        Ok(vec![SchemaInfo {
            name: table_name,
            schema_type: SchemaType::Table,
            estimated_row_count: Some(files.len() as u64),
        }])
    }

    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        self.infer_schema().await
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let files = self.list_files().await?;
        let mut all_batches = Vec::new();

        for key in &files {
            let format = match FileFormat::from_key(key) {
                Some(f) => f,
                None => continue,
            };
            let data = self.get_file(key).await?;
            let batches = self.parse_file(&data, format)?;
            all_batches.extend(batches);
        }

        // Apply projection
        let projected = Self::apply_projection(all_batches, &query.projections);
        // Apply limit
        Ok(Self::apply_limit(projected, query.limit))
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

pub struct CsvJsonConnectorFactory;

#[async_trait]
impl ConnectorFactory for CsvJsonConnectorFactory {
    fn connector_type(&self) -> &str { "csv-json" }

    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let bucket = config.properties.get("bucket")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("csv-json requires 'bucket'".into()))?
            .to_string();
        let prefix = config.properties.get("prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let region = config.properties.get("region")
            .and_then(|v| v.as_str())
            .unwrap_or("us-east-1");
        Ok(Arc::new(CsvJsonConnector::new(config.id.clone(), region, bucket, prefix).await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_detection_csv() {
        assert_eq!(FileFormat::from_key("data/users.csv"), Some(FileFormat::Csv));
        assert_eq!(FileFormat::from_key("logs/2024.CSV"), Some(FileFormat::Csv));
        assert_eq!(FileFormat::from_key("data.csv.gz"), Some(FileFormat::Csv));
    }

    #[test]
    fn test_format_detection_json() {
        assert_eq!(FileFormat::from_key("events.json"), Some(FileFormat::Json));
        assert_eq!(FileFormat::from_key("logs.jsonl"), Some(FileFormat::Json));
        assert_eq!(FileFormat::from_key("data.ndjson"), Some(FileFormat::Json));
    }

    #[test]
    fn test_format_detection_unknown() {
        assert_eq!(FileFormat::from_key("data.parquet"), None);
        assert_eq!(FileFormat::from_key("readme.txt"), None);
    }

    #[test]
    fn test_parse_csv() {
        let connector = tokio::runtime::Runtime::new().unwrap().block_on(
            CsvJsonConnector::new("t".into(), "us-east-1", "b".into(), "p/".into())
        );
        let csv = b"name,age,city\nAlice,30,Seattle\nBob,25,Portland\n";
        let batches = connector.parse_csv(csv).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 3);
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "name");
    }

    #[test]
    fn test_parse_json() {
        let connector = tokio::runtime::Runtime::new().unwrap().block_on(
            CsvJsonConnector::new("t".into(), "us-east-1", "b".into(), "p/".into())
        );
        let json = b"{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n";
        let batches = connector.parse_json(json).unwrap();
        assert!(!batches.is_empty());
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_apply_limit() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let arr = Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let limited = CsvJsonConnector::apply_limit(vec![batch], Some(3));
        let total: usize = limited.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn test_apply_limit_none() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let arr = Arc::new(arrow::array::StringArray::from(vec!["a", "b"]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let result = CsvJsonConnector::apply_limit(vec![batch], None);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[test]
    fn test_apply_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(arrow::array::StringArray::from(vec!["1"])),
            Arc::new(arrow::array::StringArray::from(vec!["2"])),
            Arc::new(arrow::array::StringArray::from(vec!["3"])),
        ]).unwrap();
        let projected = CsvJsonConnector::apply_projection(vec![batch], &["a".into(), "c".into()]);
        assert_eq!(projected[0].num_columns(), 2);
        assert_eq!(projected[0].schema().field(0).name(), "a");
        assert_eq!(projected[0].schema().field(1).name(), "c");
    }

    #[test]
    fn test_apply_projection_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let arr = Arc::new(arrow::array::StringArray::from(vec!["a"]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let result = CsvJsonConnector::apply_projection(vec![batch], &[]);
        assert_eq!(result[0].num_columns(), 1); // empty = all columns
    }

    #[test]
    fn test_capabilities() {
        let c = tokio::runtime::Runtime::new().unwrap().block_on(
            CsvJsonConnector::new("t".into(), "us-east-1", "b".into(), "p/".into())
        );
        let caps = c.capabilities();
        assert!(caps.supports_projection);
        assert!(caps.supports_limit);
        assert!(!caps.supports_filtering);
        assert!(!caps.supports_aggregation);
    }

    #[test]
    fn test_metadata() {
        let c = tokio::runtime::Runtime::new().unwrap().block_on(
            CsvJsonConnector::new("files".into(), "us-east-1", "b".into(), "data/".into())
        );
        assert_eq!(c.id(), "files");
        assert_eq!(c.connector_type(), "csv-json");
    }

    #[test]
    fn test_factory_type() {
        assert_eq!(CsvJsonConnectorFactory.connector_type(), "csv-json");
    }

    #[test]
    fn test_parse_csv_empty() {
        let c = tokio::runtime::Runtime::new().unwrap().block_on(
            CsvJsonConnector::new("t".into(), "us-east-1", "b".into(), "p/".into())
        );
        let csv = b"name,age\n";
        let batches = c.parse_csv(csv).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    // ── #304 CSV/JSON verification (tester) ──

    #[test]
    fn test_format_detection_ndjson() {
        assert_eq!(FileFormat::from_key("logs/data.ndjson"), Some(FileFormat::Json));
    }

    #[test]
    fn test_format_detection_case_insensitive() {
        assert_eq!(FileFormat::from_key("data.CSV"), Some(FileFormat::Csv));
        assert_eq!(FileFormat::from_key("data.JSON"), Some(FileFormat::Json));
    }

    #[test]
    fn test_parse_csv_multiple_rows() {
        let c = tokio::runtime::Runtime::new().unwrap().block_on(
            CsvJsonConnector::new("t".into(), "us-east-1", "b".into(), "p/".into())
        );
        let csv = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF\n";
        let batches = c.parse_csv(csv).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        assert_eq!(batches[0].num_columns(), 3);
    }

    #[test]
    fn test_parse_json_multiple_objects() {
        let c = tokio::runtime::Runtime::new().unwrap().block_on(
            CsvJsonConnector::new("t".into(), "us-east-1", "b".into(), "p/".into())
        );
        let json = b"{\"name\":\"Alice\",\"age\":30}\n{\"name\":\"Bob\",\"age\":25}\n";
        let batches = c.parse_json(json).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_apply_limit_exact() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let arr = Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let result = CsvJsonConnector::apply_limit(vec![batch], Some(2));
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_apply_projection_subset() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(arrow::array::StringArray::from(vec!["1"])),
            Arc::new(arrow::array::StringArray::from(vec!["2"])),
            Arc::new(arrow::array::StringArray::from(vec!["3"])),
        ]).unwrap();
        let result = CsvJsonConnector::apply_projection(vec![batch], &["b".into()]);
        assert_eq!(result[0].num_columns(), 1);
        assert_eq!(result[0].schema().field(0).name(), "b");
    }
}
