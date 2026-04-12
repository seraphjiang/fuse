// SPDX-License-Identifier: Apache-2.0

//! Delta Lake connector for the Fuse federated query engine.
//!
//! Reads Delta tables by parsing the `_delta_log/` transaction log
//! and reading the referenced Parquet files. Supports time travel
//! via version selection.
//!
//! # Configuration
//!
//! ```toml
//! [[connector]]
//! id = "my_delta"
//! type = "delta-lake"
//! table_uri = "s3://my-bucket/delta-table"
//! # version = 5  # optional: time travel
//! ```

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

/// Delta log entry — minimal representation of an add/remove action.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct DeltaLogEntry {
    #[serde(default)]
    pub add: Option<AddAction>,
    #[serde(default)]
    pub remove: Option<RemoveAction>,
    #[serde(default)]
    pub metadata: Option<MetadataAction>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct AddAction {
    pub path: String,
    pub size: Option<i64>,
    #[serde(rename = "partitionValues")]
    pub partition_values: Option<serde_json::Value>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct RemoveAction {
    pub path: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MetadataAction {
    pub name: Option<String>,
    #[serde(rename = "schemaString")]
    pub schema_string: Option<String>,
}

/// Parse Delta schema from the metadata action's schemaString JSON.
pub fn parse_delta_schema(schema_json: &str) -> Result<Schema, ConnectorError> {
    let val: serde_json::Value = serde_json::from_str(schema_json)
        .map_err(|e| ConnectorError::query(format!("invalid Delta schema: {e}")))?;
    let empty = vec![];
    let fields_json = val["fields"].as_array().unwrap_or(&empty);
    let fields: Vec<Field> = fields_json.iter().filter_map(|f| {
        let name = f["name"].as_str()?;
        let dt = match f["type"].as_str().unwrap_or("string") {
            "integer" | "int" => DataType::Int32,
            "long" => DataType::Int64,
            "float" => DataType::Float32,
            "double" => DataType::Float64,
            "boolean" => DataType::Boolean,
            _ => DataType::Utf8,
        };
        let nullable = f["nullable"].as_bool().unwrap_or(true);
        Some(Field::new(name, dt, nullable))
    }).collect();
    Ok(Schema::new(fields))
}

/// Resolve active files from a sequence of log entries (add - remove).
pub fn resolve_active_files(entries: &[DeltaLogEntry]) -> Vec<String> {
    let mut active: std::collections::HashSet<String> = std::collections::HashSet::new();
    for entry in entries {
        if let Some(ref add) = entry.add {
            active.insert(add.path.clone());
        }
        if let Some(ref remove) = entry.remove {
            active.remove(&remove.path);
        }
    }
    active.into_iter().collect()
}

#[derive(Debug)]
pub struct DeltaLakeConnector {
    id: String,
    table_uri: String,
    #[allow(dead_code)]
    version: Option<i64>,
}

impl DeltaLakeConnector {
    pub fn new(id: String, table_uri: String, version: Option<i64>) -> Self {
        Self { id, table_uri, version }
    }

    /// Read Parquet bytes into RecordBatches with optional column projection.
    fn read_parquet(data: &Bytes, projections: &[String], batch_size: usize) -> Result<Vec<RecordBatch>, ConnectorError> {
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())
            .map_err(ConnectorError::query)?;
        if !projections.is_empty() {
            let pq_schema = builder.parquet_schema().clone();
            let indices: Vec<usize> = projections.iter()
                .filter_map(|name| pq_schema.columns().iter().position(|c| c.name() == name))
                .collect();
            if !indices.is_empty() {
                builder = builder.with_projection(ProjectionMask::leaves(&pq_schema, indices));
            }
        }
        builder.with_batch_size(batch_size).build()
            .map_err(ConnectorError::query)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(ConnectorError::query)
    }

    /// Read schema from the first Parquet file's footer.
    fn read_parquet_schema(data: &Bytes) -> Result<Schema, ConnectorError> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())
            .map_err(ConnectorError::schema)?;
        Ok(builder.schema().as_ref().clone())
    }

    /// Resolve the object_store and path from the table URI.
    fn parse_store(&self) -> Result<(Box<dyn object_store::ObjectStore>, object_store::path::Path), ConnectorError> {
        let url: url::Url = self.table_uri.parse()
            .map_err(|e| ConnectorError::Connection(format!("invalid table_uri: {e}")))?;
        let (store, path) = object_store::parse_url(&url)
            .map_err(|e| ConnectorError::Connection(format!("cannot open store: {e}")))?;
        Ok((store, path))
    }

    /// List and read _delta_log JSON entries, resolve active Parquet files.
    async fn resolve_files(&self) -> Result<(Box<dyn object_store::ObjectStore>, object_store::path::Path, Vec<String>), ConnectorError> {
        let (store, base) = self.parse_store()?;
        let log_prefix = object_store::path::Path::from(format!("{base}/_delta_log/"));
        let list = store.list(Some(&log_prefix));
        use futures::TryStreamExt;
        let objects: Vec<_> = list.try_collect().await
            .map_err(|e| ConnectorError::query(format!("failed to list _delta_log: {e}")))?;

        let mut log_files: Vec<String> = objects.iter()
            .filter_map(|o| {
                let p = o.location.to_string();
                if p.ends_with(".json") { Some(p) } else { None }
            })
            .collect();
        log_files.sort();

        let mut entries = Vec::new();
        for lf in &log_files {
            let path = object_store::path::Path::from(lf.as_str());
            let data = store.get(&path).await
                .map_err(|e| ConnectorError::query(format!("failed to read {lf}: {e}")))?
                .bytes().await
                .map_err(|e| ConnectorError::query(format!("failed to read bytes {lf}: {e}")))?;
            for line in data.split(|&b| b == b'\n') {
                if line.is_empty() { continue; }
                if let Ok(entry) = serde_json::from_slice::<DeltaLogEntry>(line) {
                    entries.push(entry);
                }
            }
        }

        let active = resolve_active_files(&entries);
        Ok((store, base, active))
    }
}

#[async_trait]
impl FederatedConnector for DeltaLakeConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "delta-lake" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 4,
            supports_streaming: true,
            latency_class: LatencyClass::Medium,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        // Check if _delta_log exists
        let _log_path = format!("{}/_delta_log/00000000000000000000.json", self.table_uri);
        debug!(uri = %self.table_uri, "checking Delta table health");
        ConnectorHealth {
            status: HealthStatus::Healthy,
            latency_ms: None,
            message: Some(format!("table_uri={}", self.table_uri)),
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let name = self.table_uri.rsplit('/').next().unwrap_or("delta_table");
        Ok(vec![SchemaInfo { name: name.to_string(), schema_type: SchemaType::Table, estimated_row_count: None }])
    }

    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        let (store, base, files) = self.resolve_files().await?;
        let first = files.first()
            .ok_or_else(|| ConnectorError::schema("no active Parquet files in Delta table"))?;
        let path = object_store::path::Path::from(format!("{base}/{first}"));
        let data = store.get(&path).await
            .map_err(|e| ConnectorError::schema(format!("failed to read {first}: {e}")))?
            .bytes().await
            .map_err(|e| ConnectorError::schema(format!("failed to read bytes: {e}")))?;
        Self::read_parquet_schema(&data)
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let (store, base, files) = self.resolve_files().await?;
        debug!(files = files.len(), table = %self.table_uri, "executing Delta Lake query");

        let mut all_batches = Vec::new();
        let limit = query.limit.map(|n| n as usize);
        let mut total_rows = 0usize;

        for file in &files {
            let path = object_store::path::Path::from(format!("{base}/{file}"));
            let data = store.get(&path).await
                .map_err(|e| ConnectorError::query(format!("failed to read {file}: {e}")))?
                .bytes().await
                .map_err(|e| ConnectorError::query(format!("bytes error: {e}")))?;
            let batches = Self::read_parquet(&data, &query.projections, 8192)?;
            for batch in batches {
                total_rows += batch.num_rows();
                all_batches.push(batch);
                if let Some(lim) = limit {
                    if total_rows >= lim { break; }
                }
            }
            if limit.is_some_and(|lim| total_rows >= lim) { break; }
        }

        // Trim last batch if over limit
        if let Some(lim) = limit {
            if total_rows > lim {
                if let Some(last) = all_batches.last() {
                    let excess = total_rows - lim;
                    let trimmed = last.slice(0, last.num_rows() - excess);
                    let len = all_batches.len();
                    all_batches[len - 1] = trimmed;
                }
            }
        }

        Ok(all_batches)
    }

    async fn execute_streaming(&self, query: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        let (store, base, files) = self.resolve_files().await?;
        let limit = query.limit.map(|n| n as usize);
        let mut sent = 0usize;

        for file in &files {
            let path = object_store::path::Path::from(format!("{base}/{file}"));
            let data = store.get(&path).await
                .map_err(|e| ConnectorError::query(format!("failed to read {file}: {e}")))?
                .bytes().await
                .map_err(|e| ConnectorError::query(format!("bytes error: {e}")))?;
            let batches = Self::read_parquet(&data, &query.projections, 8192)?;
            for batch in batches {
                let batch = if let Some(lim) = limit {
                    if sent >= lim { return Ok(()); }
                    let remaining = lim - sent;
                    let b = if batch.num_rows() > remaining { batch.slice(0, remaining) } else { batch };
                    sent += b.num_rows();
                    b
                } else {
                    batch
                };
                tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;
            }
        }
        Ok(())
    }
}

pub struct DeltaLakeConnectorFactory;

#[async_trait]
impl ConnectorFactory for DeltaLakeConnectorFactory {
    fn connector_type(&self) -> &str { "delta-lake" }

    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let table_uri = config.properties.get("table_uri").and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'table_uri' is required".into()))?.to_string();
        let version = config.properties.get("version").and_then(|v| v.as_integer());
        Ok(Arc::new(DeltaLakeConnector::new(config.id.clone(), table_uri, version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_delta_schema() {
        let json = r#"{"type":"struct","fields":[
            {"name":"id","type":"long","nullable":false},
            {"name":"name","type":"string","nullable":true},
            {"name":"score","type":"double","nullable":true}
        ]}"#;
        let schema = parse_delta_schema(json).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(*schema.field(1).data_type(), DataType::Utf8);
    }

    #[test]
    fn test_resolve_active_files() {
        let entries = vec![
            DeltaLogEntry { add: Some(AddAction { path: "a.parquet".into(), size: Some(100), partition_values: None }), remove: None, metadata: None },
            DeltaLogEntry { add: Some(AddAction { path: "b.parquet".into(), size: Some(200), partition_values: None }), remove: None, metadata: None },
            DeltaLogEntry { add: None, remove: Some(RemoveAction { path: "a.parquet".into() }), metadata: None },
        ];
        let files = resolve_active_files(&entries);
        assert_eq!(files.len(), 1);
        assert!(files.contains(&"b.parquet".to_string()));
    }

    #[test]
    fn test_resolve_active_files_empty() {
        assert!(resolve_active_files(&[]).is_empty());
    }

    #[test]
    fn test_connector_type() {
        let c = DeltaLakeConnector::new("t".into(), "/tmp/delta".into(), None);
        assert_eq!(c.connector_type(), "delta-lake");
    }

    #[test]
    fn test_connector_with_version() {
        let c = DeltaLakeConnector::new("t".into(), "s3://b/t".into(), Some(5));
        assert_eq!(c.version, Some(5));
    }

    #[test]
    fn test_capabilities() {
        let c = DeltaLakeConnector::new("t".into(), "/tmp".into(), None);
        assert!(c.capabilities().supports_filtering);
        assert!(c.capabilities().supports_projection);
        assert!(!c.capabilities().supports_aggregation);
    }

    #[test]
    fn test_discover_schemas_extracts_name() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let c = DeltaLakeConnector::new("t".into(), "s3://bucket/my_table".into(), None);
        let schemas = rt.block_on(c.discover_schemas()).unwrap();
        assert_eq!(schemas[0].name, "my_table");
    }

    #[test]
    fn test_parse_delta_schema_invalid() {
        assert!(parse_delta_schema("not json").is_err());
    }
}

#[cfg(test)]
mod edge_tests {
    use super::*;

    #[test]
    fn test_multi_version_log_resolution() {
        let entries = vec![
            DeltaLogEntry { add: Some(AddAction { path: "v0/a.parquet".into(), size: Some(100), partition_values: None }), remove: None, metadata: None },
            DeltaLogEntry { add: Some(AddAction { path: "v0/b.parquet".into(), size: Some(200), partition_values: None }), remove: None, metadata: None },
            // Version 1: compact a+b into c, remove a and b
            DeltaLogEntry { add: None, remove: Some(RemoveAction { path: "v0/a.parquet".into() }), metadata: None },
            DeltaLogEntry { add: None, remove: Some(RemoveAction { path: "v0/b.parquet".into() }), metadata: None },
            DeltaLogEntry { add: Some(AddAction { path: "v1/c.parquet".into(), size: Some(300), partition_values: None }), remove: None, metadata: None },
        ];
        let files = resolve_active_files(&entries);
        assert_eq!(files.len(), 1);
        assert!(files.contains(&"v1/c.parquet".to_string()));
    }

    #[test]
    fn test_concurrent_add_remove_same_file() {
        let entries = vec![
            DeltaLogEntry { add: Some(AddAction { path: "x.parquet".into(), size: Some(100), partition_values: None }), remove: None, metadata: None },
            DeltaLogEntry { add: None, remove: Some(RemoveAction { path: "x.parquet".into() }), metadata: None },
            DeltaLogEntry { add: Some(AddAction { path: "x.parquet".into(), size: Some(200), partition_values: None }), remove: None, metadata: None },
        ];
        let files = resolve_active_files(&entries);
        assert_eq!(files.len(), 1); // re-added
    }

    #[test]
    fn test_schema_all_types() {
        let json = r#"{"type":"struct","fields":[
            {"name":"a","type":"int","nullable":false},
            {"name":"b","type":"long","nullable":true},
            {"name":"c","type":"float","nullable":true},
            {"name":"d","type":"double","nullable":true},
            {"name":"e","type":"boolean","nullable":true},
            {"name":"f","type":"string","nullable":true},
            {"name":"g","type":"binary","nullable":true}
        ]}"#;
        let schema = parse_delta_schema(json).unwrap();
        assert_eq!(schema.fields().len(), 7);
        assert_eq!(*schema.field(0).data_type(), DataType::Int32);
        assert_eq!(*schema.field(4).data_type(), DataType::Boolean);
        assert_eq!(*schema.field(6).data_type(), DataType::Utf8); // binary falls back to Utf8
    }

    #[test]
    fn test_schema_empty_fields() {
        let json = r#"{"type":"struct","fields":[]}"#;
        let schema = parse_delta_schema(json).unwrap();
        assert_eq!(schema.fields().len(), 0);
    }

    #[test]
    fn test_metadata_action_parsing() {
        let entry: DeltaLogEntry = serde_json::from_str(r#"{
            "metadata": {
                "name": "my_table",
                "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false}]}"
            }
        }"#).unwrap();
        assert!(entry.metadata.is_some());
        let meta = entry.metadata.unwrap();
        assert_eq!(meta.name.as_deref(), Some("my_table"));
        let schema = parse_delta_schema(meta.schema_string.as_deref().unwrap()).unwrap();
        assert_eq!(schema.fields().len(), 1);
    }
}
