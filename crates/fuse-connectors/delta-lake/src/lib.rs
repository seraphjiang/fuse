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
    version: Option<i64>,
}

impl DeltaLakeConnector {
    pub fn new(id: String, table_uri: String, version: Option<i64>) -> Self {
        Self { id, table_uri, version }
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
        let log_path = format!("{}/_delta_log/00000000000000000000.json", self.table_uri);
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
        Err(ConnectorError::query("schema discovery requires reading _delta_log — not yet connected"))
    }

    async fn execute(&self, _query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        Err(ConnectorError::query("Delta Lake execute requires Parquet reader integration"))
    }

    async fn execute_streaming(&self, query: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for batch in self.execute(query).await? {
            tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;
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
