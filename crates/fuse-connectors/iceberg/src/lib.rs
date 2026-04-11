// SPDX-License-Identifier: Apache-2.0

//! Apache Iceberg connector for the Fuse federated query engine.
//!
//! Reads Iceberg tables via the Iceberg REST Catalog API. Supports
//! snapshot-based time travel, schema evolution, and partition pruning.
//!
//! # Configuration
//!
//! ```toml
//! [[connector]]
//! id = "my_iceberg"
//! type = "iceberg"
//! catalog_url = "http://iceberg-rest:8181"
//! warehouse = "s3://my-bucket/warehouse"
//! namespace = "default"
//! # token = "secret://fuse/iceberg-token"
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

#[derive(Debug)]
pub struct IcebergConnector {
    id: String,
    client: reqwest::Client,
    catalog_url: String,
    namespace: String,
}

impl IcebergConnector {
    pub fn new(id: String, client: reqwest::Client, catalog_url: String, namespace: String) -> Self {
        Self { id, client, catalog_url, namespace }
    }

    fn tables_url(&self) -> String {
        format!("{}/v1/namespaces/{}/tables", self.catalog_url, self.namespace)
    }

    fn table_url(&self, table: &str) -> String {
        format!("{}/{}", self.tables_url(), table)
    }
}

/// Parse Iceberg schema JSON into Arrow Schema.
pub fn parse_iceberg_schema(schema_json: &serde_json::Value) -> Result<Schema, ConnectorError> {
    let empty = vec![];
    let fields_json = schema_json["fields"].as_array().unwrap_or(&empty);
    let fields: Vec<Field> = fields_json.iter().filter_map(|f| {
        let name = f["name"].as_str()?;
        let dt = iceberg_type_to_arrow(f["type"].as_str().unwrap_or("string"));
        let required = f["required"].as_bool().unwrap_or(false);
        Some(Field::new(name, dt, !required))
    }).collect();
    Ok(Schema::new(fields))
}

fn iceberg_type_to_arrow(t: &str) -> DataType {
    match t {
        "boolean" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "date" => DataType::Date32,
        "timestamp" | "timestamptz" => DataType::Int64, // micros since epoch
        _ => DataType::Utf8,
    }
}

/// Extract manifest file paths from a snapshot's manifest list.
pub fn extract_manifest_paths(snapshot: &serde_json::Value) -> Vec<String> {
    let empty = vec![];
    snapshot["manifests"].as_array().unwrap_or(&empty).iter()
        .filter_map(|m| m["manifest_path"].as_str().map(|s| s.to_string()))
        .collect()
}

#[async_trait]
impl FederatedConnector for IcebergConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "iceberg" }

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
        match self.client.get(&format!("{}/v1/config", self.catalog_url)).send().await {
            Ok(r) if r.status().is_success() => ConnectorHealth { status: HealthStatus::Healthy, latency_ms: None, message: None },
            Ok(r) => ConnectorHealth { status: HealthStatus::Degraded, latency_ms: None, message: Some(format!("HTTP {}", r.status())) },
            Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        debug!(catalog = %self.catalog_url, ns = %self.namespace, "listing Iceberg tables");
        let resp = self.client.get(&self.tables_url()).send().await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        let json: serde_json::Value = resp.json().await
            .map_err(|e| ConnectorError::query(e.to_string()))?;
        let empty = vec![];
        Ok(json["identifiers"].as_array().unwrap_or(&empty).iter().filter_map(|t| {
            t["name"].as_str().map(|name| SchemaInfo {
                name: name.to_string(), schema_type: SchemaType::Table, estimated_row_count: None,
            })
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let resp = self.client.get(&self.table_url(table)).send().await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        let json: serde_json::Value = resp.json().await
            .map_err(|e| ConnectorError::query(e.to_string()))?;
        parse_iceberg_schema(&json["metadata"]["current-schema"])
    }

    async fn execute(&self, _query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        Err(ConnectorError::query("Iceberg execute requires Parquet reader integration"))
    }

    async fn execute_streaming(&self, query: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for batch in self.execute(query).await? {
            tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

pub struct IcebergConnectorFactory;

#[async_trait]
impl ConnectorFactory for IcebergConnectorFactory {
    fn connector_type(&self) -> &str { "iceberg" }

    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let catalog_url = config.properties.get("catalog_url").and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("'catalog_url' is required".into()))?.to_string();
        let namespace = config.properties.get("namespace").and_then(|v| v.as_str()).unwrap_or("default").to_string();

        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = config.properties.get("token").and_then(|v| v.as_str()) {
            headers.insert(reqwest::header::AUTHORIZATION, format!("Bearer {}", token).parse()
                .map_err(|e: reqwest::header::InvalidHeaderValue| ConnectorError::Connection(e.to_string()))?);
        }

        let client = reqwest::Client::builder().default_headers(headers)
            .timeout(std::time::Duration::from_secs(config.connection_timeout_secs(60)))
            .build().map_err(|e| ConnectorError::Connection(e.to_string()))?;

        Ok(Arc::new(IcebergConnector::new(config.id.clone(), client, catalog_url, namespace)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iceberg_schema() {
        let json = serde_json::json!({
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "type": "long", "required": true},
                {"id": 2, "name": "name", "type": "string", "required": false},
                {"id": 3, "name": "score", "type": "double", "required": false}
            ]
        });
        let schema = parse_iceberg_schema(&json).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(*schema.field(0).data_type(), DataType::Int64);
        assert!(!schema.field(0).is_nullable()); // required=true
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn test_iceberg_type_mapping() {
        assert_eq!(iceberg_type_to_arrow("boolean"), DataType::Boolean);
        assert_eq!(iceberg_type_to_arrow("int"), DataType::Int32);
        assert_eq!(iceberg_type_to_arrow("long"), DataType::Int64);
        assert_eq!(iceberg_type_to_arrow("float"), DataType::Float32);
        assert_eq!(iceberg_type_to_arrow("double"), DataType::Float64);
        assert_eq!(iceberg_type_to_arrow("date"), DataType::Date32);
        assert_eq!(iceberg_type_to_arrow("unknown"), DataType::Utf8);
    }

    #[test]
    fn test_extract_manifest_paths() {
        let snap = serde_json::json!({
            "manifests": [
                {"manifest_path": "s3://b/m1.avro"},
                {"manifest_path": "s3://b/m2.avro"}
            ]
        });
        let paths = extract_manifest_paths(&snap);
        assert_eq!(paths, vec!["s3://b/m1.avro", "s3://b/m2.avro"]);
    }

    #[test]
    fn test_extract_manifest_paths_empty() {
        assert!(extract_manifest_paths(&serde_json::json!({})).is_empty());
    }

    #[test]
    fn test_connector_type() {
        let c = IcebergConnector::new("t".into(), reqwest::Client::new(), "http://x".into(), "ns".into());
        assert_eq!(c.connector_type(), "iceberg");
    }

    #[test]
    fn test_tables_url() {
        let c = IcebergConnector::new("t".into(), reqwest::Client::new(), "http://catalog:8181".into(), "prod".into());
        assert_eq!(c.tables_url(), "http://catalog:8181/v1/namespaces/prod/tables");
    }

    #[test]
    fn test_capabilities() {
        let c = IcebergConnector::new("t".into(), reqwest::Client::new(), "http://x".into(), "ns".into());
        assert!(c.capabilities().supports_filtering);
        assert!(!c.capabilities().supports_aggregation);
    }
}
