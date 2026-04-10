// SPDX-License-Identifier: Apache-2.0

//! Connector conformance test suite.
//!
//! Reusable helpers that verify any FederatedConnector meets the contract.
//! New connectors (DynamoDB, PostgreSQL, Elasticsearch, Redis, CSV/JSON)
//! should use these helpers.

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;

use fuse_core::connector::*;
use fuse_core::error::ConnectorError;

/// Minimal SubQuery for testing.
fn test_query(table: &str) -> SubQuery {
    SubQuery {
        table: table.into(),
        projections: vec![],
        filter: None,
        aggregations: vec![],
        group_by: vec![],
        having: None,
        sort: vec![],
        limit: Some(10),
        passthrough: None,
    }
}

/// Verify a connector passes the basic contract.
async fn assert_connector_contract(connector: &dyn FederatedConnector, table: &str) {
    assert!(!connector.id().is_empty());
    assert!(!connector.connector_type().is_empty());

    let health = connector.health_check().await;
    assert!(matches!(health.status, HealthStatus::Healthy | HealthStatus::Degraded | HealthStatus::Unhealthy));

    let caps = connector.capabilities();
    assert!(caps.max_concurrent_queries > 0);

    let schemas = connector.discover_schemas().await.unwrap();
    assert!(!schemas.is_empty());

    let q = test_query(table);
    let batches = connector.execute(&q).await.unwrap();
    if batches.len() > 1 {
        let schema = batches[0].schema();
        for (i, b) in batches.iter().enumerate().skip(1) {
            assert_eq!(b.schema(), schema, "batch {} schema mismatch", i);
        }
    }
}

async fn assert_limit_respected(connector: &dyn FederatedConnector, table: &str, limit: u64) {
    let mut q = test_query(table);
    q.limit = Some(limit);
    let rows: u64 = connector.execute(&q).await.unwrap()
        .iter().map(|b| b.num_rows() as u64).sum();
    assert!(rows <= limit, "got {} rows, expected <= {}", rows, limit);
}

/// Mock connector for testing the helpers.
#[derive(Debug)]
struct MockConnector;

#[async_trait]
impl FederatedConnector for MockConnector {
    fn id(&self) -> &str { "mock" }
    fn connector_type(&self) -> &str { "mock" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None }
    }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![SchemaInfo {
            name: "test_table".into(),
            schema_type: SchemaType::Table,
            estimated_row_count: Some(3),
        }])
    }
    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]))
    }
    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let n = query.limit.unwrap_or(10).min(3) as usize;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let ids: Vec<&str> = ["1", "2", "3"][..n].to_vec();
        let vals: Vec<&str> = ["a", "b", "c"][..n].to_vec();
        Ok(vec![RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(vals)),
        ]).unwrap()])
    }
    async fn execute_streaming(&self, _q: &SubQuery, _tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[tokio::test]
async fn test_contract_passes_for_mock() {
    assert_connector_contract(&MockConnector, "test_table").await;
}

#[tokio::test]
async fn test_limit_respected_for_mock() {
    assert_limit_respected(&MockConnector, "test_table", 2).await;
}

#[tokio::test]
async fn test_schema_discovery_returns_fields() {
    let schemas = MockConnector.discover_schemas().await.unwrap();
    assert_eq!(schemas[0].name, "test_table");
    assert!(schemas[0].estimated_row_count.is_some());
}

#[tokio::test]
async fn test_get_schema_returns_arrow_schema() {
    let schema = MockConnector.get_schema("test_table").await.unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
}

#[tokio::test]
async fn test_execute_returns_consistent_batches() {
    let q = test_query("test_table");
    let batches = MockConnector.execute(&q).await.unwrap();
    assert!(!batches.is_empty());
    assert!(batches[0].num_rows() > 0);
}
