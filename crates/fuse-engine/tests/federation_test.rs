// SPDX-License-Identifier: Apache-2.0

//! Integration test: mock connector federation.
//!
//! Creates a ConnectorRegistry with a fake OpenSearch connector that returns
//! hardcoded RecordBatches, builds a FuseEngine, and verifies query execution.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;

use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorRegistry;

/// A mock connector that returns hardcoded data for `test_index`.
#[derive(Debug)]
struct MockOpenSearchConnector {
    id: String,
}

impl MockOpenSearchConnector {
    fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
            Field::new("message", DataType::Utf8, true),
        ])
    }

    fn test_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Self::test_schema());
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["abc-001", "abc-002", "abc-003"])),
                Arc::new(Int64Array::from(vec![500, 200, 500])),
                Arc::new(StringArray::from(vec![
                    "internal server error",
                    "ok",
                    "gateway timeout",
                ])),
            ],
        )
        .unwrap();
        vec![batch]
    }
}

#[async_trait]
impl FederatedConnector for MockOpenSearchConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn connector_type(&self) -> &str {
        "opensearch"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::full()
    }

    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth {
            status: HealthStatus::Healthy,
            latency_ms: Some(1),
            message: None,
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![SchemaInfo {
            name: "test_index".to_string(),
            schema_type: SchemaType::Index,
            estimated_row_count: Some(3),
        }])
    }

    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        Ok(Self::test_schema())
    }

    async fn execute(&self, _query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        Ok(Self::test_batches())
    }

    async fn execute_streaming(
        &self,
        _query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for batch in Self::test_batches() {
            tx.send(Ok(batch))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_mock_connector_registry() {
    let registry = ConnectorRegistry::new();
    let connector = Arc::new(MockOpenSearchConnector::new("mock_cluster"));
    registry.register(connector.clone()).unwrap();

    // Verify registration
    assert!(registry.get("mock_cluster").is_some());
    assert!(registry.get("nonexistent").is_none());
    assert_eq!(registry.datasource_names().len(), 1);
}

#[tokio::test]
async fn test_mock_connector_schema_discovery() {
    let connector = MockOpenSearchConnector::new("mock_cluster");

    let schemas = connector.discover_schemas().await.unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, "test_index");

    let table_names = connector.table_names().await.unwrap();
    assert_eq!(table_names, vec!["test_index"]);

    let schema = connector.get_schema("test_index").await.unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "trace_id");
    assert_eq!(schema.field(1).name(), "status");
    assert_eq!(schema.field(2).name(), "message");
}

#[tokio::test]
async fn test_mock_connector_execute() {
    let connector = MockOpenSearchConnector::new("mock_cluster");

    let query = SubQuery {
        table: "test_index".to_string(),
        projections: vec![],
        filter: None,
        aggregations: vec![],
        group_by: vec![],
        sort: vec![],
        limit: Some(100),
        offset: None,
        passthrough: None,
        having: None,
    };

    let batches = connector.execute(&query).await.unwrap();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 3);

    // Verify column names
    let schema = batch.schema();
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(col_names, vec!["trace_id", "status", "message"]);

    // Verify status column values
    let status_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(status_col.value(0), 500);
    assert_eq!(status_col.value(1), 200);
    assert_eq!(status_col.value(2), 500);
}

#[tokio::test]
async fn test_mock_connector_health() {
    let registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(MockOpenSearchConnector::new("mock_cluster")))
        .unwrap();

    let health = registry.health_check_all().await;
    assert_eq!(health.len(), 1);

    let h = health.get("mock_cluster").unwrap();
    assert_eq!(h.status, HealthStatus::Healthy);
    assert!(h.latency_ms.is_some());
}

#[tokio::test]
async fn test_mock_connector_streaming() {
    let connector = MockOpenSearchConnector::new("mock_cluster");
    let query = SubQuery {
        table: "test_index".to_string(),
        projections: vec![],
        filter: None,
        aggregations: vec![],
        group_by: vec![],
        sort: vec![],
        limit: None,
        offset: None,
        passthrough: None,
        having: None,
    };

    let (tx, mut rx) = mpsc::channel(10);
    connector.execute_streaming(&query, tx).await.unwrap();

    let mut total_rows = 0;
    while let Some(result) = rx.recv().await {
        let batch = result.unwrap();
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_result_merger() {
    // Test the merge_batches utility from fuse-engine
    let schema = Arc::new(MockOpenSearchConnector::test_schema());

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Int64Array::from(vec![500])),
            Arc::new(StringArray::from(vec!["error"])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["b", "c"])),
            Arc::new(Int64Array::from(vec![200, 500])),
            Arc::new(StringArray::from(vec!["ok", "timeout"])),
        ],
    )
    .unwrap();

    // Union
    let merged = fuse_engine::union_batches(vec![vec![batch1], vec![batch2]]).unwrap();
    assert_eq!(merged.len(), 2);

    // Merge with limit
    let limited = fuse_engine::merge_batches(merged, Some(2)).unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].num_rows(), 2);
}

// ── Cache middleware tests ──

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fuse_engine::cache::QueryCache;
use fuse_engine::cache_middleware::CachingConnectorWrapper;

/// A mock connector that counts how many times execute() is called.
#[derive(Debug)]
struct CountingConnector {
    id: String,
    call_count: AtomicU32,
}

impl CountingConnector {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            call_count: AtomicU32::new(0),
        }
    }

    fn calls(&self) -> u32 {
        self.call_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl FederatedConnector for CountingConnector {
    fn id(&self) -> &str {
        &self.id
    }
    fn connector_type(&self) -> &str {
        "mock"
    }
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::full()
    }
    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth {
            status: HealthStatus::Healthy,
            latency_ms: Some(1),
            message: None,
        }
    }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![])
    }
    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        Ok(MockOpenSearchConnector::test_schema())
    }
    async fn execute(&self, _query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(MockOpenSearchConnector::test_batches())
    }
    async fn execute_streaming(
        &self,
        _query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for batch in MockOpenSearchConnector::test_batches() {
            tx.send(Ok(batch))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

fn test_subquery() -> SubQuery {
    SubQuery {
        table: "test_index".to_string(),
        projections: vec![],
        filter: None,
        aggregations: vec![],
        group_by: vec![],
        sort: vec![],
        limit: Some(100),
        offset: None,
        passthrough: None,
        having: None,
    }
}

#[tokio::test]
async fn test_caching_wrapper_cache_miss_then_hit() {
    let inner = Arc::new(CountingConnector::new("c1"));
    let cache = Arc::new(QueryCache::new());
    let wrapper =
        CachingConnectorWrapper::new(inner.clone(), cache.clone(), Duration::from_secs(60));

    let query = test_subquery();

    // First call — cache miss, delegates to inner
    let r1 = wrapper.execute(&query).await.unwrap();
    assert_eq!(inner.calls(), 1);
    assert_eq!(r1[0].num_rows(), 3);

    // Second call — cache hit, inner NOT called again
    let r2 = wrapper.execute(&query).await.unwrap();
    assert_eq!(inner.calls(), 1); // still 1
    assert_eq!(r2[0].num_rows(), 3);

    let stats = cache.stats();
    assert_eq!(stats.hits, 1);
    assert_eq!(stats.misses, 1);
}

#[tokio::test]
async fn test_caching_wrapper_expired_entry_refetches() {
    let inner = Arc::new(CountingConnector::new("c1"));
    let cache = Arc::new(QueryCache::new());
    let wrapper =
        CachingConnectorWrapper::new(inner.clone(), cache.clone(), Duration::from_millis(0));

    let query = test_subquery();

    // First call
    wrapper.execute(&query).await.unwrap();
    assert_eq!(inner.calls(), 1);

    // Wait for expiry
    tokio::time::sleep(Duration::from_millis(1)).await;

    // Second call — expired, should re-fetch
    wrapper.execute(&query).await.unwrap();
    assert_eq!(inner.calls(), 2);
}

#[tokio::test]
async fn test_caching_wrapper_delegates_metadata() {
    let inner = Arc::new(CountingConnector::new("c1"));
    let cache = Arc::new(QueryCache::new());
    let wrapper = CachingConnectorWrapper::new(inner.clone(), cache, Duration::from_secs(60));

    assert_eq!(wrapper.id(), "c1");
    assert_eq!(wrapper.connector_type(), "mock");

    let h = wrapper.health_check().await;
    assert_eq!(h.status, HealthStatus::Healthy);
}

#[tokio::test]
async fn test_caching_wrapper_streaming_bypasses_cache() {
    let inner = Arc::new(CountingConnector::new("c1"));
    let cache = Arc::new(QueryCache::new());
    let wrapper =
        CachingConnectorWrapper::new(inner.clone(), cache.clone(), Duration::from_secs(60));

    let query = test_subquery();
    let (tx, mut rx) = mpsc::channel(10);

    wrapper.execute_streaming(&query, tx).await.unwrap();

    let mut rows = 0;
    while let Some(Ok(batch)) = rx.recv().await {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 3);

    // Streaming should not populate cache
    assert_eq!(cache.stats().entries, 0);
}
