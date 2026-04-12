// SPDX-License-Identifier: Apache-2.0

//! Caching wrapper for any [`FederatedConnector`].
//!
//! Checks the shared [`QueryCache`] before delegating to the inner connector,
//! and caches results after successful execution.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;

use fuse_core::connector::*;
use fuse_core::error::ConnectorError;

use crate::cache::{cache_key, QueryCache};

/// Wraps a connector and adds transparent query caching.
pub struct CachingConnectorWrapper {
    inner: Arc<dyn FederatedConnector>,
    cache: Arc<QueryCache>,
    ttl: Duration,
}

impl CachingConnectorWrapper {
    pub fn new(inner: Arc<dyn FederatedConnector>, cache: Arc<QueryCache>, ttl: Duration) -> Self {
        Self { inner, cache, ttl }
    }

    fn make_key(&self, query: &SubQuery) -> u64 {
        // Use a stable string representation of the SubQuery for the cache key
        let query_str = format!(
            "{}:{}:{:?}:{:?}:{:?}:{:?}:{:?}",
            query.table,
            query.projections.join(","),
            query.filter,
            query.aggregations,
            query.group_by,
            query.sort,
            query.limit,
        );
        cache_key(self.inner.id(), &query_str)
    }
}

impl fmt::Debug for CachingConnectorWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachingConnectorWrapper")
            .field("inner", &self.inner)
            .field("ttl", &self.ttl)
            .finish()
    }
}

#[async_trait]
impl FederatedConnector for CachingConnectorWrapper {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn connector_type(&self) -> &str {
        self.inner.connector_type()
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        self.inner.capabilities()
    }

    async fn health_check(&self) -> ConnectorHealth {
        self.inner.health_check().await
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        self.inner.discover_schemas().await
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        self.inner.get_schema(table).await
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let key = self.make_key(query);

        // Check cache
        if let Some(batches) = self.cache.get(key) {
            tracing::debug!(connector = self.inner.id(), "cache hit");
            return Ok(batches);
        }

        // Execute and cache
        let batches = self.inner.execute(query).await?;
        self.cache.put(key, batches.clone(), self.ttl);
        Ok(batches)
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        // Streaming bypasses cache — delegate directly
        self.inner.execute_streaming(query, tx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};

    #[derive(Debug)]
    struct MockInner {
        call_count: AtomicU32,
    }

    impl MockInner {
        fn new() -> Self {
            Self {
                call_count: AtomicU32::new(0),
            }
        }
        fn calls(&self) -> u32 {
            self.call_count.load(Ordering::SeqCst)
        }
        fn batch() -> Vec<RecordBatch> {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            vec![
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap(),
            ]
        }
    }

    #[async_trait]
    impl FederatedConnector for MockInner {
        fn id(&self) -> &str {
            "mock"
        }
        fn connector_type(&self) -> &str {
            "test"
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
        async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
            Ok(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
        }
        async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(Self::batch())
        }
        async fn execute_streaming(
            &self,
            _: &SubQuery,
            tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
        ) -> Result<(), ConnectorError> {
            for b in Self::batch() {
                tx.send(Ok(b))
                    .await
                    .map_err(|_| ConnectorError::ChannelClosed)?;
            }
            Ok(())
        }
    }

    fn query() -> SubQuery {
        SubQuery {
            table: "t".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None,
            offset: None,
            passthrough: None,
        }
    }

    #[tokio::test]
    async fn test_cache_miss_then_hit() {
        let inner = Arc::new(MockInner::new());
        let cache = Arc::new(QueryCache::new());
        let w = CachingConnectorWrapper::new(inner.clone(), cache.clone(), Duration::from_secs(60));

        // Miss — delegates
        let r = w.execute(&query()).await.unwrap();
        assert_eq!(r[0].num_rows(), 2);
        assert_eq!(inner.calls(), 1);

        // Hit — cached
        let r = w.execute(&query()).await.unwrap();
        assert_eq!(r[0].num_rows(), 2);
        assert_eq!(inner.calls(), 1); // still 1

        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().misses, 1);
    }

    #[tokio::test]
    async fn test_expired_entry_refetches() {
        let inner = Arc::new(MockInner::new());
        let cache = Arc::new(QueryCache::new());
        let w = CachingConnectorWrapper::new(inner.clone(), cache, Duration::from_millis(0));

        w.execute(&query()).await.unwrap();
        assert_eq!(inner.calls(), 1);

        tokio::time::sleep(Duration::from_millis(1)).await;

        w.execute(&query()).await.unwrap();
        assert_eq!(inner.calls(), 2);
    }

    #[tokio::test]
    async fn test_streaming_bypasses_cache() {
        let inner = Arc::new(MockInner::new());
        let cache = Arc::new(QueryCache::new());
        let w = CachingConnectorWrapper::new(inner, cache.clone(), Duration::from_secs(60));

        let (tx, mut rx) = mpsc::channel(10);
        w.execute_streaming(&query(), tx).await.unwrap();
        let mut rows = 0;
        while let Some(Ok(b)) = rx.recv().await {
            rows += b.num_rows();
        }
        assert_eq!(rows, 2);
        assert_eq!(cache.stats().entries, 0);
    }

    #[tokio::test]
    async fn test_delegates_metadata() {
        let inner = Arc::new(MockInner::new());
        let cache = Arc::new(QueryCache::new());
        let w = CachingConnectorWrapper::new(inner, cache, Duration::from_secs(60));

        assert_eq!(w.id(), "mock");
        assert_eq!(w.connector_type(), "test");
        assert!(w.capabilities().supports_filtering);
        assert_eq!(w.health_check().await.status, HealthStatus::Healthy);
        assert!(w.discover_schemas().await.unwrap().is_empty());
        assert_eq!(w.get_schema("t").await.unwrap().fields().len(), 1);
    }
}
