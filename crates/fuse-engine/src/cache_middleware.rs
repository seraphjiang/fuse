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
    pub fn new(
        inner: Arc<dyn FederatedConnector>,
        cache: Arc<QueryCache>,
        ttl: Duration,
    ) -> Self {
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
