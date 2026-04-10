// SPDX-License-Identifier: Apache-2.0
//! Example Fuse connector — minimal working template.
//!
//! This connector serves static in-memory data. Use it as a starting point
//! for building a real connector against any datasource.
//!
//! To build a real connector:
//! 1. Replace `ExampleConnector` with your datasource client
//! 2. Implement `execute()` to translate `SubQuery` into your datasource's query language
//! 3. Implement `discover_schemas()` / `get_schema()` to reflect your datasource's schema
//! 4. Set `ConnectorCapabilities` to match what you actually push down
//! 5. Register a `ConnectorFactory` in `fuse-server/src/main.rs`
//!
//! See `docs/guides/writing-a-connector.md` for the full walkthrough.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;

use fuse_core::connector::{
    ConnectorCapabilities, ConnectorHealth, FederatedConnector, HealthStatus, LatencyClass,
    SchemaInfo, SchemaType, SubQuery,
};
use fuse_core::config::ConnectorConfig;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

/// A minimal connector that returns static in-memory data.
///
/// Replace the static data with real datasource calls.
#[derive(Debug)]
pub struct ExampleConnector {
    id: String,
    // Your datasource client goes here, e.g.:
    //   client: reqwest::Client,
    //   base_url: String,
}

impl ExampleConnector {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

#[async_trait]
impl FederatedConnector for ExampleConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn connector_type(&self) -> &str {
        "example"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: false,   // set true when execute() respects SubQuery.filter
            supports_projection: false,  // set true when execute() respects SubQuery.projections
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,        // we respect SubQuery.limit below
            supports_join: false,
            max_concurrent_queries: 4,
            supports_streaming: true,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        // Replace with a real ping to your datasource
        ConnectorHealth {
            status: HealthStatus::Healthy,
            latency_ms: Some(1),
            message: None,
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        // Replace with real schema discovery (e.g., list tables from your datasource)
        Ok(vec![SchemaInfo {
            name: "events".to_string(),
            schema_type: SchemaType::Table,
            estimated_row_count: Some(1000),
        }])
    }

    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        // Replace with real schema fetching
        Ok(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        // Replace with a real query against your datasource.
        //
        // The SubQuery contains:
        //   query.table       — table name (e.g., "events")
        //   query.projections — columns to return (empty = all)
        //   query.filter      — WHERE conditions to push down
        //   query.limit       — LIMIT to push down
        //   query.sort        — ORDER BY to push down
        //   query.aggregations / query.group_by — GROUP BY to push down
        //
        // Only push down what your capabilities() declares as supported.

        let schema = Arc::new(self.get_schema(&query.table).await?);

        // Static data — replace with real datasource results
        let mut ids = vec![1_i64, 2, 3, 4, 5];
        let mut names = vec!["alpha", "beta", "gamma", "delta", "epsilon"];
        let mut values = vec![10_i64, 20, 30, 40, 50];

        // Respect LIMIT (we declared supports_limit: true)
        if let Some(n) = query.limit {
            let n = n as usize;
            ids.truncate(n);
            names.truncate(n);
            values.truncate(n);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .map_err(ConnectorError::query)?;

        Ok(vec![batch])
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        // For streaming, send batches one at a time via tx.
        // For large datasets, fetch in pages and send each page as a batch.
        for batch in self.execute(query).await? {
            tx.send(Ok(batch))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

/// Factory for creating ExampleConnector instances from config.
///
/// Register this in fuse-server/src/main.rs:
/// ```ignore
/// factory_registry.register("example", Arc::new(ExampleConnectorFactory));
/// ```
pub struct ExampleConnectorFactory;

#[async_trait::async_trait]
impl ConnectorFactory for ExampleConnectorFactory {
    fn connector_type(&self) -> &str {
        "example"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(ExampleConnector::new(&config.id)))
    }
}

/// Convenience: get the Arrow schema as a `SchemaRef`.
pub fn example_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connector() -> ExampleConnector {
        ExampleConnector::new("test")
    }

    #[tokio::test]
    async fn test_health_check() {
        let c = connector();
        let h = c.health_check().await;
        assert_eq!(h.status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_discover_schemas() {
        let schemas = connector().discover_schemas().await.unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "events");
    }

    #[tokio::test]
    async fn test_execute_returns_all_rows() {
        let sq = SubQuery {
            table: "events".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None, passthrough: None, offset: None,
        };
        let batches = connector().execute(&sq).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
    }

    #[tokio::test]
    async fn test_execute_respects_limit() {
        let sq = SubQuery {
            table: "events".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: Some(2),
            having: None, passthrough: None, offset: None,
        };
        let batches = connector().execute(&sq).await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_execute_streaming() {
        let (tx, mut rx) = mpsc::channel(10);
        let sq = SubQuery {
            table: "events".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None, passthrough: None, offset: None,
        };
        connector().execute_streaming(&sq, tx).await.unwrap();
        let batch = rx.recv().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 5);
    }

    #[tokio::test]
    async fn test_schema_has_three_fields() {
        let schema = connector().get_schema("events").await.unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "value");
    }

    #[test]
    fn test_capabilities_supports_limit() {
        let caps = connector().capabilities();
        assert!(caps.supports_limit);
        assert!(!caps.supports_filtering); // not yet implemented
    }
}
