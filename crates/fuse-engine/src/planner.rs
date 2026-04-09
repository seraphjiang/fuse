// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
use datafusion_federation::sql::{
    RemoteTable, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::FederatedTableProviderAdaptor;
use futures::StreamExt;
use tracing::info;

use fuse_core::connector::FederatedConnector;
use fuse_core::ConnectorRegistry;

use crate::sql_to_subquery;

/// The main Fuse engine. Holds a DataFusion `SessionContext` configured with
/// federation optimizer rules and a registry of connectors.
pub struct FuseEngine {
    ctx: SessionContext,
    registry: Arc<ConnectorRegistry>,
}

impl FuseEngine {
    /// Create a new engine. For each connector in the registry, discovers
    /// tables and registers them as federated table providers in the
    /// DataFusion session.
    pub async fn new(registry: ConnectorRegistry) -> Result<Self> {
        let registry = Arc::new(registry);
        let state = datafusion_federation::default_session_state();
        let ctx = SessionContext::new_with_state(state);

        let ds_count = registry.datasource_names().len();

        for (ds_name, connector) in registry.connectors() {
            let table_names = connector
                .table_names()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let executor: Arc<dyn SQLExecutor> =
                Arc::new(FuseExecutor::new(ds_name.clone(), connector.clone()));
            let provider = Arc::new(SQLFederationProvider::new(executor));

            for table_name in &table_names {
                let schema = connector
                    .get_table_schema(table_name)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let table_ref = RemoteTableRef::try_from(table_name.as_str())?;
                let remote_table = Arc::new(RemoteTable::new(table_ref, schema));
                let source = Arc::new(SQLTableSource::new_with_table(
                    provider.clone(),
                    remote_table,
                ));
                let adaptor = Arc::new(FederatedTableProviderAdaptor::new(source));

                // Register as "datasource.table" for qualified access
                let qualified = format!("{}.{}", ds_name, table_name);
                ctx.register_table(&qualified, adaptor.clone())?;

                // Also register unqualified when there is only one datasource
                if ds_count == 1 {
                    ctx.register_table(table_name, adaptor)?;
                }
            }

            info!(
                datasource = ds_name.as_str(),
                tables = table_names.len(),
                "Registered federated datasource"
            );
        }

        Ok(Self { ctx, registry })
    }

    /// Execute a query (SQL or PPL), returning collected RecordBatches.
    ///
    /// Detects PPL by checking if the input starts with `source` or `search`.
    /// PPL queries are translated to SQL before execution.
    pub async fn execute(
        &self,
        query: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let sql = self.resolve_query(query)?;
        let df = self.ctx.sql(&sql).await?;
        df.collect().await
    }

    /// Execute a query (SQL or PPL), returning a stream of RecordBatches.
    pub async fn execute_stream(&self, query: &str) -> Result<SendableRecordBatchStream> {
        let sql = self.resolve_query(query)?;
        let df = self.ctx.sql(&sql).await?;
        df.execute_stream().await
    }

    /// If the input is PPL, translate to SQL. Otherwise pass through as-is.
    fn resolve_query(&self, query: &str) -> Result<String> {
        if crate::ppl::is_ppl(query) {
            let parsed = crate::ppl::parse_ppl(query)?;
            let sql = crate::ppl::ppl_to_sql(&parsed)?;
            tracing::debug!(ppl = query, sql = sql.as_str(), "PPL translated to SQL");
            Ok(sql)
        } else {
            Ok(query.to_string())
        }
    }

    /// Get a reference to the underlying SessionContext.
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get a reference to the connector registry.
    pub fn registry(&self) -> &ConnectorRegistry {
        &self.registry
    }
}

// ── FuseExecutor ──

/// Bridges a [`FederatedConnector`] to datafusion-federation's [`SQLExecutor`].
///
/// When the federation optimizer identifies a sub-plan belonging to a single
/// connector, it unparses the logical plan back to SQL and calls
/// [`SQLExecutor::execute`]. This executor will forward that SQL to the
/// underlying connector for translation to the native query language.
#[derive(Debug)]
pub struct FuseExecutor {
    datasource_name: String,
    connector: Arc<dyn FederatedConnector>,
}

impl FuseExecutor {
    pub fn new(datasource_name: String, connector: Arc<dyn FederatedConnector>) -> Self {
        Self {
            datasource_name,
            connector,
        }
    }
}

#[async_trait]
impl SQLExecutor for FuseExecutor {
    fn name(&self) -> &str {
        self.connector.connector_type()
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.datasource_name.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(DefaultDialect {})
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<SendableRecordBatchStream> {
        let connector = self.connector.clone();
        let query_str = query.to_string();

        let stream = futures::stream::once(async move {
            let sub_query = sql_to_subquery::sql_to_subquery(&query_str)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let batches = connector
                .execute(&sub_query)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok(batches)
        })
        .flat_map(|result| match result {
            Ok(batches) => {
                futures::stream::iter(batches.into_iter().map(Ok).collect::<Vec<_>>())
            }
            Err(e) => futures::stream::iter(vec![Err(e)]),
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        self.connector
            .table_names()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        self.connector
            .get_table_schema(table_name)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use fuse_core::connector::*;
    use fuse_core::error::ConnectorError;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    struct MockConn {
        name: String,
    }

    impl MockConn {
        fn new(name: &str) -> Self {
            Self { name: name.into() }
        }
        fn schema() -> Schema {
            Schema::new(vec![
                Field::new("host", DataType::Utf8, false),
                Field::new("status", DataType::Int64, false),
            ])
        }
    }

    #[async_trait]
    impl FederatedConnector for MockConn {
        fn id(&self) -> &str { &self.name }
        fn connector_type(&self) -> &str { "mock" }
        fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
        async fn health_check(&self) -> ConnectorHealth {
            ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None }
        }
        async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
            Ok(vec![SchemaInfo { name: "logs".into(), schema_type: SchemaType::Index, estimated_row_count: Some(10) }])
        }
        async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
            Ok(Self::schema())
        }
        async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
            let schema = Arc::new(Self::schema());
            Ok(vec![RecordBatch::try_new(schema, vec![
                Arc::new(StringArray::from(vec!["h1", "h2"])),
                Arc::new(Int64Array::from(vec![200, 500])),
            ]).map_err(ConnectorError::query)?])
        }
        async fn execute_streaming(
            &self, q: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
        ) -> Result<(), ConnectorError> {
            for b in self.execute(q).await? { tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?; }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_engine_resolve_ppl() {
        let reg = ConnectorRegistry::new();
        // Use FuseEngine just for resolve_query — bypass table registration
        // by testing the method directly
        let engine = FuseEngine {
            ctx: SessionContext::new_with_state(datafusion_federation::default_session_state()),
            registry: Arc::new(reg),
        };
        let sql = engine.resolve_query("source = logs | where status = 200").unwrap();
        assert!(sql.to_lowercase().contains("select"));
        assert!(sql.to_lowercase().contains("where"));
    }

    #[tokio::test]
    async fn test_engine_resolve_sql_passthrough() {
        let reg = ConnectorRegistry::new();
        let engine = FuseEngine {
            ctx: SessionContext::new_with_state(datafusion_federation::default_session_state()),
            registry: Arc::new(reg),
        };
        let sql = engine.resolve_query("SELECT * FROM logs").unwrap();
        assert_eq!(sql, "SELECT * FROM logs");
    }

    #[tokio::test]
    async fn test_engine_resolve_ppl_with_stats() {
        let reg = ConnectorRegistry::new();
        let engine = FuseEngine {
            ctx: SessionContext::new_with_state(datafusion_federation::default_session_state()),
            registry: Arc::new(reg),
        };
        let sql = engine.resolve_query("source = logs | stats count() by host").unwrap();
        assert!(sql.to_lowercase().contains("count"));
        assert!(sql.to_lowercase().contains("group by"));
    }

    #[tokio::test]
    async fn test_engine_accessors() {
        let reg = ConnectorRegistry::new();
        reg.register(Arc::new(MockConn::new("ds1"))).unwrap();
        let engine = FuseEngine {
            ctx: SessionContext::new_with_state(datafusion_federation::default_session_state()),
            registry: Arc::new(reg),
        };
        assert!(engine.registry().get("ds1").is_some());
        assert!(engine.registry().get("nope").is_none());
        let _ = engine.session_context();
    }

    #[test]
    fn test_executor_metadata() {
        let conn: Arc<dyn FederatedConnector> = Arc::new(MockConn::new("myds"));
        let exec = FuseExecutor::new("myds".into(), conn);
        assert_eq!(exec.name(), "mock");
        assert_eq!(exec.compute_context(), Some("myds".to_string()));
        // dialect is DefaultDialect
        let _ = exec.dialect();
    }

    #[tokio::test]
    async fn test_executor_table_names() {
        let conn: Arc<dyn FederatedConnector> = Arc::new(MockConn::new("myds"));
        let exec = FuseExecutor::new("myds".into(), conn);
        let names = exec.table_names().await.unwrap();
        assert_eq!(names, vec!["logs"]);
    }

    #[tokio::test]
    async fn test_executor_get_table_schema() {
        let conn: Arc<dyn FederatedConnector> = Arc::new(MockConn::new("myds"));
        let exec = FuseExecutor::new("myds".into(), conn);
        let schema = exec.get_table_schema("logs").await.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "host");
    }
}
