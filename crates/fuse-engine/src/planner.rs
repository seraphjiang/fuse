// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
use datafusion_federation::sql::{
    RemoteTable, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::FederatedTableProviderAdaptor;
use tracing::info;

use fuse_core::connector::FederatedConnector;
use fuse_core::ConnectorRegistry;

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

    /// Execute a SQL query, returning collected RecordBatches.
    pub async fn execute(
        &self,
        sql: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        df.collect().await
    }

    /// Execute a SQL query, returning a stream of RecordBatches.
    pub async fn execute_stream(&self, sql: &str) -> Result<SendableRecordBatchStream> {
        let df = self.ctx.sql(sql).await?;
        df.execute_stream().await
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
        _query: &str,
        _schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<SendableRecordBatchStream> {
        // TODO(phase1): Wire to connector.execute() by translating the SQL
        // string into a SubQuery and streaming results back via a channel.
        Err(DataFusionError::NotImplemented(format!(
            "FuseExecutor::execute not yet wired for connector '{}'",
            self.datasource_name
        )))
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
