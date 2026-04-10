// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::config::ConnectorConfig;
use crate::connector::{ConnectorHealth, FederatedConnector};
use crate::error::{ConnectorError, RegistryError};

/// Central registry for connector instances.
pub struct ConnectorRegistry {
    inner: RwLock<HashMap<String, Arc<dyn FederatedConnector>>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, connector: Arc<dyn FederatedConnector>) -> Result<(), RegistryError> {
        let mut map = self.inner.write().unwrap();
        let id = connector.id().to_string();
        if map.contains_key(&id) {
            return Err(RegistryError::DuplicateId(id));
        }
        map.insert(id, connector);
        Ok(())
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn FederatedConnector>> {
        self.inner.read().unwrap().get(id).cloned()
    }

    pub fn list(&self) -> Vec<Arc<dyn FederatedConnector>> {
        self.inner.read().unwrap().values().cloned().collect()
    }

    /// Iterator over (datasource_name, connector) pairs — used by the planner.
    pub fn connectors(&self) -> Vec<(String, Arc<dyn FederatedConnector>)> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// List all registered datasource names.
    pub fn datasource_names(&self) -> Vec<String> {
        self.inner.read().unwrap().keys().cloned().collect()
    }

    pub async fn health_check_all(&self) -> HashMap<String, ConnectorHealth> {
        let connectors = self.list();
        let futs = connectors.iter().map(|c| {
            let c = c.clone();
            async move {
                let id = c.id().to_string();
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    c.health_check(),
                )
                .await
                {
                    Ok(h) => (id, h),
                    Err(_) => (
                        id,
                        ConnectorHealth {
                            status: crate::connector::HealthStatus::Unhealthy,
                            latency_ms: Some(5000),
                            message: Some("health check timed out (5s)".into()),
                        },
                    ),
                }
            }
        });
        futures::future::join_all(futs).await.into_iter().collect()
    }
}

impl Default for ConnectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory trait for creating connector instances from config.
#[async_trait::async_trait]
pub trait ConnectorFactory: Send + Sync {
    fn connector_type(&self) -> &str;
    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::*;
    use async_trait::async_trait;
    use tokio::sync::mpsc;
    use arrow::record_batch::RecordBatch;

    #[derive(Debug)]
    struct StubConnector { id: String, health: HealthStatus }

    #[async_trait]
    impl FederatedConnector for StubConnector {
        fn id(&self) -> &str { &self.id }
        fn connector_type(&self) -> &str { "stub" }
        fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
        async fn health_check(&self) -> ConnectorHealth {
            ConnectorHealth { status: self.health.clone(), latency_ms: Some(1), message: None }
        }
        async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, crate::error::ConnectorError> { Ok(vec![]) }
        async fn get_schema(&self, _: &str) -> Result<arrow::datatypes::Schema, crate::error::ConnectorError> {
            Ok(arrow::datatypes::Schema::empty())
        }
        async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, crate::error::ConnectorError> { Ok(vec![]) }
        async fn execute_streaming(&self, _: &SubQuery, _: mpsc::Sender<Result<RecordBatch, crate::error::ConnectorError>>) -> Result<(), crate::error::ConnectorError> { Ok(()) }
    }

    fn stub(id: &str) -> Arc<dyn FederatedConnector> {
        Arc::new(StubConnector { id: id.to_string(), health: HealthStatus::Healthy })
    }

    fn unhealthy_stub(id: &str) -> Arc<dyn FederatedConnector> {
        Arc::new(StubConnector { id: id.to_string(), health: HealthStatus::Unhealthy })
    }

    #[test]
    fn test_register_and_get() {
        let reg = ConnectorRegistry::new();
        reg.register(stub("a")).unwrap();
        assert!(reg.get("a").is_some());
        assert!(reg.get("b").is_none());
    }

    #[test]
    fn test_duplicate_register_fails() {
        let reg = ConnectorRegistry::new();
        reg.register(stub("a")).unwrap();
        assert!(reg.register(stub("a")).is_err());
    }

    #[test]
    fn test_list_and_datasource_names() {
        let reg = ConnectorRegistry::new();
        reg.register(stub("x")).unwrap();
        reg.register(stub("y")).unwrap();
        assert_eq!(reg.list().len(), 2);
        let mut names = reg.datasource_names();
        names.sort();
        assert_eq!(names, vec!["x", "y"]);
    }

    #[tokio::test]
    async fn test_health_check_all() {
        let reg = ConnectorRegistry::new();
        reg.register(stub("ok")).unwrap();
        reg.register(unhealthy_stub("bad")).unwrap();
        let checks = reg.health_check_all().await;
        assert_eq!(checks.len(), 2);
        assert_eq!(checks["ok"].status, HealthStatus::Healthy);
        assert_eq!(checks["bad"].status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_empty_registry() {
        let reg = ConnectorRegistry::new();
        assert!(reg.list().is_empty());
        assert!(reg.datasource_names().is_empty());
        assert!(reg.get("anything").is_none());
    }
}
