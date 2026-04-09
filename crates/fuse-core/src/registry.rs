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
            async move { (c.id().to_string(), c.health_check().await) }
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
