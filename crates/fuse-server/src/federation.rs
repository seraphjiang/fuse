// SPDX-License-Identifier: Apache-2.0
//! #1002 Federation health + topology API.
//!
//! GET /api/fuse/federation — show connected Fuse instances, their
//! datasources, health status, and topology.

use std::collections::HashMap;
use std::sync::Mutex;
use serde::{Deserialize, Serialize};

/// A remote Fuse instance in the federation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedInstance {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub datasources: Vec<String>,
    pub status: InstanceStatus,
    pub latency_ms: Option<u64>,
    pub last_checked: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstanceStatus {
    Healthy,
    Unhealthy,
    Unknown,
}

/// Federation topology — tracks all connected Fuse instances.
pub struct FederationRegistry {
    instances: Mutex<HashMap<String, FederatedInstance>>,
}

impl FederationRegistry {
    pub fn new() -> Self {
        Self {
            instances: Mutex::new(HashMap::new()),
        }
    }

    pub fn register(&self, instance: FederatedInstance) {
        self.instances.lock().unwrap().insert(instance.id.clone(), instance);
    }

    pub fn remove(&self, id: &str) -> bool {
        self.instances.lock().unwrap().remove(id).is_some()
    }

    pub fn get(&self, id: &str) -> Option<FederatedInstance> {
        self.instances.lock().unwrap().get(id).cloned()
    }

    pub fn list(&self) -> Vec<FederatedInstance> {
        self.instances.lock().unwrap().values().cloned().collect()
    }

    pub fn update_status(&self, id: &str, status: InstanceStatus, latency_ms: Option<u64>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if let Some(inst) = self.instances.lock().unwrap().get_mut(id) {
            inst.status = status;
            inst.latency_ms = latency_ms;
            inst.last_checked = Some(now);
        }
    }

    /// Find which remote instance owns a given datasource.
    /// Returns the instance ID and URL if found.
    pub fn find_owner(&self, datasource: &str) -> Option<(String, String)> {
        let instances = self.instances.lock().unwrap();
        for inst in instances.values() {
            if inst.status == InstanceStatus::Healthy
                && inst.datasources.iter().any(|ds| ds == datasource)
            {
                return Some((inst.id.clone(), inst.url.clone()));
            }
        }
        None
    }

    /// Resolve a datasource reference: check local registry first, then federation.
    /// Returns `RouteTarget::Local` if the datasource is local, or
    /// `RouteTarget::Remote { instance_id, url }` if owned by a federated instance.
    pub fn resolve_route(
        &self,
        datasource: &str,
        local_registry: &fuse_core::registry::ConnectorRegistry,
    ) -> RouteTarget {
        if local_registry.get(datasource).is_some() {
            RouteTarget::Local
        } else if let Some((instance_id, url)) = self.find_owner(datasource) {
            RouteTarget::Remote { instance_id, url }
        } else {
            RouteTarget::NotFound
        }
    }

    /// Cost-based route selection (#1532). When multiple instances can serve
    /// a datasource, pick the one with lowest cost (latency + health score).
    pub fn cost_based_route(
        &self,
        datasource: &str,
        local_registry: &fuse_core::registry::ConnectorRegistry,
    ) -> RouteTarget {
        // Local is always cheapest (zero network hop)
        if local_registry.get(datasource).is_some() {
            return RouteTarget::Local;
        }

        let instances = self.instances.lock().unwrap();
        let mut candidates: Vec<(&FederatedInstance, u64)> = instances
            .values()
            .filter(|i| i.status == InstanceStatus::Healthy && i.datasources.contains(&datasource.to_string()))
            .map(|i| {
                let cost = route_cost(i);
                (i, cost)
            })
            .collect();

        candidates.sort_by_key(|(_, cost)| *cost);

        if let Some((best, _)) = candidates.first() {
            RouteTarget::Remote {
                instance_id: best.id.clone(),
                url: best.url.clone(),
            }
        } else {
            RouteTarget::NotFound
        }
    }

    pub fn topology(&self) -> FederationTopology {
        let instances = self.list();
        let healthy = instances.iter().filter(|i| i.status == InstanceStatus::Healthy).count();
        let total_datasources: usize = instances.iter().map(|i| i.datasources.len()).sum();
        FederationTopology {
            instance_count: instances.len(),
            healthy_count: healthy,
            total_datasources,
            instances,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FederationTopology {
    pub instance_count: usize,
    pub healthy_count: usize,
    pub total_datasources: usize,
    pub instances: Vec<FederatedInstance>,
}

/// Where a datasource query should be routed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RouteTarget {
    /// Datasource is available on this Fuse instance.
    Local,
    /// Datasource is owned by a remote Fuse instance.
    Remote {
        instance_id: String,
        url: String,
    },
    /// Datasource not found locally or in any federated instance.
    NotFound,
}

/// Compute routing cost for an instance. Lower is better.
/// Cost = latency_ms + health penalty. Healthy = 0, Degraded would add 500.
fn route_cost(instance: &FederatedInstance) -> u64 {
    let latency = instance.latency_ms.unwrap_or(100); // default 100ms if unknown
    let health_penalty: u64 = match instance.status {
        InstanceStatus::Healthy => 0,
        InstanceStatus::Unknown => 200,
        InstanceStatus::Unhealthy => 10000, // effectively excluded by filter, but just in case
    };
    // Fewer datasources = more specialized = slight preference
    let load_factor = instance.datasources.len() as u64;
    latency + health_penalty + load_factor
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance(id: &str) -> FederatedInstance {
        FederatedInstance {
            id: id.into(),
            url: format!("http://{id}:9400"),
            name: Some(format!("Fuse {id}")),
            datasources: vec!["logs".into(), "metrics".into()],
            status: InstanceStatus::Healthy,
            latency_ms: Some(5),
            last_checked: None,
        }
    }

    #[test]
    fn test_register_and_list() {
        let reg = FederationRegistry::new();
        reg.register(instance("a"));
        reg.register(instance("b"));
        assert_eq!(reg.list().len(), 2);
    }

    #[test]
    fn test_remove() {
        let reg = FederationRegistry::new();
        reg.register(instance("a"));
        assert!(reg.remove("a"));
        assert!(!reg.remove("a"));
        assert!(reg.list().is_empty());
    }

    #[test]
    fn test_get() {
        let reg = FederationRegistry::new();
        reg.register(instance("x"));
        assert_eq!(reg.get("x").unwrap().url, "http://x:9400");
        assert!(reg.get("missing").is_none());
    }

    #[test]
    fn test_update_status() {
        let reg = FederationRegistry::new();
        reg.register(instance("a"));
        reg.update_status("a", InstanceStatus::Unhealthy, Some(500));
        let inst = reg.get("a").unwrap();
        assert_eq!(inst.status, InstanceStatus::Unhealthy);
        assert_eq!(inst.latency_ms, Some(500));
        assert!(inst.last_checked.is_some());
    }

    #[test]
    fn test_topology() {
        let reg = FederationRegistry::new();
        reg.register(instance("a"));
        let mut b = instance("b");
        b.status = InstanceStatus::Unhealthy;
        reg.register(b);
        let topo = reg.topology();
        assert_eq!(topo.instance_count, 2);
        assert_eq!(topo.healthy_count, 1);
        assert_eq!(topo.total_datasources, 4);
    }

    #[test]
    fn test_empty_topology() {
        let reg = FederationRegistry::new();
        let topo = reg.topology();
        assert_eq!(topo.instance_count, 0);
        assert_eq!(topo.healthy_count, 0);
        assert_eq!(topo.total_datasources, 0);
    }

    #[test]
    fn test_update_nonexistent_noop() {
        let reg = FederationRegistry::new();
        reg.update_status("ghost", InstanceStatus::Healthy, None);
        assert!(reg.list().is_empty());
    }

    #[test]
    fn test_find_owner() {
        let reg = FederationRegistry::new();
        reg.register(instance("a"));
        assert_eq!(reg.find_owner("logs").unwrap().0, "a");
        assert!(reg.find_owner("nonexistent").is_none());
    }

    #[test]
    fn test_find_owner_skips_unhealthy() {
        let reg = FederationRegistry::new();
        let mut inst = instance("a");
        inst.status = InstanceStatus::Unhealthy;
        reg.register(inst);
        assert!(reg.find_owner("logs").is_none());
    }

    #[test]
    fn test_resolve_route_local() {
        use fuse_core::connector::*;
        use std::sync::Arc;

        // Minimal mock connector for testing
        #[derive(Debug)]
        struct MockConn;
        #[async_trait::async_trait]
        impl FederatedConnector for MockConn {
            fn id(&self) -> &str { "local_ds" }
            fn connector_type(&self) -> &str { "mock" }
            fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
            async fn health_check(&self) -> ConnectorHealth {
                ConnectorHealth { status: HealthStatus::Healthy, latency_ms: None, message: None }
            }
            async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, fuse_core::error::ConnectorError> { Ok(vec![]) }
            async fn get_schema(&self, _: &str) -> Result<arrow::datatypes::Schema, fuse_core::error::ConnectorError> {
                Ok(arrow::datatypes::Schema::empty())
            }
            async fn execute(&self, _: &SubQuery) -> Result<Vec<arrow::record_batch::RecordBatch>, fuse_core::error::ConnectorError> { Ok(vec![]) }
            async fn execute_streaming(&self, _: &SubQuery, _: tokio::sync::mpsc::Sender<Result<arrow::record_batch::RecordBatch, fuse_core::error::ConnectorError>>) -> Result<(), fuse_core::error::ConnectorError> { Ok(()) }
        }

        let reg = FederationRegistry::new();
        let local = fuse_core::registry::ConnectorRegistry::new();
        local.register(Arc::new(MockConn)).unwrap();
        assert_eq!(reg.resolve_route("local_ds", &local), RouteTarget::Local);
    }

    #[test]
    fn test_resolve_route_remote() {
        let reg = FederationRegistry::new();
        reg.register(instance("remote1"));
        let local = fuse_core::registry::ConnectorRegistry::new();
        match reg.resolve_route("logs", &local) {
            RouteTarget::Remote { instance_id, .. } => assert_eq!(instance_id, "remote1"),
            other => panic!("expected Remote, got {:?}", other),
        }
    }

    #[test]
    fn test_resolve_route_not_found() {
        let reg = FederationRegistry::new();
        let local = fuse_core::registry::ConnectorRegistry::new();
        assert_eq!(reg.resolve_route("ghost", &local), RouteTarget::NotFound);
    }

    #[test]
    fn test_cost_based_route_picks_lowest_latency() {
        let reg = FederationRegistry::new();
        let mut fast = instance("fast");
        fast.latency_ms = Some(10);
        fast.datasources = vec!["logs".into()];
        let mut slow = instance("slow");
        slow.latency_ms = Some(200);
        slow.datasources = vec!["logs".into()];
        reg.register(fast);
        reg.register(slow);
        let local = fuse_core::registry::ConnectorRegistry::new();
        match reg.cost_based_route("logs", &local) {
            RouteTarget::Remote { instance_id, .. } => assert_eq!(instance_id, "fast"),
            other => panic!("expected Remote, got {:?}", other),
        }
    }

    #[test]
    fn test_cost_based_route_prefers_local() {
        use fuse_core::connector::*;
        let reg = FederationRegistry::new();
        let mut remote = instance("remote");
        remote.latency_ms = Some(1);
        remote.datasources = vec!["local_ds".into()];
        reg.register(remote);

        let local = fuse_core::registry::ConnectorRegistry::new();
        // Register a local connector
        #[derive(Debug)]
        struct MockConn;
        #[async_trait::async_trait]
        impl fuse_core::connector::FederatedConnector for MockConn {
            fn id(&self) -> &str { "local_ds" }
            fn connector_type(&self) -> &str { "mock" }
            fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
            async fn health_check(&self) -> ConnectorHealth {
                ConnectorHealth { status: fuse_core::connector::HealthStatus::Healthy, latency_ms: None, message: None }
            }
            async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, fuse_core::error::ConnectorError> { Ok(vec![]) }
            async fn get_schema(&self, _: &str) -> Result<arrow::datatypes::Schema, fuse_core::error::ConnectorError> {
                Ok(arrow::datatypes::Schema::empty())
            }
            async fn execute(&self, _: &SubQuery) -> Result<Vec<arrow::record_batch::RecordBatch>, fuse_core::error::ConnectorError> { Ok(vec![]) }
            async fn execute_streaming(&self, _: &SubQuery, _: tokio::sync::mpsc::Sender<Result<arrow::record_batch::RecordBatch, fuse_core::error::ConnectorError>>) -> Result<(), fuse_core::error::ConnectorError> { Ok(()) }
        }
        local.register(std::sync::Arc::new(MockConn));
        assert_eq!(reg.cost_based_route("local_ds", &local), RouteTarget::Local);
    }

    #[test]
    fn test_cost_based_route_skips_unhealthy() {
        let reg = FederationRegistry::new();
        let mut sick = instance("sick");
        sick.status = InstanceStatus::Unhealthy;
        sick.latency_ms = Some(1);
        sick.datasources = vec!["data".into()];
        reg.register(sick);
        let local = fuse_core::registry::ConnectorRegistry::new();
        assert_eq!(reg.cost_based_route("data", &local), RouteTarget::NotFound);
    }

    #[test]
    fn test_route_cost_function() {
        let mut inst = instance("x");
        inst.latency_ms = Some(50);
        inst.datasources = vec!["a".into(), "b".into()];
        let cost = route_cost(&inst);
        assert_eq!(cost, 50 + 0 + 2); // latency + healthy penalty + 2 datasources
    }
}
