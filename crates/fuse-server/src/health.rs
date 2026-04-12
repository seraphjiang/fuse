// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use fuse_core::connector::HealthStatus;
use fuse_core::registry::ConnectorRegistry;
use serde::Serialize;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub connectors: HashMap<String, ConnectorHealthInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub federation: Option<FederationHealthInfo>,
}

#[derive(Serialize)]
pub struct FederationHealthInfo {
    pub instance_count: usize,
    pub healthy_count: usize,
    pub instances: HashMap<String, ConnectorHealthInfo>,
}

#[derive(Serialize)]
pub struct ConnectorHealthInfo {
    pub status: String,
    pub latency_ms: Option<u64>,
    pub message: Option<String>,
}

pub async fn check_health(registry: &ConnectorRegistry) -> HealthResponse {
    check_health_with_federation(registry, None).await
}

pub async fn check_health_with_federation(
    registry: &ConnectorRegistry,
    federation: Option<&crate::federation::FederationRegistry>,
) -> HealthResponse {
    let checks = registry.health_check_all().await;

    let all_healthy = checks.values().all(|h| h.status == HealthStatus::Healthy);
    let any_unhealthy = checks.values().any(|h| h.status == HealthStatus::Unhealthy);

    let overall = if all_healthy {
        "healthy"
    } else if any_unhealthy {
        "unhealthy"
    } else {
        "degraded"
    };

    let connectors = checks
        .into_iter()
        .map(|(id, h)| {
            let info = ConnectorHealthInfo {
                status: format!("{:?}", h.status).to_lowercase(),
                latency_ms: h.latency_ms,
                message: h.message,
            };
            (id, info)
        })
        .collect();

    let federation_info = federation.map(|fed| {
        let topo = fed.topology();
        let instances = topo
            .instances
            .iter()
            .map(|inst| {
                (
                    inst.id.clone(),
                    ConnectorHealthInfo {
                        status: format!("{:?}", inst.status).to_lowercase(),
                        latency_ms: inst.latency_ms,
                        message: inst.name.clone(),
                    },
                )
            })
            .collect();
        FederationHealthInfo {
            instance_count: topo.instance_count,
            healthy_count: topo.healthy_count,
            instances,
        }
    });

    HealthResponse {
        status: overall.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        connectors,
        federation: federation_info,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use fuse_core::connector::*;
    use fuse_core::error::ConnectorError;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[derive(Debug)]
    struct HealthStub {
        id: String,
        status: HealthStatus,
        msg: Option<String>,
    }

    #[async_trait]
    impl FederatedConnector for HealthStub {
        fn id(&self) -> &str {
            &self.id
        }
        fn connector_type(&self) -> &str {
            "stub"
        }
        fn capabilities(&self) -> ConnectorCapabilities {
            ConnectorCapabilities::full()
        }
        async fn health_check(&self) -> ConnectorHealth {
            ConnectorHealth {
                status: self.status.clone(),
                latency_ms: Some(1),
                message: self.msg.clone(),
            }
        }
        async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
            Ok(vec![])
        }
        async fn get_schema(&self, _: &str) -> Result<arrow::datatypes::Schema, ConnectorError> {
            Ok(arrow::datatypes::Schema::empty())
        }
        async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
            Ok(vec![])
        }
        async fn execute_streaming(
            &self,
            _: &SubQuery,
            _: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_all_healthy() {
        let reg = ConnectorRegistry::new();
        reg.register(Arc::new(HealthStub {
            id: "a".into(),
            status: HealthStatus::Healthy,
            msg: None,
        }))
        .unwrap();
        reg.register(Arc::new(HealthStub {
            id: "b".into(),
            status: HealthStatus::Healthy,
            msg: None,
        }))
        .unwrap();
        let resp = check_health(&reg).await;
        assert_eq!(resp.status, "healthy");
        assert_eq!(resp.connectors.len(), 2);
    }

    #[tokio::test]
    async fn test_one_unhealthy() {
        let reg = ConnectorRegistry::new();
        reg.register(Arc::new(HealthStub {
            id: "ok".into(),
            status: HealthStatus::Healthy,
            msg: None,
        }))
        .unwrap();
        reg.register(Arc::new(HealthStub {
            id: "bad".into(),
            status: HealthStatus::Unhealthy,
            msg: Some("down".into()),
        }))
        .unwrap();
        let resp = check_health(&reg).await;
        assert_eq!(resp.status, "unhealthy");
        assert_eq!(resp.connectors["bad"].message.as_deref(), Some("down"));
    }

    #[tokio::test]
    async fn test_degraded_status() {
        let reg = ConnectorRegistry::new();
        reg.register(Arc::new(HealthStub {
            id: "ok".into(),
            status: HealthStatus::Healthy,
            msg: None,
        }))
        .unwrap();
        reg.register(Arc::new(HealthStub {
            id: "meh".into(),
            status: HealthStatus::Degraded,
            msg: None,
        }))
        .unwrap();
        let resp = check_health(&reg).await;
        assert_eq!(resp.status, "degraded");
    }

    #[tokio::test]
    async fn test_empty_registry_healthy() {
        let reg = ConnectorRegistry::new();
        let resp = check_health(&reg).await;
        assert_eq!(resp.status, "healthy");
        assert!(resp.connectors.is_empty());
    }
}
