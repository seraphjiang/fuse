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
}

#[derive(Serialize)]
pub struct ConnectorHealthInfo {
    pub status: String,
    pub latency_ms: Option<u64>,
    pub message: Option<String>,
}

pub async fn check_health(registry: &ConnectorRegistry) -> HealthResponse {
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

    HealthResponse {
        status: overall.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        connectors,
    }
}
