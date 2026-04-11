// SPDX-License-Identifier: Apache-2.0
//! Background connector health monitor — periodically checks all connectors.

use std::sync::Arc;
use std::time::Duration;
use fuse_core::registry::ConnectorRegistry;

/// Spawn a background task that checks connector health periodically.
pub fn spawn_health_monitor(
    registry: Arc<ConnectorRegistry>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let checks = registry.health_check_all().await;
            for (id, health) in &checks {
                let healthy = health.status == fuse_core::connector::HealthStatus::Healthy;
                crate::metrics::record_connector_health(id, healthy);
                if !healthy {
                    tracing::warn!(
                        connector = id.as_str(),
                        status = ?health.status,
                        message = health.message.as_deref().unwrap_or(""),
                        "Connector unhealthy"
                    );
                }
            }
            tracing::debug!(connectors = checks.len(), "Health check complete");
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_spawn_health_monitor_starts() {
        let registry = Arc::new(ConnectorRegistry::new());
        let handle = spawn_health_monitor(registry, Duration::from_millis(100));
        // Let it run one tick
        tokio::time::sleep(Duration::from_millis(150)).await;
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
    }
}
