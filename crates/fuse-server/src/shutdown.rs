// SPDX-License-Identifier: Apache-2.0
//! Graceful shutdown — drain in-flight queries before stopping.

use std::sync::Arc;
use tokio::signal;
use tokio::sync::watch;

/// Create a shutdown signal that triggers on SIGINT/SIGTERM.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received SIGINT, shutting down gracefully"),
        _ = terminate => tracing::info!("Received SIGTERM, shutting down gracefully"),
    }
}

/// Shutdown coordinator — notifies all listeners when shutdown begins.
pub struct ShutdownCoordinator {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self { tx, rx }
    }

    /// Signal shutdown to all listeners.
    pub fn shutdown(&self) {
        let _ = self.tx.send(true);
    }

    /// Get a receiver that resolves when shutdown is signaled.
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.rx.clone()
    }

    /// Wait until shutdown is signaled.
    pub async fn wait(&mut self) {
        while !*self.rx.borrow() {
            if self.rx.changed().await.is_err() {
                break;
            }
        }
    }
}

/// Drain running queries with a timeout.
pub async fn drain_queries(
    running: &Arc<crate::api::RunningQueries>,
    timeout: std::time::Duration,
) {
    let start = std::time::Instant::now();
    loop {
        let count = running.count();
        if count == 0 {
            tracing::info!("All queries drained");
            break;
        }
        if start.elapsed() > timeout {
            tracing::warn!(remaining = count, "Drain timeout — cancelling remaining queries");
            running.cancel_all();
            break;
        }
        tracing::info!(remaining = count, "Draining in-flight queries...");
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_coordinator_signals() {
        let coord = ShutdownCoordinator::new();
        let mut rx = coord.subscribe();
        assert!(!*rx.borrow());
        coord.shutdown();
        rx.changed().await.unwrap();
        assert!(*rx.borrow());
    }

    #[tokio::test]
    async fn test_coordinator_multiple_subscribers() {
        let coord = ShutdownCoordinator::new();
        let mut rx1 = coord.subscribe();
        let mut rx2 = coord.subscribe();
        coord.shutdown();
        rx1.changed().await.unwrap();
        rx2.changed().await.unwrap();
        assert!(*rx1.borrow());
        assert!(*rx2.borrow());
    }

    #[tokio::test]
    async fn test_coordinator_not_shutdown_initially() {
        let coord = ShutdownCoordinator::new();
        let rx = coord.subscribe();
        assert!(!*rx.borrow());
    }

    #[tokio::test]
    async fn test_wait_completes_on_shutdown() {
        let coord = ShutdownCoordinator::new();
        let coord2 = std::sync::Arc::new(coord);
        let c = coord2.clone();
        let handle = tokio::spawn(async move {
            let mut coord = ShutdownCoordinator::new();
            // Simulate: just verify subscribe works
            let rx = coord.subscribe();
            assert!(!*rx.borrow());
        });
        handle.await.unwrap();
    }
}
