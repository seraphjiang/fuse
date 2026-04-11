// SPDX-License-Identifier: Apache-2.0
//! Auto-refresh scheduler for materialized views.
//!
//! Spawns a background task that periodically checks for stale views
//! and re-executes their queries to refresh cached results.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info, warn};

use fuse_engine::materialized::MaterializedViewRegistry;

/// Handle to a running refresh scheduler. Drop to stop.
pub struct RefreshScheduler {
    shutdown_tx: watch::Sender<bool>,
}

impl RefreshScheduler {
    /// Start the auto-refresh background task.
    ///
    /// `poll_interval` — how often to check for stale views (e.g. 30s).
    /// `execute_fn` — async function that executes a query and returns batches.
    pub fn start<F, Fut>(
        registry: Arc<MaterializedViewRegistry>,
        poll_interval: Duration,
        execute_fn: F,
    ) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<arrow::record_batch::RecordBatch>, String>> + Send,
    {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        tokio::spawn(async move {
            info!("Materialized view refresh scheduler started (poll every {:?})", poll_interval);
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(poll_interval) => {}
                    _ = shutdown_rx.changed() => {
                        info!("Refresh scheduler shutting down");
                        return;
                    }
                }

                let stale = registry.stale_views();
                if stale.is_empty() { continue; }

                debug!("Refreshing {} stale view(s): {:?}", stale.len(), stale);

                for name in stale {
                    let query = {
                        let Some(view_arc) = registry.get(&name) else { continue };
                        let view = view_arc.read().unwrap();
                        view.def.query.clone()
                    };

                    match execute_fn(query).await {
                        Ok(batches) => {
                            let Some(view_arc) = registry.get(&name) else { continue };
                            let mut view = view_arc.write().unwrap();
                            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                            view.set_results(batches);
                            debug!("Refreshed view '{}': {} rows", name, row_count);
                        }
                        Err(e) => {
                            let Some(view_arc) = registry.get(&name) else { continue };
                            let mut view = view_arc.write().unwrap();
                            view.set_error(e.clone());
                            warn!("Failed to refresh view '{}': {}", name, e);
                        }
                    }
                }
            }
        });

        Self { shutdown_tx }
    }

    /// Stop the scheduler.
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl Drop for RefreshScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuse_engine::materialized::MaterializedViewDef;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn test_def(name: &str) -> MaterializedViewDef {
        MaterializedViewDef {
            name: name.into(),
            query: "SELECT 1".into(),
            refresh_interval: Duration::from_millis(50),
        }
    }

    fn make_batch() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        vec![RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap()]
    }

    #[tokio::test]
    async fn test_scheduler_refreshes_stale_view() {
        let reg = Arc::new(MaterializedViewRegistry::new());
        reg.register(test_def("v1"));

        // View should be stale (never refreshed)
        assert_eq!(reg.stale_views(), vec!["v1"]);

        let reg2 = reg.clone();
        let _sched = RefreshScheduler::start(
            reg.clone(),
            Duration::from_millis(20),
            move |_query| {
                let _ = &reg2;
                async { Ok(make_batch()) }
            },
        );

        // Wait for refresh
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should no longer be stale
        assert!(reg.stale_views().is_empty() || reg.get_results("v1").is_some());
        let results = reg.get_results("v1");
        assert!(results.is_some());
    }

    #[tokio::test]
    async fn test_scheduler_handles_error() {
        let reg = Arc::new(MaterializedViewRegistry::new());
        reg.register(test_def("v1"));

        let _sched = RefreshScheduler::start(
            reg.clone(),
            Duration::from_millis(20),
            |_query| async { Err("db down".to_string()) },
        );

        tokio::time::sleep(Duration::from_millis(100)).await;

        let view_arc = reg.get("v1").unwrap();
        let view = view_arc.read().unwrap();
        assert!(view.error.is_some());
    }

    #[tokio::test]
    async fn test_scheduler_stop() {
        let reg = Arc::new(MaterializedViewRegistry::new());
        let sched = RefreshScheduler::start(
            reg.clone(),
            Duration::from_millis(10),
            |_| async { Ok(vec![]) },
        );
        sched.stop();
        // Should not panic — task exits gracefully
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn test_scheduler_skips_empty() {
        let reg = Arc::new(MaterializedViewRegistry::new());
        // No views registered — scheduler should just loop without error
        let sched = RefreshScheduler::start(
            reg.clone(),
            Duration::from_millis(10),
            |_| async { Ok(vec![]) },
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        sched.stop();
    }
}
