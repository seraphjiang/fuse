// SPDX-License-Identifier: Apache-2.0

//! Prometheus metrics for Fuse server.
//!
//! Exposes `/metrics` endpoint with query counts, latency, active queries, and errors.

use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use metrics_exporter_prometheus::PrometheusHandle;

use crate::api::AppState;

/// Initialize the Prometheus metrics recorder and return the handle for rendering.
pub fn init() -> PrometheusHandle {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder.install_recorder().expect("failed to install metrics recorder")
}

/// Record a completed query.
pub fn record_query(format: &str, success: bool, duration_ms: u64) {
    let status = if success { "ok" } else { "error" };
    metrics::counter!("fuse_queries_total", "format" => format.to_string(), "status" => status).increment(1);
    metrics::histogram!("fuse_query_duration_ms", "format" => format.to_string()).record(duration_ms as f64);
}

/// Update the active queries gauge.
pub fn set_active_queries(count: usize) {
    metrics::gauge!("fuse_active_queries").set(count as f64);
}

/// Record a connector health check result.
pub fn record_connector_health(connector_id: &str, healthy: bool) {
    let val = if healthy { 1.0 } else { 0.0 };
    metrics::gauge!("fuse_connector_healthy", "connector" => connector_id.to_string()).set(val);
}

/// Record plan cache stats.
pub fn record_cache_stats(hits: u64, misses: u64) {
    metrics::gauge!("fuse_plan_cache_hits").set(hits as f64);
    metrics::gauge!("fuse_plan_cache_misses").set(misses as f64);
}

/// Record connector count.
pub fn set_connector_count(count: usize) {
    metrics::gauge!("fuse_connectors_total").set(count as f64);
}

/// Record tenant count.
pub fn set_tenant_count(count: usize) {
    metrics::gauge!("fuse_tenants_total").set(count as f64);
}

/// GET /metrics — Prometheus scrape endpoint.
pub async fn metrics_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    set_active_queries(state.running_queries.count());
    let handle = HANDLE.get().expect("metrics not initialized");
    handle.render()
}

/// Global handle — set once at startup.
static HANDLE: std::sync::OnceLock<PrometheusHandle> = std::sync::OnceLock::new();

pub fn set_handle(handle: PrometheusHandle) {
    let _ = HANDLE.set(handle);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_query_does_not_panic() {
        // metrics macros are no-ops if no recorder is installed
        record_query("sql", true, 42);
        record_query("ppl", false, 100);
    }

    #[test]
    fn test_set_active_queries_does_not_panic() {
        set_active_queries(5);
        set_active_queries(0);
    }

    #[test]
    fn test_record_connector_health_does_not_panic() {
        record_connector_health("cluster_a", true);
        record_connector_health("cluster_b", false);
    }
}
