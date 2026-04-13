// SPDX-License-Identifier: Apache-2.0
//! Integration tests for ops endpoints: /metrics, /health, /status, /admin,
//! /settings, /api/fuse/stats, /api/fuse/queries/running, /api/fuse/datasources.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tokio::sync::mpsc;
use tower::ServiceExt;

use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorRegistry;
use fuse_server::api::{AppState, RunningQueries};

#[derive(Debug)]
struct OpsTestConnector(String);

#[async_trait]
impl FederatedConnector for OpsTestConnector {
    fn id(&self) -> &str { &self.0 }
    fn connector_type(&self) -> &str { "mock" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(5), message: None }
    }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![SchemaInfo { name: "logs".into(), schema_type: SchemaType::Table, estimated_row_count: Some(100) }])
    }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
        ]))
    }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let schema = Arc::new(self.get_schema("").await?);
        Ok(vec![RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["h1"])),
            Arc::new(Int64Array::from(vec![200])),
        ]).unwrap()])
    }
    async fn execute_streaming(&self, q: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for b in self.execute(q).await? { tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?; }
        Ok(())
    }
}

fn build_ops_app() -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(OpsTestConnector("ops_ds".into()))).unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()),
        plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
        result_cache: Arc::new(fuse_server::plan_cache::ResultCache::new(60, 500)),
        tenant_registry: Arc::new(fuse_server::tenant::TenantRegistry::disabled()),
        audit_log: Arc::new(fuse_server::audit::AuditLog::new(100)),
        adaptive_timeout: Arc::new(fuse_server::adaptive_timeout::AdaptiveTimeout::new()),
        prepared_statements: fuse_server::prepared::new_store(),
        shared_saved_queries: fuse_server::shared_state::SharedSavedQueries::from_env(),
        shared_history: fuse_server::shared_state::SharedQueryHistory::from_env(),
        shared_audit_log: fuse_server::shared_state::SharedAuditLog::from_env(),
        transactions: Arc::new(fuse_server::transaction::TransactionStore::new()),
        max_result_bytes: 0,
        datasource_limiter: Arc::new(fuse_server::rate_limit::DatasourceLimiter::new()),
        adaptive_parallelism: Arc::new(fuse_server::adaptive_parallelism::AdaptiveParallelism::new()),
        otel_store: None,
        query_recorder: Arc::new(fuse_server::query_replay::QueryRecorder::new(100)),
        webhook_registry: Arc::new(fuse_server::webhook::WebhookRegistry::new()),
        compilation_cache: Arc::new(fuse_server::query_compilation::CompilationCache::new(300, 5000)),
        cdc_tracker: Arc::new(fuse_server::cdc::CdcTracker::new(1000)),
        adaptive_cache: Arc::new(fuse_server::adaptive_cache::AdaptiveCache::new(60, 3, 10000)),
        column_rbac: None,
        key_rotation: Arc::new(fuse_server::auth::KeyRotationManager::new(vec![])),
        schema_cache: Arc::new(fuse_server::api::SchemaCache::new(300)),
        smart_router: Arc::new(fuse_server::smart_routing::SmartRouter::new()),
        health_history: Arc::new(fuse_server::connector_health_history::HealthHistory::new()),
        pool_tracker: Arc::new(fuse_server::pool_stats::PoolStatsTracker::new()),
    });
    fuse_server::build_router(state)
}

async fn get(app: axum::Router, path: &str) -> (StatusCode, String) {
    let req = Request::builder().uri(path).body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1_000_000).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

async fn get_json(app: axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
    let (status, body) = get(app, path).await;
    let json = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

// ── /api/fuse/health ──

#[tokio::test]
async fn test_health_returns_200() {
    let (status, _) = get(build_ops_app(), "/api/fuse/health").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn test_health_has_status_and_connectors() {
    let (_, json) = get_json(build_ops_app(), "/api/fuse/health").await;
    assert!(json["status"].is_string());
    assert!(json["connectors"].is_object());
}

#[tokio::test]
async fn test_health_connector_has_status() {
    let (_, json) = get_json(build_ops_app(), "/api/fuse/health").await;
    let conn = &json["connectors"]["ops_ds"];
    assert_eq!(conn["status"].as_str().unwrap(), "healthy");
}

#[tokio::test]
async fn test_health_connector_has_latency() {
    let (_, json) = get_json(build_ops_app(), "/api/fuse/health").await;
    assert!(json["connectors"]["ops_ds"]["latency_ms"].is_number());
}

// ── /api/fuse/datasources ──

#[tokio::test]
async fn test_datasources_returns_list() {
    let (status, json) = get_json(build_ops_app(), "/api/fuse/datasources").await;
    assert_eq!(status, StatusCode::OK);
    let arr = json.as_array().unwrap();
    assert_eq!(arr.len(), 1);
}

#[tokio::test]
async fn test_datasources_entry_has_id() {
    let (_, json) = get_json(build_ops_app(), "/api/fuse/datasources").await;
    let ds = &json[0];
    assert!(ds["id"].as_str().unwrap_or("") == "ops_ds" || ds["name"].as_str().unwrap_or("") == "ops_ds");
}

// ── /api/fuse/stats ──

#[tokio::test]
async fn test_stats_returns_200() {
    let (status, _) = get_json(build_ops_app(), "/api/fuse/stats").await;
    assert_eq!(status, StatusCode::OK);
}

// ── /api/fuse/queries/running ──

#[tokio::test]
async fn test_running_queries_empty() {
    let (status, json) = get_json(build_ops_app(), "/api/fuse/queries/running").await;
    assert_eq!(status, StatusCode::OK);
    let is_empty = json.as_array().map(|a| a.is_empty()).unwrap_or(false)
        || json["running"].as_array().map(|a| a.is_empty()).unwrap_or(false)
        || json["queries"].as_array().map(|a| a.is_empty()).unwrap_or(false);
    assert!(is_empty, "expected empty running queries: {:?}", json);
}

// ── HTML pages ──

#[tokio::test]
async fn test_status_page_serves_html() {
    let (status, body) = get(build_ops_app(), "/status").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("<!DOCTYPE html>") || body.contains("<html"));
}

#[tokio::test]
async fn test_admin_page_serves_html() {
    let (status, body) = get(build_ops_app(), "/admin").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("<!DOCTYPE html>") || body.contains("<html"));
}

#[tokio::test]
async fn test_settings_page_serves_html() {
    let (status, body) = get(build_ops_app(), "/settings").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("<!DOCTYPE html>") || body.contains("<html"));
}

#[tokio::test]
async fn test_status_page_has_viewport() {
    let (_, body) = get(build_ops_app(), "/status").await;
    assert!(body.contains("viewport"));
}

#[tokio::test]
async fn test_playground_index_serves() {
    let (status, body) = get(build_ops_app(), "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("<html") || body.contains("Fuse"));
}

// ── /metrics — requires metrics::init() which is done in main() ──
// These are covered by E2E tests (ops_api_smoke_test.sh) against a running server.

// ── Error handling ──

#[tokio::test]
async fn test_unknown_api_returns_404() {
    let (status, _) = get(build_ops_app(), "/api/fuse/nonexistent").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_invalid_json_returns_error() {
    let app = build_ops_app();
    let req = Request::builder()
        .method("POST")
        .uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from("not-json"))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status().is_client_error() || resp.status().is_server_error());
}
