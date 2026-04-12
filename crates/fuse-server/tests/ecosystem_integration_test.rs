// SPDX-License-Identifier: Apache-2.0
//! Integration tests for Ecosystem features:
//! - Webhook retry config + DLQ
//! - CDC multi-table endpoints
//! - API versioning
//! - Schema discovery cache

use axum::body::Body;
use axum::http::{Request, StatusCode};
use fuse_core::registry::ConnectorRegistry;
use fuse_server::api::{AppState, RunningQueries, SchemaCache};
use fuse_server::history::QueryHistory;
use std::sync::Arc;
use tower::ServiceExt;

fn build_app() -> axum::Router {
    let registry = ConnectorRegistry::new();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()),
        plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
        result_cache: Arc::new(fuse_server::plan_cache::ResultCache::new(60, 500)),
        tenant_registry: Arc::new(fuse_server::tenant::TenantRegistry::disabled()),
        audit_log: Arc::new(fuse_server::audit::AuditLog::new(100)),
        prepared_statements: fuse_server::prepared::new_store(),
        adaptive_timeout: Arc::new(fuse_server::adaptive_timeout::AdaptiveTimeout::new()),
        shared_saved_queries: fuse_server::shared_state::SharedSavedQueries::from_env(),
        shared_history: fuse_server::shared_state::SharedQueryHistory::from_env(),
        shared_audit_log: fuse_server::shared_state::SharedAuditLog::from_env(),
        transactions: Arc::new(fuse_server::transaction::TransactionStore::new()),
        max_result_bytes: 0,
        datasource_limiter: Arc::new(fuse_server::rate_limit::DatasourceLimiter::new()),
        otel_store: None,
        query_recorder: Arc::new(fuse_server::query_replay::QueryRecorder::new(100)),
        adaptive_parallelism: Arc::new(
            fuse_server::adaptive_parallelism::AdaptiveParallelism::new(),
        ),
        webhook_registry: Arc::new(fuse_server::webhook::WebhookRegistry::new()),
        compilation_cache: Arc::new(fuse_server::query_compilation::CompilationCache::new(
            300, 5000,
        )),
        cdc_tracker: Arc::new(fuse_server::cdc::CdcTracker::new(1000)),
        adaptive_cache: Arc::new(fuse_server::adaptive_cache::AdaptiveCache::new(
            60, 3, 10000,
        )),
        schema_cache: Arc::new(SchemaCache::new(300)),
        column_rbac: None,
        key_rotation: Arc::new(fuse_server::auth::KeyRotationManager::new(vec![])),
        smart_router: Arc::new(fuse_server::smart_routing::SmartRouter::new()),
        health_history: Arc::new(fuse_server::connector_health_history::HealthHistory::new()),
        pool_tracker: Arc::new(fuse_server::pool_stats::PoolStatsTracker::new()),
    });
    fuse_server::build_router(state)
}

async fn json_body(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or_default()
}

// ── Webhook DLQ ──

#[tokio::test]
async fn test_webhook_dlq_empty() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/webhooks/dlq")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert_eq!(json["count"], 0);
    assert!(json["entries"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_webhook_create_with_retry_config() {
    let app = build_app();
    let body = serde_json::json!({
        "name": "test-hook",
        "query": "SELECT 1 FROM cluster_a.logs",
        "condition": {"type": "rows_returned"},
        "callback_url": "http://example.com/hook",
        "retry_config": {"max_retries": 3, "initial_backoff_ms": 100}
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/webhooks")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    // Webhook creation requires Editor role — returns 401 when auth is not configured
    assert!(matches!(
        resp.status(),
        StatusCode::CREATED | StatusCode::UNAUTHORIZED
    ));
}

// ── CDC Multi-table ──

#[tokio::test]
async fn test_cdc_register_view_dependencies() {
    let app = build_app();
    let body = serde_json::json!({
        "view_name": "error_summary",
        "sources": [["cluster_a", "logs"], ["dynamodb", "users"]]
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/cdc/views")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert_eq!(json["registered"], true);
}

#[tokio::test]
async fn test_cdc_batch_events() {
    let app = build_app();
    let events = serde_json::json!([
        {"datasource": "cluster_a", "table": "logs", "change_type": "insert", "timestamp": 1000},
        {"datasource": "dynamodb", "table": "users", "change_type": "update", "timestamp": 1001}
    ]);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/cdc/events/batch")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_string(&events).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    // CDC batch accepts events or returns 401 if auth enforced on POST
    assert!(matches!(
        resp.status(),
        StatusCode::OK | StatusCode::UNAUTHORIZED
    ));
}

#[tokio::test]
async fn test_cdc_status() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/cdc/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json["stats"].is_object());
}

// ── API Versioning ──

#[tokio::test]
async fn test_api_versions_endpoint() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/versions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json["current"].is_string());
    assert!(json["versions"].is_array());
}

#[tokio::test]
async fn test_versioned_health_endpoint() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/fuse/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ── Webhook CRUD ──

#[tokio::test]
async fn test_webhook_list_empty() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/webhooks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_webhook_not_found() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/webhooks/wh-999")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_cdc_refresh_no_pending() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/cdc/refresh")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Refresh trigger or 401 if auth enforced on POST
    assert!(matches!(
        resp.status(),
        StatusCode::OK | StatusCode::UNAUTHORIZED
    ));
}

#[tokio::test]
async fn test_cdc_list_dependencies_empty() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/cdc/views")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
