// SPDX-License-Identifier: Apache-2.0
//! Integration tests for alert-rules CRUD API endpoints.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use fuse_core::registry::ConnectorRegistry;
use fuse_server::api::{AppState, RunningQueries};
use fuse_server::history::QueryHistory;
use std::sync::Arc;
use tower::ServiceExt;

fn build_app() -> axum::Router {
    let state = Arc::new(AppState {
        registry: Arc::new(ConnectorRegistry::new()),
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
        column_rbac: None,
        key_rotation: std::sync::Arc::new(fuse_server::auth::KeyRotationManager::new(vec![])),
        schema_cache: std::sync::Arc::new(fuse_server::api::SchemaCache::new(300)),
        health_history: std::sync::Arc::new(
            fuse_server::connector_health_history::HealthHistory::new(),
        ),
        pool_tracker: std::sync::Arc::new(fuse_server::pool_stats::PoolStatsTracker::new()),
        smart_router: std::sync::Arc::new(fuse_server::smart_routing::SmartRouter::new()),
    });
    fuse_server::build_router(state)
}

async fn json_body(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or_default()
}

fn rule_json(id: &str) -> serde_json::Value {
    serde_json::json!({"id": id, "name": format!("Rule {}", id), "metric": "latency_p95", "threshold": 1000.0, "window_secs": 60, "enabled": true})
}

fn post_json(uri: &str, body: &serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(body).unwrap()))
        .unwrap()
}

#[tokio::test]
async fn test_list_rules_empty() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/alert-rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(json_body(resp).await["rules"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn test_create_rule_returns_201() {
    let app = build_app();
    let resp = app
        .oneshot(post_json("/api/fuse/alert-rules", &rule_json("r1")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert_eq!(json_body(resp).await["created"], "r1");
}

#[tokio::test]
async fn test_create_rule_then_list() {
    let app = build_app();
    app.clone()
        .oneshot(post_json("/api/fuse/alert-rules", &rule_json("r1")))
        .await
        .unwrap();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/alert-rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let json = json_body(resp).await;
    assert_eq!(json["rules"].as_array().unwrap().len(), 1);
    assert_eq!(json["rules"][0]["id"], "r1");
}

#[tokio::test]
async fn test_create_duplicate_rule_returns_409() {
    let app = build_app();
    app.clone()
        .oneshot(post_json("/api/fuse/alert-rules", &rule_json("dup")))
        .await
        .unwrap();
    let resp = app
        .oneshot(post_json("/api/fuse/alert-rules", &rule_json("dup")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_create_rule_empty_id_returns_400() {
    let app = build_app();
    let mut body = rule_json("x");
    body["id"] = serde_json::json!("");
    let resp = app
        .oneshot(post_json("/api/fuse/alert-rules", &body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_delete_existing_rule() {
    let app = build_app();
    app.clone()
        .oneshot(post_json("/api/fuse/alert-rules", &rule_json("del1")))
        .await
        .unwrap();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/fuse/alert-rules/del1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(json_body(resp).await["deleted"], "del1");
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/alert-rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(json_body(resp).await["rules"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn test_delete_nonexistent_rule() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/fuse/alert-rules/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(json_body(resp).await["error"]
        .as_str()
        .unwrap()
        .contains("not found"));
}

#[tokio::test]
async fn test_active_alerts_empty() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/alert-rules/active")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(json_body(resp).await["active"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn test_alert_history_empty() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/alert-rules/history")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(json_body(resp).await["history"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn test_alert_history_with_max_param() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/alert-rules/history?max=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(json_body(resp).await["history"].as_array().is_some());
}

#[tokio::test]
async fn test_acknowledge_no_active_alert() {
    let app = build_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/alert-rules/nope/acknowledge")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(json_body(resp).await["error"]
        .as_str()
        .unwrap()
        .contains("no active alert"));
}

#[tokio::test]
async fn test_create_multiple_rules_and_list() {
    let app = build_app();
    for id in ["a", "b", "c"] {
        app.clone()
            .oneshot(post_json("/api/fuse/alert-rules", &rule_json(id)))
            .await
            .unwrap();
    }
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/alert-rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(json_body(resp).await["rules"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_create_delete_recreate_same_id() {
    let app = build_app();
    app.clone()
        .oneshot(post_json("/api/fuse/alert-rules", &rule_json("reuse")))
        .await
        .unwrap();
    app.clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/fuse/alert-rules/reuse")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let resp = app
        .oneshot(post_json("/api/fuse/alert-rules", &rule_json("reuse")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}
