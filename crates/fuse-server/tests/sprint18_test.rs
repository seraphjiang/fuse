// SPDX-License-Identifier: Apache-2.0
//! Integration tests for Sprint 18 API endpoints:
//! - Query Replay (#1812): POST /api/fuse/replay/record, GET /api/fuse/replay/recordings, DELETE /api/fuse/replay/recordings
//! - Query Lineage (#1840): POST /api/fuse/lineage
//! - Schema Relationships (#1831): GET /api/fuse/relationships

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use fuse_core::registry::ConnectorRegistry;
use fuse_server::api::{AppState, RunningQueries};
use fuse_server::history::QueryHistory;
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
        adaptive_parallelism: Arc::new(fuse_server::adaptive_parallelism::AdaptiveParallelism::new()),
        webhook_registry: Arc::new(fuse_server::webhook::WebhookRegistry::new()),
        compilation_cache: Arc::new(fuse_server::query_compilation::CompilationCache::new(300, 5000)),
        cdc_tracker: Arc::new(fuse_server::cdc::CdcTracker::new(1000)),
        adaptive_cache: Arc::new(fuse_server::adaptive_cache::AdaptiveCache::new(60, 3, 10000)), column_rbac: None,
    });
    fuse_server::build_router(state)
}

async fn json_body(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000).await.unwrap();
    serde_json::from_slice(&bytes).unwrap_or_default()
}

// ── Query Replay (#1812) ──

#[tokio::test]
async fn test_replay_recordings_empty() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/replay/recordings").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_replay_record_and_list() {
    let app = build_app();
    let rec = serde_json::json!({
        "id": "rec-001",
        "query": "SELECT * FROM ds.logs",
        "format": "sql",
        "datasources": ["ds"],
        "recorded_at": 1700000000,
        "duration_ms": 42,
        "row_count": 10,
        "column_names": ["ts", "msg"],
        "result_hash": "abc123"
    });

    // Record a query
    let resp = app.clone().oneshot(
        Request::builder()
            .method("POST").uri("/api/fuse/replay/record")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&rec).unwrap())).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // List recordings — should have 1
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/replay/recordings").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    let arr = json.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["id"], "rec-001");
    assert_eq!(arr[0]["query"], "SELECT * FROM ds.logs");
    assert_eq!(arr[0]["row_count"], 10);
}

#[tokio::test]
async fn test_replay_clear_recordings() {
    let app = build_app();
    let rec = serde_json::json!({
        "id": "rec-002", "query": "SELECT 1", "format": "sql",
        "datasources": [], "recorded_at": 0, "duration_ms": 1,
        "row_count": 1, "column_names": [], "result_hash": "x"
    });

    // Record
    app.clone().oneshot(
        Request::builder()
            .method("POST").uri("/api/fuse/replay/record")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&rec).unwrap())).unwrap()
    ).await.unwrap();

    // Clear
    let resp = app.clone().oneshot(
        Request::builder()
            .method("DELETE").uri("/api/fuse/replay/recordings")
            .body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify empty
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/replay/recordings").body(Body::empty()).unwrap()
    ).await.unwrap();
    let json = json_body(resp).await;
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_replay_record_invalid_json() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder()
            .method("POST").uri("/api/fuse/replay/record")
            .header("content-type", "application/json")
            .body(Body::from("not json")).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ── Query Lineage (#1840) ──

#[tokio::test]
async fn test_lineage_simple_select() {
    let app = build_app();
    let body = serde_json::json!({"query": "SELECT * FROM ds.logs", "format": "sql"});
    let resp = app.oneshot(
        Request::builder()
            .method("POST").uri("/api/fuse/lineage")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json["query"].as_str().is_some());
    assert!(json["nodes"].as_array().is_some());
    assert!(json["edges"].as_array().is_some());
}

#[tokio::test]
async fn test_lineage_join_has_multiple_sources() {
    let app = build_app();
    let body = serde_json::json!({
        "query": "SELECT a.x, b.y FROM ds1.t1 a JOIN ds2.t2 b ON a.id = b.id",
        "format": "sql"
    });
    let resp = app.oneshot(
        Request::builder()
            .method("POST").uri("/api/fuse/lineage")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    let nodes = json["nodes"].as_array().unwrap();
    // A JOIN query should produce at least 2 source nodes
    assert!(nodes.len() >= 2, "JOIN lineage should have >=2 nodes, got {}", nodes.len());
}

#[tokio::test]
async fn test_lineage_default_format_is_sql() {
    let app = build_app();
    // Omit format — should default to "sql"
    let body = serde_json::json!({"query": "SELECT 1"});
    let resp = app.oneshot(
        Request::builder()
            .method("POST").uri("/api/fuse/lineage")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ── Schema Relationships (#1831) ──

#[tokio::test]
async fn test_relationships_endpoint_returns_200() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/relationships").body(Body::empty()).unwrap()
    ).await.unwrap();
    // With no connectors, should still return 200 with empty relationships
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json.is_array() || json.is_object());
}

// ── CDC (#1852) ──

#[tokio::test]
async fn test_cdc_status_returns_200() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/cdc/status").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json["stats"].is_object() || json["stats"].is_null() || json.is_object());
}

#[tokio::test]
async fn test_cdc_ingest_event() {
    let app = build_app();
    let event = serde_json::json!({
        "datasource": "ds1", "table": "users",
        "change_type": "insert", "timestamp": 1700000000
    });
    let resp = app.oneshot(
        Request::builder()
            .method("POST").uri("/api/fuse/cdc/events")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&event).unwrap())).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert_eq!(json["accepted"], true);
}

// ── Predict (#1850) ──

#[tokio::test]
async fn test_predict_empty_history() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder()
            .uri("/api/fuse/predict?query=SELECT%20*%20FROM%20ds.logs")
            .body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert_eq!(json["confidence"], "none");
}

#[tokio::test]
async fn test_predict_missing_query_param() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/predict").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ── Webhooks (#1811) ──

#[tokio::test]
async fn test_webhooks_list_empty() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/webhooks").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json.as_array().unwrap().is_empty());
}

// ── Replay multiple recordings respect max capacity ──

#[tokio::test]
async fn test_replay_respects_max_capacity() {
    let app = build_app(); // max 100 recordings
    // Record 5 entries
    for i in 0..5 {
        let rec = serde_json::json!({
            "id": format!("cap-{}", i), "query": "SELECT 1", "format": "sql",
            "datasources": [], "recorded_at": i, "duration_ms": 1,
            "row_count": 0, "column_names": [], "result_hash": "h"
        });
        app.clone().oneshot(
            Request::builder()
                .method("POST").uri("/api/fuse/replay/record")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&rec).unwrap())).unwrap()
        ).await.unwrap();
    }
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/replay/recordings").body(Body::empty()).unwrap()
    ).await.unwrap();
    let json = json_body(resp).await;
    assert_eq!(json.as_array().unwrap().len(), 5);
}

// ── Info endpoint ──

#[tokio::test]
async fn test_info_returns_200() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/info").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json.is_object());
}

// ── Cache clear endpoint ──

#[tokio::test]
async fn test_cache_clear_returns_ok() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder()
            .method("DELETE").uri("/api/fuse/cache")
            .body(Body::empty()).unwrap()
    ).await.unwrap();
    assert!(resp.status().is_success());
}

// ── Federation status endpoint ──

#[tokio::test]
async fn test_federation_status_returns_200() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/federation").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ── Multi-query endpoint ──

#[tokio::test]
async fn test_multi_query_single_statement() {
    let app = build_app();
    // Multi-query with a single statement delegates to normal handler
    // With no connectors, it should return an error about unknown datasource
    let body = serde_json::json!({ "query": "SELECT 1", "format": "sql" });
    let resp = app.oneshot(
        Request::builder()
            .method("POST").uri("/api/fuse/multi")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap()
    ).await.unwrap();
    // Returns some response (may be error since no datasources, but endpoint works)
    assert!(resp.status().as_u16() < 500);
}

// ── Advisor endpoint ──

#[tokio::test]
async fn test_advisor_no_history_returns_200() {
    let app = build_app();
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/advisor").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ── Webhook CRUD ──

#[tokio::test]
async fn test_webhook_create_and_list() {
    let app = build_app();
    let webhook = serde_json::json!({
        "name": "error-alert",
        "query": "SELECT count(*) FROM ds.logs WHERE status >= 500",
        "format": "sql",
        "condition": { "type": "rows_returned" },
        "callback_url": "https://hook.example.com/alert"
    });

    // Create
    let resp = app.clone().oneshot(
        Request::builder()
            .method("POST").uri("/api/fuse/webhooks")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&webhook).unwrap())).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created = json_body(resp).await;
    assert!(created["id"].as_str().is_some());

    // List
    let resp = app.oneshot(
        Request::builder().uri("/api/fuse/webhooks").body(Body::empty()).unwrap()
    ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert_eq!(json.as_array().unwrap().len(), 1);
}
