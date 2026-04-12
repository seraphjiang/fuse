// SPDX-License-Identifier: Apache-2.0
//! Integration tests for the GraphQL HTTP endpoint: POST /api/fuse/graphql

use std::sync::Arc;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use fuse_core::registry::ConnectorRegistry;
use fuse_server::api::{AppState, RunningQueries};
use fuse_server::history::QueryHistory;
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
        adaptive_parallelism: Arc::new(fuse_server::adaptive_parallelism::AdaptiveParallelism::new()),
        webhook_registry: Arc::new(fuse_server::webhook::WebhookRegistry::new()),
        compilation_cache: Arc::new(fuse_server::query_compilation::CompilationCache::new(300, 5000)),
        cdc_tracker: Arc::new(fuse_server::cdc::CdcTracker::new(1000)),
        adaptive_cache: Arc::new(fuse_server::adaptive_cache::AdaptiveCache::new(60, 3, 10000)), column_rbac: None, key_rotation: std::sync::Arc::new(fuse_server::auth::KeyRotationManager::new(vec![])),
    });
    fuse_server::build_router(state)
}

async fn json_body(resp: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000).await.unwrap();
    serde_json::from_slice(&bytes).unwrap_or_default()
}

fn gql_req(query: &str) -> Request<Body> {
    let body = serde_json::json!({"query": query});
    Request::builder()
        .method("POST").uri("/api/fuse/graphql")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap()
}

#[tokio::test]
async fn test_graphql_health_via_http() {
    let app = build_app();
    let resp = app.oneshot(gql_req("{ health }")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert_eq!(json["data"]["health"], "ok");
    assert!(json.get("errors").is_none() || json["errors"].is_null());
}

#[tokio::test]
async fn test_graphql_datasources_empty_via_http() {
    let app = build_app();
    let resp = app.oneshot(gql_req("{ datasources { id connectorType } }")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json["data"]["datasources"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_graphql_introspection_via_http() {
    let app = build_app();
    let resp = app.oneshot(gql_req("{ __schema { queryType { name } mutationType { name } } }")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert_eq!(json["data"]["__schema"]["queryType"]["name"], "QueryRoot");
    assert_eq!(json["data"]["__schema"]["mutationType"]["name"], "MutationRoot");
}

#[tokio::test]
async fn test_graphql_saved_query_crud_via_http() {
    let app = build_app();

    // Save
    let resp = app.clone().oneshot(gql_req(
        r#"mutation { saveQuery(input: { name: "q1", query: "SELECT 1", format: "sql" }) { name query } }"#
    )).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert_eq!(json["data"]["saveQuery"]["name"], "q1");

    // List
    let resp = app.clone().oneshot(gql_req("{ savedQueries { name query format } }")).await.unwrap();
    let json = json_body(resp).await;
    assert_eq!(json["data"]["savedQueries"][0]["name"], "q1");

    // Delete
    let resp = app.oneshot(gql_req(r#"mutation { deleteSavedQuery(name: "q1") }"#)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_graphql_view_crud_via_http() {
    let app = build_app();

    // Create view
    let resp = app.clone().oneshot(gql_req(
        r#"mutation { createView(input: { name: "v1", query: "SELECT 1" }) { name stale } }"#
    )).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert_eq!(json["data"]["createView"]["name"], "v1");
    assert_eq!(json["data"]["createView"]["stale"], true);

    // List views
    let resp = app.clone().oneshot(gql_req("{ views { name } }")).await.unwrap();
    let json = json_body(resp).await;
    assert_eq!(json["data"]["views"][0]["name"], "v1");

    // Delete view
    let resp = app.oneshot(gql_req(r#"mutation { deleteView(name: "v1") }"#)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_graphql_history_empty_via_http() {
    let app = build_app();
    let resp = app.oneshot(gql_req("{ history { query format latencyMs } }")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(json["data"]["history"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_graphql_invalid_query_returns_errors() {
    let app = build_app();
    let resp = app.oneshot(gql_req("{ nonExistentField }")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = json_body(resp).await;
    assert!(!json["errors"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_graphiql_playground_get() {
    let app = build_app();
    let resp = app.oneshot(Request::builder().uri("/api/fuse/graphql").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1_000_000).await.unwrap();
    let html = String::from_utf8_lossy(&bytes);
    assert!(html.contains("graphiql") || html.contains("GraphiQL") || html.contains("graphql"));
}
