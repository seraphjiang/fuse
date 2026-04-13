// SPDX-License-Identifier: Apache-2.0
//! API integration tests validating UI backend contracts.
//!
//! Each playground page depends on specific API endpoints. These tests verify
//! those contracts so UI regressions are caught at the Rust layer.

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

// ── Mock connector ──

#[derive(Debug)]
struct UiMock(String);

#[async_trait]
impl FederatedConnector for UiMock {
    fn id(&self) -> &str {
        &self.0
    }
    fn connector_type(&self) -> &str {
        "mock"
    }
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::full()
    }
    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth {
            status: HealthStatus::Healthy,
            latency_ms: Some(1),
            message: None,
        }
    }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![SchemaInfo {
            name: "logs".into(),
            schema_type: SchemaType::Table,
            estimated_row_count: Some(10),
        }])
    }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
        ]))
    }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let schema = Arc::new(self.get_schema("").await?);
        Ok(vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["h1", "h2"])),
                Arc::new(Int64Array::from(vec![200, 500])),
            ],
        )
        .map_err(ConnectorError::query)?])
    }
    async fn execute_streaming(
        &self,
        q: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for b in self.execute(q).await? {
            tx.send(Ok(b))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

fn build_ui_app() -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(UiMock("ds1".into()))).unwrap();
    registry.register(Arc::new(UiMock("ds2".into()))).unwrap();
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
        adaptive_parallelism: Arc::new(
            fuse_server::adaptive_parallelism::AdaptiveParallelism::new(),
        ),
        otel_store: None,
        query_recorder: Arc::new(fuse_server::query_replay::QueryRecorder::new(100)),
        webhook_registry: Arc::new(fuse_server::webhook::WebhookRegistry::new()),
        compilation_cache: Arc::new(fuse_server::query_compilation::CompilationCache::new(
            300, 5000,
        )),
        cdc_tracker: Arc::new(fuse_server::cdc::CdcTracker::new(1000)),
        adaptive_cache: Arc::new(fuse_server::adaptive_cache::AdaptiveCache::new(
            60, 3, 10000,
        )),
        column_rbac: None,
        key_rotation: Arc::new(fuse_server::auth::KeyRotationManager::new(vec![])),
        schema_cache: Arc::new(fuse_server::api::SchemaCache::new(300)),
        smart_router: Arc::new(fuse_server::smart_routing::SmartRouter::new()),
        health_history: Arc::new(fuse_server::connector_health_history::HealthHistory::new()),
        pool_tracker: Arc::new(fuse_server::pool_stats::PoolStatsTracker::new()),
        feedback_store: Arc::new(fuse_server::feedback::FeedbackStore::new(100)),
    });
    fuse_server::build_router(state)
}

async fn get_json(app: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

async fn post_json(
    app: axum::Router,
    uri: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

async fn assert_html(app: axum::Router, path: &str) {
    let resp = app
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "{} not 200", path);
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let html = String::from_utf8_lossy(&body);
    assert!(
        html.contains("<html") || html.contains("<!DOCTYPE"),
        "{} not HTML",
        path
    );
}

// ── /federation page → GET /api/fuse/federation ──

#[tokio::test]
async fn test_federation_topology() {
    let (s, j) = get_json(build_ui_app(), "/api/fuse/federation").await;
    assert_eq!(s, StatusCode::OK);
    // federation.html reads instances array for topology graph
    assert!(j.get("instances").is_some(), "federation must have instances: {:?}", j);
}

// ── /lineage page → POST /api/fuse/lineage ──

#[tokio::test]
async fn test_lineage_graph() {
    let (s, j) = post_json(
        build_ui_app(),
        "/api/fuse/lineage",
        serde_json::json!({"query": "SELECT * FROM ds1.logs", "format": "sql"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        j.get("nodes").is_some() || j.get("lineage").is_some(),
        "lineage must have graph: {:?}",
        j
    );
}

// ── /replay page → GET /api/fuse/replay/recordings ──

#[tokio::test]
async fn test_replay_recordings_empty() {
    let (s, j) = get_json(build_ui_app(), "/api/fuse/replay/recordings").await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.is_array());
    assert!(j.as_array().unwrap().is_empty());
}

// ── /webhooks page → GET/POST /api/fuse/webhooks ──

#[tokio::test]
async fn test_webhooks_list_empty() {
    let (s, j) = get_json(build_ui_app(), "/api/fuse/webhooks").await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.is_array());
}

#[tokio::test]
async fn test_webhooks_create_and_list() {
    let app = build_ui_app();
    let (s, _) = post_json(
        app.clone(),
        "/api/fuse/webhooks",
        serde_json::json!({
            "name": "test_hook",
            "query": "SELECT * FROM ds1.logs WHERE status >= 500",
            "format": "sql",
            "condition": {"type": "rows_returned"},
            "callback_url": "https://example.com/hook"
        }),
    )
    .await;
    assert!(s == StatusCode::OK || s == StatusCode::CREATED, "create: {}", s);

    let (s, j) = get_json(app, "/api/fuse/webhooks").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j.as_array().unwrap().len(), 1);
    assert_eq!(j[0]["callback_url"], "https://example.com/hook");
}

// ── /graphql page → POST /api/fuse/graphql ──

#[tokio::test]
async fn test_graphql_introspection() {
    let (s, j) = post_json(
        build_ui_app(),
        "/api/fuse/graphql",
        serde_json::json!({"query": "{ __schema { queryType { name } } }"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.get("data").is_some(), "graphql must have data: {:?}", j);
}

// ── feedback widget → POST/GET /api/fuse/feedback ──

#[tokio::test]
async fn test_feedback_list_empty() {
    let (s, j) = get_json(build_ui_app(), "/api/fuse/feedback").await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.is_array());
}

#[tokio::test]
async fn test_feedback_submit_and_list() {
    let app = build_ui_app();
    let (s, _) = post_json(
        app.clone(),
        "/api/fuse/feedback",
        serde_json::json!({
            "type": "bug",
            "title": "JOIN timeout",
            "description": "timeout on large JOINs",
            "page": "/playground"
        }),
    )
    .await;
    assert!(s == StatusCode::OK || s == StatusCode::CREATED, "submit: {}", s);
}

// ── GET /api/fuse/info ──

#[tokio::test]
async fn test_info_endpoint() {
    let (s, j) = get_json(build_ui_app(), "/api/fuse/info").await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.get("version").is_some() || j.get("name").is_some(), "info: {:?}", j);
}

// ── GET /api/fuse/audit ──

#[tokio::test]
async fn test_audit_empty() {
    let (s, j) = get_json(build_ui_app(), "/api/fuse/audit").await;
    assert_eq!(s, StatusCode::OK);
    // audit returns {entries: [], count: 0}
    assert!(j.get("entries").is_some() || j.is_array(), "audit: {:?}", j);
}

// ── POST /api/fuse/nl (NL-to-SQL) ──

#[tokio::test]
async fn test_nl_to_sql() {
    let (s, j) = post_json(
        build_ui_app(),
        "/api/fuse/nl",
        serde_json::json!({"question": "count errors by host"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        !j["generated_sql"].as_str().unwrap_or("").is_empty(),
        "nl: {:?}",
        j
    );
}

// ── POST /api/fuse/query/diff ──

#[tokio::test]
async fn test_query_diff() {
    let (s, j) = post_json(
        build_ui_app(),
        "/api/fuse/query/diff",
        serde_json::json!({
            "query_a": "SELECT * FROM ds1.logs",
            "query_b": "SELECT * FROM ds1.logs WHERE status = 500",
            "format": "sql"
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        j.get("diff").is_some() || j.get("result_a").is_some() || j.get("added").is_some(),
        "diff: {:?}",
        j
    );
}

// ── POST /api/fuse/query/export/csv ──

#[tokio::test]
async fn test_export_csv() {
    let resp = build_ui_app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/export/csv")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({"query": "SELECT * FROM ds1.logs", "format": "sql"})).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers().get("content-type").map(|v| v.to_str().unwrap_or("")).unwrap_or("");
    assert!(ct.contains("csv") || ct.contains("text") || ct.contains("octet"), "csv ct: {}", ct);
}

// ── POST /api/fuse/query/export/json ──

#[tokio::test]
async fn test_export_json() {
    let (s, j) = post_json(
        build_ui_app(),
        "/api/fuse/query/export/json",
        serde_json::json!({"query": "SELECT * FROM ds1.logs", "format": "sql"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.get("columns").is_some() || j.get("rows").is_some() || j.is_array(), "export: {:?}", j);
}

// ── GET /api/fuse/advisor ──

#[tokio::test]
async fn test_advisor() {
    let (s, j) = get_json(build_ui_app(), "/api/fuse/advisor").await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        j.get("advice").is_some() || j.is_array() || j.get("recommendations").is_some(),
        "advisor: {:?}",
        j
    );
}

// ── GET /api/fuse/anomaly ──

#[tokio::test]
async fn test_anomaly() {
    let (s, j) = get_json(build_ui_app(), "/api/fuse/anomaly").await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.is_array() || j.get("anomalies").is_some(), "anomaly: {:?}", j);
}

// ── GET /api/fuse/connectors/health-history ──

#[tokio::test]
async fn test_health_history() {
    let (s, j) = get_json(build_ui_app(), "/api/fuse/connectors/health-history").await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.is_array() || j.is_object(), "health-history: {:?}", j);
}

// ── GET /api/fuse/routing, /routing/stats ──

#[tokio::test]
async fn test_routing() {
    let (s, _) = get_json(build_ui_app(), "/api/fuse/routing").await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
async fn test_routing_stats() {
    let (s, _) = get_json(build_ui_app(), "/api/fuse/routing/stats").await;
    assert_eq!(s, StatusCode::OK);
}

// ── GET /api/fuse/similarity ──

#[tokio::test]
async fn test_similarity() {
    let (s, _) = get_json(build_ui_app(), "/api/fuse/similarity").await;
    assert_eq!(s, StatusCode::OK);
}

// ── POST /api/fuse/multi — takes semicolon-separated statements in QueryRequest ──

#[tokio::test]
async fn test_multi_query() {
    let (s, j) = post_json(
        build_ui_app(),
        "/api/fuse/multi",
        serde_json::json!({
            "query": "SELECT * FROM ds1.logs; SELECT * FROM ds2.logs",
            "format": "sql"
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.get("results").is_some() || j.is_array(), "multi: {:?}", j);
}

// ── DELETE /api/fuse/cache ──

#[tokio::test]
async fn test_cache_clear() {
    let resp = build_ui_app()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/fuse/cache")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.status() == StatusCode::OK || resp.status() == StatusCode::NO_CONTENT);
}

// ── /cost page → EXPLAIN via /api/fuse/query ──

#[tokio::test]
async fn test_cost_explain_query() {
    let (s, j) = post_json(
        build_ui_app(),
        "/api/fuse/query",
        serde_json::json!({"query": "EXPLAIN SELECT * FROM ds1.logs", "format": "sql"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.get("plan").is_some() || j.get("rows").is_some(), "explain: {:?}", j);
}

// ── /terminal page → full lifecycle ──

#[tokio::test]
async fn test_terminal_lifecycle() {
    let app = build_ui_app();
    // validate
    let (s, j) = post_json(
        app.clone(),
        "/api/fuse/query/validate",
        serde_json::json!({"query": "SELECT * FROM ds1.logs", "format": "sql"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["valid"], true);
    // explain
    let (s, j) = post_json(
        app.clone(),
        "/api/fuse/query/explain",
        serde_json::json!({"query": "SELECT * FROM ds1.logs", "format": "sql"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(j["plan"].is_string());
    // execute
    let (s, j) = post_json(
        app.clone(),
        "/api/fuse/query",
        serde_json::json!({"query": "SELECT * FROM ds1.logs", "format": "sql"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(!j["rows"].as_array().unwrap().is_empty());
    // health
    let (s, j) = get_json(app, "/api/fuse/health").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["status"], "healthy");
}

// ── HTML page smoke tests ──

#[tokio::test]
async fn test_page_federation() {
    assert_html(build_ui_app(), "/federation").await;
}

#[tokio::test]
async fn test_page_lineage() {
    assert_html(build_ui_app(), "/lineage").await;
}

#[tokio::test]
async fn test_page_replay() {
    assert_html(build_ui_app(), "/replay").await;
}

#[tokio::test]
async fn test_page_cost() {
    assert_html(build_ui_app(), "/cost").await;
}

#[tokio::test]
async fn test_page_graphql() {
    assert_html(build_ui_app(), "/graphql").await;
}

#[tokio::test]
async fn test_page_webhooks() {
    assert_html(build_ui_app(), "/webhooks").await;
}

#[tokio::test]
async fn test_page_quality() {
    assert_html(build_ui_app(), "/quality").await;
}

#[tokio::test]
async fn test_page_schedules() {
    assert_html(build_ui_app(), "/schedules").await;
}

#[tokio::test]
async fn test_page_terminal() {
    assert_html(build_ui_app(), "/terminal").await;
}

#[tokio::test]
async fn test_page_views() {
    assert_html(build_ui_app(), "/views").await;
}

#[tokio::test]
async fn test_page_plugins() {
    assert_html(build_ui_app(), "/plugins").await;
}

#[tokio::test]
async fn test_page_alerts() {
    assert_html(build_ui_app(), "/alerts").await;
}

#[tokio::test]
async fn test_page_changelog() {
    assert_html(build_ui_app(), "/changelog").await;
}

#[tokio::test]
async fn test_page_help() {
    assert_html(build_ui_app(), "/help").await;
}
