// SPDX-License-Identifier: Apache-2.0
//! UI regression tests for NL-to-SQL, anomaly detection, and query advisor pages.

use std::sync::Arc;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorRegistry;
use fuse_server::api::{AppState, RunningQueries, SchemaCache};
use fuse_server::history::{HistoryEntry, QueryHistory};
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

#[derive(Debug)]
struct MockConn { id: String, tables: Vec<String> }
impl MockConn {
    fn new(id: &str, tables: Vec<&str>) -> Self {
        Self { id: id.into(), tables: tables.into_iter().map(String::from).collect() }
    }
}

#[async_trait]
impl FederatedConnector for MockConn {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "mock" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None }
    }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(self.tables.iter().map(|t| SchemaInfo {
            name: t.clone(), schema_type: SchemaType::Table, estimated_row_count: Some(100),
        }).collect())
    }
    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        if self.tables.contains(&table.to_string()) {
            Ok(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("status", DataType::Int64, true),
                Field::new("timestamp", DataType::Utf8, true),
                Field::new("host", DataType::Utf8, true),
            ]))
        } else {
            Err(ConnectorError::schema(format!("table '{}' not found", table)))
        }
    }
    async fn execute(&self, _q: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        Ok(vec![RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["r1"]))]).unwrap()])
    }
    async fn execute_streaming(&self, q: &SubQuery, tx: tokio::sync::mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for b in self.execute(q).await? { tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?; }
        Ok(())
    }
}

fn app() -> axum::Router { app_with(Arc::new(QueryHistory::new())) }

fn app_with(history: Arc<QueryHistory>) -> axum::Router {
    let reg = ConnectorRegistry::new();
    reg.register(Arc::new(MockConn::new("cluster_a", vec!["logs", "metrics"]))).unwrap();
    reg.register(Arc::new(MockConn::new("dynamodb", vec!["user_profiles"]))).unwrap();
    let pt = Arc::new(fuse_server::pool_stats::PoolStatsTracker::new());
    let state = Arc::new(AppState {
        registry: Arc::new(reg),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history,
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()),
        plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
        result_cache: Arc::new(fuse_server::plan_cache::ResultCache::new(60, 500)),
        tenant_registry: Arc::new(fuse_server::tenant::TenantRegistry::disabled()),
        audit_log: Arc::new(fuse_server::audit::AuditLog::new(10000)),
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
        schema_cache: Arc::new(SchemaCache::new(300)),
        smart_router: Arc::new(fuse_server::smart_routing::SmartRouter::new()),
        health_history: Arc::new(fuse_server::connector_health_history::HealthHistory::new()),
        pool_tracker: pt,
        feedback_store: Arc::new(fuse_server::feedback::FeedbackStore::new(100)),
    });
    fuse_server::build_router(state)
}

async fn post(app: axum::Router, uri: &str, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
    let r = app.oneshot(Request::builder().method("POST").uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap())).unwrap()).await.unwrap();
    let s = r.status();
    let b = axum::body::to_bytes(r.into_body(), usize::MAX).await.unwrap();
    (s, serde_json::from_slice(&b).unwrap_or(serde_json::json!(null)))
}

async fn get(app: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let r = app.oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap()).await.unwrap();
    let s = r.status();
    let b = axum::body::to_bytes(r.into_body(), usize::MAX).await.unwrap();
    (s, serde_json::from_slice(&b).unwrap_or(serde_json::json!(null)))
}

fn he(query: &str, lat: u64, rows: u64, err: Option<&str>) -> HistoryEntry {
    HistoryEntry { query: query.into(), format: "sql".into(), timestamp: 1700000000, latency_ms: lat, row_count: rows, error: err.map(String::from) }
}

// ── NL-to-SQL (POST /api/fuse/nl) ──

#[tokio::test]
async fn test_nl_basic_returns_sql() {
    let (s, j) = post(app(), "/api/fuse/nl", serde_json::json!({"question": "show me all logs"})).await;
    assert_eq!(s, StatusCode::OK);
    assert!(j["generated_sql"].as_str().unwrap().contains("SELECT"));
    assert_eq!(j["question"], "show me all logs");
    assert!(!j["prompt"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn test_nl_includes_schema_context() {
    let (_, j) = post(app(), "/api/fuse/nl", serde_json::json!({"question": "show errors"})).await;
    let ctx = j["schema_context"].as_array().unwrap();
    assert!(ctx.len() >= 2);
}

#[tokio::test]
async fn test_nl_count_by_generates_group_by() {
    let (_, j) = post(app(), "/api/fuse/nl", serde_json::json!({"question": "count logs by host"})).await;
    let sql = j["generated_sql"].as_str().unwrap().to_uppercase();
    assert!(sql.contains("GROUP BY") && sql.contains("COUNT"));
}

#[tokio::test]
async fn test_nl_error_query_filters_status() {
    let (_, j) = post(app(), "/api/fuse/nl", serde_json::json!({"question": "show me errors"})).await;
    assert!(j["generated_sql"].as_str().unwrap().contains("500"));
}

#[tokio::test]
async fn test_nl_top_n_generates_limit() {
    let (_, j) = post(app(), "/api/fuse/nl", serde_json::json!({"question": "top 5 recent logs"})).await;
    assert!(j["generated_sql"].as_str().unwrap().contains("LIMIT 5"));
}

#[tokio::test]
async fn test_nl_no_execute_omits_results() {
    let (_, j) = post(app(), "/api/fuse/nl", serde_json::json!({"question": "show logs", "execute": false})).await;
    assert!(j.get("results").is_none() || j["results"].is_null());
}

#[tokio::test]
async fn test_nl_response_required_fields() {
    let (_, j) = post(app(), "/api/fuse/nl", serde_json::json!({"question": "x"})).await;
    for f in &["question", "generated_sql", "schema_context", "prompt"] {
        assert!(j.get(*f).is_some(), "missing: {}", f);
    }
}

// ── Query Advisor (GET /api/fuse/advisor) ──

#[tokio::test]
async fn test_advisor_empty_history() {
    let (s, j) = get(app(), "/api/fuse/advisor").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["analyzed_queries"], 0);
    assert!(j["advice"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_advisor_detects_missing_limit() {
    let h = Arc::new(QueryHistory::new());
    for _ in 0..20 { h.push(he("SELECT * FROM cluster_a.logs LIMIT 10", 10, 10, None)); }
    h.push(he("SELECT * FROM cluster_a.logs", 2000, 5000, None));
    h.push(he("SELECT * FROM cluster_a.logs", 3000, 5000, None));
    let (_, j) = get(app_with(h), "/api/fuse/advisor").await;
    let cats: Vec<&str> = j["advice"].as_array().unwrap().iter().filter_map(|a| a["category"].as_str()).collect();
    assert!(cats.contains(&"missing_limit"));
}

#[tokio::test]
async fn test_advisor_detects_high_error_rate() {
    let h = Arc::new(QueryHistory::new());
    for _ in 0..5 { h.push(he("SELECT 1", 10, 0, Some("refused"))); }
    for _ in 0..5 { h.push(he("SELECT 1", 10, 100, None)); }
    let (_, j) = get(app_with(h), "/api/fuse/advisor").await;
    let cats: Vec<&str> = j["advice"].as_array().unwrap().iter().filter_map(|a| a["category"].as_str()).collect();
    assert!(cats.contains(&"high_error_rate"));
}

#[tokio::test]
async fn test_advisor_detects_cache_opportunity() {
    let h = Arc::new(QueryHistory::new());
    for _ in 0..5 { h.push(he("SELECT * FROM cluster_a.logs LIMIT 10", 10, 10, None)); }
    let (_, j) = get(app_with(h), "/api/fuse/advisor").await;
    let cats: Vec<&str> = j["advice"].as_array().unwrap().iter().filter_map(|a| a["category"].as_str()).collect();
    assert!(cats.contains(&"cache_opportunity"));
}

#[tokio::test]
async fn test_advisor_response_shape() {
    let h = Arc::new(QueryHistory::new());
    h.push(he("SELECT 1", 5, 1, None));
    let (s, j) = get(app_with(h), "/api/fuse/advisor").await;
    assert_eq!(s, StatusCode::OK);
    assert!(j.get("advice").is_some());
    assert_eq!(j["analyzed_queries"], 1);
}

// ── Anomaly Detection (GET /api/fuse/anomaly) ──

#[tokio::test]
async fn test_anomaly_empty_history() {
    let (s, j) = get(app(), "/api/fuse/anomaly").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["analyzed_queries"], 0);
    assert_eq!(j["analyzed_datasources"], 0);
    assert!(j["anomalies"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_anomaly_response_shape() {
    let h = Arc::new(QueryHistory::new());
    h.push(he("SELECT * FROM cluster_a.logs", 50, 100, None));
    let (s, j) = get(app_with(h), "/api/fuse/anomaly").await;
    assert_eq!(s, StatusCode::OK);
    for f in &["anomalies", "analyzed_datasources", "analyzed_queries", "tolerance"] {
        assert!(j.get(*f).is_some(), "missing: {}", f);
    }
}

#[tokio::test]
async fn test_anomaly_consistent_latency_clean() {
    let h = Arc::new(QueryHistory::new());
    for i in 0..10u64 {
        h.push(HistoryEntry { query: "SELECT * FROM cluster_a.logs".into(), format: "sql".into(), timestamp: 1700000000 + i, latency_ms: 50, row_count: 100, error: None });
    }
    let (_, j) = get(app_with(h), "/api/fuse/anomaly").await;
    assert!(j["anomalies"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_anomaly_spike_detected() {
    let h = Arc::new(QueryHistory::new());
    // recent() returns newest-first, anomaly handler uses points.last() as current.
    // So push the spike first (oldest) — it becomes last after reversal.
    h.push(HistoryEntry { query: "SELECT * FROM cluster_a.logs".into(), format: "sql".into(), timestamp: 1700000000, latency_ms: 5000, row_count: 100, error: None });
    for i in 1..=20u64 {
        h.push(HistoryEntry { query: "SELECT * FROM cluster_a.logs".into(), format: "sql".into(), timestamp: 1700000000 + i, latency_ms: 40 + (i % 3) * 10, row_count: 100, error: None });
    }
    let (_, j) = get(app_with(h), "/api/fuse/anomaly").await;
    let a = j["anomalies"].as_array().unwrap();
    assert!(!a.is_empty(), "spike should trigger anomaly");
    for item in a {
        assert!(item.get("datasource").is_some());
        assert!(item.get("kind").is_some());
        assert!(item.get("message").is_some());
        assert!(item.get("severity").is_some());
    }
}

#[tokio::test]
async fn test_anomaly_tolerance_param() {
    let h = Arc::new(QueryHistory::new());
    h.push(he("SELECT * FROM cluster_a.logs", 50, 100, None));
    let (_, j) = get(app_with(h), "/api/fuse/anomaly?tolerance=1.0").await;
    assert_eq!(j["tolerance"], 1.0);
}

#[tokio::test]
async fn test_anomaly_limit_param() {
    let h = Arc::new(QueryHistory::new());
    for i in 0..10u64 {
        h.push(HistoryEntry { query: "SELECT * FROM cluster_a.logs".into(), format: "sql".into(), timestamp: 1700000000 + i, latency_ms: 50, row_count: 100, error: None });
    }
    let (_, j) = get(app_with(h), "/api/fuse/anomaly?limit=5").await;
    assert!(j["analyzed_queries"].as_u64().unwrap() <= 5);
}

// ── Page Load Smoke Tests ──

#[tokio::test]
async fn test_index_page_loads() {
    let r = app().oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_dashboard_page_loads() {
    let r = app().oneshot(Request::builder().uri("/dashboard").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_cost_page_loads() {
    let r = app().oneshot(Request::builder().uri("/cost").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
}

// ── generate_sql_from_nl unit tests ──

#[test]
fn test_gen_sql_fallback() {
    let s = vec![fuse_server::api::DatasourceSchema { datasource: "ds".into(), tables: vec!["ev".into()] }];
    let sql = fuse_server::api::generate_sql_from_nl("anything", &s);
    assert!(sql.to_uppercase().starts_with("SELECT"));
    assert!(sql.contains("ds") && sql.contains("ev"));
}

#[test]
fn test_gen_sql_count_by() {
    let s = vec![fuse_server::api::DatasourceSchema { datasource: "ds".into(), tables: vec!["logs".into()] }];
    let sql = fuse_server::api::generate_sql_from_nl("count events by service", &s).to_uppercase();
    assert!(sql.contains("COUNT") && sql.contains("GROUP BY"));
}

#[test]
fn test_gen_sql_top_n() {
    let s = vec![fuse_server::api::DatasourceSchema { datasource: "ds".into(), tables: vec!["logs".into()] }];
    assert!(fuse_server::api::generate_sql_from_nl("top 7 entries", &s).contains("LIMIT 7"));
}

#[test]
fn test_gen_sql_error_filter() {
    let s = vec![fuse_server::api::DatasourceSchema { datasource: "ds".into(), tables: vec!["logs".into()] }];
    assert!(fuse_server::api::generate_sql_from_nl("show errors", &s).contains("500"));
}

#[test]
fn test_gen_sql_empty_schemas() {
    assert!(fuse_server::api::generate_sql_from_nl("show data", &[]).to_uppercase().contains("SELECT"));
}
