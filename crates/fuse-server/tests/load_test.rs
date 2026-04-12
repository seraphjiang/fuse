// SPDX-License-Identifier: Apache-2.0
//! #541 Load test — 50+ concurrent queries against mock connectors.
//! Measures p50/p95/p99 latency for single-source, UNION ALL, and JOIN.

use std::sync::Arc;
use std::time::Instant;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorRegistry;
use fuse_server::api::{AppState, RunningQueries};

#[derive(Debug)]
struct LoadMockConnector { id: String }

#[async_trait]
impl FederatedConnector for LoadMockConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "load-mock" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None }
    }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![SchemaInfo { name: "logs".into(), schema_type: SchemaType::Table, estimated_row_count: Some(2) }])
    }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
        ]))
    }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
        ]));
        Ok(vec![RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["h1", "h2"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![200, 500])) as ArrayRef,
        ]).map_err(ConnectorError::query)?])
    }
    async fn execute_streaming(&self, q: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for b in self.execute(q).await? { tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?; }
        Ok(())
    }
}

fn build_load_app() -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(LoadMockConnector { id: "src_a".into() })).unwrap();
    registry.register(Arc::new(LoadMockConnector { id: "src_b".into() })).unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()),
        plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
        audit_log: Arc::new(fuse_server::audit::AuditLog::new(100)),
        tenant_registry: Arc::new(fuse_server::tenant::TenantRegistry::new(vec![])),
        result_cache: Arc::new(fuse_server::plan_cache::ResultCache::new(60, 500)),
        prepared_statements: fuse_server::prepared::new_store(),
        adaptive_timeout: Arc::new(fuse_server::adaptive_timeout::AdaptiveTimeout::new()),
        shared_saved_queries: fuse_server::shared_state::SharedSavedQueries::from_env(),
        shared_history: fuse_server::shared_state::SharedQueryHistory::from_env(),
        shared_audit_log: fuse_server::shared_state::SharedAuditLog::from_env(), transactions: std::sync::Arc::new(fuse_server::transaction::TransactionStore::new()), max_result_bytes: 0, datasource_limiter: std::sync::Arc::new(fuse_server::rate_limit::DatasourceLimiter::new()), adaptive_parallelism: std::sync::Arc::new(fuse_server::adaptive_parallelism::AdaptiveParallelism::new()), otel_store: None, query_recorder: std::sync::Arc::new(fuse_server::query_replay::QueryRecorder::new(100)), webhook_registry: std::sync::Arc::new(fuse_server::webhook::WebhookRegistry::new()), compilation_cache: std::sync::Arc::new(fuse_server::query_compilation::CompilationCache::new(300, 5000)),
    });
    fuse_server::build_router(state)
}

async fn fire_query(app: axum::Router, query: &str) -> (StatusCode, std::time::Duration) {
    let body = serde_json::json!({"query": query, "format": "sql"});
    let req = Request::builder()
        .method("POST").uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap();
    let start = Instant::now();
    let resp = app.oneshot(req).await.unwrap();
    (resp.status(), start.elapsed())
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

async fn run_load(query: &str, concurrency: usize) -> (usize, usize, u128, u128, u128) {
    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let app = build_load_app();
        let q = query.to_string();
        handles.push(tokio::spawn(async move { fire_query(app, &q).await }));
    }
    let mut ok = 0usize;
    let mut fail = 0usize;
    let mut latencies = Vec::new();
    for h in handles {
        let (status, dur) = h.await.unwrap();
        if status == StatusCode::OK { ok += 1; } else { fail += 1; }
        latencies.push(dur.as_millis());
    }
    latencies.sort();
    let p50 = percentile(&latencies, 50.0);
    let p95 = percentile(&latencies, 95.0);
    let p99 = percentile(&latencies, 99.0);
    (ok, fail, p50, p95, p99)
}

#[tokio::test]
async fn test_load_single_source_50_concurrent() {
    let (ok, fail, p50, p95, p99) = run_load(
        "SELECT * FROM src_a.logs LIMIT 10", 50
    ).await;
    eprintln!("Single-source 50c: ok={ok} fail={fail} p50={p50}ms p95={p95}ms p99={p99}ms");
    assert_eq!(fail, 0, "no failures expected");
    assert_eq!(ok, 50);
    assert!(p99 < 5000, "p99 should be < 5s, got {}ms", p99);
}

#[tokio::test]
async fn test_load_union_all_50_concurrent() {
    let (ok, fail, p50, p95, p99) = run_load(
        "SELECT host, status FROM src_a.logs UNION ALL SELECT host, status FROM src_b.logs", 50
    ).await;
    eprintln!("UNION ALL 50c: ok={ok} fail={fail} p50={p50}ms p95={p95}ms p99={p99}ms");
    assert_eq!(fail, 0, "no failures expected");
    assert_eq!(ok, 50);
    assert!(p99 < 5000, "p99 should be < 5s, got {}ms", p99);
}

#[tokio::test]
async fn test_load_join_50_concurrent() {
    let (ok, fail, p50, p95, p99) = run_load(
        "SELECT * FROM src_a.logs JOIN src_b.logs ON src_a.logs.host = src_b.logs.host", 50
    ).await;
    eprintln!("JOIN 50c: ok={ok} fail={fail} p50={p50}ms p95={p95}ms p99={p99}ms");
    assert_eq!(fail, 0, "no failures expected");
    assert_eq!(ok, 50);
    assert!(p99 < 5000, "p99 should be < 5s, got {}ms", p99);
}

#[tokio::test]
async fn test_load_mixed_workload_100_concurrent() {
    let queries = [
        "SELECT * FROM src_a.logs LIMIT 10",
        "SELECT host, status FROM src_a.logs UNION ALL SELECT host, status FROM src_b.logs",
        "SELECT * FROM src_a.logs JOIN src_b.logs ON src_a.logs.host = src_b.logs.host",
    ];
    let mut handles = Vec::new();
    for i in 0..100 {
        let app = build_load_app();
        let q = queries[i % 3].to_string();
        handles.push(tokio::spawn(async move { fire_query(app, &q).await }));
    }
    let mut ok = 0;
    let mut fail = 0;
    let mut latencies = Vec::new();
    for h in handles {
        let (status, dur) = h.await.unwrap();
        if status == StatusCode::OK { ok += 1; } else { fail += 1; }
        latencies.push(dur.as_millis());
    }
    latencies.sort();
    let p50 = percentile(&latencies, 50.0);
    let p95 = percentile(&latencies, 95.0);
    let p99 = percentile(&latencies, 99.0);
    eprintln!("Mixed 100c: ok={ok} fail={fail} p50={p50}ms p95={p95}ms p99={p99}ms");
    let error_rate = fail as f64 / 100.0;
    assert!(error_rate < 0.05, "error rate should be < 5%, got {:.1}%", error_rate * 100.0);
    assert!(p99 < 10000, "p99 should be < 10s, got {}ms", p99);
}
