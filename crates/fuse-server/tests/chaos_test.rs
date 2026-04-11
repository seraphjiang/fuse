// SPDX-License-Identifier: Apache-2.0
//! #650 Chaos testing — connector failures, timeouts, partial degradation.

use std::sync::Arc;
use std::time::Duration;
use arrow::array::{ArrayRef, Int64Array, StringArray};
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

// ── Chaos connectors ──

fn mock_batch() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("status", DataType::Int64, false),
    ]));
    vec![RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["h1", "h2"])) as ArrayRef,
        Arc::new(Int64Array::from(vec![200, 500])) as ArrayRef,
    ]).unwrap()]
}

fn mock_schema() -> Schema {
    Schema::new(vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("status", DataType::Int64, false),
    ])
}

#[derive(Debug)]
struct HealthyConnector(String);
#[async_trait]
impl FederatedConnector for HealthyConnector {
    fn id(&self) -> &str { &self.0 }
    fn connector_type(&self) -> &str { "mock" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth { ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None } }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> { Ok(vec![SchemaInfo { name: "logs".into(), schema_type: SchemaType::Table, estimated_row_count: Some(2) }]) }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> { Ok(mock_schema()) }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> { Ok(mock_batch()) }
    async fn execute_streaming(&self, q: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for b in self.execute(q).await? { tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?; } Ok(())
    }
}

/// Connector that always fails with connection error.
#[derive(Debug)]
struct ConnectionRefusedConnector(String);
#[async_trait]
impl FederatedConnector for ConnectionRefusedConnector {
    fn id(&self) -> &str { &self.0 }
    fn connector_type(&self) -> &str { "chaos-refused" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth { ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some("connection refused".into()) } }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> { Ok(vec![SchemaInfo { name: "logs".into(), schema_type: SchemaType::Table, estimated_row_count: Some(0) }]) }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> { Ok(mock_schema()) }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> { Err(ConnectorError::query("connection refused")) }
    async fn execute_streaming(&self, _: &SubQuery, _: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> { Err(ConnectorError::query("connection refused")) }
}

/// Connector that hangs for 30s (simulates network partition).
#[derive(Debug)]
struct HangingConnector(String);
#[async_trait]
impl FederatedConnector for HangingConnector {
    fn id(&self) -> &str { &self.0 }
    fn connector_type(&self) -> &str { "chaos-hang" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth { ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None } }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> { Ok(vec![SchemaInfo { name: "logs".into(), schema_type: SchemaType::Table, estimated_row_count: Some(2) }]) }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> { Ok(mock_schema()) }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(mock_batch())
    }
    async fn execute_streaming(&self, q: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for b in self.execute(q).await? { tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?; } Ok(())
    }
}

/// Connector that panics on execute.
#[derive(Debug)]
struct PanicConnector(String);
#[async_trait]
impl FederatedConnector for PanicConnector {
    fn id(&self) -> &str { &self.0 }
    fn connector_type(&self) -> &str { "chaos-panic" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth { ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None } }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> { Ok(vec![SchemaInfo { name: "logs".into(), schema_type: SchemaType::Table, estimated_row_count: Some(2) }]) }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> { Ok(mock_schema()) }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> { panic!("connector crashed!") }
    async fn execute_streaming(&self, _: &SubQuery, _: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> { panic!("connector crashed!") }
}

fn build_app(connectors: Vec<Arc<dyn FederatedConnector>>) -> axum::Router {
    let registry = ConnectorRegistry::new();
    for c in connectors { registry.register(c).unwrap(); }
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
        result_cache: Arc::new(fuse_server::plan_cache::ResultCache::new(60, 100)),
        prepared_statements: fuse_server::prepared::new_store(),
        adaptive_timeout: Arc::new(fuse_server::adaptive_timeout::AdaptiveTimeout::new()),
        shared_saved_queries: fuse_server::shared_state::SharedSavedQueries::from_env(),
        shared_history: fuse_server::shared_state::SharedQueryHistory::from_env(),
        shared_audit_log: fuse_server::shared_state::SharedAuditLog::from_env(), transactions: std::sync::Arc::new(fuse_server::transaction::TransactionStore::new()), max_result_bytes: 0, datasource_limiter: std::sync::Arc::new(fuse_server::rate_limit::DatasourceLimiter::new()),
    });
    fuse_server::build_router(state)
}

async fn query(app: axum::Router, sql: &str) -> (StatusCode, serde_json::Value) {
    let body = serde_json::json!({"query": sql, "format": "sql"});
    let req = Request::builder()
        .method("POST").uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    (status, json)
}

// ── Tests ──

#[tokio::test]
async fn test_single_source_connection_refused() {
    let app = build_app(vec![Arc::new(ConnectionRefusedConnector("broken".into()))]);
    let (status, json) = query(app, "SELECT * FROM broken.logs").await;
    assert_ne!(status, StatusCode::OK);
    assert!(json["error"].as_str().unwrap_or("").contains("connection refused"));
}

#[tokio::test]
async fn test_union_one_healthy_one_refused() {
    let app = build_app(vec![
        Arc::new(HealthyConnector("good".into())),
        Arc::new(ConnectionRefusedConnector("bad".into())),
    ]);
    let (status, json) = query(app, "SELECT host, status FROM good.logs UNION ALL SELECT host, status FROM bad.logs").await;
    // Should return partial results or clear error — not crash
    let has_data = json["rows"].as_array().map(|r| !r.is_empty()).unwrap_or(false);
    let has_error = json["error"].is_string() || json["partial_errors"].is_array();
    assert!(has_data || has_error, "should degrade gracefully: {:?}", json);
}

#[tokio::test]
async fn test_union_all_sources_refused() {
    let app = build_app(vec![
        Arc::new(ConnectionRefusedConnector("bad1".into())),
        Arc::new(ConnectionRefusedConnector("bad2".into())),
    ]);
    let (status, _) = query(app, "SELECT host, status FROM bad1.logs UNION ALL SELECT host, status FROM bad2.logs").await;
    assert_ne!(status, StatusCode::OK, "all-fail should not return 200");
}

#[tokio::test]
async fn test_join_one_side_refused() {
    let app = build_app(vec![
        Arc::new(HealthyConnector("good".into())),
        Arc::new(ConnectionRefusedConnector("bad".into())),
    ]);
    let (status, json) = query(app, "SELECT * FROM good.logs JOIN bad.logs ON good.logs.host = bad.logs.host").await;
    // JOIN requires both sides — should fail gracefully
    assert!(json["error"].is_string() || status != StatusCode::OK);
}

#[tokio::test]
async fn test_hanging_connector_times_out() {
    let app = build_app(vec![Arc::new(HangingConnector("slow".into()))]);
    let body = serde_json::json!({"query": "SELECT * FROM slow.logs", "format": "sql", "timeout_ms": 500});
    let req = Request::builder()
        .method("POST").uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    assert_ne!(status, StatusCode::OK);
    let err = json["error"].as_str().unwrap_or("");
    assert!(err.contains("timed out") || err.contains("timeout"), "should timeout: {}", err);
}

#[tokio::test]
async fn test_healthy_source_unaffected_by_other_failures() {
    // Query only the healthy source — should work fine regardless of broken connectors registered
    let app = build_app(vec![
        Arc::new(HealthyConnector("good".into())),
        Arc::new(ConnectionRefusedConnector("bad".into())),
    ]);
    let (status, json) = query(app, "SELECT * FROM good.logs").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!json["rows"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_health_endpoint_reports_unhealthy_connector() {
    let app = build_app(vec![
        Arc::new(HealthyConnector("good".into())),
        Arc::new(ConnectionRefusedConnector("bad".into())),
    ]);
    let req = Request::builder().uri("/api/fuse/health").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), 1_000_000).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    // Health should report the unhealthy connector
    let bad_status = json["connectors"]["bad"]["status"].as_str().unwrap_or("");
    assert_eq!(bad_status, "unhealthy", "should report bad connector: {:?}", json);
}
