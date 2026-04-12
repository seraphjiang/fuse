// SPDX-License-Identifier: Apache-2.0
//! #840 Horizontal scaling — verify no state leakage across instances.

use std::sync::Arc;
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

fn mock_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ])
}

fn mock_batch(id: i64, name: &str) -> Vec<RecordBatch> {
    let schema = Arc::new(mock_schema());
    vec![RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(vec![id])) as ArrayRef,
        Arc::new(StringArray::from(vec![name])) as ArrayRef,
    ]).unwrap()]
}

#[derive(Debug)]
struct InstanceConnector { id_str: String, data_id: i64, data_name: String }

#[async_trait]
impl FederatedConnector for InstanceConnector {
    fn id(&self) -> &str { &self.id_str }
    fn connector_type(&self) -> &str { "mock" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth { ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None } }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> { Ok(vec![SchemaInfo { name: "data".into(), schema_type: SchemaType::Table, estimated_row_count: Some(1) }]) }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> { Ok(mock_schema()) }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> { Ok(mock_batch(self.data_id, &self.data_name)) }
    async fn execute_streaming(&self, q: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for b in self.execute(q).await? { tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?; } Ok(())
    }
}

fn build_instance(connector: InstanceConnector) -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(connector)).unwrap();
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
        shared_audit_log: fuse_server::shared_state::SharedAuditLog::from_env(), transactions: std::sync::Arc::new(fuse_server::transaction::TransactionStore::new()), max_result_bytes: 0, datasource_limiter: std::sync::Arc::new(fuse_server::rate_limit::DatasourceLimiter::new()), adaptive_parallelism: std::sync::Arc::new(fuse_server::adaptive_parallelism::AdaptiveParallelism::new()), otel_store: None, query_recorder: std::sync::Arc::new(fuse_server::query_replay::QueryRecorder::new(100)), webhook_registry: std::sync::Arc::new(fuse_server::webhook::WebhookRegistry::new()), compilation_cache: std::sync::Arc::new(fuse_server::query_compilation::CompilationCache::new(300, 5000)), cdc_tracker: std::sync::Arc::new(fuse_server::cdc::CdcTracker::new(1000)), adaptive_cache: std::sync::Arc::new(fuse_server::adaptive_cache::AdaptiveCache::new(60, 3, 10000)), column_rbac: None,
        tenant_registry: Arc::new(fuse_server::tenant::TenantRegistry::new(vec![])),
    });
    fuse_server::build_router(state)
}

async fn query_instance(app: axum::Router, sql: &str) -> (StatusCode, serde_json::Value) {
    let body = serde_json::json!({"query": sql, "format": "sql"});
    let req = Request::builder()
        .method("POST").uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

#[tokio::test]
async fn test_instances_have_isolated_data() {
    let app1 = build_instance(InstanceConnector { id_str: "ds".into(), data_id: 1, data_name: "instance-1".into() });
    let app2 = build_instance(InstanceConnector { id_str: "ds".into(), data_id: 2, data_name: "instance-2".into() });

    let (s1, j1) = query_instance(app1, "SELECT * FROM ds.data").await;
    let (s2, j2) = query_instance(app2, "SELECT * FROM ds.data").await;

    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::OK);

    let name1 = j1["rows"][0][1].as_str().unwrap();
    let name2 = j2["rows"][0][1].as_str().unwrap();
    assert_eq!(name1, "instance-1");
    assert_eq!(name2, "instance-2");
    assert_ne!(name1, name2, "instances must not share state");
}

#[tokio::test]
async fn test_saved_query_not_shared_across_instances() {
    let app1 = build_instance(InstanceConnector { id_str: "ds".into(), data_id: 1, data_name: "a".into() });
    let app2 = build_instance(InstanceConnector { id_str: "ds".into(), data_id: 2, data_name: "b".into() });

    // Save a query on instance 1
    let save_body = serde_json::json!({"name": "my_query", "query": "SELECT * FROM ds.data", "format": "sql"});
    let req = Request::builder()
        .method("POST").uri("/api/fuse/saved")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&save_body).unwrap())).unwrap();
    let resp = app1.clone().oneshot(req).await.unwrap();
    assert!(resp.status().is_success(), "save should succeed on instance 1: {}", resp.status());

    // List saved queries on instance 2 — should be empty (no shared state)
    let req2 = Request::builder().uri("/api/fuse/saved").body(Body::empty()).unwrap();
    let resp2 = app2.oneshot(req2).await.unwrap();
    let bytes = axum::body::to_bytes(resp2.into_body(), 1_000_000).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    let empty = vec![];
    let queries = json.as_array().unwrap_or(&empty);
    assert!(queries.is_empty(), "instance 2 should not see instance 1's saved queries: {:?}", queries);
}

#[tokio::test]
async fn test_history_not_shared_across_instances() {
    let app1 = build_instance(InstanceConnector { id_str: "ds".into(), data_id: 1, data_name: "a".into() });
    let app2 = build_instance(InstanceConnector { id_str: "ds".into(), data_id: 2, data_name: "b".into() });

    // Execute query on instance 1
    let _ = query_instance(app1.clone(), "SELECT * FROM ds.data").await;

    // Check history on instance 2 — should be empty
    let req = Request::builder().uri("/api/fuse/history").body(Body::empty()).unwrap();
    let resp = app2.oneshot(req).await.unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), 1_000_000).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    let empty = vec![];
    let entries = json.as_array().unwrap_or(&empty);
    assert!(entries.is_empty(), "instance 2 should not see instance 1's history: {:?}", entries);
}

#[tokio::test]
async fn test_concurrent_queries_across_instances() {
    let app1 = build_instance(InstanceConnector { id_str: "ds".into(), data_id: 1, data_name: "a".into() });
    let app2 = build_instance(InstanceConnector { id_str: "ds".into(), data_id: 2, data_name: "b".into() });

    let (r1, r2) = tokio::join!(
        query_instance(app1, "SELECT * FROM ds.data"),
        query_instance(app2, "SELECT * FROM ds.data"),
    );
    assert_eq!(r1.0, StatusCode::OK);
    assert_eq!(r2.0, StatusCode::OK);
    assert_ne!(r1.1["rows"][0][1], r2.1["rows"][0][1], "concurrent queries should return different data");
}
