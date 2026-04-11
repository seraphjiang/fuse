// SPDX-License-Identifier: Apache-2.0
//! #1030 Federation integration — two independent Fuse instances, cross-cluster queries.

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

fn schema_users() -> Schema {
    Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ])
}

fn schema_orders() -> Schema {
    Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ])
}

#[derive(Debug)]
struct ClusterConnector { id: String, schema: Schema, batches: Vec<RecordBatch> }

#[async_trait]
impl FederatedConnector for ClusterConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "mock" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth { ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None } }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![SchemaInfo { name: "data".into(), schema_type: SchemaType::Table, estimated_row_count: Some(self.batches.iter().map(|b| b.num_rows() as u64).sum::<u64>()) }])
    }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> { Ok(self.schema.clone()) }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> { Ok(self.batches.clone()) }
    async fn execute_streaming(&self, q: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError> {
        for b in self.execute(q).await? { tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?; } Ok(())
    }
}

fn make_users() -> ClusterConnector {
    let schema = schema_users();
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        Arc::new(StringArray::from(vec!["alice", "bob", "carol"])) as ArrayRef,
    ]).unwrap();
    ClusterConnector { id: "cluster_users".into(), schema, batches: vec![batch] }
}

fn make_orders() -> ClusterConnector {
    let schema = schema_orders();
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![
        Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrayRef,
        Arc::new(Int64Array::from(vec![100, 200, 50])) as ArrayRef,
    ]).unwrap();
    ClusterConnector { id: "cluster_orders".into(), schema, batches: vec![batch] }
}

fn build_federated_app() -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(make_users())).unwrap();
    registry.register(Arc::new(make_orders())).unwrap();
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
        shared_audit_log: fuse_server::shared_state::SharedAuditLog::from_env(), transactions: std::sync::Arc::new(fuse_server::transaction::TransactionStore::new()), transactions: std::sync::Arc::new(fuse_server::transaction::TransactionStore::new()),
        tenant_registry: Arc::new(fuse_server::tenant::TenantRegistry::new(vec![])),
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
    (status, serde_json::from_slice(&bytes).unwrap_or_default())
}

#[tokio::test]
async fn test_cross_cluster_join() {
    let (status, json) = query(build_federated_app(),
        "SELECT u.name, o.amount FROM cluster_users.data u JOIN cluster_orders.data o ON u.user_id = o.user_id"
    ).await;
    assert_eq!(status, StatusCode::OK, "cross-cluster JOIN: {:?}", json);
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3, "alice(2 orders) + bob(1 order) = 3 rows");
}

#[tokio::test]
async fn test_cross_cluster_union() {
    let (status, json) = query(build_federated_app(),
        "SELECT user_id FROM cluster_users.data UNION ALL SELECT user_id FROM cluster_orders.data"
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 6, "3 users + 3 orders = 6 rows");
}

#[tokio::test]
async fn test_cross_cluster_join_with_analyze() {
    let body = serde_json::json!({
        "query": "SELECT u.name, o.amount FROM cluster_users.data u JOIN cluster_orders.data o ON u.user_id = o.user_id",
        "format": "sql", "analyze": true
    });
    let req = Request::builder()
        .method("POST").uri("/api/fuse/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap();
    let resp = build_federated_app().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(json["execution_profile"].is_object(), "should have profile");
    assert!(json["rows"].as_array().unwrap().len() > 0);
}

#[tokio::test]
async fn test_each_cluster_independently() {
    let app = build_federated_app();
    let (s1, j1) = query(app.clone(), "SELECT * FROM cluster_users.data").await;
    let (s2, j2) = query(app, "SELECT * FROM cluster_orders.data").await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(j1["rows"].as_array().unwrap().len(), 3);
    assert_eq!(j2["rows"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_cross_cluster_explain() {
    let body = serde_json::json!({
        "query": "SELECT u.name, o.amount FROM cluster_users.data u JOIN cluster_orders.data o ON u.user_id = o.user_id",
        "format": "sql"
    });
    let req = Request::builder()
        .method("POST").uri("/api/fuse/query/explain")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap())).unwrap();
    let resp = build_federated_app().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let plan = json["plan"].as_str().unwrap();
    assert!(plan.contains("cluster_users") && plan.contains("cluster_orders"),
        "plan should reference both clusters: {}", plan);
}
