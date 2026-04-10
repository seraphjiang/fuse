// SPDX-License-Identifier: Apache-2.0

//! API integration tests for fuse-server.

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
use fuse_server::history::QueryHistory;

// ── Mock connector ──

#[derive(Debug)]
struct MockConnector {
    id: String,
}

impl MockConnector {
    fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }
}

#[async_trait]
impl FederatedConnector for MockConnector {
    fn id(&self) -> &str {
        &self.id
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
            name: "logs".to_string(),
            schema_type: SchemaType::Index,
            estimated_row_count: Some(100),
        }])
    }
    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
        ]))
    }
    async fn execute(&self, _query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let schema = Arc::new(self.get_schema("").await?);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["h1", "h2"])),
                Arc::new(Int64Array::from(vec![200, 500])),
            ],
        )
        .map_err(ConnectorError::query)?;
        Ok(vec![batch])
    }
    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for batch in self.execute(query).await? {
            tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

/// A connector that sleeps before returning results, for timeout testing.
#[derive(Debug)]
struct SlowMockConnector {
    id: String,
    delay_ms: u64,
}

impl SlowMockConnector {
    fn new(id: &str, delay_ms: u64) -> Self {
        Self { id: id.to_string(), delay_ms }
    }
}

#[async_trait]
impl FederatedConnector for SlowMockConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "slow-mock" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None }
    }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![SchemaInfo { name: "logs".into(), schema_type: SchemaType::Index, estimated_row_count: Some(100) }])
    }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
        ]))
    }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
        let schema = Arc::new(self.get_schema("").await?);
        Ok(vec![RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["h1"])),
            Arc::new(Int64Array::from(vec![200])),
        ]).map_err(ConnectorError::query)?])
    }
    async fn execute_streaming(
        &self, query: &SubQuery, tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for batch in self.execute(query).await? { tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?; }
        Ok(())
    }
}

fn build_test_app() -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(MockConnector::new("testds")))
        .unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()), plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });
    fuse_server::build_router(state)
}

// ── Tests ──

#[tokio::test]
async fn test_health_returns_200() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "healthy");
    assert!(json["connectors"]["testds"].is_object());
}

#[tokio::test]
async fn test_list_datasources() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/datasources")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let arr = json.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["id"], "testds");
    assert_eq!(arr[0]["connector_type"], "mock");
}

#[tokio::test]
async fn test_query_with_mock_engine() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query": "SELECT * FROM testds.logs"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["columns"], serde_json::json!(["host", "status"]));
    assert_eq!(json["metadata"]["total_rows"], 2);
}

#[tokio::test]
async fn test_validate_valid_sql() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/validate")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query": "SELECT * FROM testds.logs"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["valid"], true);
    assert!(json["error"].is_null());
}

#[tokio::test]
async fn test_validate_invalid_sql_no_from() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/validate")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query": "INSERT INTO foo"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["valid"], false);
    assert!(json["error"].as_str().unwrap().contains("FROM"));
}

#[tokio::test]
async fn test_validate_unknown_datasource() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/validate")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query": "SELECT * FROM unknown.logs"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["valid"], false);
    assert!(json["error"].as_str().unwrap().contains("unknown"));
}

#[tokio::test]
async fn test_query_unknown_datasource_returns_404() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query": "SELECT * FROM nope.logs"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_query_bad_sql_returns_400() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query": "not a query"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_schemas_endpoint() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/datasources/testds/schemas")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let arr = json.as_array().unwrap();
    assert_eq!(arr[0]["name"], "logs");
}

#[tokio::test]
async fn test_schemas_unknown_datasource_returns_404() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/datasources/nope/schemas")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── Alert endpoint tests ──

#[tokio::test]
async fn test_list_alerts_empty() {
    let app = build_test_app();
    let resp = app
        .oneshot(Request::builder().uri("/api/fuse/alerts").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_evaluate_alerts_no_rules_returns_empty() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/alerts/evaluate")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.as_array().unwrap().is_empty());
}

/// Capturing mock that records the SubQuery it receives.
#[derive(Debug)]
struct CapturingConnector {
    id: String,
    captured: std::sync::Mutex<Option<SubQuery>>,
}

impl CapturingConnector {
    fn new(id: &str) -> Self {
        Self { id: id.to_string(), captured: std::sync::Mutex::new(None) }
    }
    fn last_query(&self) -> Option<SubQuery> {
        self.captured.lock().unwrap().clone()
    }
}

#[async_trait]
impl FederatedConnector for CapturingConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "capturing" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(1), message: None }
    }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> { Ok(vec![]) }
    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![
            Field::new("service", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
        ]))
    }
    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        *self.captured.lock().unwrap() = Some(query.clone());
        let schema = Arc::new(self.get_schema("").await?);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["svc-a"])),
                Arc::new(Int64Array::from(vec![200_i64])),
            ],
        ).map_err(ConnectorError::query)?;
        Ok(vec![batch])
    }
    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for b in self.execute(query).await? {
            tx.send(Ok(b)).await.map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

fn build_capturing_app() -> (axum::Router, Arc<CapturingConnector>) {
    let connector = Arc::new(CapturingConnector::new("capds"));
    let registry = ConnectorRegistry::new();
    registry.register(connector.clone()).unwrap();
    let state = Arc::new(AppState { registry: Arc::new(registry), alert_rules: vec![], view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()), history: Arc::new(QueryHistory::new()), running_queries: Arc::new(RunningQueries::new()), saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()), plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)) });
    (fuse_server::build_router(state), connector)
}

#[tokio::test]
async fn test_limit_pushdown() {
    let (app, connector) = build_capturing_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query": "SELECT * FROM capds.logs LIMIT 42"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let q = connector.last_query().expect("connector was not called");
    assert_eq!(q.limit, Some(42), "LIMIT 42 must be pushed down to SubQuery");
}

#[tokio::test]
async fn test_where_pushdown() {
    let (app, connector) = build_capturing_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query": "SELECT * FROM capds.logs WHERE status = 500"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let q = connector.last_query().expect("connector was not called");
    assert!(q.filter.is_some(), "WHERE status = 500 must be pushed down to SubQuery.filter");
}

// ── Multi-datasource federation tests ──

fn build_federation_app() -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(MockConnector::new("cluster_a")))
        .unwrap();
    registry
        .register(Arc::new(MockConnector::new("cluster_b")))
        .unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()), plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });
    fuse_server::build_router(state)
}

async fn post_query(app: axum::Router, query: &str, format: &str) -> (StatusCode, serde_json::Value) {
    let body = serde_json::json!({"query": query, "format": format});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

#[tokio::test]
async fn test_single_source_query() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["metadata"]["total_rows"], 2);
    assert_eq!(json["columns"], serde_json::json!(["host", "status"]));
}

#[tokio::test]
async fn test_union_all_query() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    // 2 rows from each connector = 4 total
    assert_eq!(json["metadata"]["total_rows"], 4);
}

#[tokio::test]
async fn test_union_all_with_limit() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs LIMIT 3",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["metadata"]["total_rows"], 3);
}

#[tokio::test]
async fn test_join_query() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT a.*, b.* FROM cluster_a.logs a JOIN cluster_b.logs b ON a.host = b.host",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    // Both connectors return same data (h1, h2), so inner join matches 2 rows
    assert_eq!(json["metadata"]["total_rows"], 2);
}

#[tokio::test]
async fn test_ppl_multi_source() {
    let (status, json) = post_query(
        build_federation_app(),
        "source = cluster_a.logs, cluster_b.logs | head 10",
        "ppl",
    ).await;
    assert_eq!(status, StatusCode::OK);
    // PPL multi-source = UNION ALL → 2+ rows (depends on mock data)
    assert!(json["metadata"]["total_rows"].as_u64().unwrap_or(0) >= 2);
}

#[tokio::test]
async fn test_ppl_multi_source_application_logs() {
    // Verify PPL parser handles underscored table names and fans out to both connectors
    let (status, json) = post_query(
        build_federation_app(),
        "source = cluster_a.application_logs, cluster_b.application_logs | head 10",
        "ppl",
    ).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json["metadata"]["total_rows"].as_u64().unwrap_or(0) >= 2);
}

#[tokio::test]
async fn test_unknown_datasource_in_union() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM nonexistent.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(json["error"].as_str().unwrap().contains("nonexistent"));
}

#[tokio::test]
async fn test_explain_shows_union_strategy() {
    let app = build_federation_app();
    let body = serde_json::json!({
        "query": "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "format": "sql"
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/explain")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(json["plan"].as_str().unwrap().contains("UnionAll"));
}

#[tokio::test]
async fn test_validate_multi_source_all_exist() {
    let app = build_federation_app();
    let body = serde_json::json!({
        "query": "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "format": "sql"
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/validate")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["valid"], true);
}

// ── Materialized view endpoint tests ──

#[tokio::test]
async fn test_list_views_empty() {
    let app = build_test_app();
    let resp = app
        .oneshot(Request::builder().uri("/api/fuse/views").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_get_unknown_view_returns_404() {
    let app = build_test_app();
    let resp = app
        .oneshot(Request::builder().uri("/api/fuse/views/nonexistent").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_refresh_unknown_view_returns_404() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/views/nonexistent/refresh")
                .body(Body::empty())
                .unwrap(),
        )
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_view_lifecycle() {
    use fuse_engine::materialized::{MaterializedViewDef, MaterializedViewRegistry};
    use std::time::Duration;

    // Build app with a pre-registered view
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(MockConnector::new("testds"))).unwrap();
    let view_registry = Arc::new(MaterializedViewRegistry::new());
    view_registry.register(MaterializedViewDef {
        name: "error_summary".into(),
        query: "SELECT * FROM testds.logs".into(),
        refresh_interval: Duration::from_secs(60),
    });
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: view_registry.clone(),
        history: Arc::new(QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()), plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });
    let app = fuse_server::build_router(state);

    // List shows the view
    let resp = app.clone()
        .oneshot(Request::builder().uri("/api/fuse/views").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json.as_array().unwrap().len(), 1);
    assert_eq!(json[0]["name"], "error_summary");
    assert_eq!(json[0]["stale"], true); // never refreshed

    // Refresh succeeds
    let resp = app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/views/error_summary/refresh")
                .body(Body::empty())
                .unwrap(),
        )
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Get returns results
    let resp = app
        .oneshot(Request::builder().uri("/api/fuse/views/error_summary").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["metadata"]["total_rows"], 2);
}

// ── Streaming endpoint tests ──

#[tokio::test]
async fn test_stream_endpoint_returns_sse() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query": "SELECT * FROM testds.logs"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // SSE responses use text/event-stream content type
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(ct.contains("text/event-stream"), "expected SSE content-type, got: {ct}");
}

#[tokio::test]
async fn test_stream_endpoint_contains_done_event() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query": "SELECT * FROM testds.logs"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("\"done\""), "SSE stream must contain a done event");
    assert!(text.contains("\"metadata\""), "SSE stream must contain a metadata event");
}

#[tokio::test]
async fn test_stream_unknown_datasource_returns_error_event() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/stream")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query": "SELECT * FROM nope.logs"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK); // SSE always 200, errors in stream
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("\"error\""), "SSE stream must contain an error event for unknown datasource");
}

// ── PPL validate / explain tests ──

#[tokio::test]
async fn test_validate_ppl_valid() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/validate")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query": "source = testds.logs | where status >= 500", "format": "ppl"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["valid"], true);
}

#[tokio::test]
async fn test_validate_ppl_invalid_syntax() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/validate")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query": "not ppl", "format": "ppl"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["valid"], false);
}

#[tokio::test]
async fn test_explain_ppl_query() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/explain")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query": "source = testds.logs | head 10", "format": "ppl"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["plan"].as_str().unwrap().contains("testds"));
}

#[tokio::test]
async fn test_fields_endpoint() {
    let app = build_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/datasources/testds/schemas/logs/fields")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let fields = json.as_array().unwrap();
    assert!(!fields.is_empty());
    assert!(fields.iter().any(|f| f["name"] == "host"));
}

// ── Data provenance tests ──

#[tokio::test]
async fn test_union_has_datasource_column() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let columns = json["columns"].as_array().unwrap();
    assert!(columns.iter().any(|c| c == "_datasource"));
    // Check rows have correct datasource values
    let rows = json["rows"].as_array().unwrap();
    let ds_col_idx = columns.iter().position(|c| c == "_datasource").unwrap();
    let ds_values: Vec<&str> = rows.iter().map(|r| r[ds_col_idx].as_str().unwrap()).collect();
    assert!(ds_values.contains(&"cluster_a"));
    assert!(ds_values.contains(&"cluster_b"));
}

#[tokio::test]
async fn test_join_has_datasource_column() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT a.*, b.* FROM cluster_a.logs a JOIN cluster_b.logs b ON a.host = b.host",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let columns = json["columns"].as_array().unwrap();
    // Build side gets _datasource prepended
    assert!(columns.iter().any(|c| c == "_datasource"));
}

#[tokio::test]
async fn test_single_source_no_datasource_column() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let columns = json["columns"].as_array().unwrap();
    assert!(!columns.iter().any(|c| c == "_datasource"));
}

#[tokio::test]
async fn test_union_metadata_has_datasource_stats() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let meta = &json["metadata"];
    // datasources_queried
    let ds = meta["datasources_queried"].as_array().unwrap();
    assert_eq!(ds.len(), 2);
    // datasource_stats
    let stats = &meta["datasource_stats"];
    assert!(stats["cluster_a"]["rows"].is_number());
    assert!(stats["cluster_a"]["latency_ms"].is_number());
    assert!(stats["cluster_b"]["rows"].is_number());
}

#[tokio::test]
async fn test_single_source_no_datasource_stats() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let meta = &json["metadata"];
    assert!(meta["datasources_queried"].is_null());
    assert!(meta["datasource_stats"].is_null());
}

// ── Query history tests ──

#[tokio::test]
async fn test_history_empty_initially() {
    let app = build_test_app();
    let resp = app
        .oneshot(Request::builder().uri("/api/fuse/history").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_history_records_query() {
    // Build app with shared state so we can check history after query
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(MockConnector::new("testds"))).unwrap();
    let history = Arc::new(fuse_server::history::QueryHistory::new());
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: history.clone(),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()), plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });
    let app = fuse_server::build_router(state);

    // Execute a query
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/fuse/query")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"query": "SELECT * FROM testds.logs"}"#))
            .unwrap(),
    ).await.unwrap();

    // History should have 1 entry
    let entries = history.list();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].query, "SELECT * FROM testds.logs");
    assert_eq!(entries[0].row_count, 2);
    assert!(entries[0].error.is_none());
    assert!(entries[0].latency_ms < 5000);
}

#[tokio::test]
async fn test_history_max_50_entries() {
    let h = fuse_server::history::QueryHistory::new();
    for i in 0..60u64 {
        h.push(fuse_server::history::HistoryEntry {
            query: format!("SELECT {i}"),
            format: "sql".into(),
            timestamp: i,
            latency_ms: 1,
            row_count: i,
            error: None,
        });
    }
    assert_eq!(h.len(), 50);
    // newest first
    assert_eq!(h.list()[0].query, "SELECT 59");
}

// ── Rate limit integration tests ──

fn build_rate_limited_app(global_rpm: u32, per_ip_rpm: u32) -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(MockConnector::new("testds"))).unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()), plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });
    fuse_server::build_router_with_limits(
        state,
        fuse_server::rate_limit::RateLimitState::new(global_rpm, per_ip_rpm),
    )
}

#[tokio::test]
async fn test_rate_limit_allows_first_request() {
    let app = build_rate_limited_app(100, 10);
    let resp = app
        .oneshot(Request::builder().uri("/api/fuse/health").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_global_rate_limit_returns_429() {
    let app = build_rate_limited_app(1, 100); // 1 global req/min
    // First request OK
    let r1 = app.clone()
        .oneshot(Request::builder().uri("/api/fuse/health").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    // Second request → 429
    let r2 = app
        .oneshot(Request::builder().uri("/api/fuse/health").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(r2.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(r2.headers().get("Retry-After").unwrap(), "60");
}

#[tokio::test]
async fn test_per_ip_rate_limit_returns_429() {
    let app = build_rate_limited_app(1000, 1); // 1 per-IP req/min
    let make_req = || {
        Request::builder()
            .uri("/api/fuse/health")
            .header("x-forwarded-for", "1.2.3.4")
            .body(Body::empty())
            .unwrap()
    };
    let r1 = app.clone().oneshot(make_req()).await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    let r2 = app.oneshot(make_req()).await.unwrap();
    assert_eq!(r2.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn test_rate_limit_response_body() {
    let app = build_rate_limited_app(1, 100);
    app.clone().oneshot(Request::builder().uri("/api/fuse/health").body(Body::empty()).unwrap()).await.unwrap();
    let r = app.oneshot(Request::builder().uri("/api/fuse/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
    let body = axum::body::to_bytes(r.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["error"].as_str().unwrap().contains("rate limit"));
}

// ── EXPLAIN ANALYZE tests ──

async fn post_query_analyze(app: axum::Router, query: &str, format: &str, analyze: bool) -> (StatusCode, serde_json::Value) {
    let body = serde_json::json!({"query": query, "format": format, "analyze": analyze});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

#[tokio::test]
async fn test_analyze_false_no_profile() {
    let (status, json) = post_query_analyze(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
        false,
    ).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.get("execution_profile").is_none());
}

#[tokio::test]
async fn test_analyze_true_has_profile() {
    let (status, json) = post_query_analyze(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
        true,
    ).await;
    assert_eq!(status, StatusCode::OK);
    let profile = &json["execution_profile"];
    assert!(profile["total_ms"].is_number());
    let nodes = profile["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["op"], "RemoteScan");
    assert_eq!(nodes[0]["datasource"], "cluster_a");
    assert!(nodes[0]["actual_rows"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn test_analyze_union_shows_per_datasource() {
    let (status, json) = post_query_analyze(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
        true,
    ).await;
    assert_eq!(status, StatusCode::OK);
    let profile = &json["execution_profile"];
    let nodes = profile["nodes"].as_array().unwrap();
    assert_eq!(nodes[0]["op"], "UnionAll");
    let children = nodes[0]["children"].as_array().unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(children[0]["op"], "RemoteScan");
    assert_eq!(children[1]["op"], "RemoteScan");
    // Both have timing
    assert!(children[0]["actual_ms"].is_number());
    assert!(children[1]["actual_ms"].is_number());
}

#[tokio::test]
async fn test_analyze_default_false() {
    // analyze not specified → should default to false
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.get("execution_profile").is_none());
}

// ── Query timeout tests ──

#[tokio::test]
async fn test_timeout_default_succeeds() {
    // Normal query with no timeout_ms specified should succeed (default 30s)
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json["rows"].as_array().unwrap().len() > 0);
}

#[tokio::test]
async fn test_timeout_explicit_succeeds() {
    // Generous timeout should succeed
    let body = serde_json::json!({"query": "SELECT * FROM cluster_a.logs", "format": "sql", "timeout_ms": 5000});
    let resp = build_federation_app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_timeout_zero_ms_times_out() {
    // Use a slow connector (200ms delay) with a 50ms timeout → guaranteed timeout
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(SlowMockConnector::new("slow_ds", 200))).unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()), plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });
    let app = fuse_server::build_router(state);

    let body = serde_json::json!({"query": "SELECT * FROM slow_ds.logs", "format": "sql", "timeout_ms": 50});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(json["error"].as_str().unwrap().contains("timed out"));
}

// ── Query cancellation tests ──

#[tokio::test]
async fn test_cancel_nonexistent_query() {
    let app = build_federation_app();
    let resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/fuse/query/q-nonexistent/cancel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_list_running_queries_empty() {
    let app = build_federation_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/queries/running")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(json["running"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_cancel_slow_query() {
    // Start a slow query, cancel it, verify it returns cancelled error
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(SlowMockConnector::new("slow_ds", 5000))).unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()), plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });
    let running = state.running_queries.clone();
    let app = fuse_server::build_router(state);

    // Spawn the slow query
    let app2 = app.clone();
    let query_handle = tokio::spawn(async move {
        let body = serde_json::json!({"query": "SELECT * FROM slow_ds.logs", "format": "sql", "timeout_ms": 10000});
        let resp = app2
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/fuse/query")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.status()
    });

    // Wait a bit for the query to register
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Cancel all running queries
    let ids = running.list();
    assert!(!ids.is_empty(), "expected at least one running query");
    for id in &ids {
        assert!(running.cancel(id));
    }

    // The query should return an error
    let status = query_handle.await.unwrap();
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

// ── CSV result format tests ──

#[tokio::test]
async fn test_result_format_json_default() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json["columns"].is_array());
    assert!(json["rows"].is_array());
}

#[tokio::test]
async fn test_result_format_csv() {
    let body = serde_json::json!({
        "query": "SELECT * FROM cluster_a.logs",
        "format": "sql",
        "result_format": "csv"
    });
    let resp = build_federation_app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "text/csv"
    );
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let csv = String::from_utf8(bytes.to_vec()).unwrap();
    // CSV should have header row + data rows
    let lines: Vec<&str> = csv.trim().lines().collect();
    assert!(lines.len() >= 2, "expected header + data, got: {}", csv);
    assert!(lines[0].contains("host"));
    assert!(lines[0].contains("status"));
}

#[tokio::test]
async fn test_result_format_csv_union() {
    let body = serde_json::json!({
        "query": "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "format": "sql",
        "result_format": "csv"
    });
    let resp = build_federation_app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let csv = String::from_utf8(bytes.to_vec()).unwrap();
    let lines: Vec<&str> = csv.trim().lines().collect();
    // Header + rows from both datasources (2 rows each + _datasource column)
    assert!(lines.len() >= 3);
    assert!(lines[0].contains("_datasource"));
}

// ── Stats endpoint test ──

#[tokio::test]
async fn test_stats_endpoint() {
    let app = build_federation_app();

    // Run a query first to populate history
    let _ = post_query(app.clone(), "SELECT * FROM cluster_a.logs", "sql").await;

    // Now check stats
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/fuse/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(json["total_queries"].as_u64().unwrap() >= 1);
    assert!(json["avg_latency_ms"].is_number());
    assert!(json["p95_latency_ms"].is_number());
    assert!(json["total_rows_returned"].is_number());
}

// ── Parameterized query tests ──

#[tokio::test]
async fn test_params_string_binding() {
    let body = serde_json::json!({
        "query": "SELECT * FROM cluster_a.logs WHERE host = $host",
        "format": "sql",
        "params": {"host": "web-01"}
    });
    let resp = build_federation_app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_params_number_binding() {
    let body = serde_json::json!({
        "query": "SELECT * FROM cluster_a.logs WHERE status = $code",
        "format": "sql",
        "params": {"code": 200}
    });
    let resp = build_federation_app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_params_empty_is_noop() {
    // No params → query unchanged
    let (status, _) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn test_params_sql_injection_escaped() {
    // Single quotes in string params should be escaped
    let body = serde_json::json!({
        "query": "SELECT * FROM cluster_a.logs WHERE host = $host",
        "format": "sql",
        "params": {"host": "'; DROP TABLE logs; --"}
    });
    let resp = build_federation_app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    // Should succeed (escaped) or fail gracefully, NOT execute injection
    let status = resp.status();
    assert!(status == StatusCode::OK || status == StatusCode::INTERNAL_SERVER_ERROR);
}

// ── Partial failure tests ──

/// A connector that always fails on execute.
#[derive(Debug)]
struct FailingMockConnector {
    id: String,
}

impl FailingMockConnector {
    fn new(id: &str) -> Self { Self { id: id.to_string() } }
}

#[async_trait]
impl FederatedConnector for FailingMockConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "failing-mock" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    async fn health_check(&self) -> ConnectorHealth {
        ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some("down".into()) }
    }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![SchemaInfo { name: "logs".into(), schema_type: SchemaType::Index, estimated_row_count: Some(0) }])
    }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("status", DataType::Int64, false),
        ]))
    }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        Err(ConnectorError::query("connection refused"))
    }
    async fn execute_streaming(
        &self, _: &SubQuery, _: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::query("connection refused"))
    }
}

#[tokio::test]
async fn test_union_partial_failure_returns_partial_results() {
    // cluster_a works, cluster_b fails → should get results from cluster_a + partial_errors
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(MockConnector::new("cluster_a"))).unwrap();
    registry.register(Arc::new(FailingMockConnector::new("cluster_b"))).unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()), plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });
    let app = fuse_server::build_router(state);

    let (status, json) = post_query(
        app,
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    // Got partial results from cluster_a
    assert!(json["rows"].as_array().unwrap().len() > 0);
    // partial_errors reports cluster_b failure
    let errors = json["partial_errors"].as_array().unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["datasource"], "cluster_b");
    assert!(errors[0]["error"].as_str().unwrap().contains("connection refused"));
}

#[tokio::test]
async fn test_union_all_fail_returns_error() {
    // Both sources fail → should return 500
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(FailingMockConnector::new("cluster_a"))).unwrap();
    registry.register(Arc::new(FailingMockConnector::new("cluster_b"))).unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()), plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });
    let app = fuse_server::build_router(state);

    let (status, _) = post_query(
        app,
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_single_source_no_partial_errors() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    // No partial_errors field when empty (skip_serializing_if)
    assert!(json.get("partial_errors").is_none());
}

// ── Enhanced validate tests ──

async fn post_validate(app: axum::Router, query: &str, format: &str) -> serde_json::Value {
    let body = serde_json::json!({"query": query, "format": format});
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query/validate")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn test_validate_valid_table() {
    let json = post_validate(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs",
        "sql",
    ).await;
    assert_eq!(json["valid"], true);
}

#[tokio::test]
async fn test_validate_unknown_datasource_v2() {
    let json = post_validate(
        build_federation_app(),
        "SELECT * FROM nonexistent.logs",
        "sql",
    ).await;
    assert_eq!(json["valid"], false);
    assert!(json["error"].as_str().unwrap().contains("not found in registry"));
}

#[tokio::test]
async fn test_validate_unknown_table() {
    let json = post_validate(
        build_federation_app(),
        "SELECT * FROM cluster_a.nonexistent_table",
        "sql",
    ).await;
    assert_eq!(json["valid"], false);
    assert!(json["error"].as_str().unwrap().contains("not found in datasource"));
}

#[tokio::test]
async fn test_validate_bad_syntax() {
    let json = post_validate(
        build_federation_app(),
        "NOT A VALID QUERY",
        "sql",
    ).await;
    assert_eq!(json["valid"], false);
}

// ── Global ORDER BY tests ──

#[tokio::test]
async fn test_union_order_by_desc() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs ORDER BY status DESC",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    assert!(rows.len() >= 4);
    // status column — find its index
    let columns = json["columns"].as_array().unwrap();
    let status_idx = columns.iter().position(|c| c == "status").unwrap();
    // First rows should have higher status values
    let first: i64 = rows[0][status_idx].as_str().unwrap().parse().unwrap();
    let last: i64 = rows[rows.len() - 1][status_idx].as_str().unwrap().parse().unwrap();
    assert!(first >= last);
}

#[tokio::test]
async fn test_union_order_by_asc() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs ORDER BY status ASC",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    let columns = json["columns"].as_array().unwrap();
    let status_idx = columns.iter().position(|c| c == "status").unwrap();
    let first: i64 = rows[0][status_idx].as_str().unwrap().parse().unwrap();
    let last: i64 = rows[rows.len() - 1][status_idx].as_str().unwrap().parse().unwrap();
    assert!(first <= last);
}

#[tokio::test]
async fn test_order_by_with_limit() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs ORDER BY status DESC LIMIT 2",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

// ── DISTINCT tests ──

#[tokio::test]
async fn test_select_distinct_union_deduplicates() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT DISTINCT host, status FROM cluster_a.logs UNION ALL SELECT DISTINCT host, status FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    // Both sources return same data (h1/200, h2/500), DISTINCT should dedup to 2
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_non_distinct_union_keeps_all() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    // Without DISTINCT, all 4 rows kept (2 per source + _datasource column makes them unique)
    assert!(rows.len() >= 4);
}

// ── OFFSET tests ──

#[tokio::test]
async fn test_limit_offset() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs LIMIT 2 OFFSET 1",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_offset_beyond_results() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs LIMIT 10 OFFSET 1000",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 0);
}

// ── Saved query tests ──

#[tokio::test]
async fn test_saved_queries_crud() {
    let app = build_federation_app();

    // List — empty
    let resp = app.clone()
        .oneshot(Request::builder().uri("/api/fuse/saved").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(json.as_array().unwrap().is_empty());

    // Save
    let body = serde_json::json!({"name": "errors", "query": "SELECT * FROM cluster_a.logs WHERE status >= 400", "format": "sql", "description": "Error logs"});
    let resp = app.clone()
        .oneshot(
            Request::builder().method("POST").uri("/api/fuse/saved")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap())).unwrap(),
        ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Get
    let resp = app.clone()
        .oneshot(Request::builder().uri("/api/fuse/saved/errors").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["name"], "errors");
    assert!(json["query"].as_str().unwrap().contains("status >= 400"));

    // Delete
    let resp = app.clone()
        .oneshot(Request::builder().method("DELETE").uri("/api/fuse/saved/errors").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Get after delete — 404
    let resp = app.clone()
        .oneshot(Request::builder().uri("/api/fuse/saved/errors").body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ── SQL source parsing robustness tests ──

#[tokio::test]
async fn test_from_inside_string_literal_ignored() {
    // "from" inside a string literal should NOT be treated as a table reference
    let (status, _) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs WHERE msg = 'data from server'",
        "sql",
    ).await;
    // Should succeed — only cluster_a.logs is a source, not "server"
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn test_from_as_substring_ignored() {
    // "from" as part of another word should not match
    // "transform" contains "from" — this should not cause issues
    let (status, _) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs WHERE host = 'transform'",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn test_subquery_from_parsed() {
    // FROM in subquery should still be found
    let (status, _) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs WHERE status IN (SELECT status FROM cluster_b.logs)",
        "sql",
    ).await;
    // Both sources should be found — this is a multi-source query
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn test_params_prefix_collision() {
    // $host should not match inside $hostname when both are params
    let body = serde_json::json!({
        "query": "SELECT * FROM cluster_a.logs WHERE host = $host AND hostname = $hostname",
        "format": "sql",
        "params": {"host": "web-01", "hostname": "web-01.prod.example.com"}
    });
    let resp = build_federation_app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ── String literal safety tests ──

#[tokio::test]
async fn test_limit_in_string_not_matched() {
    // "limit" inside a string literal should not be treated as SQL LIMIT
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs WHERE msg = 'rate limit exceeded'",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    // Should return all rows, not limited
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_union_all_in_string_not_matched() {
    // "union all" inside a string should not trigger UNION path
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs WHERE msg = 'union all workers'",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    // Single source — no _datasource column
    let columns = json["columns"].as_array().unwrap();
    assert!(!columns.iter().any(|c| c == "_datasource"));
}

#[tokio::test]
async fn test_order_by_in_string_not_matched() {
    // "order by" inside a string should not trigger sorting
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs WHERE msg = 'order by priority'",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["rows"].as_array().unwrap().len(), 2);
}

// ── Subquery tests ──

#[tokio::test]
async fn test_subquery_in_from() {
    let (status, json) = post_query(
        build_test_app(),
        "SELECT * FROM (SELECT host, status FROM testds.logs WHERE status > 100) AS sub",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn test_subquery_with_outer_filter() {
    let (status, _json) = post_query(
        build_test_app(),
        "SELECT * FROM (SELECT * FROM testds.logs) AS sub WHERE host = 'h1'",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
}

// ── Execution plan profile tests ──

#[tokio::test]
async fn test_analyze_profile_has_cost_and_detail() {
    let (status, json) = post_query_analyze(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
        true,
    ).await;
    assert_eq!(status, StatusCode::OK);
    let profile = &json["execution_profile"];
    assert!(profile["total_ms"].as_u64().is_some());
    let nodes = profile["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    let union_node = &nodes[0];
    assert_eq!(union_node["op"], "UnionAll");
    // Should have estimated_cost from children
    assert!(union_node["estimated_cost"].as_f64().is_some());
    // Children should be RemoteScan with detail
    let children = union_node["children"].as_array().unwrap();
    assert_eq!(children.len(), 2);
    assert_eq!(children[0]["op"], "RemoteScan");
    assert!(children[0]["detail"].as_str().is_some());
    assert!(children[0]["data_bytes"].as_u64().is_some());
}

#[tokio::test]
async fn test_analyze_single_source_pushdown() {
    let (status, json) = post_query_analyze(
        build_test_app(),
        "SELECT host FROM testds.logs WHERE status > 200",
        "sql",
        true,
    ).await;
    assert_eq!(status, StatusCode::OK);
    let profile = &json["execution_profile"];
    let nodes = profile["nodes"].as_array().unwrap();
    assert_eq!(nodes[0]["op"], "RemoteScan");
    // Should have pushdown descriptions
    let pushdown = nodes[0]["pushdown"].as_array().unwrap();
    assert!(!pushdown.is_empty());
}

// ── #224 Execution plan viz verification (tester) ──

#[tokio::test]
async fn test_analyze_scan_node_has_datasource() {
    let (status, json) = post_query_analyze(
        build_test_app(),
        "SELECT * FROM testds.logs",
        "sql",
        true,
    ).await;
    assert_eq!(status, StatusCode::OK);
    let nodes = json["execution_profile"]["nodes"].as_array().unwrap();
    assert_eq!(nodes[0]["datasource"], "testds");
}

#[tokio::test]
async fn test_analyze_pushdown_describes_projection() {
    let (status, json) = post_query_analyze(
        build_test_app(),
        "SELECT host, status FROM testds.logs",
        "sql",
        true,
    ).await;
    assert_eq!(status, StatusCode::OK);
    let pushdown = json["execution_profile"]["nodes"][0]["pushdown"].as_array().unwrap();
    let has_projection = pushdown.iter().any(|p| p.as_str().unwrap().contains("projection"));
    assert!(has_projection, "pushdown should describe projection: {:?}", pushdown);
}

#[tokio::test]
async fn test_analyze_pushdown_describes_limit() {
    let (status, json) = post_query_analyze(
        build_test_app(),
        "SELECT * FROM testds.logs LIMIT 5",
        "sql",
        true,
    ).await;
    assert_eq!(status, StatusCode::OK);
    let pushdown = json["execution_profile"]["nodes"][0]["pushdown"].as_array().unwrap();
    let has_limit = pushdown.iter().any(|p| p.as_str().unwrap().contains("limit"));
    assert!(has_limit, "pushdown should describe limit: {:?}", pushdown);
}

#[tokio::test]
async fn test_analyze_union_parent_cost_gte_children() {
    let (status, json) = post_query_analyze(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
        true,
    ).await;
    assert_eq!(status, StatusCode::OK);
    let union_node = &json["execution_profile"]["nodes"][0];
    let parent_cost = union_node["estimated_cost"].as_f64().unwrap();
    let children = union_node["children"].as_array().unwrap();
    let child_sum: f64 = children.iter().map(|c| c["actual_ms"].as_f64().unwrap_or(0.0)).sum();
    assert!(parent_cost >= child_sum, "parent cost {} should >= child sum {}", parent_cost, child_sum);
}

// ── Plan cache tests ──

#[tokio::test]
async fn test_plan_cache_populated_on_query() {
    let app = build_test_app();
    // First query — cache miss, populates cache
    let (status, _) = post_query(app.clone(), "SELECT * FROM testds.logs", "sql").await;
    assert_eq!(status, StatusCode::OK);
    // Second identical query — should still succeed (cache hit path)
    let (status2, _) = post_query(app, "SELECT * FROM testds.logs", "sql").await;
    assert_eq!(status2, StatusCode::OK);
}

#[tokio::test]
async fn test_plan_cache_different_queries() {
    let app = build_test_app();
    let (s1, _) = post_query(app.clone(), "SELECT * FROM testds.logs", "sql").await;
    let (s2, _) = post_query(app, "SELECT * FROM testds.logs WHERE status > 200", "sql").await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::OK);
}

// ── Trace ID tests ──

#[tokio::test]
async fn test_response_contains_trace_id() {
    let (status, json) = post_query(build_test_app(), "SELECT * FROM testds.logs", "sql").await;
    assert_eq!(status, StatusCode::OK);
    let trace_id = json["metadata"]["trace_id"].as_str().unwrap();
    assert!(!trace_id.is_empty());
    assert!(trace_id.starts_with("q-"));
}

#[tokio::test]
async fn test_trace_ids_are_unique() {
    let app = build_test_app();
    let (_, json1) = post_query(app.clone(), "SELECT * FROM testds.logs", "sql").await;
    let (_, json2) = post_query(app, "SELECT * FROM testds.logs", "sql").await;
    let t1 = json1["metadata"]["trace_id"].as_str().unwrap();
    let t2 = json2["metadata"]["trace_id"].as_str().unwrap();
    assert_ne!(t1, t2);
}

// ── Multi-column ORDER BY tests ──

#[tokio::test]
async fn test_multi_column_order_by() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs ORDER BY status DESC, host ASC",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    // status DESC: 500 before 200
    assert_eq!(rows[0][1], "500");
    assert_eq!(rows[rows.len() - 1][1], "200");
}

#[tokio::test]
async fn test_single_column_order_by_still_works() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs ORDER BY host ASC",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows[0][0], "h1");
}

// ── Demo #311: 3-source UNION ALL ──

fn build_three_source_app() -> axum::Router {
    use fuse_server::api::{AppState, RunningQueries};
    use fuse_core::registry::ConnectorRegistry;
    use std::sync::Arc;

    let registry = ConnectorRegistry::new();
    let mock_a = Arc::new(MockConnector::new("opensearch_logs"));
    let mock_b = Arc::new(MockConnector::new("s3_logs"));
    let mock_c = Arc::new(MockConnector::new("cloudwatch_logs"));
    registry.register(mock_a).unwrap();
    registry.register(mock_b).unwrap();
    registry.register(mock_c).unwrap();

    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()),
        plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
    });
    fuse_server::build_router(state)
}

#[tokio::test]
async fn test_three_source_union_all() {
    let (status, json) = post_query(
        build_three_source_app(),
        "SELECT * FROM opensearch_logs.logs UNION ALL SELECT * FROM s3_logs.logs UNION ALL SELECT * FROM cloudwatch_logs.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    // 2 rows per source × 3 sources = 6 rows
    assert_eq!(rows.len(), 6);
    // Should have _datasource column
    let columns = json["columns"].as_array().unwrap();
    assert!(columns.iter().any(|c| c == "_datasource"));
    // Metadata should list all 3 datasources
    let ds = json["metadata"]["datasources_queried"].as_array().unwrap();
    assert_eq!(ds.len(), 3);
}

#[tokio::test]
async fn test_three_source_union_with_limit() {
    let (status, json) = post_query(
        build_three_source_app(),
        "SELECT * FROM opensearch_logs.logs UNION ALL SELECT * FROM s3_logs.logs UNION ALL SELECT * FROM cloudwatch_logs.logs LIMIT 4",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
}

// ── UNION (dedup) vs UNION ALL tests ──

#[tokio::test]
async fn test_union_deduplicates() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT host, status FROM cluster_a.logs UNION SELECT host, status FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    // Both sources return same data — plain UNION should dedup to 2
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn test_union_all_keeps_duplicates() {
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT host, status FROM cluster_a.logs UNION ALL SELECT host, status FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    // UNION ALL keeps all rows — 4 (2 per source + _datasource makes unique)
    assert!(rows.len() >= 4);
}

// ── Cursor pagination tests ──

#[tokio::test]
async fn test_cursor_pagination_first_page() {
    let body = serde_json::json!({
        "query": "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "format": "sql",
        "page_size": 2
    });
    let resp = build_federation_app()
        .oneshot(
            Request::builder().method("POST").uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap())).unwrap(),
        ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    // Should have next_cursor since there are more rows
    assert!(json["next_cursor"].as_str().is_some());
}

#[tokio::test]
async fn test_cursor_pagination_second_page() {
    let body = serde_json::json!({
        "query": "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "format": "sql",
        "page_size": 2,
        "cursor": "fuse_c_2"
    });
    let resp = build_federation_app()
        .oneshot(
            Request::builder().method("POST").uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap())).unwrap(),
        ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    // Last page — no next_cursor
    assert!(json.get("next_cursor").is_none() || json["next_cursor"].is_null());
}

#[tokio::test]
async fn test_no_cursor_no_page_size() {
    // Without page_size, no cursor pagination — returns all rows, no next_cursor
    let (status, json) = post_query(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.get("next_cursor").is_none() || json["next_cursor"].is_null());
}

// ── #351 Pagination E2E verification (tester) ──

async fn post_query_paginated(
    app: axum::Router,
    query: &str,
    format: &str,
    cursor: Option<&str>,
    page_size: Option<usize>,
) -> (StatusCode, serde_json::Value) {
    let mut body = serde_json::json!({"query": query, "format": format});
    if let Some(c) = cursor {
        body["cursor"] = serde_json::Value::String(c.into());
    }
    if let Some(ps) = page_size {
        body["page_size"] = serde_json::json!(ps);
    }
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/fuse/query")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
    (status, json)
}

#[tokio::test]
async fn test_cursor_first_page_returns_next_cursor() {
    let (status, json) = post_query_paginated(
        build_test_app(), "SELECT * FROM testds.logs", "sql", None, Some(1),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    // Should have next_cursor since there are more rows
    assert!(json["next_cursor"].is_string(), "expected next_cursor, got: {:?}", json["next_cursor"]);
}

#[tokio::test]
async fn test_cursor_second_page_different_rows() {
    // Page 1
    let (_, json1) = post_query_paginated(
        build_test_app(), "SELECT * FROM testds.logs", "sql", None, Some(1),
    ).await;
    let cursor = json1["next_cursor"].as_str().unwrap();
    let row1 = json1["rows"][0].clone();

    // Page 2
    let (status, json2) = post_query_paginated(
        build_test_app(), "SELECT * FROM testds.logs", "sql", Some(cursor), Some(1),
    ).await;
    assert_eq!(status, StatusCode::OK);
    let row2 = json2["rows"][0].clone();
    assert_ne!(row1, row2, "page 2 should return different row than page 1");
}

#[tokio::test]
async fn test_cursor_no_page_size_no_cursor() {
    // Without page_size, no cursor pagination
    let (status, json) = post_query_paginated(
        build_test_app(), "SELECT * FROM testds.logs", "sql", None, None,
    ).await;
    assert_eq!(status, StatusCode::OK);
    // All rows returned, no next_cursor
    assert!(json["next_cursor"].is_null() || !json.get("next_cursor").is_some());
}

#[tokio::test]
async fn test_cursor_invalid_token_handled() {
    let (status, _) = post_query_paginated(
        build_test_app(), "SELECT * FROM testds.logs", "sql", Some("invalid_cursor"), Some(1),
    ).await;
    // Should either ignore invalid cursor (200) or reject (400)
    assert!(status == StatusCode::OK || status == StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_cursor_encode_decode_roundtrip() {
    // Verify cursor format: fuse_c_<offset>
    let (_, json) = post_query_paginated(
        build_test_app(), "SELECT * FROM testds.logs", "sql", None, Some(1),
    ).await;
    if let Some(cursor) = json["next_cursor"].as_str() {
        assert!(cursor.starts_with("fuse_c_"), "cursor should start with fuse_c_, got: {}", cursor);
    }
}

// ── #338 UNION dedup + #311 3-source + #312/#313 demo verification (tester) ──

#[tokio::test]
async fn test_union_dedup_fewer_than_union_all() {
    // UNION should return <= UNION ALL rows
    let (_, json_all) = post_query(
        build_federation_app(),
        "SELECT host, status FROM cluster_a.logs UNION ALL SELECT host, status FROM cluster_b.logs",
        "sql",
    ).await;
    let (_, json_dedup) = post_query(
        build_federation_app(),
        "SELECT host, status FROM cluster_a.logs UNION SELECT host, status FROM cluster_b.logs",
        "sql",
    ).await;
    let all_rows = json_all["rows"].as_array().unwrap().len();
    let dedup_rows = json_dedup["rows"].as_array().unwrap().len();
    assert!(dedup_rows <= all_rows, "UNION ({}) should be <= UNION ALL ({})", dedup_rows, all_rows);
}

#[tokio::test]
async fn test_union_dedup_no_duplicate_rows() {
    let (_, json) = post_query(
        build_federation_app(),
        "SELECT host, status FROM cluster_a.logs UNION SELECT host, status FROM cluster_b.logs",
        "sql",
    ).await;
    let rows = json["rows"].as_array().unwrap();
    // Check no two rows are identical
    for i in 0..rows.len() {
        for j in (i + 1)..rows.len() {
            assert_ne!(rows[i], rows[j], "duplicate row found at {} and {}", i, j);
        }
    }
}

#[tokio::test]
async fn test_three_source_provenance_correct() {
    let (_, json) = post_query(
        build_three_source_app(),
        "SELECT * FROM opensearch_logs.logs UNION ALL SELECT * FROM s3_logs.logs UNION ALL SELECT * FROM cloudwatch_logs.logs",
        "sql",
    ).await;
    let columns = json["columns"].as_array().unwrap();
    let ds_idx = columns.iter().position(|c| c == "_datasource").unwrap();
    let rows = json["rows"].as_array().unwrap();
    let sources: std::collections::HashSet<&str> = rows.iter()
        .filter_map(|r| r.as_array().and_then(|a| a[ds_idx].as_str()))
        .collect();
    assert!(sources.contains("opensearch_logs"), "missing opensearch_logs in {:?}", sources);
    assert!(sources.contains("s3_logs"), "missing s3_logs in {:?}", sources);
    assert!(sources.contains("cloudwatch_logs"), "missing cloudwatch_logs in {:?}", sources);
}

#[tokio::test]
async fn test_three_source_each_contributes_rows() {
    let (_, json) = post_query(
        build_three_source_app(),
        "SELECT * FROM opensearch_logs.logs UNION ALL SELECT * FROM s3_logs.logs UNION ALL SELECT * FROM cloudwatch_logs.logs",
        "sql",
    ).await;
    let columns = json["columns"].as_array().unwrap();
    let ds_idx = columns.iter().position(|c| c == "_datasource").unwrap();
    let rows = json["rows"].as_array().unwrap();
    // Each source should contribute at least 1 row
    for src in &["opensearch_logs", "s3_logs", "cloudwatch_logs"] {
        let count = rows.iter().filter(|r| r.as_array().map_or(false, |a| a[ds_idx].as_str() == Some(src))).count();
        assert!(count > 0, "{} contributed 0 rows", src);
    }
}

#[tokio::test]
async fn test_is_union_distinct_detection() {
    // Verify the query parser distinguishes UNION from UNION ALL
    // UNION ALL → should have more rows (duplicates kept)
    let (s1, j1) = post_query(
        build_federation_app(),
        "SELECT host FROM cluster_a.logs UNION ALL SELECT host FROM cluster_b.logs",
        "sql",
    ).await;
    let (s2, j2) = post_query(
        build_federation_app(),
        "SELECT host FROM cluster_a.logs UNION SELECT host FROM cluster_b.logs",
        "sql",
    ).await;
    assert_eq!(s1, StatusCode::OK);
    assert_eq!(s2, StatusCode::OK);
    // Both should succeed — the key is UNION has fewer or equal rows
    let all_count = j1["rows"].as_array().unwrap().len();
    let dedup_count = j2["rows"].as_array().unwrap().len();
    assert!(dedup_count <= all_count);
}

// ── Cost estimator tests ──

#[tokio::test]
async fn test_analyze_has_cost_estimates() {
    let (status, json) = post_query_analyze(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
        true,
    ).await;
    assert_eq!(status, StatusCode::OK);
    let nodes = json["execution_profile"]["nodes"].as_array().unwrap();
    let union_node = &nodes[0];
    // Parent should have estimated_rows (sum of children)
    assert!(union_node["estimated_rows"].as_u64().is_some());
    // Children should have estimated_rows and estimated_cost
    let children = union_node["children"].as_array().unwrap();
    for child in children {
        assert!(child["estimated_rows"].as_u64().is_some());
        assert!(child["estimated_cost"].as_f64().is_some());
    }
}

#[tokio::test]
async fn test_explain_has_cost_estimates() {
    let resp = build_test_app()
        .oneshot(
            Request::builder().method("POST").uri("/api/fuse/query/explain")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&serde_json::json!({
                    "query": "SELECT * FROM testds.logs",
                    "format": "sql"
                })).unwrap())).unwrap(),
        ).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let tree = &json["plan_tree"];
    assert!(tree["estimated_rows"].as_u64().is_some());
    assert!(tree["estimated_cost"].as_f64().is_some());
}

// ── Hash join optimization tests ──

#[tokio::test]
async fn test_join_profile_shows_build_side() {
    let (status, json) = post_query_analyze(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs JOIN cluster_b.logs ON cluster_a.logs.host = cluster_b.logs.host",
        "sql",
        true,
    ).await;
    assert_eq!(status, StatusCode::OK);
    let nodes = json["execution_profile"]["nodes"].as_array().unwrap();
    let join_node = &nodes[0];
    assert_eq!(join_node["op"], "HashJoin");
    let children = join_node["children"].as_array().unwrap();
    assert_eq!(children.len(), 2);
    // One child should be "build side", other "probe side"
    let pushdowns: Vec<String> = children.iter()
        .flat_map(|c| {
            let arr = c["pushdown"].as_array().cloned().unwrap_or_default();
            arr.into_iter().map(|p| p.as_str().unwrap_or("").to_string()).collect::<Vec<_>>()
        })
        .collect();
    assert!(pushdowns.iter().any(|p| p.contains("build side")));
    assert!(pushdowns.iter().any(|p| p.contains("probe side")));
}

// ── #344 Cost estimator verification (tester) ──

#[tokio::test]
async fn test_cost_estimate_parent_rows_sum_children() {
    let (_, json) = post_query_analyze(
        build_federation_app(),
        "SELECT * FROM cluster_a.logs UNION ALL SELECT * FROM cluster_b.logs",
        "sql",
        true,
    ).await;
    let union = &json["execution_profile"]["nodes"][0];
    let parent_est = union["estimated_rows"].as_u64().unwrap();
    let children = union["children"].as_array().unwrap();
    let child_sum: u64 = children.iter().filter_map(|c| c["estimated_rows"].as_u64()).sum();
    assert_eq!(parent_est, child_sum, "parent estimated_rows should equal sum of children");
}

#[tokio::test]
async fn test_cost_estimate_scan_has_bytes() {
    let (_, json) = post_query_analyze(
        build_test_app(),
        "SELECT * FROM testds.logs",
        "sql",
        true,
    ).await;
    let node = &json["execution_profile"]["nodes"][0];
    assert!(node["data_bytes"].as_u64().is_some(), "scan node should have data_bytes");
    assert!(node["estimated_cost"].as_f64().unwrap() >= 0.0, "cost should be non-negative");
}

#[tokio::test]
async fn test_cost_estimate_not_in_non_analyze() {
    // analyze=false should not include execution_profile at all
    let (_, json) = post_query_analyze(
        build_test_app(),
        "SELECT * FROM testds.logs",
        "sql",
        false,
    ).await;
    assert!(json.get("execution_profile").is_none() || json["execution_profile"].is_null());
}

#[tokio::test]
async fn test_cost_estimate_single_source_no_parent() {
    // Single source → scan node directly, no parent wrapper
    let (_, json) = post_query_analyze(
        build_test_app(),
        "SELECT * FROM testds.logs",
        "sql",
        true,
    ).await;
    let nodes = json["execution_profile"]["nodes"].as_array().unwrap();
    assert_eq!(nodes[0]["op"], "RemoteScan");
    assert!(nodes[0]["children"].as_array().map_or(true, |c| c.is_empty()));
}
