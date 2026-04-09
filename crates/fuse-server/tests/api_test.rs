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
use fuse_server::api::AppState;
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
    let state = Arc::new(AppState { registry: Arc::new(registry), alert_rules: vec![], view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()), history: Arc::new(fuse_server::history::QueryHistory::new()) });
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
