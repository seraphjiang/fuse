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

fn build_test_app() -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(MockConnector::new("testds")))
        .unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
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
