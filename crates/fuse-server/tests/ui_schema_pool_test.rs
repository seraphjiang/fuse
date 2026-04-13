// SPDX-License-Identifier: Apache-2.0
//! UI regression tests for schema explorer, field discovery, and pool stats pages.
//!
//! These tests validate the API contracts that the playground UI depends on:
//! - /explore page → datasources, schemas, fields endpoints
//! - /status page → stats endpoint (includes pool_stats)
//! - /api/fuse/pool-stats → dedicated pool stats endpoint

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorRegistry;
use fuse_server::api::{AppState, RunningQueries, SchemaCache};
use fuse_server::history::QueryHistory;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

// ── Mock connector ──

#[derive(Debug)]
struct SchemaMockConnector {
    id: String,
    tables: Vec<String>,
}

impl SchemaMockConnector {
    fn new(id: &str, tables: Vec<&str>) -> Self {
        Self {
            id: id.into(),
            tables: tables.into_iter().map(String::from).collect(),
        }
    }
}

#[async_trait]
impl FederatedConnector for SchemaMockConnector {
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
        Ok(self
            .tables
            .iter()
            .map(|t| SchemaInfo {
                name: t.clone(),
                schema_type: SchemaType::Table,
                estimated_row_count: Some(100),
            })
            .collect())
    }
    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        if self.tables.contains(&table.to_string()) {
            Ok(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("value", DataType::Float64, true),
            ]))
        } else {
            Err(ConnectorError::schema(format!(
                "table '{}' not found",
                table
            )))
        }
    }
    async fn execute(&self, _q: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        Ok(vec![RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["1"]))],
        )
        .unwrap()])
    }
    async fn execute_streaming(
        &self,
        q: &SubQuery,
        tx: tokio::sync::mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for b in self.execute(q).await? {
            tx.send(Ok(b))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

// ── Test app builder ──

fn build_schema_test_app() -> (axum::Router, Arc<fuse_server::pool_stats::PoolStatsTracker>) {
    let registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(SchemaMockConnector::new(
            "cluster_a",
            vec!["logs", "metrics"],
        )))
        .unwrap();
    registry
        .register(Arc::new(SchemaMockConnector::new(
            "my_ddb",
            vec!["users", "orders"],
        )))
        .unwrap();

    let pool_tracker = Arc::new(fuse_server::pool_stats::PoolStatsTracker::new());
    pool_tracker.register("cluster_a", 16);
    pool_tracker.register("my_ddb", 8);

    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(QueryHistory::new()),
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
        schema_cache: std::sync::Arc::new(SchemaCache::new(300)),
        smart_router: std::sync::Arc::new(fuse_server::smart_routing::SmartRouter::new()),
        health_history: std::sync::Arc::new(
            fuse_server::connector_health_history::HealthHistory::new(),
        ),
        feedback_store: Arc::new(fuse_server::feedback::FeedbackStore::new(100)),
        pool_tracker: pool_tracker.clone(),
    });
    (fuse_server::build_router(state), pool_tracker)
}

async fn get_json(app: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap_or(serde_json::json!(null));
    (status, json)
}

// ── Schema Explorer Tests (explore page) ──

#[tokio::test]
async fn test_ui_datasources_list() {
    let (app, _) = build_schema_test_app();
    let (status, json) = get_json(app, "/api/fuse/datasources").await;
    assert_eq!(status, StatusCode::OK);
    let ds = json.as_array().unwrap();
    assert_eq!(ds.len(), 2);
    let ids: Vec<&str> = ds.iter().map(|d| d["id"].as_str().unwrap()).collect();
    assert!(ids.contains(&"cluster_a"));
    assert!(ids.contains(&"my_ddb"));
}

#[tokio::test]
async fn test_ui_datasource_schemas() {
    let (app, _) = build_schema_test_app();
    let (status, json) = get_json(app, "/api/fuse/datasources/cluster_a/schemas").await;
    assert_eq!(status, StatusCode::OK);
    let tables = json.as_array().unwrap();
    assert_eq!(tables.len(), 2);
    let names: Vec<&str> = tables.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(names.contains(&"logs"));
    assert!(names.contains(&"metrics"));
}

#[tokio::test]
async fn test_ui_datasource_schemas_not_found() {
    let (app, _) = build_schema_test_app();
    let (status, _) = get_json(app, "/api/fuse/datasources/nonexistent/schemas").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── Field Discovery Tests (index page schema sidebar) ──

#[tokio::test]
async fn test_ui_fields_returns_columns() {
    let (app, _) = build_schema_test_app();
    let (status, json) = get_json(app, "/api/fuse/datasources/cluster_a/schemas/logs/fields").await;
    assert_eq!(status, StatusCode::OK);
    let fields = json.as_array().unwrap();
    assert_eq!(fields.len(), 3);
    let names: Vec<&str> = fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"name"));
    assert!(names.contains(&"value"));
    // UI needs data_type and nullable for display
    assert!(fields[0].get("data_type").is_some());
    assert!(fields[0].get("nullable").is_some());
}

#[tokio::test]
async fn test_ui_fields_table_not_found() {
    let (app, _) = build_schema_test_app();
    let (status, json) = get_json(
        app,
        "/api/fuse/datasources/cluster_a/schemas/nonexistent/fields",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(json["error"].as_str().unwrap().contains("not found"));
}

#[tokio::test]
async fn test_ui_fields_datasource_not_found() {
    let (app, _) = build_schema_test_app();
    let (status, _) = get_json(app, "/api/fuse/datasources/bad_ds/schemas/logs/fields").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_ui_fields_cached_on_second_call() {
    let (app, _) = build_schema_test_app();
    // First call populates cache
    let (s1, j1) = get_json(
        app.clone(),
        "/api/fuse/datasources/my_ddb/schemas/users/fields",
    )
    .await;
    assert_eq!(s1, StatusCode::OK);
    // Second call should hit cache and return same result
    let (s2, j2) = get_json(app, "/api/fuse/datasources/my_ddb/schemas/users/fields").await;
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(j1, j2);
}

// ── Pool Stats Tests (status page) ──

#[tokio::test]
async fn test_ui_stats_includes_pool_stats() {
    let (app, _) = build_schema_test_app();
    let (status, json) = get_json(app, "/api/fuse/stats").await;
    assert_eq!(status, StatusCode::OK);
    // Status page reads pool_stats from stats endpoint
    let pool = json["pool_stats"].as_array().unwrap();
    assert_eq!(pool.len(), 2);
}

#[tokio::test]
async fn test_ui_pool_stats_endpoint() {
    let (app, _) = build_schema_test_app();
    let (status, json) = get_json(app, "/api/fuse/pool-stats").await;
    assert_eq!(status, StatusCode::OK);
    let pool = json["pool_stats"].as_array().unwrap();
    assert_eq!(pool.len(), 2);
    // UI needs these fields for rendering
    for entry in pool {
        assert!(entry.get("connector_id").is_some());
        assert!(entry.get("active").is_some());
        assert!(entry.get("idle").is_some());
        assert!(entry.get("max_size").is_some());
        assert!(entry.get("utilization_pct").is_some());
        assert!(entry.get("total_acquired").is_some());
        assert!(entry.get("total_timeouts").is_some());
    }
}

#[tokio::test]
async fn test_ui_pool_stats_after_activity() {
    let (app, tracker) = build_schema_test_app();
    // Simulate query activity
    tracker.acquire("cluster_a");
    tracker.acquire("cluster_a");
    tracker.acquire("my_ddb");

    let (status, json) = get_json(app, "/api/fuse/pool-stats").await;
    assert_eq!(status, StatusCode::OK);
    let pool = json["pool_stats"].as_array().unwrap();
    let ca = pool
        .iter()
        .find(|p| p["connector_id"] == "cluster_a")
        .unwrap();
    assert_eq!(ca["active"], 2);
    assert_eq!(ca["max_size"], 16);
    let ddb = pool.iter().find(|p| p["connector_id"] == "my_ddb").unwrap();
    assert_eq!(ddb["active"], 1);
}

// ── Playground Page Smoke Tests ──

#[tokio::test]
async fn test_ui_explore_page_loads() {
    let (app, _) = build_schema_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/explore")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8_lossy(&body);
    assert!(html.contains("html"), "explore page should return HTML");
}

#[tokio::test]
async fn test_ui_status_page_loads() {
    let (app, _) = build_schema_test_app();
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8_lossy(&body);
    assert!(html.contains("html"), "status page should return HTML");
}

// ── Stats Endpoint Contract (status page depends on shape) ──

#[tokio::test]
async fn test_ui_stats_contract() {
    let (app, _) = build_schema_test_app();
    let (status, json) = get_json(app, "/api/fuse/stats").await;
    assert_eq!(status, StatusCode::OK);
    // Status page reads all these fields
    assert!(json.get("history").is_some());
    assert!(json.get("cache").is_some());
    assert!(json.get("connectors").is_some());
    assert!(json.get("running_queries").is_some());
    assert!(json.get("pool_stats").is_some());
}
