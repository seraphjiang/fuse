// SPDX-License-Identifier: Apache-2.0
//! Deep ops page content tests — verify HTML pages render correct data,
//! API contracts for admin/ops endpoints, and cross-endpoint consistency.

use std::sync::Arc;

use arrow::array::StringArray;
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

#[derive(Debug)]
struct DeepTestConnector {
    id: String,
    healthy: bool,
}

#[async_trait]
impl FederatedConnector for DeepTestConnector {
    fn id(&self) -> &str {
        &self.id
    }
    fn connector_type(&self) -> &str {
        "mock-deep"
    }
    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities::full()
    }
    async fn health_check(&self) -> ConnectorHealth {
        if self.healthy {
            ConnectorHealth {
                status: HealthStatus::Healthy,
                latency_ms: Some(3),
                message: None,
            }
        } else {
            ConnectorHealth {
                status: HealthStatus::Unhealthy,
                latency_ms: None,
                message: Some("down".into()),
            }
        }
    }
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![SchemaInfo {
            name: "events".into(),
            schema_type: SchemaType::Table,
            estimated_row_count: Some(50),
        }])
    }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]))
    }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        if !self.healthy {
            return Err(ConnectorError::query("down"));
        }
        let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, false)]));
        Ok(vec![RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["ok"]))],
        )
        .unwrap()])
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

fn build_app_with(connectors: Vec<(String, bool)>) -> axum::Router {
    let registry = ConnectorRegistry::new();
    for (id, healthy) in connectors {
        registry
            .register(Arc::new(DeepTestConnector { id, healthy }))
            .unwrap();
    }
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()),
        plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
        result_cache: Arc::new(fuse_server::plan_cache::ResultCache::new(60, 500)),
        tenant_registry: Arc::new(fuse_server::tenant::TenantRegistry::disabled()),
        audit_log: Arc::new(fuse_server::audit::AuditLog::new(100)),
        adaptive_timeout: Arc::new(fuse_server::adaptive_timeout::AdaptiveTimeout::new()),
        prepared_statements: fuse_server::prepared::new_store(),
        shared_saved_queries: fuse_server::shared_state::SharedSavedQueries::from_env(),
        shared_history: fuse_server::shared_state::SharedQueryHistory::from_env(),
        shared_audit_log: fuse_server::shared_state::SharedAuditLog::from_env(),
        transactions: Arc::new(fuse_server::transaction::TransactionStore::new()),
        max_result_bytes: 0,
        datasource_limiter: Arc::new(fuse_server::rate_limit::DatasourceLimiter::new()),
        adaptive_parallelism: Arc::new(
            fuse_server::adaptive_parallelism::AdaptiveParallelism::new(),
        ),
        otel_store: None,
        query_recorder: Arc::new(fuse_server::query_replay::QueryRecorder::new(100)),
        webhook_registry: Arc::new(fuse_server::webhook::WebhookRegistry::new()),
        compilation_cache: Arc::new(fuse_server::query_compilation::CompilationCache::new(
            300, 5000,
        )),
        cdc_tracker: Arc::new(fuse_server::cdc::CdcTracker::new(1000)),
        adaptive_cache: Arc::new(fuse_server::adaptive_cache::AdaptiveCache::new(
            60, 3, 10000,
        )),
        column_rbac: None,
        key_rotation: Arc::new(fuse_server::auth::KeyRotationManager::new(vec![])),
        schema_cache: Arc::new(fuse_server::api::SchemaCache::new(300)),
        smart_router: Arc::new(fuse_server::smart_routing::SmartRouter::new()),
        health_history: Arc::new(fuse_server::connector_health_history::HealthHistory::new()),
        pool_tracker: Arc::new(fuse_server::pool_stats::PoolStatsTracker::new()),
    });
    fuse_server::build_router(state)
}

async fn get_json(app: axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
    let req = Request::builder().uri(path).body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1_000_000)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

async fn get_html(app: axum::Router, path: &str) -> (StatusCode, String) {
    let req = Request::builder().uri(path).body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1_000_000)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

// ── Health: mixed healthy/unhealthy connectors ──

#[tokio::test]
async fn test_health_mixed_connectors() {
    let app = build_app_with(vec![("good".into(), true), ("bad".into(), false)]);
    let (_, json) = get_json(app, "/api/fuse/health").await;
    assert_eq!(
        json["connectors"]["good"]["status"].as_str().unwrap(),
        "healthy"
    );
    assert_eq!(
        json["connectors"]["bad"]["status"].as_str().unwrap(),
        "unhealthy"
    );
}

#[tokio::test]
async fn test_health_unhealthy_has_message() {
    let app = build_app_with(vec![("bad".into(), false)]);
    let (_, json) = get_json(app, "/api/fuse/health").await;
    assert!(json["connectors"]["bad"]["message"]
        .as_str()
        .unwrap()
        .contains("down"));
}

#[tokio::test]
async fn test_health_healthy_has_latency() {
    let app = build_app_with(vec![("good".into(), true)]);
    let (_, json) = get_json(app, "/api/fuse/health").await;
    assert!(json["connectors"]["good"]["latency_ms"].as_i64().unwrap() >= 0);
}

// ── Datasources: multiple connectors ──

#[tokio::test]
async fn test_datasources_lists_all() {
    let app = build_app_with(vec![
        ("ds_a".into(), true),
        ("ds_b".into(), true),
        ("ds_c".into(), false),
    ]);
    let (_, json) = get_json(app, "/api/fuse/datasources").await;
    assert_eq!(json.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn test_datasources_schema_discovery() {
    let app = build_app_with(vec![("myds".into(), true)]);
    let (status, json) = get_json(app, "/api/fuse/datasources/myds/schemas").await;
    assert_eq!(status, StatusCode::OK);
    let schemas = json.as_array().unwrap();
    assert!(!schemas.is_empty());
}

#[tokio::test]
async fn test_datasources_field_discovery() {
    let app = build_app_with(vec![("myds".into(), true)]);
    let (status, json) = get_json(app, "/api/fuse/datasources/myds/schemas/events/fields").await;
    assert_eq!(status, StatusCode::OK);
    let fields = json.as_array().unwrap();
    assert!(fields.iter().any(|f| f["name"].as_str() == Some("msg")));
}

#[tokio::test]
async fn test_datasources_unknown_returns_404() {
    let app = build_app_with(vec![("myds".into(), true)]);
    let (status, _) = get_json(app, "/api/fuse/datasources/nonexistent/schemas").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── HTML pages: content checks ──

#[tokio::test]
async fn test_status_page_has_nav_links() {
    let (_, body) = get_html(build_app_with(vec![("x".into(), true)]), "/status").await;
    // Should link to other pages
    assert!(body.contains("href=") || body.contains("nav"));
}

#[tokio::test]
async fn test_admin_page_has_title() {
    let (_, body) = get_html(build_app_with(vec![("x".into(), true)]), "/admin").await;
    assert!(body.to_lowercase().contains("admin"));
}

#[tokio::test]
async fn test_settings_page_has_config_reference() {
    let (_, body) = get_html(build_app_with(vec![("x".into(), true)]), "/settings").await;
    let lower = body.to_lowercase();
    assert!(lower.contains("config") || lower.contains("setting") || lower.contains("cache"));
}

#[tokio::test]
async fn test_all_ops_pages_have_charset() {
    for path in ["/status", "/admin", "/settings"] {
        let (status, body) = get_html(build_app_with(vec![("x".into(), true)]), path).await;
        assert_eq!(status, StatusCode::OK, "page {} failed", path);
        assert!(body.contains("charset"), "page {} missing charset", path);
    }
}

// ── API: content-type headers ──

#[tokio::test]
async fn test_health_content_type_is_json() {
    let app = build_app_with(vec![("x".into(), true)]);
    let req = Request::builder()
        .uri("/api/fuse/health")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        ct.contains("json"),
        "expected json content-type, got: {}",
        ct
    );
}

#[tokio::test]
async fn test_datasources_content_type_is_json() {
    let app = build_app_with(vec![("x".into(), true)]);
    let req = Request::builder()
        .uri("/api/fuse/datasources")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(ct.contains("json"));
}

// ── Consistency: health connectors match datasources ──

#[tokio::test]
async fn test_health_and_datasources_consistent() {
    let connectors = vec![("alpha".into(), true), ("beta".into(), true)];
    let app1 = build_app_with(connectors.clone());
    let app2 = build_app_with(connectors);

    let (_, health) = get_json(app1, "/api/fuse/health").await;
    let (_, ds) = get_json(app2, "/api/fuse/datasources").await;

    let health_ids: Vec<&str> = health["connectors"]
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    let ds_ids: Vec<&str> = ds
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|d| d["id"].as_str().or(d["name"].as_str()))
        .collect();

    for id in &health_ids {
        assert!(
            ds_ids.contains(id),
            "health connector {} not in datasources",
            id
        );
    }
}
