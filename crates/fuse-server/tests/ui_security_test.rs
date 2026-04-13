// SPDX-License-Identifier: Apache-2.0

//! UI security tests: XSS, CSRF, auth flow, error leakage, security headers.

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

// ── Mock connectors ──

#[derive(Debug)]
struct MockConn;

#[async_trait]
impl FederatedConnector for MockConn {
    fn id(&self) -> &str {
        "testds"
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
            name: "logs".into(),
            schema_type: SchemaType::Table,
            estimated_row_count: Some(2),
        }])
    }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![Field::new("name", DataType::Utf8, false)]))
    }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let schema = Arc::new(self.get_schema("").await?);
        Ok(vec![RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["alice"])) as _],
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

/// Connector that errors with internal details (IP, config paths).
#[derive(Debug)]
struct ErrorConn;

#[async_trait]
impl FederatedConnector for ErrorConn {
    fn id(&self) -> &str {
        "errds"
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
            name: "logs".into(),
            schema_type: SchemaType::Table,
            estimated_row_count: Some(1),
        }])
    }
    async fn get_schema(&self, _: &str) -> Result<Schema, ConnectorError> {
        Ok(Schema::new(vec![Field::new("x", DataType::Utf8, false)]))
    }
    async fn execute(&self, _: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        Err(ConnectorError::query(
            "connection refused at 10.0.0.42:5432 (pg_hba.conf rejects host)",
        ))
    }
    async fn execute_streaming(
        &self,
        _: &SubQuery,
        _: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::query("connection refused at 10.0.0.42:5432"))
    }
}

fn build_test_app() -> axum::Router {
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(MockConn)).unwrap();
    registry.register(Arc::new(ErrorConn)).unwrap();
    let state = Arc::new(AppState {
        registry: Arc::new(registry),
        alert_rules: vec![],
        view_registry: Arc::new(fuse_engine::materialized::MaterializedViewRegistry::new()),
        history: Arc::new(fuse_server::history::QueryHistory::new()),
        running_queries: Arc::new(RunningQueries::new()),
        saved_queries: Arc::new(fuse_server::saved_queries::SavedQueryRegistry::new()),
        plan_cache: Arc::new(fuse_server::plan_cache::PlanCache::new(300, 1000)),
        result_cache: Arc::new(fuse_server::plan_cache::ResultCache::new(60, 500)),
        tenant_registry: Arc::new(fuse_server::tenant::TenantRegistry::new(vec![])),
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
        adaptive_cache: Arc::new(fuse_server::adaptive_cache::AdaptiveCache::new(60, 3, 10000)),
        column_rbac: None,
        key_rotation: Arc::new(fuse_server::auth::KeyRotationManager::new(vec![])),
        schema_cache: Arc::new(fuse_server::api::SchemaCache::new(300)),
        smart_router: Arc::new(fuse_server::smart_routing::SmartRouter::new()),
        health_history: Arc::new(fuse_server::connector_health_history::HealthHistory::new()),
        pool_tracker: Arc::new(fuse_server::pool_stats::PoolStatsTracker::new()),
        feedback_store: Arc::new(fuse_server::feedback::FeedbackStore::new(100)),
    });
    fuse_server::build_router(state)
}

async fn do_req(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<&str>,
    headers: Vec<(&str, &str)>,
) -> (StatusCode, axum::http::HeaderMap, String) {
    let mut builder = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        builder = builder.header(k, v);
    }
    let b = body
        .map(|s| Body::from(s.to_string()))
        .unwrap_or(Body::empty());
    let resp = app.oneshot(builder.body(b).unwrap()).await.unwrap();
    let status = resp.status();
    let hdrs = resp.headers().clone();
    let bytes = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    (status, hdrs, String::from_utf8_lossy(&bytes).to_string())
}

async fn post_json(app: axum::Router, uri: &str, json: &str) -> (StatusCode, String) {
    let (s, _, body) = do_req(
        app,
        "POST",
        uri,
        Some(json),
        vec![("content-type", "application/json")],
    )
    .await;
    (s, body)
}

// ═══════════════════════════════════════════════════════════
// 1. XSS — API responses must be JSON, never raw HTML
// ═══════════════════════════════════════════════════════════

const XSS_PAYLOADS: &[&str] = &[
    "<script>alert(1)</script>",
    "<img src=x onerror=alert(1)>",
    "'\"><svg/onload=alert(1)>",
    "<iframe src=\"javascript:alert(1)\">",
];

#[tokio::test]
async fn test_xss_query_error_returns_json_not_html() {
    for payload in XSS_PAYLOADS {
        let query = format!("SELECT * FROM {}", payload);
        let body = serde_json::json!({"query": query, "format": "sql"}).to_string();
        let (_, text) = post_json(build_test_app(), "/api/fuse/query", &body).await;
        let json: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
        assert!(json.is_object(), "Non-JSON response for XSS payload: {}", payload);
    }
}

#[tokio::test]
async fn test_xss_in_datasource_path_returns_json() {
    for payload in XSS_PAYLOADS {
        let encoded = payload
            .replace('%', "%25")
            .replace('<', "%3C")
            .replace('>', "%3E")
            .replace('"', "%22")
            .replace('\'', "%27")
            .replace(' ', "%20");
        let uri = format!("/api/fuse/datasources/{}/schemas", encoded);
        let (_, _, text) = do_req(build_test_app(), "GET", &uri, None, vec![]).await;
        if !text.is_empty() {
            assert!(
                serde_json::from_str::<serde_json::Value>(&text).is_ok(),
                "Non-JSON response for datasource XSS path: {}",
                payload
            );
        }
    }
}

#[tokio::test]
async fn test_xss_validate_endpoint_returns_json() {
    for payload in XSS_PAYLOADS {
        let body = serde_json::json!({"query": payload}).to_string();
        let (_, text) = post_json(build_test_app(), "/api/fuse/query/validate", &body).await;
        let json: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
        assert!(json.is_object(), "Non-JSON validate response for: {}", payload);
    }
}

// ═══════════════════════════════════════════════════════════
// 2. Security headers
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn test_security_headers_on_html_page() {
    let (_, hdrs, _) = do_req(build_test_app(), "GET", "/", None, vec![]).await;
    assert_eq!(hdrs.get("x-content-type-options").unwrap(), "nosniff");
    assert_eq!(hdrs.get("x-frame-options").unwrap(), "DENY");
    assert!(hdrs.contains_key("content-security-policy"));
}

#[tokio::test]
async fn test_security_headers_on_api_json() {
    let (_, hdrs, _) = do_req(build_test_app(), "GET", "/api/fuse/health", None, vec![]).await;
    assert_eq!(hdrs.get("x-content-type-options").unwrap(), "nosniff");
    assert_eq!(hdrs.get("x-frame-options").unwrap(), "DENY");
}

#[tokio::test]
async fn test_csp_restricts_default_src() {
    let (_, hdrs, _) = do_req(build_test_app(), "GET", "/", None, vec![]).await;
    let csp = hdrs.get("content-security-policy").unwrap().to_str().unwrap();
    assert!(
        csp.contains("default-src 'self'"),
        "CSP missing default-src 'self': {}",
        csp
    );
}

// ═══════════════════════════════════════════════════════════
// 3. CSRF — state-changing endpoints reject non-JSON content types
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn test_query_rejects_form_urlencoded() {
    let (status, _, _) = do_req(
        build_test_app(),
        "POST",
        "/api/fuse/query",
        Some("query=SELECT+1"),
        vec![("content-type", "application/x-www-form-urlencoded")],
    )
    .await;
    assert_ne!(status, StatusCode::OK, "Accepted form-urlencoded (CSRF risk)");
}

#[tokio::test]
async fn test_query_rejects_text_plain() {
    let (status, _, _) = do_req(
        build_test_app(),
        "POST",
        "/api/fuse/query",
        Some("{\"query\": \"SELECT 1\"}"),
        vec![("content-type", "text/plain")],
    )
    .await;
    assert_ne!(status, StatusCode::OK, "Accepted text/plain (CSRF risk)");
}

// ═══════════════════════════════════════════════════════════
// 4. Auth flow (unit-level — middleware is disabled in test router)
// ═══════════════════════════════════════════════════════════

use fuse_server::auth::{ApiKeyEntry, AuthState, Role};

#[test]
fn test_auth_enabled_validates_known_key() {
    let auth = AuthState::new(vec![ApiKeyEntry {
        key: "k-admin".into(),
        identity: "admin".into(),
        role: Role::Admin,
    }]);
    assert!(auth.is_enabled());
    assert!(auth.validate("k-admin").is_some());
    assert_eq!(auth.validate("k-admin").unwrap().role, Role::Admin);
}

#[test]
fn test_auth_rejects_unknown_key() {
    let auth = AuthState::new(vec![ApiKeyEntry {
        key: "k-1".into(),
        identity: "a".into(),
        role: Role::Admin,
    }]);
    assert!(auth.validate("wrong").is_none());
    assert!(auth.validate("").is_none());
}

#[test]
fn test_auth_disabled_by_default() {
    let auth = AuthState::default();
    assert!(!auth.is_enabled());
}

#[test]
fn test_auth_role_hierarchy() {
    assert!(Role::Admin.has(Role::Viewer));
    assert!(Role::Admin.has(Role::Editor));
    assert!(Role::Editor.has(Role::Viewer));
    assert!(!Role::Viewer.has(Role::Editor));
    assert!(!Role::Viewer.has(Role::Admin));
}

#[tokio::test]
async fn test_public_paths_always_accessible() {
    for path in &["/api/fuse/health", "/", "/playground"] {
        let (status, _, _) = do_req(build_test_app(), "GET", path, None, vec![]).await;
        assert_eq!(status, StatusCode::OK, "Public path {} blocked", path);
    }
}

// ═══════════════════════════════════════════════════════════
// 5. Error leakage — 5xx must not expose internal details
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn test_error_does_not_leak_internal_ip() {
    let body = serde_json::json!({"query": "SELECT * FROM errds.logs", "format": "sql"}).to_string();
    let (status, _, text) = do_req(
        build_test_app(),
        "POST",
        "/api/fuse/query",
        Some(&body),
        vec![("content-type", "application/json")],
    )
    .await;
    if status.is_server_error() {
        assert!(!text.contains("10.0.0.42"), "Internal IP leaked in 5xx");
        assert!(!text.contains("pg_hba.conf"), "Config detail leaked in 5xx");
    }
}

#[tokio::test]
async fn test_error_does_not_leak_stack_trace() {
    let body = serde_json::json!({"query": "SELECT * FROM errds.logs", "format": "sql"}).to_string();
    let (_, _, text) = do_req(
        build_test_app(),
        "POST",
        "/api/fuse/query",
        Some(&body),
        vec![("content-type", "application/json")],
    )
    .await;
    let lower = text.to_lowercase();
    assert!(!lower.contains("stack trace"), "Stack trace leaked");
    assert!(!lower.contains("backtrace"), "Backtrace leaked");
    assert!(!lower.contains("panicked at"), "Panic info leaked");
}

#[tokio::test]
async fn test_404_does_not_leak_server_info() {
    let (status, _, body) =
        do_req(build_test_app(), "GET", "/api/fuse/nonexistent", None, vec![]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let lower = body.to_lowercase();
    assert!(!lower.contains("axum"), "Server framework leaked in 404");
    assert!(!lower.contains("tokio"), "Runtime leaked in 404");
}

// ═══════════════════════════════════════════════════════════
// 6. Playground HTML — every page with innerHTML defines esc()
// ═══════════════════════════════════════════════════════════

#[test]
fn test_all_playground_pages_define_esc() {
    let pages: &[(&str, &str)] = &[
        ("index.html", include_str!("../../../playground/index.html")),
        (
            "dashboard.html",
            include_str!("../../../playground/dashboard.html"),
        ),
        (
            "explore.html",
            include_str!("../../../playground/explore.html"),
        ),
        (
            "settings.html",
            include_str!("../../../playground/settings.html"),
        ),
        (
            "alerts.html",
            include_str!("../../../playground/alerts.html"),
        ),
        ("views.html", include_str!("../../../playground/views.html")),
        (
            "terminal.html",
            include_str!("../../../playground/terminal.html"),
        ),
        (
            "federation.html",
            include_str!("../../../playground/federation.html"),
        ),
        (
            "schedules.html",
            include_str!("../../../playground/schedules.html"),
        ),
        (
            "quality.html",
            include_str!("../../../playground/quality.html"),
        ),
        (
            "lineage.html",
            include_str!("../../../playground/lineage.html"),
        ),
        (
            "replay.html",
            include_str!("../../../playground/replay.html"),
        ),
        ("cost.html", include_str!("../../../playground/cost.html")),
        (
            "graphql.html",
            include_str!("../../../playground/graphql.html"),
        ),
        (
            "webhooks.html",
            include_str!("../../../playground/webhooks.html"),
        ),
        (
            "plugins.html",
            include_str!("../../../playground/plugins.html"),
        ),
        ("admin.html", include_str!("../../../playground/admin.html")),
        (
            "status.html",
            include_str!("../../../playground/status.html"),
        ),
    ];
    for (name, html) in pages {
        // Count innerHTML usages excluding the feedback widget loader
        // (which injects server-controlled HTML from /feedback-widget, not user input)
        let has_user_facing_innerhtml = html
            .lines()
            .any(|l| l.contains("innerHTML") && !l.contains("feedback-widget"));
        if has_user_facing_innerhtml {
            assert!(
                html.contains("function esc") || html.contains("const esc"),
                "{} uses innerHTML but does not define esc()",
                name
            );
        }
    }
}

#[test]
fn test_esc_function_escapes_required_chars() {
    // The esc() in index.html must handle & < > "
    let index = include_str!("../../../playground/index.html");
    let esc_block: String = index
        .lines()
        .skip_while(|l| !l.contains("function esc"))
        .take(3)
        .collect::<Vec<_>>()
        .join(" ");
    for entity in &["&amp;", "&lt;", "&gt;", "&quot;"] {
        assert!(
            esc_block.contains(entity),
            "esc() in index.html missing escape for {}",
            entity
        );
    }
}

// ═══════════════════════════════════════════════════════════
// 7. API error responses are always valid JSON
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn test_api_error_responses_are_json() {
    let cases: Vec<(&str, &str, Option<&str>)> = vec![
        ("POST", "/api/fuse/query", Some("{\"query\": \"\"}")),
        ("GET", "/api/fuse/datasources/nonexistent/schemas", None),
    ];
    for (method, uri, body) in cases {
        let headers = if body.is_some() {
            vec![("content-type", "application/json")]
        } else {
            vec![]
        };
        let (_, _, text) = do_req(build_test_app(), method, uri, body, headers).await;
        if !text.is_empty() {
            assert!(
                serde_json::from_str::<serde_json::Value>(&text).is_ok(),
                "Non-JSON API response at {} {}: {}",
                method,
                uri,
                &text[..text.len().min(200)]
            );
        }
    }
}

// ═══════════════════════════════════════════════════════════
// 8. Malformed input — no 5xx on bad JSON
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn test_malformed_json_does_not_500() {
    let (status, _, _) = do_req(
        build_test_app(),
        "POST",
        "/api/fuse/query",
        Some("{\"bad json"),
        vec![("content-type", "application/json")],
    )
    .await;
    assert!(
        status.is_client_error(),
        "Malformed JSON should return 4xx, got {}",
        status
    );
}

// ═══════════════════════════════════════════════════════════
// 9. SSRF protection (url_validator)
// ═══════════════════════════════════════════════════════════

use fuse_server::url_validator::validate_callback_url;

#[test]
fn test_ssrf_blocks_internal_addresses() {
    for url in &[
        "http://localhost/hook",
        "http://127.0.0.1/hook",
        "http://10.0.0.1/hook",
        "http://169.254.169.254/latest/meta-data/",
        "ftp://example.com/hook",
        "file:///etc/passwd",
    ] {
        assert!(
            validate_callback_url(url).is_err(),
            "SSRF: should block {}",
            url
        );
    }
}

#[test]
fn test_ssrf_allows_public_https() {
    assert!(validate_callback_url("https://hooks.slack.com/services/T/B/x").is_ok());
    assert!(validate_callback_url("https://example.com/webhook").is_ok());
}
