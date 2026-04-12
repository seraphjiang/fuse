// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use axum::extract::{Json, Path, State, Extension};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use fuse_core::registry::ConnectorRegistry;
use fuse_core::alerting::{AlertEvaluator, AlertRule};
use fuse_engine::materialized::MaterializedViewRegistry;
use crate::shared_state::{SharedSavedQueries, SharedQueryHistory, SharedAuditLog};
use crate::transaction::TransactionStore;
use crate::history::QueryHistory;
use crate::saved_queries::SavedQueryRegistry;
use crate::plan_cache::{PlanCache, CachedPlan};
use crate::tenant::{TenantRegistry, QueryGovernor};

use crate::health;

/// Tracks running queries for cancellation.
pub struct RunningQueries {
    inner: std::sync::Mutex<std::collections::HashMap<String, CancellationToken>>,
}

impl Default for RunningQueries {
    fn default() -> Self {
        Self::new()
    }
}

impl RunningQueries {
    pub fn new() -> Self {
        Self { inner: std::sync::Mutex::new(std::collections::HashMap::new()) }
    }
    fn insert(&self, id: String, token: CancellationToken) {
        self.inner.lock().unwrap().insert(id, token);
    }
    fn remove(&self, id: &str) {
        self.inner.lock().unwrap().remove(id);
    }
    /// Cancel a running query. Returns true if found.
    pub fn cancel(&self, id: &str) -> bool {
        if let Some(token) = self.inner.lock().unwrap().remove(id) {
            token.cancel();
            true
        } else {
            false
        }
    }
    pub fn list(&self) -> Vec<String> {
        self.inner.lock().unwrap().keys().cloned().collect()
    }
    pub fn count(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
    pub fn cancel_all(&self) {
        let mut map = self.inner.lock().unwrap();
        for (_, token) in map.drain() {
            token.cancel();
        }
    }
}

/// Shared application state passed to all handlers.
pub struct AppState {
    pub registry: Arc<ConnectorRegistry>,
    #[allow(dead_code)]
    pub alert_rules: Vec<AlertRule>,
    pub view_registry: Arc<MaterializedViewRegistry>,
    pub history: Arc<QueryHistory>,
    pub running_queries: Arc<RunningQueries>,
    pub saved_queries: Arc<SavedQueryRegistry>,
    pub plan_cache: Arc<PlanCache>,
    pub result_cache: Arc<crate::plan_cache::ResultCache>,
    pub tenant_registry: Arc<TenantRegistry>,
    pub audit_log: Arc<crate::audit::AuditLog>,
    pub prepared_statements: crate::prepared::PreparedStatementStore,
    pub adaptive_timeout: Arc<crate::adaptive_timeout::AdaptiveTimeout>,
    /// Redis-backed shared stores for stateless horizontal scaling.
    /// When FUSE_REDIS_URL is set, these use Redis; otherwise in-memory fallback.
    pub shared_saved_queries: SharedSavedQueries,
    pub shared_history: SharedQueryHistory,
    pub shared_audit_log: SharedAuditLog,
    pub transactions: Arc<TransactionStore>,
    /// Global max result size in bytes (0 = unlimited). From engine.max_result_bytes.
    pub max_result_bytes: u64,
    /// Per-datasource concurrency limiter. Limits concurrent queries per connector.
    pub datasource_limiter: Arc<crate::rate_limit::DatasourceLimiter>,
    /// OTel collector store — present when an `otel` connector is configured.
    pub otel_store: Option<Arc<fuse_connector_otel::store::OtelStore>>,
    /// Query recorder for replay & regression testing (#1812).
    pub query_recorder: Arc<crate::query_replay::QueryRecorder>,
    /// #1820 Adaptive fan-out concurrency — auto-tunes per datasource.
    pub adaptive_parallelism: Arc<crate::adaptive_parallelism::AdaptiveParallelism>,
    /// #1811 Webhook subscriptions — event-driven query monitoring.
    pub webhook_registry: Arc<crate::webhook::WebhookRegistry>,
    /// #1851 Query compilation cache — skip re-parsing for repeated query patterns.
    pub compilation_cache: Arc<crate::query_compilation::CompilationCache>,
    /// #1852 CDC tracker — change data capture for materialized view refresh.
    pub cdc_tracker: Arc<crate::cdc::CdcTracker>,
    /// #1821 Adaptive cache — auto-promote hot queries with per-datasource TTL.
    pub adaptive_cache: Arc<crate::adaptive_cache::AdaptiveCache>,
    /// Column-level RBAC — deny or mask sensitive fields per role.
    pub column_rbac: Option<Arc<fuse_core::security::ResultFilter>>,
    /// API key rotation manager with grace period support.
    pub key_rotation: Arc<crate::auth::KeyRotationManager>,
}

/// Result from multi-datasource execution, carrying batches + per-source stats.
struct FederatedResult {
    batches: Vec<arrow::record_batch::RecordBatch>,
    stats: Option<std::collections::HashMap<String, DatasourceStat>>,
    datasources: Vec<String>,
    /// Maps datasource_id → connector_type for cost estimation.
    connector_types: std::collections::HashMap<String, String>,
    profile_nodes: Vec<ProfileNode>,
    /// Per-datasource errors for partial failure reporting.
    partial_errors: Vec<PartialError>,
}

#[derive(Serialize, Clone, Debug)]
pub struct PartialError {
    pub datasource: String,
    pub error: String,
}

// ── Request / Response types ──

#[derive(Deserialize, Clone)]
pub struct QueryRequest {
    pub query: String,
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default)]
    pub analyze: bool,
    /// Per-query timeout in milliseconds. If not set, uses server default (30s).
    pub timeout_ms: Option<u64>,
    /// Output format: "json" (default), "csv", or "ndjson" (newline-delimited JSON, chunked).
    #[serde(default = "default_result_format")]
    pub result_format: String,
    /// Named query parameters. Keys without $ prefix are matched against $key in query.
    #[serde(default)]
    pub params: std::collections::HashMap<String, serde_json::Value>,
    /// Prometheus range query: start time (RFC3339 or Unix timestamp string).
    pub start: Option<String>,
    /// Prometheus range query: end time (RFC3339 or Unix timestamp string).
    pub end: Option<String>,
    /// Prometheus range query: step duration (e.g. "15s", "1m").
    pub step: Option<String>,
    /// Cursor token for pagination. Returned as next_cursor in previous response.
    pub cursor: Option<String>,
    /// Page size for cursor pagination. Defaults to LIMIT value or 1000.
    pub page_size: Option<usize>,
}

fn default_format() -> String {
    "sql".to_string()
}

fn default_result_format() -> String {
    "json".to_string()
}

const DEFAULT_TIMEOUT_MS: u64 = 30_000;

#[derive(Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub metadata: QueryMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_profile: Option<ExecutionProfile>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub partial_errors: Vec<PartialError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize)]
pub struct QueryMetadata {
    pub total_rows: u64,
    pub format: String,
    pub trace_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasources_queried: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasource_stats: Option<std::collections::HashMap<String, DatasourceStat>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_estimate: Option<crate::cost_estimator::QueryCostEstimate>,
}

#[derive(Serialize, Clone, Debug)]
pub struct DatasourceStat {
    pub rows: u64,
    pub latency_ms: u64,
}

#[derive(Serialize, Clone, Debug)]
pub struct ExecutionProfile {
    pub total_ms: u64,
    pub nodes: Vec<ProfileNode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_hit: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub optimizer_rules_applied: Vec<String>,
}

#[derive(Serialize, Clone, Debug)]
pub struct ProfileNode {
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasource: Option<String>,
    pub actual_rows: u64,
    pub actual_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pushdown: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimate_accuracy: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<ProfileNode>,
}

impl ProfileNode {
    fn scan(ds: &str, rows: u64, ms: u64, bytes: u64, pushdown: Vec<String>) -> Self {
        let default_estimate = 10_000u64;
        let est_cost = bytes as f64 + ms as f64 * 10.0;
        let accuracy = if rows > 0 {
            let ratio = default_estimate as f64 / rows as f64;
            Some(format!("{:.1}x (est {} vs actual {})", ratio, default_estimate, rows))
        } else {
            None
        };
        Self {
            op: "RemoteScan".into(),
            datasource: Some(ds.into()),
            actual_rows: rows,
            actual_ms: ms,
            data_bytes: Some(bytes),
            estimated_rows: Some(default_estimate),
            estimated_cost: Some(est_cost),
            detail: Some(format!("Scan {}", ds)),
            pushdown,
            estimate_accuracy: accuracy,
            children: vec![],
        }
    }

    fn parent(op: &str, rows: u64, ms: u64, children: Vec<ProfileNode>) -> Self {
        let cost: f64 = children.iter().map(|c| c.actual_ms as f64).sum::<f64>() + ms as f64;
        let est_rows: u64 = children.iter().filter_map(|c| c.estimated_rows).sum();
        let accuracy = if est_rows > 0 && rows > 0 {
            let ratio = est_rows as f64 / rows as f64;
            Some(format!("{:.1}x (est {} vs actual {})", ratio, est_rows, rows))
        } else {
            None
        };
        Self {
            op: op.into(),
            datasource: None,
            actual_rows: rows,
            actual_ms: ms,
            data_bytes: None,
            estimated_rows: if est_rows > 0 { Some(est_rows) } else { None },
            estimated_cost: Some(cost),
            detail: None,
            pushdown: vec![],
            estimate_accuracy: accuracy,
            children,
        }
    }
}

#[derive(Serialize)]
pub struct DatasourceInfo {
    pub id: String,
    pub connector_type: String,
    pub capabilities: fuse_core::connector::ConnectorCapabilities,
}

#[derive(Serialize)]
pub struct ExplainResponse {
    pub plan: String,
    /// Tree-formatted plan with box-drawing characters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_display: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_tree: Option<fuse_engine::plan::PlanNode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_profile: Option<ExecutionProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complexity: Option<crate::complexity::ComplexityScore>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_estimate: Option<crate::cost_estimator::QueryCostEstimate>,
}

#[derive(Serialize)]
pub struct ValidateResponse {
    pub valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

fn error_json(status: StatusCode, msg: impl ToString) -> impl IntoResponse {
    let message = msg.to_string();
    // For server errors, log the full detail but return a sanitized message to the client.
    // Timeout/cancellation messages are safe to expose (no internal details).
    let client_message = if status.is_server_error() && !message.contains("timed out") && !message.contains("cancelled") {
        tracing::error!(status = %status, detail = %message, "internal error");
        "internal server error".to_string()
    } else {
        if status.is_server_error() {
            tracing::warn!(status = %status, detail = %message, "query timeout/cancellation");
        }
        message
    };
    (
        status,
        Json(ErrorResponse {
            error: client_message,
        }),
    )
}

/// Extract client IP from request headers (X-Forwarded-For, then X-Real-IP).
fn extract_client_ip(headers: &axum::http::HeaderMap) -> Option<String> {
    headers.get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            headers.get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim().to_string())
        })
}

/// Substitute `$key` placeholders in query with parameter values.
/// String values are single-quoted and escaped. Numbers/bools are inlined.
/// Rewrite CONTAINS 'term' → LIKE '%term%' for full-text search syntax.
/// Also handles MATCH(field, 'term') → field LIKE '%term%'.
/// Parse CREATE VIEW name AS query.
pub fn parse_create_view(query: &str) -> Option<(String, String)> {
    let lower = query.trim().to_lowercase();
    if !lower.starts_with("create view ") {
        return None;
    }
    let after = query.trim()[12..].trim(); // skip "CREATE VIEW "
    let as_pos = after.to_lowercase().find(" as ")?;
    let name = after[..as_pos].trim().to_string();
    let view_query = after[as_pos + 4..].trim().to_string();
    if name.is_empty() || view_query.is_empty() { return None; }
    Some((name, view_query))
}

/// Parse `CREATE MATERIALIZED VIEW <name> AS <query>`.
pub fn parse_create_materialized_view(query: &str) -> Option<(String, String)> {
    let lower = query.trim().to_lowercase();
    if !lower.starts_with("create materialized view ") {
        return None;
    }
    let after = query.trim()["create materialized view ".len()..].trim();
    let as_pos = after.to_lowercase().find(" as ")?;
    let name = after[..as_pos].trim().to_string();
    let view_query = after[as_pos + 4..].trim().to_string();
    if name.is_empty() || view_query.is_empty() { return None; }
    Some((name, view_query))
}

/// Parse `REFRESH MATERIALIZED VIEW <name>`.
pub fn parse_refresh_materialized_view(query: &str) -> Option<String> {
    let lower = query.trim().to_lowercase();
    if !lower.starts_with("refresh materialized view ") {
        return None;
    }
    let name = query.trim()["refresh materialized view ".len()..].trim().to_string();
    if name.is_empty() { return None; }
    Some(name)
}

/// Parse `BEGIN [TRANSACTION [id]]`.
pub fn parse_begin(query: &str) -> Option<String> {
    let lower = query.trim().to_lowercase();
    if lower == "begin" || lower == "begin transaction" {
        return Some(uuid_v4());
    }
    if lower.starts_with("begin transaction ") {
        let id = query.trim()["begin transaction ".len()..].trim().to_string();
        if !id.is_empty() { return Some(id); }
    }
    if lower.starts_with("begin ") && !lower.starts_with("begin transaction") {
        let id = query.trim()["begin ".len()..].trim().to_string();
        if !id.is_empty() { return Some(id); }
    }
    None
}

/// Parse `COMMIT [id]`.
pub fn parse_commit(query: &str) -> Option<String> {
    let lower = query.trim().to_lowercase();
    if lower == "commit" { return None; } // needs txn_id from params
    if lower.starts_with("commit ") {
        let id = query.trim()["commit ".len()..].trim().to_string();
        if !id.is_empty() { return Some(id); }
    }
    None
}

/// Parse `ROLLBACK [id]`.
pub fn parse_rollback(query: &str) -> Option<String> {
    let lower = query.trim().to_lowercase();
    if lower == "rollback" { return None; }
    if lower.starts_with("rollback ") {
        let id = query.trim()["rollback ".len()..].trim().to_string();
        if !id.is_empty() { return Some(id); }
    }
    None
}

fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    format!("txn-{:x}-{:x}", t.as_secs(), t.subsec_nanos())
}

/// Parse `CREATE TABLE <datasource.table> AS <query>`.
/// Returns `(datasource, table, inner_query)` if matched.
pub fn parse_ctas(query: &str) -> Option<(String, String, String)> {
    let lower = query.trim().to_lowercase();
    if !lower.starts_with("create table ") {
        return None;
    }
    let after = query.trim()["create table ".len()..].trim();
    let as_pos = after.to_lowercase().find(" as ")?;
    let dest = after[..as_pos].trim();
    let inner_query = after[as_pos + 4..].trim().to_string();
    if dest.is_empty() || inner_query.is_empty() { return None; }
    let (ds, tbl) = parse_qualified_name(dest).ok()?;
    Some((ds, tbl, inner_query))
}

/// Parse `INSERT INTO <datasource.table> SELECT ...`.
/// Returns `(datasource, table, select_query)` if matched.
pub fn parse_insert_into_select(query: &str) -> Option<(String, String, String)> {
    let lower = query.trim().to_lowercase();
    if !lower.starts_with("insert into ") {
        return None;
    }
    let after = query.trim()["insert into ".len()..].trim();
    // Find the SELECT keyword
    let sel_pos = after.to_lowercase().find(" select ")?;
    let dest = after[..sel_pos].trim();
    let select_query = after[sel_pos + 1..].trim().to_string();
    if dest.is_empty() || select_query.is_empty() { return None; }
    let (ds, tbl) = parse_qualified_name(dest).ok()?;
    Some((ds, tbl, select_query))
}

pub fn rewrite_contains(query: &str) -> String {
    let lower = query.to_lowercase();
    if !lower.contains(" contains '") {
        return query.to_string();
    }

    let mut result = String::with_capacity(query.len());
    let mut pos = 0;

    while pos < query.len() {
        if let Some(rel) = lower[pos..].find(" contains '") {
            let abs = pos + rel;
            // Copy everything before this match
            result.push_str(&query[pos..abs]);
            // Find closing quote after " contains '"
            let term_start = abs + 11; // skip " contains '"
            if let Some(end_quote) = query[term_start..].find('\'') {
                let term = &query[term_start..term_start + end_quote];
                result.push_str(&format!(" LIKE '%{}%'", term));
                pos = term_start + end_quote + 1;
            } else {
                // No closing quote — copy as-is
                result.push_str(&query[abs..abs + 11]);
                pos = term_start;
            }
        } else {
            result.push_str(&query[pos..]);
            break;
        }
    }

    result
}

fn bind_params(query: &str, params: &std::collections::HashMap<String, serde_json::Value>) -> String {
    // Sort by key length descending to avoid $host matching inside $hostname
    let mut sorted: Vec<_> = params.iter().collect();
    sorted.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

    let mut result = query.to_string();
    for (key, val) in sorted {
        let placeholder = format!("${}", key);
        let replacement = match val {
            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => "NULL".to_string(),
            other => format!("'{}'", other.to_string().replace('\'', "''")),
        };
        result = result.replace(&placeholder, &replacement);
    }
    result
}

// ── Handlers ──

/// POST /api/fuse/query
pub async fn query_handler(
    State(state): State<Arc<AppState>>,
    auth_identity: Option<Extension<crate::auth::AuthIdentity>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let t0 = std::time::Instant::now();
    let format = req.format.to_lowercase();
    let query_id = format!("q-{:016x}", QUERY_COUNTER.fetch_add(1, Ordering::Relaxed));

    // Support timeout via header: X-Fuse-Timeout-Ms
    let header_timeout = headers.get("x-fuse-timeout-ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    tracing::info!(
        query_id = %query_id,
        format = %format,
        analyze = req.analyze,
        query_len = req.query.len(),
        "Query received"
    );

    // Bind parameters if provided
    let query = if req.params.is_empty() {
        req.query.clone()
    } else {
        bind_params(&req.query, &req.params)
    };

    // Handle PREPARE <name> AS <query> — store template, return confirmation
    if let Some((name, template)) = crate::prepared::parse_prepare(&query) {
        let param_count = crate::prepared::count_params(&template);
        let stmt = crate::prepared::PreparedStatement {
            query: template.clone(),
            param_count,
        };
        state.prepared_statements.lock().unwrap().insert(name.clone(), stmt);
        return Json(serde_json::json!({
            "prepared": name,
            "param_count": param_count,
            "template": template,
        })).into_response();
    }

    // Handle EXECUTE <name> USING <params> — resolve template, bind params, continue
    let query = if let Some((name, params)) = crate::prepared::parse_execute(&query) {
        let stmt = state.prepared_statements.lock().unwrap().get(&name).cloned();
        match stmt {
            Some(s) => {
                if !params.is_empty() && params.len() != s.param_count {
                    return error_json(
                        StatusCode::BAD_REQUEST,
                        format!("expected {} parameters, got {}", s.param_count, params.len()),
                    ).into_response();
                }
                crate::prepared::bind_positional(&s.query, &params)
            }
            None => {
                return error_json(
                    StatusCode::NOT_FOUND,
                    format!("prepared statement '{}' not found", name),
                ).into_response();
            }
        }
    } else {
        query
    };

    // Rewrite CONTAINS 'term' → LIKE '%term%' for full-text search
    let query = rewrite_contains(&query);

    // Check result cache
    let result_cache_key = format!("{}:{}", format, query);
    if let Some(cached_result) = state.result_cache.get(&result_cache_key) {
        let mut resp = cached_result.response_json.clone();
        if let Some(meta) = resp.get_mut("metadata") {
            meta["trace_id"] = serde_json::Value::String(query_id.clone());
        }
        return (StatusCode::OK, Json(resp)).into_response();
    }

    // Handle BEGIN
    let lower_trimmed = query.trim().to_lowercase();
    if lower_trimmed == "begin" || lower_trimmed.starts_with("begin ") || lower_trimmed == "begin transaction" {
        if let Some(txn_id) = parse_begin(&query) {
            if state.transactions.begin(&txn_id) {
                return Json(serde_json::json!({
                    "transaction_id": txn_id,
                    "status": "active",
                })).into_response();
            } else {
                return error_json(StatusCode::CONFLICT, format!("transaction '{}' already exists", txn_id)).into_response();
            }
        }
    }

    // Handle COMMIT
    if lower_trimmed == "commit" || lower_trimmed.starts_with("commit ") {
        let txn_id = parse_commit(&query)
            .or_else(|| req.params.get("transaction_id").and_then(|v| v.as_str().map(String::from)));
        let txn_id = match txn_id {
            Some(id) => id,
            None => return error_json(StatusCode::BAD_REQUEST, "COMMIT requires transaction_id").into_response(),
        };
        let writes = match state.transactions.take(&txn_id) {
            Some(w) => w,
            None => return error_json(StatusCode::NOT_FOUND, format!("transaction '{}' not found", txn_id)).into_response(),
        };
        let mut results = Vec::new();
        let mut errors = Vec::new();
        for pw in writes {
            match state.registry.get(&pw.datasource) {
                Some(conn) => match conn.write_batches(&pw.table, pw.batches).await {
                    Ok(n) => results.push(serde_json::json!({"datasource": pw.datasource, "table": pw.table, "rows_written": n})),
                    Err(e) => errors.push(serde_json::json!({"datasource": pw.datasource, "table": pw.table, "error": e.to_string()})),
                },
                None => errors.push(serde_json::json!({"datasource": pw.datasource, "error": "not found"})),
            }
        }
        return Json(serde_json::json!({
            "committed": true,
            "transaction_id": txn_id,
            "results": results,
            "errors": errors,
        })).into_response();
    }

    // Handle ROLLBACK
    if lower_trimmed == "rollback" || lower_trimmed.starts_with("rollback ") {
        let txn_id = parse_rollback(&query)
            .or_else(|| req.params.get("transaction_id").and_then(|v| v.as_str().map(String::from)));
        let txn_id = match txn_id {
            Some(id) => id,
            None => return error_json(StatusCode::BAD_REQUEST, "ROLLBACK requires transaction_id").into_response(),
        };
        if state.transactions.rollback(&txn_id) {
            return Json(serde_json::json!({"rolled_back": true, "transaction_id": txn_id})).into_response();
        } else {
            return error_json(StatusCode::NOT_FOUND, format!("transaction '{}' not found", txn_id)).into_response();
        }
    }

    // Handle CREATE TABLE datasource.table AS SELECT ... (CTAS)
    if let Some((dest_ds, dest_table, select_query)) = parse_ctas(&query) {
        // Execute the SELECT query
        let src_refs = match parse_sql_sources(&select_query) {
            Ok(r) if !r.is_empty() => r,
            _ => return error_json(StatusCode::BAD_REQUEST, "failed to parse source query").into_response(),
        };
        let (src_ds, src_table) = &src_refs[0];
        let src_connector = match state.registry.get(src_ds) {
            Some(c) => c,
            None => return error_json(StatusCode::NOT_FOUND, format!("source datasource '{}' not found", src_ds)).into_response(),
        };
        let sq = match build_sub_query(&select_query, "sql", src_table) {
            Ok(sq) => sq,
            Err(e) => return error_json(StatusCode::BAD_REQUEST, e).into_response(),
        };
        let batches = match src_connector.execute(&sq).await {
            Ok(b) => b,
            Err(e) => return error_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        };
        // Write to destination connector
        let dest_connector = match state.registry.get(&dest_ds) {
            Some(c) => c,
            None => return error_json(StatusCode::NOT_FOUND, format!("destination datasource '{}' not found", dest_ds)).into_response(),
        };
        let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        match dest_connector.write_batches(&dest_table, batches).await {
            Ok(written) => return (StatusCode::CREATED, Json(serde_json::json!({
                "message": format!("table '{}.{}' created", dest_ds, dest_table),
                "rows_written": written,
                "rows_selected": row_count,
            }))).into_response(),
            Err(e) => return error_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    }

    // Handle INSERT INTO datasource.table SELECT ...
    if let Some((dest_ds, dest_table, select_query)) = parse_insert_into_select(&query) {
        let src_refs = match parse_sql_sources(&select_query) {
            Ok(r) if !r.is_empty() => r,
            _ => return error_json(StatusCode::BAD_REQUEST, "failed to parse source query").into_response(),
        };
        let (src_ds, src_table) = &src_refs[0];
        let src_connector = match state.registry.get(src_ds) {
            Some(c) => c,
            None => return error_json(StatusCode::NOT_FOUND, format!("source datasource '{}' not found", src_ds)).into_response(),
        };
        let sq = match build_sub_query(&select_query, "sql", src_table) {
            Ok(sq) => sq,
            Err(e) => return error_json(StatusCode::BAD_REQUEST, e).into_response(),
        };
        let batches = match src_connector.execute(&sq).await {
            Ok(b) => b,
            Err(e) => return error_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        };
        let dest_connector = match state.registry.get(&dest_ds) {
            Some(c) => c,
            None => return error_json(StatusCode::NOT_FOUND, format!("destination datasource '{}' not found", dest_ds)).into_response(),
        };
        let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        match dest_connector.write_batches(&dest_table, batches).await {
            Ok(written) => return Json(serde_json::json!({
                "message": format!("inserted into '{}.{}'", dest_ds, dest_table),
                "rows_written": written,
                "rows_selected": row_count,
            })).into_response(),
            Err(e) => return error_json(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        }
    }

    // Handle CREATE MATERIALIZED VIEW name AS query — store + execute immediately
    if let Some((view_name, view_query)) = parse_create_materialized_view(&query) {
        let def = fuse_engine::materialized::MaterializedViewDef {
            name: view_name.clone(),
            query: view_query.clone(),
            refresh_interval: std::time::Duration::from_secs(300),
        };
        state.view_registry.register(def);

        // Execute the query immediately to populate the view
        let exec_result = match parse_sql_sources(&view_query) {
            Ok(refs) if !refs.is_empty() => {
                let (ds_id, table) = &refs[0];
                match state.registry.get(ds_id) {
                    Some(connector) => {
                        match build_sub_query(&view_query, "sql", table) {
                            Ok(sq) => match connector.execute(&sq).await {
                                Ok(batches) => {
                                    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                                    if let Some(v) = state.view_registry.get(&view_name) {
                                        v.write().unwrap().set_results(batches);
                                    }
                                    Ok(row_count)
                                }
                                Err(e) => Err(e.to_string()),
                            },
                            Err(e) => Err(e),
                        }
                    }
                    None => Err(format!("datasource '{}' not found", ds_id)),
                }
            }
            _ => Err("failed to parse view query sources".into()),
        };

        let (row_count, error) = match exec_result {
            Ok(n) => (n, None),
            Err(e) => {
                if let Some(v) = state.view_registry.get(&view_name) {
                    v.write().unwrap().set_error(e.clone());
                }
                (0, Some(e))
            }
        };

        let mut resp = serde_json::json!({
            "message": format!("materialized view '{}' created", view_name),
            "name": view_name,
            "query": view_query,
            "row_count": row_count,
        });
        if let Some(e) = error {
            resp["initial_refresh_error"] = serde_json::Value::String(e);
        }
        return (StatusCode::CREATED, Json(resp)).into_response();
    }

    // Handle REFRESH MATERIALIZED VIEW name — re-execute and replace cached results
    if let Some(view_name) = parse_refresh_materialized_view(&query) {
        let view_arc = match state.view_registry.get(&view_name) {
            Some(v) => v,
            None => return error_json(StatusCode::NOT_FOUND, format!("materialized view '{}' not found", view_name)).into_response(),
        };
        let view_query = view_arc.read().unwrap().def.query.clone();

        let result = match parse_sql_sources(&view_query) {
            Ok(refs) if !refs.is_empty() => {
                let (ds_id, table) = &refs[0];
                match state.registry.get(ds_id) {
                    Some(connector) => match build_sub_query(&view_query, "sql", table) {
                        Ok(sq) => connector.execute(&sq).await.map_err(|e| e.to_string()),
                        Err(e) => Err(e),
                    },
                    None => Err(format!("datasource '{}' not found", ds_id)),
                }
            }
            _ => Err("failed to parse view query sources".into()),
        };

        return match result {
            Ok(batches) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                view_arc.write().unwrap().set_results(batches);
                Json(serde_json::json!({
                    "refreshed": true,
                    "view": view_name,
                    "row_count": row_count,
                })).into_response()
            }
            Err(e) => {
                view_arc.write().unwrap().set_error(e.clone());
                error_json(StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
            }
        };
    }

    // Handle CREATE VIEW name AS query
    if let Some((view_name, view_query)) = parse_create_view(&query) {
        let def = fuse_engine::materialized::MaterializedViewDef {
            name: view_name.clone(),
            query: view_query.clone(),
            refresh_interval: std::time::Duration::from_secs(300),
        };
        state.view_registry.register(def);
        return (StatusCode::CREATED, Json(serde_json::json!({
            "message": format!("view '{}' created", view_name),
            "name": view_name,
            "query": view_query,
        }))).into_response();
    }

    // Parse all datasource.table references from the query (with plan cache)
    let cache_key = format!("{}:{}", format, query);
    let cached = state.plan_cache.get(&cache_key);

    let refs = if let Some(ref plan) = cached {
        Ok(plan.sources.clone())
    } else {
        match format.as_str() {
            "ppl" => parse_ppl_sources(&query),
            _ => parse_sql_sources(&query),
        }
    };

    let refs = match refs {
        Ok(r) if r.is_empty() => {
            return error_json(StatusCode::BAD_REQUEST, "no datasource.table references found")
                .into_response()
        }
        Ok(r) => r,
        Err(e) => return error_json(StatusCode::BAD_REQUEST, e).into_response(),
    };

    // Resolve CTEs first — they register temp datasources needed by validation
    let (query, refs, _cte_connectors) = resolve_ctes(&state, &query, &format, &refs).await;

    // Validate all datasources exist
    for (ds_id, _) in &refs {
        if state.registry.get(ds_id).is_none() {
            return error_json(
                StatusCode::NOT_FOUND,
                format!("datasource '{}' not found", ds_id),
            )
            .into_response();
        }
    }

    // Tenant isolation: prefer authenticated identity, fall back to _tenant_id param.
    let tenant_id = auth_identity
        .as_ref()
        .map(|ext| ext.0.identity.clone())
        .or_else(|| req.params.get("_tenant_id").and_then(|v| v.as_str()).map(String::from));
    if let Some(ref tid) = tenant_id {
        if state.tenant_registry.is_enabled() {
            let ds_ids: Vec<String> = refs.iter().map(|(ds, _)| ds.clone()).collect();
            let allowed = state.tenant_registry.filter_datasources(tid, &ds_ids);
            for (ds_id, _) in &refs {
                if !allowed.contains(ds_id) {
                    return error_json(
                        StatusCode::FORBIDDEN,
                        format!("tenant '{}' does not have access to datasource '{}'", tid, ds_id),
                    )
                    .into_response();
                }
            }
        }
    }

    // Apply timeout: explicit body > header > adaptive (per-datasource p95) > default
    let base_timeout_ms = if let Some(ms) = req.timeout_ms {
        ms
    } else if let Some(ms) = header_timeout {
        ms
    } else if let Some(first_ds) = refs.first().map(|(id, _)| id.as_str()) {
        state.adaptive_timeout.timeout_ms_for(first_ds)
    } else {
        DEFAULT_TIMEOUT_MS
    };
    let effective_timeout_ms = if let Some(ref tid) = tenant_id {
        state.tenant_registry.get(tid)
            .map(|c| QueryGovernor::effective_timeout_ms(c, base_timeout_ms))
            .unwrap_or(base_timeout_ms)
    } else {
        base_timeout_ms
    };
    let timeout = std::time::Duration::from_millis(effective_timeout_ms);

    // Register cancellable query
    let cancel_token = CancellationToken::new();
    state.running_queries.insert(query_id.clone(), cancel_token.clone());

    // Resolve IN (SELECT ...) subqueries by executing inner queries first
    let mut resolved_query = query.clone();
    let in_subqueries = extract_in_subqueries(&query);
    for isq in &in_subqueries {
        if let Some(connector) = state.registry.get(&isq.datasource) {
            let sq = build_sub_query(&isq.inner_query, &format, &isq.table).unwrap_or_else(|_| {
                fuse_core::connector::SubQuery {
                    table: isq.table.clone(),
                    projections: vec![isq.inner_column.clone()],
                    filter: None, aggregations: vec![], group_by: vec![],
                    sort: vec![], limit: None, having: None, offset: None, passthrough: None,
                }
            });
            if let Ok(batches) = connector.execute(&sq).await {
                // Collect values from first column
                let values: Vec<String> = batches.iter()
                    .flat_map(|b| {
                        let col = b.column(0);
                        (0..b.num_rows()).filter_map(move |i| {
                            if col.is_null(i) { return None; }
                            arrow::util::display::array_value_to_string(col, i).ok()
                        })
                    })
                    .collect();
                if !values.is_empty() {
                    let in_list = values.iter()
                        .map(|v| format!("'{}'", v.replace('\'', "''")))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let replacement = format!(" IN ({})", in_list);
                    resolved_query = resolved_query.replace(&isq.full_match, &replacement);
                }
            }
        }
    }
    // Re-parse sources from resolved query (IN subquery sources removed)
    let (query, refs) = if !in_subqueries.is_empty() {
        let new_refs = match format.as_str() {
            "ppl" => parse_ppl_sources(&resolved_query),
            _ => parse_sql_sources(&resolved_query),
        }.unwrap_or(refs.clone());
        (resolved_query, new_refs)
    } else {
        (query, refs)
    };

    let exec_future = async {
        let range = match (&req.start, &req.end, &req.step) {
            (Some(s), Some(e), Some(st)) => Some((s.as_str(), e.as_str(), st.as_str())),
            _ => None,
        };
        if refs.len() == 1 {
            execute_single(&state, &query, &format, &refs[0], range).await
        } else if format == "ppl" || is_union_query(&query) {
            execute_union(&state, &query, &format, &refs).await
        } else {
            execute_join(&state, &query, &refs).await
        }
    };

    let result = tokio::select! {
        r = tokio::time::timeout(timeout, exec_future) => match r {
            Ok(r) => r,
            Err(_) => Err(format!("query timed out after {}ms", timeout.as_millis())),
        },
        _ = cancel_token.cancelled() => Err("query cancelled".into()),
    };

    state.running_queries.remove(&query_id);

    match result {
        Ok(fed) => {
            // Use cached plan or parse fresh
            let (order_by, limit, is_distinct, offset) = if let Some(ref plan) = cached {
                (plan.order_by.clone(), plan.limit, plan.is_distinct, plan.offset)
            } else {
                let ob = parse_order_by(&query);
                let lim = parse_limit(&query);
                let dist = strip_string_literals(&query).to_lowercase().contains("select distinct")
                    || is_union_distinct(&query);
                let off = parse_offset(&query).unwrap_or(0);
                // Cache the plan for next time
                state.plan_cache.insert(cache_key.clone(), CachedPlan::new(
                    refs.clone(), is_union_query(&query), refs.len() > 1 && !is_union_query(&query),
                    dist, lim, off, ob.clone(),
                ));
                (ob, lim, dist, off)
            };

            // Re-aggregate for cross-datasource GROUP BY
            let group_by_cols = parse_group_by(&query);
            let batches = if !group_by_cols.is_empty() && fed.datasources.len() > 1 {
                reaggregate_batches(fed.batches, &group_by_cols)
            } else {
                fed.batches
            };

            // Apply HAVING filter after reaggregation
            let batches = if let Some(having) = parse_having(&query) {
                apply_having_filter(&batches, &having)
            } else {
                batches
            };

            // Apply global ORDER BY if present
            let batches = if !order_by.is_empty() {
                if let Ok(schema) = batches.first().map(|b| b.schema()).ok_or(()) {
                    let indices: Vec<usize> = order_by.iter()
                        .filter_map(|(col, _)| schema.index_of(col).ok())
                        .collect();
                    let descs: Vec<bool> = order_by.iter()
                        .filter(|(col, _)| schema.index_of(col).is_ok())
                        .map(|(_, d)| *d)
                        .collect();
                    if !indices.is_empty() {
                        fuse_engine::sort_batches(batches, &indices, &descs, None)
                            .unwrap_or_default()
                    } else {
                        batches
                    }
                } else {
                    batches
                }
            } else {
                batches
            };

            // Apply DISTINCT — dedup on all non-_datasource columns
            let batches = if is_distinct && !batches.is_empty() {
                let dedup_cols: Vec<String> = batches[0].schema().fields().iter()
                    .map(|f| f.name().clone())
                    .filter(|n| n != "_datasource")
                    .collect();
                let col_refs: Vec<&str> = dedup_cols.iter().map(|s| s.as_str()).collect();
                fuse_engine::dedup_batches(batches, &col_refs).unwrap_or_default()
            } else {
                batches
            };

            // Apply cursor pagination: cursor offset overrides/adds to SQL OFFSET
            let cursor_offset = req.cursor.as_deref().and_then(decode_cursor).unwrap_or(0);
            let effective_offset = offset + cursor_offset;
            let page_size = req.page_size.or(limit);

            // Apply global OFFSET + LIMIT
            let total_available = batches.iter().map(|b| b.num_rows()).sum::<usize>();
            let batches = if effective_offset > 0 || page_size.is_some() {
                // Flatten, skip offset, take limit
                let all_rows: Vec<_> = batches.iter()
                    .flat_map(|b| (0..b.num_rows()).map(move |i| (b, i)))
                    .skip(effective_offset)
                    .collect();
                let take_n = page_size.unwrap_or(all_rows.len());
                let rows_to_take: Vec<_> = all_rows.into_iter().take(take_n).collect();
                if rows_to_take.is_empty() || batches.is_empty() {
                    vec![]
                } else {
                    // Rebuild batches from selected rows
                    let schema = batches[0].schema();
                    let mut result_cols: Vec<Vec<arrow::array::ArrayRef>> = (0..schema.fields().len()).map(|_| Vec::new()).collect();
            #[allow(clippy::needless_range_loop)]
                    for (batch, row_idx) in &rows_to_take {
                        for col_idx in 0..schema.fields().len() {
                            result_cols[col_idx].push(batch.column(col_idx).slice(*row_idx, 1));
                        }
                    }
                    let arrays: Result<Vec<arrow::array::ArrayRef>, _> = result_cols.into_iter()
                        .map(|slices| {
                            let refs: Vec<&dyn arrow::array::Array> = slices.iter().map(|a| a.as_ref()).collect();
                            arrow::compute::concat(&refs)
                        })
                        .collect();
                    match arrays.and_then(|a| arrow::record_batch::RecordBatch::try_new(schema, a)) {
                        Ok(batch) => vec![batch],
                        Err(_) => vec![],
                    }
                }
            } else {
                batches
            };
            // Apply column-level RBAC: deny or mask sensitive fields per role
            let batches = if let Some(ref rbac) = state.column_rbac {
                let user_ctx = fuse_core::security::UserContext {
                    username: tenant_id.clone().unwrap_or_default(),
                    roles: auth_identity.as_ref()
                        .map(|ext| vec![format!("{:?}", ext.0.role).to_lowercase()])
                        .unwrap_or_default(),
                };
                let ds_id = fed.datasources.first().map(|s| s.as_str()).unwrap_or("");
                let table = refs.first().map(|(_, t)| t.as_str()).unwrap_or("");
                rbac.filter_batches(batches, ds_id, table, &user_ctx).unwrap_or_default()
            } else {
                batches
            };
            let (columns, rows) = batches_to_json(&batches);
            let row_count = rows.len();
            let total_rows = row_count as u64;
            let result_bytes: u64 = batches.iter().map(|b| b.get_array_memory_size() as u64).sum();
            let elapsed_ms = t0.elapsed().as_millis() as u64;

            // Compute dollar cost estimate from connector types + data bytes
            let cost_estimate = {
                let ds_inputs: Vec<(&str, &str, u64, u64)> = fed.datasources.iter()
                    .map(|ds| {
                        let ct = fed.connector_types.get(ds).map(|s| s.as_str()).unwrap_or("unknown");
                        let (rows, bytes) = fed.stats.as_ref()
                            .and_then(|s| s.get(ds))
                            .map(|s| (s.rows, result_bytes))
                            .unwrap_or((total_rows, result_bytes));
                        (ds.as_str(), ct, rows, bytes)
                    })
                    .collect();
                let est = crate::cost_estimator::estimate_query_cost(&ds_inputs);
                if est.total_cost_usd > 0.0 { Some(est) } else { None }
            };

            // Global result size limit
            if state.max_result_bytes > 0 && result_bytes > state.max_result_bytes {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!("result size {} bytes exceeds max_result_bytes limit of {} bytes", result_bytes, state.max_result_bytes),
                        "query_id": query_id,
                    })),
                ).into_response();
            }

            // Query governor: enforce tenant resource limits
            if let Some(ref tid) = tenant_id {
                if let Some(config) = state.tenant_registry.get(tid) {
                    if let Err(e) = QueryGovernor::check_limits(config, total_rows, result_bytes, elapsed_ms) {
                        return error_json(StatusCode::TOO_MANY_REQUESTS, e).into_response();
                    }
                }
            }
            state.history.push(crate::history::HistoryEntry {
                query: req.query.clone(),
                format: req.format.clone(),
                timestamp: crate::history::now_secs(),
                latency_ms: elapsed_ms,
                row_count: total_rows,
                error: None,
            });
            // Record per-datasource latency for adaptive timeout
            for ds in &fed.datasources {
                state.adaptive_timeout.record(ds, elapsed_ms);
            }
            state.audit_log.record(crate::audit::AuditEntry {
                timestamp: crate::history::now_secs(),
                identity: tenant_id.clone().unwrap_or_else(|| "anonymous".into()),
                action: crate::audit::AuditAction::Query,
                query: Some(req.query.clone()),
                datasources: fed.datasources.clone(),
                duration_ms: elapsed_ms,
                row_count: total_rows,
                status: crate::audit::AuditStatus::Success,
                error: None,
                client_ip: extract_client_ip(&headers),
            }).await;
            crate::metrics::record_query(&req.format, true, elapsed_ms);
            tracing::info!(
                query_id = %query_id,
                format = %format,
                total_rows = total_rows,
                latency_ms = t0.elapsed().as_millis() as u64,
                datasources = ?fed.datasources,
                "Query completed"
            );
            if req.result_format == "ndjson" {
                let ndjson = batches_to_ndjson(&batches);
                (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
                    ndjson,
                ).into_response()
            } else if req.result_format == "csv" {
                let csv = batches_to_csv(&batches);
                (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "text/csv")],
                    csv,
                ).into_response()
            } else {
                let ds_list: Vec<String> = fed.datasources.clone();
                let cost_estimate_ref = cost_estimate.clone();
                let resp = QueryResponse {
                    columns,
                    rows,
                    metadata: QueryMetadata {
                        total_rows,
                        format: req.format,
                        trace_id: query_id.clone(),
                        datasources_queried: if fed.datasources.len() > 1 {
                            Some(fed.datasources)
                        } else {
                            None
                        },
                        datasource_stats: fed.stats,
                        cost_estimate,
                    },
                    execution_profile: if req.analyze {
                        Some(ExecutionProfile {
                            total_ms: t0.elapsed().as_millis() as u64,
                            nodes: fed.profile_nodes,
                            cache_hit: Some(false),
                            optimizer_rules_applied: vec![],
                        })
                    } else {
                        None
                    },
                    partial_errors: fed.partial_errors,
                    next_cursor: if page_size.is_some() && (effective_offset + row_count) < total_available {
                        Some(encode_cursor(effective_offset + row_count))
                    } else {
                        None
                    },
                };
                // Cache result — use adaptive cache to decide TTL promotion
                if let Ok(json_val) = serde_json::to_value(&resp) {
                    state.adaptive_cache.record(&result_cache_key, &ds_list);
                    state.result_cache.insert(result_cache_key, json_val);
                }
                // Emit cost metric per datasource
                if let Some(ref ce) = cost_estimate_ref {
                    for entry in &ce.per_datasource {
                        if entry.estimated_cost_usd > 0.0 {
                            crate::metrics::record_query_cost(
                                &entry.datasource,
                                &entry.connector_type,
                                entry.estimated_cost_usd,
                            );
                        }
                    }
                }
                Json(resp).into_response()
            }
        }
        Err(e) => {
            state.history.push(crate::history::HistoryEntry {
                query: req.query.clone(),
                format: req.format.clone(),
                timestamp: crate::history::now_secs(),
                latency_ms: t0.elapsed().as_millis() as u64,
                row_count: 0,
                error: Some(e.clone()),
            });
            crate::metrics::record_query(&req.format, false, t0.elapsed().as_millis() as u64);
            state.audit_log.record(crate::audit::AuditEntry {
                timestamp: crate::history::now_secs(),
                identity: tenant_id.clone().unwrap_or_else(|| "anonymous".into()),
                action: crate::audit::AuditAction::Query,
                query: Some(req.query.clone()),
                datasources: vec![],
                duration_ms: t0.elapsed().as_millis() as u64,
                row_count: 0,
                status: crate::audit::AuditStatus::Error,
                error: Some(e.clone()),
                client_ip: extract_client_ip(&headers),
            }).await;
            tracing::warn!(
                query_id = %query_id,
                format = %format,
                latency_ms = t0.elapsed().as_millis() as u64,
                error = %e,
                "Query failed"
            );
            let status = if e.contains("timed out") || e.contains("cancelled") {
                StatusCode::GATEWAY_TIMEOUT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            error_json(status, e).into_response()
        }
    }
}
async fn execute_single(
    state: &AppState,
    query: &str,
    format: &str,
    (ds_id, table): &(String, String),
    range: Option<(&str, &str, &str)>, // (start, end, step) for Prometheus range queries
) -> Result<FederatedResult, String> {
    let connector = state.registry.get(ds_id)
        .ok_or_else(|| format!("datasource '{}' not found", ds_id))?;
    let mut sub_query = build_sub_query(query, format, table)?;
    // Inject range params as passthrough for Prometheus connector
    if let Some((start, end, step)) = range {
        let pt = sub_query.passthrough.get_or_insert(serde_json::json!({}));
        if let Some(obj) = pt.as_object_mut() {
            obj.insert("start".into(), serde_json::Value::String(start.to_string()));
            obj.insert("end".into(), serde_json::Value::String(end.to_string()));
            obj.insert("step".into(), serde_json::Value::String(step.to_string()));
        }
    }
    let start = std::time::Instant::now();
    let batches = connector.execute(&sub_query).await.map_err(|e| e.to_string())?;
    let elapsed = start.elapsed().as_millis() as u64;
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let data_bytes: u64 = batches.iter().map(|b| b.get_array_memory_size() as u64).sum();
    Ok(FederatedResult {
        batches,
        stats: None,
        datasources: vec![ds_id.clone()],
        connector_types: [(ds_id.clone(), connector.connector_type().to_string())].into_iter().collect(),
        profile_nodes: vec![ProfileNode::scan(ds_id, row_count, elapsed, data_bytes, describe_pushdown(&sub_query))],
        partial_errors: vec![],
    })
}

/// Execute a UNION ALL query — fan out to each connector in parallel, merge.
async fn execute_union(
    state: &AppState,
    query: &str,
    format: &str,
    refs: &[(String, String)],
) -> Result<FederatedResult, String> {
    let base_sq = build_sub_query(query, format, &refs[0].1)
        .unwrap_or_else(|_| fuse_core::connector::SubQuery {
            table: String::new(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None, offset: None, passthrough: None,
        });

    let mut per_source: Vec<fuse_core::connector::SubQuery> = refs
        .iter()
        .map(|(_, table)| fuse_core::connector::SubQuery {
            table: table.clone(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: Some(10_000), // Default limit to avoid scroll for UNION ALL fan-out
            having: None, offset: None, passthrough: None,
        })
        .collect();
    fuse_engine::rewrite::push_down_to_sources(&base_sq, &mut per_source);

    let mut handles = Vec::new();
    // #1820: Use adaptive parallelism to limit concurrent fan-out per datasource
    let max_concurrency: usize = refs.iter()
        .filter_map(|(ds_id, _)| state.registry.get(ds_id).map(|_| state.adaptive_parallelism.concurrency_for(ds_id)))
        .max()
        .unwrap_or(4);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrency));
    let ap = state.adaptive_parallelism.clone();

    for (i, (ds_id, _)) in refs.iter().enumerate() {
        let connector = match state.registry.get(ds_id) {
            Some(c) => c,
            None => continue, // skip missing datasource
        };
        let sub_query = per_source[i].clone();
        let conn = connector.clone();
        let ds = ds_id.clone();
        let sem = semaphore.clone();
        let ap = ap.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            let start = std::time::Instant::now();
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(25), // per-connector timeout (< global 30s)
                conn.execute(&sub_query),
            ).await;
            let latency_ms = start.elapsed().as_millis() as u64;
            let result = match result {
                Ok(r) => r,
                Err(_) => Err(fuse_core::error::ConnectorError::QueryFailed(
                    format!("connector '{}' timed out", ds),
                )),
            };
            // #1820: Record success/failure for adaptive concurrency tuning
            match &result {
                Ok(_) => ap.record_success(&ds, latency_ms),
                Err(_) => ap.record_failure(&ds),
            }
            (ds, result, latency_ms)
        }));
    }

    let mut batch_sets = Vec::new();
    let mut ds_stats = std::collections::HashMap::new();
    let mut datasources = Vec::new();
    let mut scan_nodes = Vec::new();
    let mut partial_errors = Vec::new();
    let conn_types: std::collections::HashMap<String, String> = refs.iter()
        .filter_map(|(ds_id, _)| {
            state.registry.get(ds_id).map(|c| (ds_id.clone(), c.connector_type().to_string()))
        })
        .collect();

    for handle in handles {
        let (ds_id, result, latency_ms) = handle.await.map_err(|e| format!("task join error: {e}"))?;
        datasources.push(ds_id.clone());

        match result {
            Ok(batches) => {
                let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                let data_bytes: u64 = batches.iter().map(|b| b.get_array_memory_size() as u64).sum();
                let tagged = add_datasource_column(&batches, &ds_id);

                scan_nodes.push(ProfileNode::scan(&ds_id, row_count, latency_ms, data_bytes, vec![]));

                ds_stats.insert(ds_id, DatasourceStat { rows: row_count, latency_ms });
                batch_sets.push(tagged);
            }
            Err(e) => {
                partial_errors.push(PartialError { datasource: ds_id.clone(), error: e.to_string() });
                ds_stats.insert(ds_id, DatasourceStat { rows: 0, latency_ms });
            }
        }
    }

    // If ALL sources failed, return error
    if batch_sets.is_empty() && !partial_errors.is_empty() {
        return Err(partial_errors.iter().map(|e| format!("{}: {}", e.datasource, e.error)).collect::<Vec<_>>().join("; "));
    }

    let merged = fuse_engine::union_batches(batch_sets).map_err(|e| e.to_string())?;
    let total_rows: u64 = merged.iter().map(|b| b.num_rows() as u64).sum();

    Ok(FederatedResult {
        batches: merged,
        stats: Some(ds_stats),
        datasources,
        connector_types: conn_types,
        profile_nodes: vec![ProfileNode::parent("UnionAll", total_rows, 0, scan_nodes)],
        partial_errors,
    })
}

/// Execute a cross-datasource JOIN using the join executor.
async fn execute_join(
    state: &AppState,
    query: &str,
    refs: &[(String, String)],
) -> Result<FederatedResult, String> {
    if refs.len() != 2 {
        return Err(format!("JOIN requires exactly 2 datasources, got {}", refs.len()));
    }

    let (ds_a, table_a) = &refs[0];
    let (ds_b, table_b) = &refs[1];

    let conn_a = state.registry.get(ds_a)
        .ok_or_else(|| format!("datasource '{}' not found", ds_a))?;
    let conn_b = state.registry.get(ds_b)
        .ok_or_else(|| format!("datasource '{}' not found", ds_b))?;

    let sq_a = fuse_core::connector::SubQuery {
        table: table_a.clone(),
        projections: vec![],
        filter: None,
        aggregations: vec![],
        group_by: vec![],
        sort: vec![],
        limit: Some(10_000), // Default limit to avoid scroll for JOIN fan-out
        having: None, offset: None, passthrough: None,
    };
    let mut sq_b = sq_a.clone();
    sq_b.table = table_b.clone();

    let start_a = std::time::Instant::now();
    let start_b = start_a;
    let per_conn_timeout = std::time::Duration::from_secs(25);
    let (res_a, res_b) = tokio::join!(
        tokio::time::timeout(per_conn_timeout, conn_a.execute(&sq_a)),
        tokio::time::timeout(per_conn_timeout, conn_b.execute(&sq_b)),
    );
    let latency_a = start_a.elapsed().as_millis() as u64;
    let latency_b = start_b.elapsed().as_millis() as u64;

    let res_a = res_a.map_err(|_| format!("connector '{}' timed out", ds_a))?.map_err(|e| e.to_string());
    let res_b = res_b.map_err(|_| format!("connector '{}' timed out", ds_b))?.map_err(|e| e.to_string());

    // #1820: Record success/failure for adaptive concurrency tuning
    match &res_a {
        Ok(_) => state.adaptive_parallelism.record_success(ds_a, latency_a),
        Err(_) => state.adaptive_parallelism.record_failure(ds_a),
    }
    match &res_b {
        Ok(_) => state.adaptive_parallelism.record_success(ds_b, latency_b),
        Err(_) => state.adaptive_parallelism.record_failure(ds_b),
    }

    let batches_a = res_a?;
    let batches_b = res_b?;

    let rows_a: u64 = batches_a.iter().map(|b| b.num_rows() as u64).sum();
    let rows_b: u64 = batches_b.iter().map(|b| b.num_rows() as u64).sum();

    let mut ds_stats = std::collections::HashMap::new();
    ds_stats.insert(ds_a.clone(), DatasourceStat { rows: rows_a, latency_ms: latency_a });
    ds_stats.insert(ds_b.clone(), DatasourceStat { rows: rows_b, latency_ms: latency_b });

    if batches_a.is_empty() || batches_b.is_empty() {
        return Ok(FederatedResult {
            batches: vec![],
            stats: Some(ds_stats),
            datasources: vec![ds_a.clone(), ds_b.clone()],
            connector_types: [
                (ds_a.clone(), conn_a.connector_type().to_string()),
                (ds_b.clone(), conn_b.connector_type().to_string()),
            ].into_iter().collect(),
            profile_nodes: vec![],
            partial_errors: vec![],
        });
    }

    let schema_a = batches_a[0].schema();
    let schema_b = batches_b[0].schema();
    let join_key = find_join_key(&schema_a, &schema_b)
        .ok_or_else(|| "no common column found for JOIN key".to_string())?;

    // Build-side selection: smaller table as build (right) side for memory efficiency
    let (left_batches, right_batches, left_ds, right_ds, left_rows, right_rows, left_lat, right_lat) =
        if rows_a >= rows_b {
            (batches_a, batches_b, ds_a, ds_b, rows_a, rows_b, latency_a, latency_b)
        } else {
            (batches_b, batches_a, ds_b, ds_a, rows_b, rows_a, latency_b, latency_a)
        };

    let tagged_left = add_datasource_column(&left_batches, left_ds);
    let tagged_right = add_datasource_column(&right_batches, right_ds);

    let join_start = std::time::Instant::now();
    let joined = fuse_engine::hash_join(
        &tagged_left,
        &join_key,
        &tagged_right,
        &join_key,
        fuse_engine::JoinType::Inner,
    )
    .map_err(|e| e.to_string())?;

    // Apply time-window filter if ON clause has BETWEEN condition
    let joined = if let Some(tw) = parse_time_window(query) {
        filter_time_window(&joined, &tw)
    } else {
        joined
    };

    let join_ms = join_start.elapsed().as_millis() as u64;

    let join_rows: u64 = joined.iter().map(|b| b.num_rows() as u64).sum();
    let bytes_left: u64 = left_batches.iter().map(|b| b.get_array_memory_size() as u64).sum();
    let bytes_right: u64 = right_batches.iter().map(|b| b.get_array_memory_size() as u64).sum();

    let scan_left = ProfileNode::scan(left_ds, left_rows, left_lat, bytes_left, vec!["probe side".into()]);
    let scan_right = ProfileNode::scan(right_ds, right_rows, right_lat, bytes_right, vec!["build side (smaller)".into()]);

    Ok(FederatedResult {
        batches: joined,
        stats: Some(ds_stats),
        datasources: vec![ds_a.clone(), ds_b.clone()],
        connector_types: [
            (ds_a.clone(), conn_a.connector_type().to_string()),
            (ds_b.clone(), conn_b.connector_type().to_string()),
        ].into_iter().collect(),
        profile_nodes: vec![ProfileNode::parent("HashJoin", join_rows, join_ms, vec![scan_left, scan_right])],
        partial_errors: vec![],
    })
}

/// Find the first column name that exists in both schemas.
/// Time window condition parsed from JOIN ON clause.
pub struct TimeWindow {
    pub column_a: String,
    pub column_b: String,
    pub interval_secs: i64,
}

/// Parse time-window BETWEEN from JOIN ON clause.
pub fn parse_time_window(query: &str) -> Option<TimeWindow> {
    let stripped = strip_string_literals(query);
    let lower = stripped.to_lowercase();

    // Find BETWEEN in ON clause context
    let on_pos = lower.find(" on ")?;
    let on_clause = &lower[on_pos + 4..];

    // Look for: col BETWEEN col - INTERVAL 'Ns' AND col + INTERVAL 'Ns'
    let between_pos = on_clause.find(" between ")?;
    let before_between = on_clause[..between_pos].trim();
    // Column A is the last token before BETWEEN
    let col_a = before_between.rsplit(|c: char| c.is_whitespace() || c == '(')
        .next()?.trim().to_string();
    if col_a.is_empty() { return None; }

    let after_between = &on_clause[between_pos + 9..];

    // Extract interval value — look for a number followed by time unit
    let interval_secs = extract_interval_secs(after_between)?;

    // Column B is referenced in the BETWEEN bounds
    // Find the first column reference after BETWEEN that isn't col_a
    let col_b = after_between.split(|c: char| !c.is_alphanumeric() && c != '.' && c != '_')
        .find(|t| !t.is_empty() && t.contains('.') && *t != col_a)?
        .to_string();

    Some(TimeWindow { column_a: col_a, column_b: col_b, interval_secs })
}

/// Extract interval in seconds from a string containing INTERVAL or time notation.
fn extract_interval_secs(s: &str) -> Option<i64> {
    // Match patterns like: INTERVAL '5 minutes', INTERVAL '300 seconds', '5m', '1h'
    let re_num = s.chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>();
    let num: i64 = re_num.parse().ok()?;

    let after_num = &s[s.find(&re_num)? + re_num.len()..].trim_start();
    let unit = after_num.chars().take_while(|c| c.is_alphabetic()).collect::<String>().to_lowercase();

    match unit.as_str() {
        "s" | "sec" | "second" | "seconds" => Some(num),
        "m" | "min" | "minute" | "minutes" => Some(num * 60),
        "h" | "hour" | "hours" => Some(num * 3600),
        "d" | "day" | "days" => Some(num * 86400),
        _ => Some(num), // default to seconds
    }
}

/// Filter joined batches by time-window condition.
fn filter_time_window(
    batches: &[arrow::record_batch::RecordBatch],
    tw: &TimeWindow,
) -> Vec<arrow::record_batch::RecordBatch> {
    // Strip table alias prefix to find column in schema
    let col_a_name = tw.column_a.rsplit('.').next().unwrap_or(&tw.column_a);
    let col_b_name = tw.column_b.rsplit('.').next().unwrap_or(&tw.column_b);

    batches.iter().filter_map(|batch| {
        let schema = batch.schema();
        let idx_a = schema.index_of(col_a_name).ok()?;
        let idx_b = schema.index_of(col_b_name).ok()?;

        let col_a = batch.column(idx_a);
        let col_b = batch.column(idx_b);

        // Build boolean mask: |a - b| <= interval_secs
        let mut keep = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            if col_a.is_null(i) || col_b.is_null(i) {
                keep.push(false);
                continue;
            }
            let val_a = arrow::util::display::array_value_to_string(col_a, i).unwrap_or_default();
            let val_b = arrow::util::display::array_value_to_string(col_b, i).unwrap_or_default();

            // Try numeric comparison (epoch seconds/millis)
            let within = if let (Ok(a), Ok(b)) = (val_a.parse::<i64>(), val_b.parse::<i64>()) {
                (a - b).abs() <= tw.interval_secs
            } else if let (Ok(a), Ok(b)) = (val_a.parse::<f64>(), val_b.parse::<f64>()) {
                (a - b).abs() <= tw.interval_secs as f64
            } else {
                // String comparison (ISO timestamps) — lexicographic diff not meaningful,
                // but include row if we can't parse (don't silently drop)
                true
            };
            keep.push(within);
        }

        let mask = arrow::array::BooleanArray::from(keep);
        arrow::compute::filter_record_batch(batch, &mask).ok()
    }).collect()
}

fn find_join_key(
    a: &arrow::datatypes::SchemaRef,
    b: &arrow::datatypes::SchemaRef,
) -> Option<String> {
    for field in a.fields() {
        if b.field_with_name(field.name()).is_ok() {
            return Some(field.name().clone());
        }
    }
    None
}

/// GET /api/fuse/datasources
pub async fn list_datasources(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let connectors = state.registry.list();
    let infos: Vec<DatasourceInfo> = connectors
        .iter()
        .map(|c| DatasourceInfo {
            id: c.id().to_string(),
            connector_type: c.connector_type().to_string(),
            capabilities: c.capabilities(),
        })
        .collect();
    Json(infos)
}

/// GET /api/fuse/datasources/:id/schemas
pub async fn get_schemas(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let connector = match state.registry.get(&id) {
        Some(c) => c,
        None => {
            return error_json(StatusCode::NOT_FOUND, format!("datasource '{}' not found", id))
                .into_response()
        }
    };

    match connector.discover_schemas().await {
        Ok(schemas) => Json(schemas).into_response(),
        Err(e) => error_json(StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

/// GET /api/fuse/datasources/:id/schemas/:table/fields
pub async fn get_fields(
    State(state): State<Arc<AppState>>,
    Path((id, table)): Path<(String, String)>,
) -> impl IntoResponse {
    let connector = match state.registry.get(&id) {
        Some(c) => c,
        None => {
            return error_json(StatusCode::NOT_FOUND, format!("datasource '{}' not found", id))
                .into_response()
        }
    };

    match connector.get_schema(&table).await {
        Ok(schema) => {
            let fields: Vec<FieldInfo> = schema
                .fields()
                .iter()
                .map(|f| FieldInfo {
                    name: f.name().clone(),
                    data_type: format!("{:?}", f.data_type()),
                    nullable: f.is_nullable(),
                })
                .collect();
            Json(fields).into_response()
        }
        Err(e) => error_json(StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

/// POST /api/fuse/query/explain
pub async fn explain_handler(
    State(state): State<Arc<AppState>>,
    auth_identity: Option<Extension<crate::auth::AuthIdentity>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let t0 = std::time::Instant::now();
    let identity = auth_identity.as_ref().map(|ext| ext.0.identity.clone()).unwrap_or_else(|| "anonymous".into());
    let format = req.format.to_lowercase();
    let parse_result = match format.as_str() {
        "ppl" => parse_ppl_sources(&req.query),
        _ => parse_sql_sources(&req.query),
    };

    match parse_result {
        Ok(refs) => {
            let workload = build_workload(&req.query);
            let limit = parse_limit(&req.query);

            // Collect capabilities for each datasource
            let caps: Vec<_> = refs
                .iter()
                .filter_map(|(ds, _)| state.registry.get(ds).map(|c| c.capabilities()))
                .collect();

            let plan_tree = if refs.len() == 1 {
                let c = caps.first().cloned().unwrap_or_else(fuse_core::connector::ConnectorCapabilities::full);
                fuse_engine::plan::plan_single(&refs[0].0, &refs[0].1, &c, &workload)
            } else if is_union_query(&req.query) {
                fuse_engine::plan::plan_union(&refs, &caps, &workload, limit)
            } else if refs.len() == 2 {
                let c0 = caps.first().cloned().unwrap_or_else(fuse_core::connector::ConnectorCapabilities::full);
                let c1 = caps.get(1).cloned().unwrap_or_else(fuse_core::connector::ConnectorCapabilities::full);
                fuse_engine::plan::plan_join(
                    (&refs[0].0, &refs[0].1),
                    (&refs[1].0, &refs[1].1),
                    &c0, &c1, "auto",
                )
            } else {
                fuse_engine::plan::PlanNode::leaf("Unknown", format!("{} datasources", refs.len()))
            };

            // EXPLAIN ANALYZE: execute query and collect per-node stats
            let execution_profile = if req.analyze {
                let t0 = std::time::Instant::now();
                let exec_result = if refs.len() == 1 {
                    execute_single(&state, &req.query, &format, &refs[0], None).await
                } else if format == "ppl" || is_union_query(&req.query) {
                    execute_union(&state, &req.query, &format, &refs).await
                } else {
                    execute_join(&state, &req.query, &refs).await
                };
                match exec_result {
                    Ok(fed) => Some(ExecutionProfile {
                        total_ms: t0.elapsed().as_millis() as u64,
                        nodes: fed.profile_nodes,
                        cache_hit: Some(false),
                        optimizer_rules_applied: vec![],
                    }),
                    Err(_) => None,
                }
            } else {
                None
            };

            let plan_text = plan_tree.to_text(0);
            let plan_tree_text = plan_tree.to_tree();
            let complexity = crate::complexity::score_query(&req.query);

            // Compute dollar cost estimate from connector types + estimated bytes
            let cost_estimate = {
                let owned: Vec<(String, String, u64, u64)> = refs.iter()
                    .map(|(ds, _)| {
                        let ct = state.registry.get(ds)
                            .map(|c| c.connector_type().to_string())
                            .unwrap_or_default();
                        let est_rows = plan_tree.estimated_rows.unwrap_or(10_000);
                        let est_bytes = est_rows * 1024; // ~1KB per row estimate
                        (ds.clone(), ct, est_rows, est_bytes)
                    })
                    .collect();
                let input_refs: Vec<(&str, &str, u64, u64)> = owned.iter()
                    .map(|(ds, ct, r, b)| (ds.as_str(), ct.as_str(), *r, *b))
                    .collect();
                let est = crate::cost_estimator::estimate_query_cost(&input_refs);
                if est.total_cost_usd > 0.0 { Some(est) } else { None }
            };

            state.audit_log.record(crate::audit::AuditEntry {
                timestamp: crate::history::now_secs(),
                identity: identity.clone(),
                action: crate::audit::AuditAction::Explain,
                query: Some(req.query.clone()),
                datasources: refs.iter().map(|(ds, _)| ds.clone()).collect(),
                duration_ms: t0.elapsed().as_millis() as u64,
                row_count: 0,
                status: crate::audit::AuditStatus::Success,
                error: None,
                client_ip: extract_client_ip(&headers),
            }).await;
            Json(ExplainResponse {
                plan: plan_text,
                plan_display: Some(plan_tree_text),
                plan_tree: Some(plan_tree),
                execution_profile,
                complexity: Some(complexity),
                cost_estimate,
            })
            .into_response()
        }
        Err(e) => {
            state.audit_log.record(crate::audit::AuditEntry {
                timestamp: crate::history::now_secs(),
                identity: identity.clone(),
                action: crate::audit::AuditAction::Explain,
                query: Some(req.query.clone()),
                datasources: vec![],
                duration_ms: t0.elapsed().as_millis() as u64,
                row_count: 0,
                status: crate::audit::AuditStatus::Error,
                error: Some(e.clone()),
                client_ip: extract_client_ip(&headers),
            }).await;
            error_json(StatusCode::BAD_REQUEST, e).into_response()
        }
    }
}

/// POST /api/fuse/query/validate
pub async fn validate_handler(
    State(state): State<Arc<AppState>>,
    auth_identity: Option<Extension<crate::auth::AuthIdentity>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let t0 = std::time::Instant::now();
    let identity = auth_identity.as_ref().map(|ext| ext.0.identity.clone()).unwrap_or_else(|| "anonymous".into());
    let format = req.format.to_lowercase();
    let parse_result = match format.as_str() {
        "ppl" => parse_ppl_sources(&req.query),
        _ => parse_sql_sources(&req.query),
    };

    match parse_result {
        Ok(refs) => {
            for (ds_id, table) in &refs {
                let connector = match state.registry.get(ds_id) {
                    Some(c) => c,
                    None => return Json(ValidateResponse {
                        valid: false,
                        error: Some(format!("datasource '{}' not found in registry", ds_id)),
                    }),
                };
                // Check table exists in datasource
                match connector.discover_schemas().await {
                    Ok(schemas) => {
                        if !schemas.iter().any(|s| s.name == *table) {
                            return Json(ValidateResponse {
                                valid: false,
                                error: Some(format!("table '{}' not found in datasource '{}'", table, ds_id)),
                            });
                        }
                    }
                    Err(e) => return Json(ValidateResponse {
                        valid: false,
                        error: Some(format!("failed to discover schemas for '{}': {}", ds_id, e)),
                    }),
                }
            }
            state.audit_log.record(crate::audit::AuditEntry {
                timestamp: crate::history::now_secs(),
                identity: identity.clone(),
                action: crate::audit::AuditAction::Validate,
                query: Some(req.query.clone()),
                datasources: refs.iter().map(|(ds, _)| ds.clone()).collect(),
                duration_ms: t0.elapsed().as_millis() as u64,
                row_count: 0,
                status: crate::audit::AuditStatus::Success,
                error: None,
                client_ip: extract_client_ip(&headers),
            }).await;
            Json(ValidateResponse {
                valid: true,
                error: None,
            })
        }
        Err(e) => {
            state.audit_log.record(crate::audit::AuditEntry {
                timestamp: crate::history::now_secs(),
                identity: identity.clone(),
                action: crate::audit::AuditAction::Validate,
                query: Some(req.query.clone()),
                datasources: vec![],
                duration_ms: t0.elapsed().as_millis() as u64,
                row_count: 0,
                status: crate::audit::AuditStatus::Error,
                error: Some(e.clone()),
                client_ip: extract_client_ip(&headers),
            }).await;
            Json(ValidateResponse {
                valid: false,
                error: Some(e),
            })
        }
    }
}

/// GET /api/fuse/health
pub async fn health_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let resp = health::check_health(&state.registry).await;
    Json(resp)
}

/// GET /api/fuse/info — server version, uptime, config summary.
pub async fn info_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    static START_TIME: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let start = START_TIME.get_or_init(std::time::Instant::now);
    let uptime_secs = start.elapsed().as_secs();

    Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": uptime_secs,
        "connectors": state.registry.list().len(),
        "features": {
            "federation": true,
            "write_path": true,
            "async_queries": true,
            "nl_to_sql": true,
            "prepared_statements": true,
            "wasm_plugins": true,
        }
    }))
}

// ── Helpers ──

/// Parse all datasource.table references from a PPL query.
/// PPL: `source = ds1.table1, ds2.table2 | ...`
pub fn parse_ppl_sources(query: &str) -> Result<Vec<(String, String)>, String> {
    let rest = query
        .trim()
        .strip_prefix("source")
        .or_else(|| query.trim().strip_prefix("search"))
        .and_then(|s| s.trim_start().strip_prefix('='))
        .map(|s| s.trim_start())
        .ok_or_else(|| "PPL query must start with 'source = '".to_string())?;

    let source_part = rest.split('|').next().unwrap_or(rest).trim();
    source_part
        .split(',')
        .map(|s| parse_qualified_name(s.trim()))
        .collect()
}

/// Parse all datasource.table references from a SQL query.
/// Finds all `datasource.table` patterns after FROM, JOIN, and in UNION ALL subqueries.
pub fn parse_sql_sources(query: &str) -> Result<Vec<(String, String)>, String> {
    let mut refs = Vec::new();
    let lower = query.to_lowercase();

    // Find references after FROM and JOIN keywords (word-boundary aware)
    for keyword in &["from ", "join "] {
        let mut search_from = 0;
        while let Some(pos) = lower[search_from..].find(keyword) {
            let abs_pos = search_from + pos;
            // Ensure word boundary: keyword must be preceded by whitespace, '(' or start of string
            let is_word_start = abs_pos == 0
                || lower.as_bytes()[abs_pos - 1].is_ascii_whitespace()
                || lower.as_bytes()[abs_pos - 1] == b'('
                || lower.as_bytes()[abs_pos - 1] == b',';
            let token_start = abs_pos + keyword.len();
            if is_word_start {
                let after = query[token_start..].trim_start();
                // Skip if inside a string literal (count unescaped quotes before this position)
                let quotes_before = query[..abs_pos].chars().filter(|&c| c == '\'').count();
                if quotes_before % 2 == 0 {
                    // Take the first token
                    let token = after
                        .split(|c: char| c.is_whitespace() || c == ',' || c == ')')
                        .next()
                        .unwrap_or("");
                    // Strip alias: "ds.table AS a" or "ds.table a" → "ds.table"
                    if let Ok(r) = parse_qualified_name(token) {
                        if !refs.contains(&r) {
                            refs.push(r);
                        }
                    }
                }
            }
            search_from = token_start;
        }
    }

    if refs.is_empty() {
        Err("SQL query must contain a FROM clause with a qualified datasource.table reference".into())
    } else {
        Ok(refs)
    }
}

/// Add a `_datasource` column to each RecordBatch.
fn add_datasource_column(
    batches: &[arrow::record_batch::RecordBatch],
    datasource: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};

    batches
        .iter()
        .filter_map(|batch| {
            let n = batch.num_rows();
            let ds_array = Arc::new(StringArray::from(vec![datasource; n]));
            let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
            fields.push(Arc::new(Field::new("_datasource", DataType::Utf8, false)));
            let schema = Arc::new(arrow::datatypes::Schema::new(fields));
            let mut columns: Vec<Arc<dyn arrow::array::Array>> =
                batch.columns().to_vec();
            columns.push(ds_array);
            arrow::record_batch::RecordBatch::try_new(schema, columns).ok()
        })
        .collect()
}

/// Detect IN (SELECT ...) subquery pattern and extract the inner query.
/// Returns: Vec<(outer_column, inner_datasource, inner_table, inner_column, inner_filter)>
static QUERY_COUNTER: AtomicU64 = AtomicU64::new(1);


#[derive(Debug)]
struct MemoryConnector {
    name: String,
    batches: Vec<arrow::record_batch::RecordBatch>,
}

#[async_trait]
impl fuse_core::connector::FederatedConnector for MemoryConnector {
    fn id(&self) -> &str { &self.name }
    fn connector_type(&self) -> &str { "memory" }
    fn capabilities(&self) -> fuse_core::connector::ConnectorCapabilities {
        fuse_core::connector::ConnectorCapabilities {
            supports_filtering: false, supports_projection: false,
            supports_aggregation: false, supports_sorting: false,
            supports_limit: false, supports_join: false,
            max_concurrent_queries: 1, supports_streaming: false,
            latency_class: fuse_core::connector::LatencyClass::Low,
        }
    }
    async fn health_check(&self) -> fuse_core::connector::ConnectorHealth {
        fuse_core::connector::ConnectorHealth { status: fuse_core::connector::HealthStatus::Healthy, message: None, latency_ms: Some(0) }
    }
    async fn discover_schemas(&self) -> Result<Vec<fuse_core::connector::SchemaInfo>, fuse_core::error::ConnectorError> { Ok(vec![]) }
    async fn get_schema(&self, _table: &str) -> Result<arrow::datatypes::Schema, fuse_core::error::ConnectorError> {
        self.batches.first()
            .map(|b| b.schema().as_ref().clone())
            .ok_or_else(|| fuse_core::error::ConnectorError::QueryFailed("empty CTE".into()))
    }
    async fn execute(&self, _query: &fuse_core::connector::SubQuery) -> Result<Vec<arrow::record_batch::RecordBatch>, fuse_core::error::ConnectorError> {
        Ok(self.batches.clone())
    }
    async fn execute_streaming(&self, _query: &fuse_core::connector::SubQuery, tx: tokio::sync::mpsc::Sender<Result<arrow::record_batch::RecordBatch, fuse_core::error::ConnectorError>>) -> Result<(), fuse_core::error::ConnectorError> {
        for b in &self.batches { let _ = tx.send(Ok(b.clone())).await; }
        Ok(())
    }
}

/// Execute a recursive CTE: base UNION ALL recursive.
/// Iterates until no new rows or max 100 iterations (safety limit).
async fn execute_recursive_cte(
    state: &AppState,
    cte_name: &str,
    inner_sql: &str,
    format: &str,
) -> Option<Vec<arrow::record_batch::RecordBatch>> {
    const MAX_ITERATIONS: usize = 100;

    // Split at UNION ALL (case-insensitive)
    let lower = inner_sql.to_lowercase();
    let union_pos = lower.find(" union all ")?;
    let base_sql = inner_sql[..union_pos].trim();
    let recursive_sql = inner_sql[union_pos + 11..].trim();

    // Execute base case
    let base_refs = parse_sql_sources(base_sql).ok()?;
    let base_result = if base_refs.len() == 1 {
        execute_single(state, base_sql, format, &base_refs[0], None).await.ok()?
    } else {
        return None;
    };

    let mut all_batches = base_result.batches.clone();
    let mut working_set = base_result.batches;

    for _iteration in 0..MAX_ITERATIONS {
        if working_set.is_empty() || working_set.iter().all(|b| b.num_rows() == 0) {
            break;
        }

        // Register current working set as the CTE name for self-reference
        let temp_conn: Arc<dyn fuse_core::connector::FederatedConnector> = Arc::new(MemoryConnector {
            name: cte_name.to_string(),
            batches: working_set,
        });
        let _ = state.registry.register(temp_conn);

        // Execute recursive step
        let rec_refs = parse_sql_sources(recursive_sql).ok()?;
        let rec_result = if rec_refs.len() == 1 {
            execute_single(state, recursive_sql, format, &rec_refs[0], None).await
        } else if rec_refs.len() == 2 {
            execute_join(state, recursive_sql, &rec_refs).await
        } else {
            break;
        };

        match rec_result {
            Ok(fed) if !fed.batches.is_empty() && fed.batches.iter().any(|b| b.num_rows() > 0) => {
                all_batches.extend(fed.batches.clone());
                working_set = fed.batches;
            }
            _ => break,
        }
    }

    Some(all_batches)
}

/// Parse WITH clauses, execute each CTE, register results as temp connectors.
/// Supports both regular and RECURSIVE CTEs.
/// Returns (rewritten_query, updated_refs, temp_connectors_to_keep_alive).
async fn resolve_ctes(
    state: &AppState,
    query: &str,
    format: &str,
    refs: &[(String, String)],
) -> (String, Vec<(String, String)>, Vec<(String, Arc<dyn fuse_core::connector::FederatedConnector>)>) {
    let lower = query.to_lowercase();
    let trimmed = lower.trim_start();
    if !trimmed.starts_with("with ") {
        return (query.to_string(), refs.to_vec(), vec![]);
    }

    let is_recursive = trimmed.starts_with("with recursive ");

    let mut cte_connectors = Vec::new();
    let mut cte_defs: Vec<(String, String)> = Vec::new();

    // Work on original query to preserve offsets
    let with_start = lower.find("with ").unwrap();
    let mut pos = with_start + 5;
    // Skip "RECURSIVE " keyword if present
    if is_recursive {
        pos += 10; // "recursive ".len()
    }

    loop {
        // Skip whitespace
        while pos < query.len() && query.as_bytes()[pos].is_ascii_whitespace() { pos += 1; }
        if pos >= query.len() { break; }

        // Parse CTE name
        let name_start = pos;
        while pos < query.len() && !query.as_bytes()[pos].is_ascii_whitespace() && query.as_bytes()[pos] != b'(' {
            pos += 1;
        }
        let cte_name = query[name_start..pos].trim().to_string();

        // Skip whitespace + "AS" + whitespace
        while pos < query.len() && query.as_bytes()[pos].is_ascii_whitespace() { pos += 1; }
        if pos + 2 > query.len() || query[pos..pos+2].to_lowercase() != "as" { break; }
        pos += 2;
        while pos < query.len() && query.as_bytes()[pos].is_ascii_whitespace() { pos += 1; }

        // Expect '('
        if pos >= query.len() || query.as_bytes()[pos] != b'(' { break; }
        pos += 1; // skip '('

        // Find matching ')'
        let inner_start = pos;
        match find_matching_paren(&query[inner_start..]) {
            Some(close) => {
                let inner_sql = query[inner_start..inner_start + close].trim().to_string();
                cte_defs.push((cte_name, inner_sql));
                pos = inner_start + close + 1; // skip ')'
            }
            None => break,
        }

        // After closing paren: comma → more CTEs, else → main SELECT
        while pos < query.len() && query.as_bytes()[pos].is_ascii_whitespace() { pos += 1; }
        if pos < query.len() && query.as_bytes()[pos] == b',' {
            pos += 1;
        } else {
            break;
        }
    }

    if cte_defs.is_empty() {
        return (query.to_string(), refs.to_vec(), vec![]);
    }

    let main_query = query[pos..].trim().to_string();

    // Execute each CTE and register as temp connector
    for (name, inner_sql) in &cte_defs {
        if is_recursive && inner_sql.to_lowercase().contains(" union all ") {
            // Recursive CTE: split into base + recursive parts at UNION ALL
            if let Some(batches) = execute_recursive_cte(state, name, inner_sql, format).await {
                let conn: Arc<dyn fuse_core::connector::FederatedConnector> = Arc::new(MemoryConnector { name: name.clone(), batches });
                let _ = state.registry.register(conn.clone());
                cte_connectors.push((name.clone(), conn));
            }
        } else {
            // Regular CTE
            let inner_refs = match parse_sql_sources(inner_sql) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let result = if inner_refs.len() == 1 {
                execute_single(state, inner_sql, format, &inner_refs[0], None).await
            } else if is_union_query(inner_sql) {
                execute_union(state, inner_sql, format, &inner_refs).await
            } else {
                execute_join(state, inner_sql, &inner_refs).await
            };
            if let Ok(fed) = result {
                let conn: Arc<dyn fuse_core::connector::FederatedConnector> = Arc::new(MemoryConnector { name: name.clone(), batches: fed.batches });
                let _ = state.registry.register(conn.clone());
                cte_connectors.push((name.clone(), conn));
            }
        }
    }

    // Re-parse refs from main query (CTE names are now registered datasources)
    let new_refs = parse_sql_sources(&main_query).unwrap_or_else(|_| refs.to_vec());

    (main_query, new_refs, cte_connectors)
}

fn extract_in_subqueries(query: &str) -> Vec<InSubquery> {
    let stripped = strip_string_literals(query);
    let lower = stripped.to_lowercase();

    // Early return if no IN (SELECT pattern
    if !lower.contains(" in (select ") {
        return vec![];
    }

    let mut results = Vec::new();

    // Find "IN (SELECT" patterns
    let mut pos = 0;
    while let Some(in_pos) = lower[pos..].find(" in (select ") {
        let abs_in = pos + in_pos;
        // Extract the column before IN
        let before = stripped[..abs_in].trim();
        let col = before.rsplit(|c: char| c.is_whitespace() || c == '(')
            .next().unwrap_or("").trim().to_string();

        // Find matching closing paren
        let select_start = abs_in + 5; // skip " IN ("
        let inner_sql = &stripped[select_start..];
        if let Some(close) = find_matching_paren(inner_sql) {
            let inner = inner_sql[..close].trim().to_string();
            // Parse inner query's source
            if let Ok(refs) = parse_sql_sources(&inner) {
                if let Some((ds, table)) = refs.into_iter().next() {
                    // Extract the selected column from inner query
                    let inner_lower = inner.to_lowercase();
                    let inner_col = if let Some(sel_end) = inner_lower.find(" from ") {
                        let sel = inner[7..sel_end].trim(); // skip "SELECT "
                        sel.split(',').next().unwrap_or("*").trim().to_string()
                    } else {
                        "*".to_string()
                    };
                    results.push(InSubquery {
                        outer_column: col,
                        datasource: ds,
                        table,
                        inner_column: inner_col,
                        inner_query: inner,
                        full_match: stripped[abs_in..select_start + close + 1].to_string(),
                    });
                }
            }
        }
        pos = abs_in + 4;
    }
    results
}

#[allow(dead_code)]
struct InSubquery {
    outer_column: String,
    datasource: String,
    table: String,
    inner_column: String,
    inner_query: String,
    full_match: String,
}

fn find_matching_paren(s: &str) -> Option<usize> {
    let mut depth = 1;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 { return Some(i); }
            }
            _ => {}
        }
    }
    None
}

/// Encode a cursor offset as a string token.
fn encode_cursor(offset: usize) -> String {
    format!("fuse_c_{}", offset)
}

/// Decode a cursor string back to an offset.
fn decode_cursor(cursor: &str) -> Option<usize> {
    cursor.strip_prefix("fuse_c_")?.parse().ok()
}

/// Encode a UNION ALL cursor with per-source offsets.
/// Format: fuse_u_<ds1>:<offset1>,<ds2>:<offset2>,...|<global_offset>
#[allow(dead_code)]
fn encode_union_cursor(per_source: &[(String, usize)], global_offset: usize) -> String {
    let parts: Vec<String> = per_source.iter().map(|(ds, off)| format!("{}:{}", ds, off)).collect();
    format!("fuse_u_{}|{}", parts.join(","), global_offset)
}

/// Decode a UNION ALL cursor. Returns (per-source offsets, global offset).
#[allow(dead_code)]
fn decode_union_cursor(cursor: &str) -> Option<(Vec<(String, usize)>, usize)> {
    let rest = cursor.strip_prefix("fuse_u_")?;
    let (sources_part, global_part) = rest.rsplit_once('|')?;
    let global: usize = global_part.parse().ok()?;
    let per_source: Vec<(String, usize)> = sources_part.split(',')
        .filter_map(|s| {
            let (ds, off) = s.rsplit_once(':')?;
            Some((ds.to_string(), off.parse().ok()?))
        })
        .collect();
    Some((per_source, global))
}

/// Describe pushdown operations from a SubQuery for profile display.
fn describe_pushdown(sq: &fuse_core::connector::SubQuery) -> Vec<String> {
    let mut desc = Vec::new();
    if !sq.projections.is_empty() {
        desc.push(format!("projection: {}", sq.projections.join(", ")));
    }
    if sq.filter.is_some() {
        desc.push("filter: pushed".into());
    }
    if !sq.aggregations.is_empty() {
        desc.push(format!("aggregation: {} agg(s)", sq.aggregations.len()));
    }
    if !sq.group_by.is_empty() {
        desc.push(format!("group_by: {}", sq.group_by.join(", ")));
    }
    #[allow(clippy::unnecessary_unwrap)]
    if sq.limit.is_some() {
        desc.push(format!("limit: {}", sq.limit.unwrap()));
    }
    desc
}

/// Check if a SQL query contains UNION ALL.
/// Strip single-quoted string literals from SQL to avoid false keyword matches.
/// Replaces 'content' with '' (empty string literal placeholder).
fn strip_string_literals(query: &str) -> String {
    let mut result = String::with_capacity(query.len());
    let mut in_quote = false;
    let mut chars = query.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            if in_quote {
                // Check for escaped quote ''
                if chars.peek() == Some(&'\'') {
                    chars.next();
                    continue;
                }
                in_quote = false;
                result.push('\'');
            } else {
                in_quote = true;
                result.push('\'');
            }
        } else if in_quote {
            // Skip content inside quotes
        } else {
            result.push(c);
        }
    }
    result
}

fn is_union_query(query: &str) -> bool {
    let lower = strip_string_literals(query).to_lowercase();
    lower.contains("union all") || lower.contains("union ")
}

/// Check if query uses plain UNION (deduplicated) vs UNION ALL.
fn is_union_distinct(query: &str) -> bool {
    let lower = strip_string_literals(query).to_lowercase();
    // Has "union" but NOT "union all"
    lower.contains("union ") && !lower.contains("union all")
}

/// Extract LIMIT value from end of query.
fn parse_limit(query: &str) -> Option<usize> {
    let stripped = strip_string_literals(query).to_lowercase();
    let pos = stripped.rfind("limit ")?;
    let after = stripped[pos + 6..].trim();
    let num_str = after
        .split(|c: char| !c.is_ascii_digit())
        .next()?;
    num_str.parse().ok()
}

/// Extract OFFSET value from query.
fn parse_offset(query: &str) -> Option<usize> {
    let stripped = strip_string_literals(query).to_lowercase();
    let pos = stripped.rfind("offset ")?;
    let after = stripped[pos + 7..].trim();
    let num_str = after
        .split(|c: char| !c.is_ascii_digit())
        .next()?;
    num_str.parse().ok()
}

/// Extract GROUP BY columns from query.
fn parse_group_by(query: &str) -> Vec<String> {
    let stripped = strip_string_literals(query);
    let lower = stripped.to_lowercase();
    let pos = match lower.rfind("group by ") {
        Some(p) => p,
        None => return vec![],
    };
    let after = stripped[pos + 9..].trim();
    // Stop at HAVING, ORDER BY, LIMIT, or end
    let clause = after
        .split_once("having").map(|(c, _)| c)
        .unwrap_or(after)
        .split_once("order").map(|(c, _)| c)
        .unwrap_or(after)
        .split_once("limit").map(|(c, _)| c)
        .unwrap_or(after)
        .trim();
    clause.split(',')
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .collect()
}

/// A parsed HAVING condition: column op value.
pub struct HavingCondition {
    pub column: String,
    pub op: String,   // ">", ">=", "<", "<=", "=", "!="
    pub value: f64,
}

/// Parse HAVING clause from query. E.g. HAVING COUNT(*) > 5 or HAVING cnt >= 10
pub fn parse_having(query: &str) -> Option<HavingCondition> {
    let stripped = strip_string_literals(query);
    let lower = stripped.to_lowercase();
    let pos = lower.rfind(" having ")?;
    let after = stripped[pos + 8..].trim();
    // Stop at ORDER BY, LIMIT, or end
    let clause = after
        .split_once(" order").map(|(c, _)| c)
        .unwrap_or(after)
        .split_once(" limit").map(|(c, _)| c)
        .unwrap_or(after)
        .trim();

    // Parse: column/expr op value
    // Find comparison operator
    let ops = [">=", "<=", "!=", ">", "<", "="];
    for op in &ops {
        if let Some(op_pos) = clause.find(op) {
            let col = clause[..op_pos].trim();
            let val_str = clause[op_pos + op.len()..].trim();
            // Extract column name — could be alias like "cnt" or expression like "COUNT(*)"
            let column = col.to_string();
            let value: f64 = val_str.split_whitespace().next()?.parse().ok()?;
            return Some(HavingCondition { column, op: op.to_string(), value });
        }
    }
    None
}

/// Apply HAVING filter to batches.
fn apply_having_filter(
    batches: &[arrow::record_batch::RecordBatch],
    having: &HavingCondition,
) -> Vec<arrow::record_batch::RecordBatch> {
    batches.iter().filter_map(|batch| {
        let schema = batch.schema();
        // Find column by name (case-insensitive)
        let col_name_lower = having.column.to_lowercase();
        let idx = schema.fields().iter().position(|f| f.name().to_lowercase() == col_name_lower)?;
        let col = batch.column(idx);

        let mut keep = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            if col.is_null(i) {
                keep.push(false);
                continue;
            }
            let val_str = arrow::util::display::array_value_to_string(col, i).unwrap_or_default();
            let val: f64 = match val_str.parse() {
                Ok(v) => v,
                Err(_) => { keep.push(false); continue; }
            };
            let pass = match having.op.as_str() {
                ">" => val > having.value,
                ">=" => val >= having.value,
                "<" => val < having.value,
                "<=" => val <= having.value,
                "=" => (val - having.value).abs() < f64::EPSILON,
                "!=" => (val - having.value).abs() >= f64::EPSILON,
                _ => true,
            };
            keep.push(pass);
        }

        let mask = arrow::array::BooleanArray::from(keep);
        arrow::compute::filter_record_batch(batch, &mask).ok()
    }).collect()
}

/// Re-aggregate merged batches by GROUP BY columns.
/// For multi-source queries, partial aggregates (COUNT, SUM) need to be
/// summed across sources. This does a simple in-memory group-and-sum.
fn reaggregate_batches(
    batches: Vec<arrow::record_batch::RecordBatch>,
    group_cols: &[String],
) -> Vec<arrow::record_batch::RecordBatch> {
    if batches.is_empty() || group_cols.is_empty() {
        return batches;
    }
    let schema = batches[0].schema();

    // Find group column indices and numeric (aggregate) column indices
    let group_indices: Vec<usize> = group_cols.iter()
        .filter_map(|c| schema.index_of(c).ok())
        .collect();
    let agg_indices: Vec<usize> = (0..schema.fields().len())
        .filter(|i| !group_indices.contains(i) && schema.field(*i).name() != "_datasource")
        .collect();

    if group_indices.is_empty() {
        return batches;
    }

    // Build groups: key = group column values, value = summed agg values
    let mut groups: std::collections::HashMap<Vec<String>, Vec<f64>> = std::collections::HashMap::new();

    for batch in &batches {
        for row in 0..batch.num_rows() {
            let key: Vec<String> = group_indices.iter()
                .map(|&i| arrow::util::display::array_value_to_string(batch.column(i), row).unwrap_or_default())
                .collect();
            let entry = groups.entry(key).or_insert_with(|| vec![0.0; agg_indices.len()]);
            for (j, &col_idx) in agg_indices.iter().enumerate() {
                let col = batch.column(col_idx);
                if !col.is_null(row) {
                    let val_str = arrow::util::display::array_value_to_string(col, row).unwrap_or_default();
                    if let Ok(v) = val_str.parse::<f64>() {
                        entry[j] += v;
                    }
                }
            }
        }
    }

    // Build output batch
    use arrow::array::{StringArray, Float64Array};
    use arrow::datatypes::{DataType, Field};

    let mut fields: Vec<Arc<Field>> = group_indices.iter()
        .map(|&i| Arc::new(Field::new(schema.field(i).name(), DataType::Utf8, true)))
        .collect();
    for &i in &agg_indices {
        fields.push(Arc::new(Field::new(schema.field(i).name(), DataType::Float64, true)));
    }
    let out_schema = Arc::new(arrow::datatypes::Schema::new(fields));

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
    let keys: Vec<&Vec<String>> = groups.keys().collect();

    for (gi, _) in group_indices.iter().enumerate() {
        let arr: StringArray = keys.iter().map(|k| Some(k[gi].as_str())).collect();
        columns.push(Arc::new(arr));
    }
    for (ai, _) in agg_indices.iter().enumerate() {
        let arr: Float64Array = keys.iter().map(|k| groups[*k][ai]).collect();
        columns.push(Arc::new(arr));
    }

    match arrow::record_batch::RecordBatch::try_new(out_schema, columns) {
        Ok(batch) => vec![batch],
        Err(_) => batches,
    }
}

/// Extract ORDER BY column name and direction from query.
fn parse_order_by(query: &str) -> Vec<(String, bool)> {
    let stripped = strip_string_literals(query);
    let lower = stripped.to_lowercase();
    let pos = match lower.rfind("order by ") {
        Some(p) => p,
        None => return vec![],
    };
    let after = stripped[pos + 9..].trim();
    // Stop at LIMIT, OFFSET, or end
    let clause = after
        .split_once("limit").map(|(c, _)| c)
        .unwrap_or(after)
        .split_once("offset").map(|(c, _)| c)
        .unwrap_or(after)
        .trim();

    clause.split(',')
        .filter_map(|part| {
            let tokens: Vec<&str> = part.split_whitespace().collect();
            if tokens.is_empty() { return None; }
            let col = tokens[0].trim_matches(|c: char| !c.is_alphanumeric() && c != '_').to_string();
            if col.is_empty() { return None; }
            let desc = tokens.get(1).map(|d| d.to_lowercase() == "desc").unwrap_or(false);
            Some((col, desc))
        })
        .collect()
}

/// Build a QueryWorkload from query text for cost estimation.
fn build_workload(query: &str) -> fuse_engine::QueryWorkload {
    let lower = query.to_lowercase();
    fuse_engine::QueryWorkload {
        has_filter: lower.contains("where "),
        has_aggregation: lower.contains("group by")
            || lower.contains("count(")
            || lower.contains("sum(")
            || lower.contains("avg("),
        has_sort: lower.contains("order by") || lower.contains("sort "),
        has_limit: lower.contains("limit ") || lower.contains("head "),
        limit_value: parse_limit(query).map(|n| n as u64),
        projection_count: 0,
        total_columns: 0,
        is_cached: false,
    }
}

fn parse_qualified_name(name: &str) -> Result<(String, String), String> {
    // Strip alias: "ds.table AS a" or "ds.table a" → "ds.table"
    let clean = name.split_whitespace().next().unwrap_or(name);
    clean
        .split_once('.')
        .map(|(ds, tbl)| (ds.to_string(), tbl.to_string()))
        .ok_or_else(|| format!("expected 'datasource.table', got '{}'", clean))
}

/// Single-source wrappers used by evaluate_alerts handler.
fn parse_ppl_source(query: &str) -> Result<(String, String), String> {
    parse_ppl_sources(query).and_then(|v| v.into_iter().next().ok_or_else(|| "no source found".to_string()))
}

fn parse_sql_source(query: &str) -> Result<(String, String), String> {
    parse_sql_sources(query).and_then(|v| v.into_iter().next().ok_or_else(|| "no source found".to_string()))
}

/// Build a SubQuery from a user query string using the full translation pipeline.
///
/// For PPL: parse PPL → translate to SQL → parse SQL into SubQuery.
/// For SQL: parse SQL directly into SubQuery.
/// Falls back to a minimal SubQuery if translation fails.
pub fn build_sub_query(
    query: &str,
    format: &str,
    table: &str,
) -> Result<fuse_core::connector::SubQuery, String> {
    let sql = if format == "ppl" {
        let parsed = fuse_engine::ppl::parse_ppl(query)
            .map_err(|e| format!("PPL parse error: {e}"))?;
        fuse_engine::ppl::ppl_to_sql(&parsed)
            .map_err(|e| format!("PPL translation error: {e}"))?
    } else {
        query.to_string()
    };

    match fuse_engine::sql_to_subquery::sql_to_subquery(&sql) {
        Ok(mut sq) => {
            sq.table = table.to_string();
            Ok(sq)
        }
        Err(_) => {
            // Fallback: minimal SubQuery with no hardcoded limit
            Ok(fuse_core::connector::SubQuery {
                table: table.to_string(),
                projections: vec![],
                filter: None,
                aggregations: vec![],
                group_by: vec![],
                sort: vec![],
                limit: None,
                having: None, offset: None, passthrough: None,
            })
        }
    }
}

/// Convert Arrow RecordBatches to JSON columns + rows.
/// Estimate total size of record batches in bytes.
#[allow(dead_code)]
fn estimate_batches_size(batches: &[arrow::record_batch::RecordBatch]) -> u64 {
    batches.iter().map(|b| {
        b.columns().iter().map(|a| a.get_array_memory_size() as u64).sum::<u64>()
    }).sum()
}

/// Check if result exceeds max_result_bytes. Returns Err with message if exceeded.
#[allow(dead_code)]
fn check_result_size(batches: &[arrow::record_batch::RecordBatch], max_bytes: u64) -> Result<(), String> {
    if max_bytes == 0 { return Ok(()); }
    let size = estimate_batches_size(batches);
    if size > max_bytes {
        Err(format!("result size {} bytes exceeds max_result_bytes limit of {} bytes", size, max_bytes))
    } else {
        Ok(())
    }
}

pub fn batches_to_json(
    batches: &[arrow::record_batch::RecordBatch],
) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    if batches.is_empty() {
        return (vec![], vec![]);
    }

    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<serde_json::Value> = (0..batch.num_columns())
                .map(|col_idx| {
                    let col = batch.column(col_idx);
                    // Use Arrow JSON writer for the value
                    if col.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        // Stringify via Arrow's display
                        let val = arrow::util::display::array_value_to_string(col, row_idx)
                            .unwrap_or_default();
                        serde_json::Value::String(val)
                    }
                })
                .collect();
            rows.push(row);
        }
    }

    (columns, rows)
}

/// Convert Arrow RecordBatches to CSV string.
fn batches_to_csv(batches: &[arrow::record_batch::RecordBatch]) -> String {
    if batches.is_empty() {
        return String::new();
    }
    let mut buf = Vec::new();
    {
        let mut writer = arrow::csv::WriterBuilder::new()
            .with_header(true)
            .build(&mut buf);
        for batch in batches {
            writer.write(batch).ok();
        }
    }
    String::from_utf8(buf).unwrap_or_default()
}

/// Convert Arrow RecordBatches to newline-delimited JSON (one object per row).
fn batches_to_ndjson(batches: &[arrow::record_batch::RecordBatch]) -> String {
    if batches.is_empty() {
        return String::new();
    }
    let schema = batches[0].schema();
    let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut out = String::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut map = serde_json::Map::new();
            for (col_idx, col_name) in columns.iter().enumerate() {
                let col = batch.column(col_idx);
                let val = if col.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::String(
                        arrow::util::display::array_value_to_string(col, row_idx).unwrap_or_default()
                    )
                };
                map.insert(col_name.to_string(), val);
            }
            out.push_str(&serde_json::to_string(&map).unwrap_or_default());
            out.push('\n');
        }
    }
    out
}

/// GET /api/fuse/history — last 50 queries with stats.
pub async fn history_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.history.list())
}

/// GET /api/fuse/stats — aggregated query statistics.
/// POST /api/fuse/keys/rotate — rotate an API key with grace period.
///
/// Body: `{"identity": "alice", "grace_period_secs": 3600}`
/// Returns the new key. Old key remains valid during grace period.
pub async fn rotate_key_handler(
    State(state): State<Arc<AppState>>,
    auth_identity: Option<Extension<crate::auth::AuthIdentity>>,
    Json(req): Json<RotateKeyRequest>,
) -> impl IntoResponse {
    // Only admins can rotate keys
    if let Err(resp) = crate::auth::require_role(
        auth_identity.as_ref().map(|e| &e.0),
        crate::auth::Role::Admin,
        true,
    ) {
        return resp.into_response();
    }

    let grace = req.grace_period_secs.unwrap_or(3600);
    match state.key_rotation.rotate(&req.identity, grace) {
        Some(result) => Json(result).into_response(),
        None => error_json(
            StatusCode::NOT_FOUND,
            format!("no API key found for identity '{}'", req.identity),
        ).into_response(),
    }
}

#[derive(Deserialize)]
pub struct RotateKeyRequest {
    pub identity: String,
    pub grace_period_secs: Option<u64>,
}

/// GET /api/fuse/audit — query audit log entries.
///
/// Query params: `limit` (default 50), `identity` (filter by tenant/user),
/// `since` (unix timestamp), `format` (`json` or `ndjson`).
pub async fn audit_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let limit = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(50usize);
    let format = params.get("format").map(|s| s.as_str()).unwrap_or("json");

    if let Some(identity) = params.get("identity") {
        let entries = state.audit_log.for_identity(identity, limit).await;
        if format == "ndjson" {
            let body = entries.iter()
                .filter_map(|e| serde_json::to_string(e).ok())
                .collect::<Vec<_>>().join("\n");
            return (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")], body).into_response();
        }
        return Json(serde_json::json!({"entries": entries, "count": entries.len()})).into_response();
    }

    if let Some(since) = params.get("since").and_then(|v| v.parse::<u64>().ok()) {
        let body = state.audit_log.export_since(since).await;
        if format == "ndjson" {
            return (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")], body).into_response();
        }
        let entries: Vec<serde_json::Value> = body.lines()
            .filter_map(|l| serde_json::from_str(l).ok()).collect();
        return Json(serde_json::json!({"entries": entries, "count": entries.len()})).into_response();
    }

    let entries = state.audit_log.recent(limit).await;
    if format == "ndjson" {
        let body = entries.iter()
            .filter_map(|e| serde_json::to_string(e).ok())
            .collect::<Vec<_>>().join("\n");
        return (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")], body).into_response();
    }
    Json(serde_json::json!({"entries": entries, "count": entries.len()})).into_response()
}

pub async fn stats_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let history_stats = state.history.stats();
    let connector_count = state.registry.list().len();
    let running = state.running_queries.count();
    let audit_count = state.audit_log.count().await;

    let adaptive_stats = state.adaptive_parallelism.stats();
    let compilation_stats = state.compilation_cache.stats();

    Json(serde_json::json!({
        "history": history_stats,
        "cache": {
            "plan_cache": {
                "entries": state.plan_cache.len(),
                "hits": state.plan_cache.hits(),
                "misses": state.plan_cache.misses(),
                "hit_rate_pct": state.plan_cache.hit_rate(),
            },
            "result_cache": {
                "entries": state.result_cache.len(),
            },
        },
        "connectors": connector_count,
        "running_queries": running,
        "audit_entries": audit_count,
        "adaptive_parallelism": adaptive_stats,
        "compilation_cache": compilation_stats,
    }))
}

/// DELETE /api/fuse/cache — flush result and plan caches.
pub async fn clear_cache_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.result_cache.clear();
    Json(serde_json::json!({"cleared": true}))
}

/// GET /api/fuse/federation — return federation topology.
pub async fn federation_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let registry = crate::federation::FederationRegistry::new();
    for conn in state.registry.list() {
        if conn.connector_type() == "fuse" {
            registry.register(crate::federation::FederatedInstance {
                id: conn.id().to_string(),
                name: Some(conn.id().to_string()),
                url: String::new(),
                status: crate::federation::InstanceStatus::Healthy,
                datasources: vec![],
                latency_ms: None,
                last_checked: None,
            });
        }
    }
    Json(registry.topology())
}

// ── Lineage handler (#1840) ──

/// POST /api/fuse/lineage — extract lineage graph from a query.
pub async fn lineage_handler(
    Json(req): Json<crate::lineage::LineageRequest>,
) -> impl IntoResponse {
    let graph = crate::lineage::extract_lineage(&req.query, &req.format);
    Json(graph)
}

// ── Query Replay handlers (#1812) ──

/// GET /api/fuse/replay/recordings — list recorded queries.
pub async fn list_recordings(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.query_recorder.recordings())
}

/// POST /api/fuse/replay/record — record a query for later replay.
pub async fn record_query(
    State(state): State<Arc<AppState>>,
    Json(rec): Json<crate::query_replay::RecordedQuery>,
) -> impl IntoResponse {
    state.query_recorder.record(rec);
    StatusCode::CREATED
}

/// DELETE /api/fuse/replay/recordings — clear all recordings.
pub async fn clear_recordings(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.query_recorder.clear();
    StatusCode::NO_CONTENT
}

// ── Saved query handlers ──

/// GET /api/fuse/saved — list saved queries.
pub async fn list_saved_queries(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.saved_queries.list())
}

/// POST /api/fuse/saved — save a named query.
pub async fn save_query(
    State(state): State<Arc<AppState>>,
    Json(sq): Json<crate::saved_queries::SavedQuery>,
) -> impl IntoResponse {
    state.saved_queries.save(sq);
    StatusCode::CREATED
}

/// GET /api/fuse/saved/:name — get a saved query by name.
pub async fn get_saved_query(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.saved_queries.get(&name) {
        Some(sq) => Json(serde_json::json!(sq)).into_response(),
        None => error_json(StatusCode::NOT_FOUND, format!("saved query '{}' not found", name)).into_response(),
    }
}

/// DELETE /api/fuse/saved/:name — delete a saved query.
pub async fn delete_saved_query(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if state.saved_queries.delete(&name) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        error_json(StatusCode::NOT_FOUND, format!("saved query '{}' not found", name)).into_response()
    }
}

// ── Query cancellation handlers ──

/// DELETE /api/fuse/query/:id — cancel a running query.
pub async fn cancel_query(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if state.running_queries.cancel(&id) {
        (StatusCode::OK, Json(serde_json::json!({"cancelled": true, "query_id": id}))).into_response()
    } else {
        error_json(StatusCode::NOT_FOUND, format!("query '{}' not found or already completed", id)).into_response()
    }
}

/// GET /api/fuse/queries/running — list currently running query IDs.
pub async fn list_running_queries(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    Json(serde_json::json!({"running": state.running_queries.list()}))
}

// ── Alert handlers ──

/// GET /api/fuse/alerts — list configured alert rules.
pub async fn list_alerts(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[derive(Serialize)]
    struct AlertInfo {
        name: String,
        query: String,
        format: String,
        interval_secs: u64,
    }
    let infos: Vec<AlertInfo> = state
        .alert_rules
        .iter()
        .map(|r| AlertInfo {
            name: r.name.clone(),
            query: r.query.clone(),
            format: r.format.clone(),
            interval_secs: r.interval_secs,
        })
        .collect();
    Json(infos)
}

/// POST /api/fuse/alerts/evaluate — run all alert rules now and return firing alerts.
pub async fn evaluate_alerts(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut firing = Vec::new();
    for rule in &state.alert_rules {
        // Parse the rule's query to find the datasource
        let parse_result = match rule.format.as_str() {
            "ppl" => parse_ppl_source(&rule.query),
            _ => parse_sql_source(&rule.query),
        };
        let (ds_id, table) = match parse_result {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(rule = rule.name.as_str(), "alert rule parse error: {e}");
                continue;
            }
        };
        let connector = match state.registry.get(&ds_id) {
            Some(c) => c,
            None => {
                tracing::warn!(rule = rule.name.as_str(), "datasource '{}' not found", ds_id);
                continue;
            }
        };
        let sub_query = match build_sub_query(&rule.query, &rule.format, &table) {
            Ok(sq) => sq,
            Err(e) => {
                tracing::warn!(rule = rule.name.as_str(), "sub_query build error: {e}");
                continue;
            }
        };
        match connector.execute(&sub_query).await {
            Ok(batches) => {
                let result = AlertEvaluator::evaluate(rule, &batches);
                if result.state == fuse_core::alerting::AlertState::Firing {
                    firing.push(result);
                }
            }
            Err(e) => tracing::warn!(rule = rule.name.as_str(), "alert query error: {e}"),
        }
    }
    Json(firing)
}

// ── Materialized view handlers ──

/// GET /api/fuse/views — list registered materialized views.
/// POST /api/fuse/views — create a virtual view
pub async fn create_view(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateViewRequest>,
) -> impl IntoResponse {
    if req.name.is_empty() || req.query.is_empty() {
        return error_json(StatusCode::BAD_REQUEST, "name and query are required").into_response();
    }
    let refresh_secs = req.refresh_interval_secs.unwrap_or(300);
    let def = fuse_engine::materialized::MaterializedViewDef {
        name: req.name.clone(),
        query: req.query.clone(),
        refresh_interval: std::time::Duration::from_secs(refresh_secs),
    };
    state.view_registry.register(def);
    (StatusCode::CREATED, Json(serde_json::json!({
        "name": req.name,
        "query": req.query,
        "refresh_interval_secs": refresh_secs,
    }))).into_response()
}

#[derive(Deserialize)]
pub struct CreateViewRequest {
    pub name: String,
    pub query: String,
    pub refresh_interval_secs: Option<u64>,
}

/// DELETE /api/fuse/views/:name — delete a virtual view
pub async fn delete_view(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if state.view_registry.remove(&name) {
        (StatusCode::OK, Json(serde_json::json!({"deleted": name}))).into_response()
    } else {
        error_json(StatusCode::NOT_FOUND, format!("view '{}' not found", name)).into_response()
    }
}

/// Split SQL on semicolons, respecting string literals.
pub fn split_statements(query: &str) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;

    for c in query.chars() {
        if c == '\'' {
            in_quote = !in_quote;
            current.push(c);
        } else if c == ';' && !in_quote {
            let trimmed = current.trim().to_string();
            if !trimmed.is_empty() {
                stmts.push(trimmed);
            }
            current.clear();
        } else {
            current.push(c);
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        stmts.push(trimmed);
    }
    stmts
}

/// POST /api/fuse/multi — execute multiple semicolon-separated statements
pub async fn multi_query_handler(
    State(state): State<Arc<AppState>>,
    auth_identity: Option<Extension<crate::auth::AuthIdentity>>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let statements = split_statements(&req.query);
    if statements.len() <= 1 {
        // Single statement — delegate to normal handler
        return query_handler(State(state), auth_identity, axum::http::HeaderMap::new(), Json(req)).await.into_response();
    }

    let mut results = Vec::new();
    for stmt in &statements {
        let mut sub_req = req.clone();
        sub_req.query = stmt.clone();
        let resp = query_handler(State(state.clone()), auth_identity.clone(), axum::http::HeaderMap::new(), Json(sub_req)).await;
        // Extract JSON body from response
        let (parts, body) = resp.into_response().into_parts();
        let bytes = axum::body::to_bytes(body, 10_000_000).await.unwrap_or_default();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({"error": "parse error"}));
        results.push(serde_json::json!({
            "status": parts.status.as_u16(),
            "data": json,
        }));
    }

    (StatusCode::OK, Json(serde_json::json!({ "results": results, "count": results.len() }))).into_response()
}

// ── Natural Language to SQL ──

#[derive(Deserialize)]
pub struct NlQueryRequest {
    pub question: String,
    /// If true, also execute the generated SQL and return results.
    #[serde(default)]
    pub execute: bool,
}

#[derive(Serialize)]
pub struct NlQueryResponse {
    pub question: String,
    pub generated_sql: String,
    pub schema_context: Vec<DatasourceSchema>,
    pub prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub results: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct DatasourceSchema {
    pub datasource: String,
    pub tables: Vec<String>,
}

/// POST /api/fuse/nl — translate natural language to SQL
pub async fn nl_to_sql_handler(
    State(state): State<Arc<AppState>>,
    auth_identity: Option<Extension<crate::auth::AuthIdentity>>,
    Json(req): Json<NlQueryRequest>,
) -> impl IntoResponse {
    // Gather schema context from all registered datasources
    let connectors = state.registry.list();
    let mut schemas = Vec::new();
    let mut schema_text = String::new();

    for conn in &connectors {
        let ds_id = conn.id().to_string();
        let tables = conn.table_names().await.unwrap_or_default();
        if !tables.is_empty() {
            schema_text.push_str(&format!("Datasource '{}': tables: {}\n", ds_id, tables.join(", ")));
        } else {
            schema_text.push_str(&format!("Datasource '{}': (use {}.table_name)\n", ds_id, ds_id));
        }
        schemas.push(DatasourceSchema { datasource: ds_id, tables });
    }

    // Build prompt
    let prompt = format!(
        "You are a SQL query generator for the Fuse federated query engine.\n\
         Fuse queries use datasource.table syntax (e.g. SELECT * FROM cluster_a.logs).\n\
         Fuse supports: SELECT, JOIN, UNION ALL, GROUP BY, HAVING, ORDER BY, LIMIT,\n\
         CTEs (WITH), window functions, CONTAINS for full-text search,\n\
         and cross-datasource queries.\n\n\
         Available datasources and tables:\n{}\n\
         User question: {}\n\n\
         Generate a single SQL query. Return ONLY the SQL, no explanation.",
        schema_text, req.question
    );

    // Generate SQL from the question using simple pattern matching as fallback
    // (Real deployment would call an LLM API here)
    let generated_sql = generate_sql_from_nl(&req.question, &schemas);

    let mut response = NlQueryResponse {
        question: req.question.clone(),
        generated_sql: generated_sql.clone(),
        schema_context: schemas,
        prompt,
        results: None,
        error: None,
    };

    // Optionally execute the generated SQL — only allow SELECT/EXPLAIN
    if req.execute && !generated_sql.is_empty() {
        let sql_trimmed = generated_sql.trim_start().to_uppercase();
        if !sql_trimmed.starts_with("SELECT") && !sql_trimmed.starts_with("EXPLAIN") {
            response.error = Some("generated SQL rejected: only SELECT/EXPLAIN queries can be auto-executed".into());
        } else {
            let query_req = QueryRequest {
                query: generated_sql,
                format: "sql".into(),
                analyze: false,
                timeout_ms: None,
                result_format: "json".into(),
                params: std::collections::HashMap::new(),
                start: None, end: None, step: None,
                cursor: None, page_size: None,
            };
            let exec_resp = query_handler(State(state), auth_identity, axum::http::HeaderMap::new(), Json(query_req)).await;
            let (_, body) = exec_resp.into_response().into_parts();
            if let Ok(bytes) = axum::body::to_bytes(body, 10_000_000).await {
                response.results = serde_json::from_slice(&bytes).ok();
            }
        }
    }

    (StatusCode::OK, Json(response)).into_response()
}

/// Simple rule-based NL→SQL fallback (no LLM dependency).
pub fn generate_sql_from_nl(question: &str, schemas: &[DatasourceSchema]) -> String {
    use fuse_core::sql::quote_ident;

    let q = question.to_lowercase();
    let first_ds = schemas.first().map(|s| s.datasource.as_str()).unwrap_or("datasource");
    let table = schemas.first()
        .and_then(|s| s.tables.first().map(|t| t.as_str()))
        .unwrap_or("logs");
    let source = format!("{}.{}", quote_ident(first_ds), quote_ident(table));

    // Multi-table JOIN detection: "join X with Y", "combine X and Y", "correlate/enrich"
    if let Some(join_sql) = try_generate_join(&q, schemas) {
        return join_sql;
    }

    if q.contains("count") && q.contains("by") {
        let group_col = extract_keyword_after(&q, "by").unwrap_or("host".into());
        let col = quote_ident(&sanitize_nl_identifier(&group_col));
        return format!("SELECT {col}, COUNT(*) AS count FROM {source} GROUP BY {col} ORDER BY count DESC LIMIT 20");
    }
    if q.contains("top") || q.contains("most") {
        let n = extract_number(&q).unwrap_or(10).min(1000);
        return format!("SELECT * FROM {source} ORDER BY timestamp DESC LIMIT {n}");
    }
    if q.contains("error") || q.contains("fail") {
        return format!("SELECT * FROM {source} WHERE status >= 500 ORDER BY timestamp DESC LIMIT 20");
    }
    if q.contains("between") || q.contains("last") {
        return format!("SELECT * FROM {source} ORDER BY timestamp DESC LIMIT 100");
    }
    format!("SELECT * FROM {source} LIMIT 20")
}

/// Strip non-identifier characters from NL-extracted column names.
fn sanitize_nl_identifier(s: &str) -> String {
    let clean: String = s.chars().filter(|c| c.is_alphanumeric() || *c == '_').collect();
    if clean.is_empty() { "_".to_string() } else { clean }
}

fn extract_keyword_after(text: &str, keyword: &str) -> Option<String> {
    let pos = text.find(keyword)?;
    let after = text[pos + keyword.len()..].trim();
    after.split_whitespace().next().map(|s| s.to_string())
}

fn extract_number(text: &str) -> Option<u64> {
    text.split_whitespace()
        .find_map(|w| w.parse::<u64>().ok())
}

/// GET /api/fuse/advisor — query optimization suggestions from history
pub async fn query_advisor_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let entries = state.history.recent(1000);
    let advice = crate::history::QueryAdvisor::analyze(&entries);
    (StatusCode::OK, Json(serde_json::json!({
        "advice": advice,
        "analyzed_queries": entries.len(),
    }))).into_response()
}

pub async fn list_views(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[derive(Serialize)]
    struct ViewInfo { name: String, stale: bool }
    let names = state.view_registry.list();
    let infos: Vec<ViewInfo> = names.into_iter().map(|name| {
        let stale = state.view_registry.get(&name)
            .map(|v| v.read().unwrap().needs_refresh())
            .unwrap_or(true);
        ViewInfo { name, stale }
    }).collect();
    Json(infos)
}

/// GET /api/fuse/views/:name — query a materialized view (returns cached results).
pub async fn get_view(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.view_registry.get_results(&name) {
        None => {
            if state.view_registry.get(&name).is_none() {
                error_json(StatusCode::NOT_FOUND, format!("view '{}' not found", name)).into_response()
            } else {
                error_json(StatusCode::SERVICE_UNAVAILABLE, format!("view '{}' not yet refreshed", name)).into_response()
            }
        }
        Some(batches) => {
            let (columns, rows) = batches_to_json(&batches);
            Json(QueryResponse {
                columns,
                rows: rows.clone(),
                metadata: QueryMetadata { total_rows: rows.len() as u64, format: "view".into(), trace_id: format!("v-{:x}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos()), datasources_queried: None, datasource_stats: None, cost_estimate: None },
                execution_profile: None,
                partial_errors: vec![],
                next_cursor: None,
            }).into_response()
        }
    }
}

/// POST /api/fuse/views/:name/refresh — trigger a synchronous refresh of a view.
pub async fn refresh_view(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let view_arc = match state.view_registry.get(&name) {
        Some(v) => v,
        None => return error_json(StatusCode::NOT_FOUND, format!("view '{}' not found", name)).into_response(),
    };

    let query = view_arc.read().unwrap().def.query.clone();
    let format = "sql";

    let refs = match parse_sql_sources(&query) {
        Ok(r) if !r.is_empty() => r,
        _ => {
            view_arc.write().unwrap().set_error("failed to parse view query".into());
            return error_json(StatusCode::BAD_REQUEST, "failed to parse view query").into_response();
        }
    };

    let (ds_id, table) = &refs[0];
    let connector = match state.registry.get(ds_id) {
        Some(c) => c,
        None => {
            let msg = format!("datasource '{}' not found", ds_id);
            view_arc.write().unwrap().set_error(msg.clone());
            return error_json(StatusCode::NOT_FOUND, msg).into_response();
        }
    };

    let sub_query = match build_sub_query(&query, format, table) {
        Ok(sq) => sq,
        Err(e) => {
            view_arc.write().unwrap().set_error(e.clone());
            return error_json(StatusCode::BAD_REQUEST, e).into_response();
        }
    };

    match connector.execute(&sub_query).await {
        Ok(batches) => {
            view_arc.write().unwrap().set_results(batches);
            Json(serde_json::json!({ "refreshed": true, "view": name })).into_response()
        }
        Err(e) => {
            view_arc.write().unwrap().set_error(e.to_string());
            error_json(StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
    }
}

// ── Trace Reconstruction (#421) ──

#[derive(Serialize)]
pub struct TraceSpan {
    pub datasource: String,
    pub timestamp: Option<serde_json::Value>,
    pub fields: std::collections::HashMap<String, serde_json::Value>,
}

#[derive(Serialize)]
pub struct TraceResponse {
    pub trace_id: String,
    pub spans: Vec<TraceSpan>,
    pub datasources_searched: Vec<String>,
    pub datasources_matched: Vec<String>,
    pub total_spans: u64,
    pub search_ms: u64,
}

/// GET /api/fuse/trace/{trace_id}
/// Fan out to all datasources, query for trace_id, merge into timeline.
pub async fn trace_handler(
    State(state): State<Arc<AppState>>,
    Path(trace_id): Path<String>,
) -> impl IntoResponse {
    use std::time::Instant;
    use fuse_core::connector::*;

    let start = Instant::now();
    let connectors = state.registry.connectors();
    let datasources_searched: Vec<String> = connectors.iter().map(|(id, _)| id.clone()).collect();

    // Fan out: query each connector for trace_id
    let mut handles = Vec::new();
    for (ds_id, connector) in &connectors {
        let ds_id = ds_id.clone();
        let connector = connector.clone();
        let tid = trace_id.clone();
        handles.push(tokio::spawn(async move {
            // Try common trace_id field names
            let schemas = connector.discover_schemas().await.unwrap_or_default();
            let table = schemas.first().map(|s| s.name.clone()).unwrap_or_default();
            if table.is_empty() {
                return (ds_id, vec![]);
            }
            let sub = SubQuery {
                table,
                projections: vec![],
                filter: Some(FilterExpr::Comparison {
                    field: "trace_id".into(),
                    op: ComparisonOp::Eq,
                    value: ScalarValue::Utf8(tid),
                }),
                aggregations: vec![],
                group_by: vec![],
                having: None,
                sort: vec![],
                limit: Some(1000),
                offset: None, passthrough: None,
            };
            let batches = connector.execute(&sub).await.unwrap_or_default();
            (ds_id, batches)
        }));
    }

    let mut spans = Vec::new();
    let mut datasources_matched = Vec::new();

    for handle in handles {
        if let Ok((ds_id, batches)) = handle.await {
            if batches.is_empty() { continue; }
            let (cols, rows) = batches_to_json(&batches);
            if rows.is_empty() { continue; }
            datasources_matched.push(ds_id.clone());

            let ts_idx = cols.iter().position(|c| {
                let l = c.to_lowercase();
                l == "timestamp" || l == "@timestamp" || l == "time" || l == "ts"
            });

            for row in &rows {
                let mut fields = std::collections::HashMap::new();
                for (i, col) in cols.iter().enumerate() {
                    if let Some(val) = row.get(i) {
                        fields.insert(col.clone(), val.clone());
                    }
                }
                let timestamp = ts_idx.and_then(|i| row.get(i).cloned());
                spans.push(TraceSpan { datasource: ds_id.clone(), timestamp, fields });
            }
        }
    }

    // Sort by timestamp if available
    spans.sort_by(|a, b| {
        let ta = a.timestamp.as_ref().and_then(|v| v.as_str()).unwrap_or("");
        let tb = b.timestamp.as_ref().and_then(|v| v.as_str()).unwrap_or("");
        ta.cmp(tb)
    });

    let total_spans = spans.len() as u64;
    let resp = TraceResponse {
        trace_id,
        spans,
        datasources_searched,
        datasources_matched,
        total_spans,
        search_ms: start.elapsed().as_millis() as u64,
    };
    (StatusCode::OK, axum::Json(resp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_running_queries_cancel_returns_true_when_found() {
        let rq = RunningQueries::new();
        let token = CancellationToken::new();
        rq.insert("q-001".into(), token.clone());
        assert!(rq.cancel("q-001"));
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_running_queries_cancel_returns_false_when_not_found() {
        let rq = RunningQueries::new();
        assert!(!rq.cancel("q-nonexistent"));
    }

    #[test]
    fn test_running_queries_list() {
        let rq = RunningQueries::new();
        assert!(rq.list().is_empty());
        rq.insert("q-001".into(), CancellationToken::new());
        rq.insert("q-002".into(), CancellationToken::new());
        let mut ids = rq.list();
        ids.sort();
        assert_eq!(ids, vec!["q-001", "q-002"]);
    }

    #[test]
    fn test_running_queries_remove_cleans_up() {
        let rq = RunningQueries::new();
        rq.insert("q-001".into(), CancellationToken::new());
        rq.remove("q-001");
        assert!(rq.list().is_empty());
    }

    #[test]
    fn test_running_queries_cancel_removes_entry() {
        let rq = RunningQueries::new();
        rq.insert("q-001".into(), CancellationToken::new());
        rq.cancel("q-001");
        assert!(!rq.cancel("q-001"));
    }

    #[test]
    fn test_running_queries_count() {
        let rq = RunningQueries::new();
        assert_eq!(rq.count(), 0);
        rq.insert("a".into(), CancellationToken::new());
        rq.insert("b".into(), CancellationToken::new());
        assert_eq!(rq.count(), 2);
        rq.remove("a");
        assert_eq!(rq.count(), 1);
    }

    #[test]
    fn test_running_queries_cancel_all() {
        let rq = RunningQueries::new();
        let t1 = CancellationToken::new();
        let t2 = CancellationToken::new();
        let t1c = t1.clone();
        let t2c = t2.clone();
        rq.insert("a".into(), t1);
        rq.insert("b".into(), t2);
        rq.cancel_all();
        assert_eq!(rq.count(), 0);
        assert!(t1c.is_cancelled());
        assert!(t2c.is_cancelled());
    }

    #[test]
    fn test_query_request_range_fields_deserialize() {
        let json = r#"{"query":"SELECT * FROM prom.http_requests","format":"sql","start":"2024-01-01T00:00:00Z","end":"2024-01-02T00:00:00Z","step":"1m"}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.start.as_deref(), Some("2024-01-01T00:00:00Z"));
        assert_eq!(req.end.as_deref(), Some("2024-01-02T00:00:00Z"));
        assert_eq!(req.step.as_deref(), Some("1m"));
    }

    #[test]
    fn test_query_request_range_fields_optional() {
        let json = r#"{"query":"SELECT * FROM prom.http_requests","format":"sql"}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        assert!(req.start.is_none());
        assert!(req.end.is_none());
        assert!(req.step.is_none());
    }

    // ── describe_pushdown verification (tester) ──

    #[test]
    fn test_describe_pushdown_all_fields() {
        use fuse_core::connector::*;
        let sq = SubQuery {
            table: "logs".into(),
            projections: vec!["host".into(), "status".into()],
            filter: Some(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Gte, value: ScalarValue::Int64(500) }),
            aggregations: vec![AggregationExpr { function: AggFunction::Count, field: None, alias: "cnt".into() }],
            group_by: vec!["host".into()],
            having: None,
            sort: vec![],
            limit: Some(10),
            offset: None, passthrough: None,
        };
        let desc = describe_pushdown(&sq);
        assert!(desc.iter().any(|d| d.contains("projection")));
        assert!(desc.iter().any(|d| d.contains("filter")));
        assert!(desc.iter().any(|d| d.contains("aggregation")));
        assert!(desc.iter().any(|d| d.contains("group_by")));
        assert!(desc.iter().any(|d| d.contains("limit")));
    }

    #[test]
    fn test_describe_pushdown_empty_query() {
        use fuse_core::connector::*;
        let sq = SubQuery {
            table: "logs".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            offset: None, passthrough: None,
        };
        let desc = describe_pushdown(&sq);
        assert!(desc.is_empty());
    }

    // ── #242 Prometheus range query verification (tester) ──

    #[test]
    fn test_range_fields_all_present_creates_tuple() {
        let json = r#"{"query":"SELECT * FROM prom.metrics","format":"sql","start":"2024-01-01T00:00:00Z","end":"2024-01-02T00:00:00Z","step":"1m"}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        let range = match (&req.start, &req.end, &req.step) {
            (Some(s), Some(e), Some(st)) => Some((s.as_str(), e.as_str(), st.as_str())),
            _ => None,
        };
        assert!(range.is_some());
        let (s, e, st) = range.unwrap();
        assert_eq!(s, "2024-01-01T00:00:00Z");
        assert_eq!(e, "2024-01-02T00:00:00Z");
        assert_eq!(st, "1m");
    }

    #[test]
    fn test_range_partial_fields_returns_none() {
        // Only start, no end/step → should NOT be a range query
        let json = r#"{"query":"SELECT * FROM prom.metrics","format":"sql","start":"2024-01-01T00:00:00Z"}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        let range = match (&req.start, &req.end, &req.step) {
            (Some(s), Some(e), Some(st)) => Some((s.as_str(), e.as_str(), st.as_str())),
            _ => None,
        };
        assert!(range.is_none());
    }

    #[test]
    fn test_range_params_injected_into_passthrough() {
        use fuse_core::connector::SubQuery;
        let mut sq = SubQuery {
            table: "metrics".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            offset: None, passthrough: None,
        };
        // Simulate what execute_single does
        let (start, end, step) = ("2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "1m");
        let pt = sq.passthrough.get_or_insert(serde_json::json!({}));
        if let Some(obj) = pt.as_object_mut() {
            obj.insert("start".into(), serde_json::Value::String(start.into()));
            obj.insert("end".into(), serde_json::Value::String(end.into()));
            obj.insert("step".into(), serde_json::Value::String(step.into()));
        }
        let pt = sq.passthrough.as_ref().unwrap();
        assert_eq!(pt["start"], "2024-01-01T00:00:00Z");
        assert_eq!(pt["end"], "2024-01-02T00:00:00Z");
        assert_eq!(pt["step"], "1m");
    }

    #[test]
    fn test_trace_response_serialization() {
        let resp = TraceResponse {
            trace_id: "abc-123".into(),
            spans: vec![
                TraceSpan {
                    datasource: "cluster_a".into(),
                    timestamp: Some(serde_json::json!("2024-01-01T00:00:01Z")),
                    fields: [("service".into(), serde_json::json!("api"))].into(),
                },
                TraceSpan {
                    datasource: "cluster_b".into(),
                    timestamp: Some(serde_json::json!("2024-01-01T00:00:02Z")),
                    fields: [("service".into(), serde_json::json!("db"))].into(),
                },
            ],
            datasources_searched: vec!["cluster_a".into(), "cluster_b".into(), "s3".into()],
            datasources_matched: vec!["cluster_a".into(), "cluster_b".into()],
            total_spans: 2,
            search_ms: 42,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["trace_id"], "abc-123");
        assert_eq!(json["total_spans"], 2);
        assert_eq!(json["datasources_matched"].as_array().unwrap().len(), 2);
        assert_eq!(json["spans"][0]["datasource"], "cluster_a");
        assert_eq!(json["spans"][1]["timestamp"], "2024-01-01T00:00:02Z");
    }

    #[test]
    fn test_trace_span_sort_by_timestamp() {
        let mut spans = [TraceSpan {
                datasource: "b".into(),
                timestamp: Some(serde_json::json!("2024-01-01T00:00:05Z")),
                fields: Default::default(),
            },
            TraceSpan {
                datasource: "a".into(),
                timestamp: Some(serde_json::json!("2024-01-01T00:00:01Z")),
                fields: Default::default(),
            },
            TraceSpan {
                datasource: "c".into(),
                timestamp: None,
                fields: Default::default(),
            }];
        spans.sort_by(|a, b| {
            let ta = a.timestamp.as_ref().and_then(|v| v.as_str()).unwrap_or("");
            let tb = b.timestamp.as_ref().and_then(|v| v.as_str()).unwrap_or("");
            ta.cmp(tb)
        });
        // None timestamp sorts first (empty string), then chronological
        assert_eq!(spans[0].datasource, "c");
        assert_eq!(spans[1].datasource, "a");
        assert_eq!(spans[2].datasource, "b");
    }

    #[test]
    fn test_encode_decode_cursor_roundtrip() {
        let encoded = encode_cursor(42);
        assert_eq!(decode_cursor(&encoded), Some(42));
    }

    #[test]
    fn test_union_cursor_roundtrip() {
        let sources = vec![("cluster_a".into(), 100), ("cluster_b".into(), 50)];
        let encoded = encode_union_cursor(&sources, 150);
        let (decoded_sources, global) = decode_union_cursor(&encoded).unwrap();
        assert_eq!(global, 150);
        assert_eq!(decoded_sources.len(), 2);
        assert_eq!(decoded_sources[0], ("cluster_a".into(), 100));
        assert_eq!(decoded_sources[1], ("cluster_b".into(), 50));
    }

    #[test]
    fn test_union_cursor_single_source() {
        let sources = vec![("ds1".into(), 25)];
        let encoded = encode_union_cursor(&sources, 25);
        let (decoded, global) = decode_union_cursor(&encoded).unwrap();
        assert_eq!(global, 25);
        assert_eq!(decoded, vec![("ds1".into(), 25)]);
    }

    #[test]
    fn test_decode_invalid_union_cursor() {
        assert!(decode_union_cursor("not_a_cursor").is_none());
        assert!(decode_union_cursor("fuse_c_42").is_none());
    }

    #[test]
    fn test_decode_simple_cursor_rejects_union() {
        assert!(decode_cursor("fuse_u_ds1:10|10").is_none());
    }

    #[test]
    fn test_recursive_cte_detection() {
        let q = "WITH RECURSIVE chain AS (SELECT * FROM ds.t UNION ALL SELECT * FROM chain JOIN ds.t ON chain.id = ds.t.parent_id) SELECT * FROM chain";
        let lower = q.to_lowercase();
        assert!(lower.trim_start().starts_with("with recursive "));
    }

    #[test]
    fn test_non_recursive_cte_not_detected() {
        let q = "WITH temp AS (SELECT * FROM ds.t) SELECT * FROM temp";
        let lower = q.to_lowercase();
        assert!(!lower.trim_start().starts_with("with recursive "));
        assert!(lower.trim_start().starts_with("with "));
    }

    #[test]
    fn test_trace_response_empty() {
        let resp = TraceResponse {
            trace_id: "not-found".into(),
            spans: vec![],
            datasources_searched: vec!["ds1".into()],
            datasources_matched: vec![],
            total_spans: 0,
            search_ms: 5,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["total_spans"], 0);
        assert!(json["datasources_matched"].as_array().unwrap().is_empty());
        assert!(json["spans"].as_array().unwrap().is_empty());
    }

    // ── CREATE MATERIALIZED VIEW parser tests ──

    #[test]
    fn test_parse_create_mat_view_basic() {
        let (n, q) = parse_create_materialized_view(
            "CREATE MATERIALIZED VIEW err AS SELECT status FROM cluster_a.logs",
        ).unwrap();
        assert_eq!(n, "err");
        assert_eq!(q, "SELECT status FROM cluster_a.logs");
    }

    #[test]
    fn test_parse_create_mat_view_case_insensitive() {
        assert!(parse_create_materialized_view("create materialized view mv as SELECT 1").is_some());
    }

    #[test]
    fn test_parse_create_mat_view_whitespace() {
        let (n, _) = parse_create_materialized_view(
            "  CREATE MATERIALIZED VIEW   s   AS   SELECT 1  ",
        ).unwrap();
        assert_eq!(n, "s");
    }

    #[test]
    fn test_parse_create_mat_view_none_plain_view() {
        assert!(parse_create_materialized_view("CREATE VIEW v AS SELECT 1").is_none());
    }

    #[test]
    fn test_parse_create_mat_view_none_no_as() {
        assert!(parse_create_materialized_view("CREATE MATERIALIZED VIEW v SELECT 1").is_none());
    }

    #[test]
    fn test_parse_create_mat_view_none_empty_name() {
        assert!(parse_create_materialized_view("CREATE MATERIALIZED VIEW  AS SELECT 1").is_none());
    }

    #[test]
    fn test_parse_create_mat_view_none_empty_query() {
        assert!(parse_create_materialized_view("CREATE MATERIALIZED VIEW v AS ").is_none());
    }

    #[test]
    fn test_parse_create_mat_view_none_unrelated() {
        assert!(parse_create_materialized_view("SELECT * FROM t").is_none());
    }

    // ── REFRESH MATERIALIZED VIEW parser tests ──

    #[test]
    fn test_parse_refresh_mat_view_basic() {
        assert_eq!(parse_refresh_materialized_view("REFRESH MATERIALIZED VIEW err").unwrap(), "err");
    }

    #[test]
    fn test_parse_refresh_mat_view_case_insensitive() {
        assert_eq!(parse_refresh_materialized_view("refresh materialized view mv").unwrap(), "mv");
    }

    #[test]
    fn test_parse_refresh_mat_view_whitespace() {
        assert_eq!(parse_refresh_materialized_view("  REFRESH MATERIALIZED VIEW   s  ").unwrap(), "s");
    }

    #[test]
    fn test_parse_refresh_mat_view_none_empty() {
        assert!(parse_refresh_materialized_view("REFRESH MATERIALIZED VIEW ").is_none());
    }

    #[test]
    fn test_parse_refresh_mat_view_none_unrelated() {
        assert!(parse_refresh_materialized_view("SELECT 1").is_none());
    }

    #[test]
    fn test_parse_refresh_mat_view_none_create() {
        assert!(parse_refresh_materialized_view("CREATE MATERIALIZED VIEW v AS SELECT 1").is_none());
    }

    // ── NDJSON output tests ──

    #[test]
    fn test_batches_to_ndjson_empty() {
        assert_eq!(batches_to_ndjson(&[]), "");
    }

    #[test]
    fn test_batches_to_ndjson_single_row() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(StringArray::from(vec!["alice"]))],
        ).unwrap();
        let ndjson = batches_to_ndjson(&[batch]);
        let parsed: serde_json::Value = serde_json::from_str(ndjson.trim()).unwrap();
        assert_eq!(parsed["name"], "alice");
    }

    #[test]
    fn test_batches_to_ndjson_multiple_rows() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        ).unwrap();
        let ndjson = batches_to_ndjson(&[batch]);
        let lines: Vec<&str> = ndjson.trim().split('\n').collect();
        assert_eq!(lines.len(), 3);
        for line in &lines {
            assert!(serde_json::from_str::<serde_json::Value>(line).is_ok());
        }
    }

    #[test]
    fn test_batches_to_ndjson_null_values() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("val", DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(StringArray::from(vec![Some("x"), None]))],
        ).unwrap();
        let ndjson = batches_to_ndjson(&[batch]);
        let lines: Vec<&str> = ndjson.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        let row2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert!(row2["val"].is_null());
    }

    // ── CTAS parser tests ──

    #[test]
    fn test_parse_ctas_basic() {
        let (ds, tbl, q) = parse_ctas("CREATE TABLE my_pg.results AS SELECT * FROM cluster_a.logs").unwrap();
        assert_eq!(ds, "my_pg");
        assert_eq!(tbl, "results");
        assert_eq!(q, "SELECT * FROM cluster_a.logs");
    }

    #[test]
    fn test_parse_ctas_case_insensitive() {
        assert!(parse_ctas("create table ds.t as SELECT 1").is_some());
    }

    #[test]
    fn test_parse_ctas_none_no_as() {
        assert!(parse_ctas("CREATE TABLE ds.t SELECT 1").is_none());
    }

    #[test]
    fn test_parse_ctas_none_unqualified() {
        assert!(parse_ctas("CREATE TABLE t AS SELECT 1").is_none());
    }

    #[test]
    fn test_parse_ctas_none_empty_query() {
        assert!(parse_ctas("CREATE TABLE ds.t AS ").is_none());
    }

    #[test]
    fn test_parse_ctas_none_unrelated() {
        assert!(parse_ctas("SELECT * FROM t").is_none());
    }

    // ── INSERT INTO ... SELECT parser tests ──

    #[test]
    fn test_parse_insert_into_select_basic() {
        let (ds, tbl, q) = parse_insert_into_select("INSERT INTO my_pg.results SELECT * FROM cluster_a.logs").unwrap();
        assert_eq!(ds, "my_pg");
        assert_eq!(tbl, "results");
        assert_eq!(q, "SELECT * FROM cluster_a.logs");
    }

    #[test]
    fn test_parse_insert_into_select_case_insensitive() {
        assert!(parse_insert_into_select("insert into ds.t select 1").is_some());
    }

    #[test]
    fn test_parse_insert_into_select_none_no_select() {
        assert!(parse_insert_into_select("INSERT INTO ds.t VALUES (1)").is_none());
    }

    #[test]
    fn test_parse_insert_into_select_none_unqualified() {
        assert!(parse_insert_into_select("INSERT INTO t SELECT 1").is_none());
    }

    #[test]
    fn test_parse_insert_into_select_none_unrelated() {
        assert!(parse_insert_into_select("SELECT * FROM t").is_none());
    }

    // ── Transaction parser tests ──

    #[test]
    fn test_parse_begin_bare() {
        let id = parse_begin("BEGIN").unwrap();
        assert!(id.starts_with("txn-"));
    }

    #[test]
    fn test_parse_begin_transaction() {
        let id = parse_begin("BEGIN TRANSACTION").unwrap();
        assert!(id.starts_with("txn-"));
    }

    #[test]
    fn test_parse_begin_with_id() {
        assert_eq!(parse_begin("BEGIN my_txn").unwrap(), "my_txn");
    }

    #[test]
    fn test_parse_begin_transaction_with_id() {
        assert_eq!(parse_begin("BEGIN TRANSACTION tx1").unwrap(), "tx1");
    }

    #[test]
    fn test_parse_commit_with_id() {
        assert_eq!(parse_commit("COMMIT tx1").unwrap(), "tx1");
    }

    #[test]
    fn test_parse_commit_bare_returns_none() {
        assert!(parse_commit("COMMIT").is_none());
    }

    #[test]
    fn test_parse_rollback_with_id() {
        assert_eq!(parse_rollback("ROLLBACK tx1").unwrap(), "tx1");
    }

    #[test]
    fn test_parse_rollback_bare_returns_none() {
        assert!(parse_rollback("ROLLBACK").is_none());
    }

    #[test]
    fn test_estimate_batches_size() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
                std::sync::Arc::new(StringArray::from(vec!["a", "b", "c"])) as arrow::array::ArrayRef,
            ],
        ).unwrap();
        let size = super::estimate_batches_size(&[batch]);
        assert!(size > 0);
    }

    #[test]
    fn test_check_result_size_unlimited() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema, vec![std::sync::Arc::new(Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef],
        ).unwrap();
        assert!(super::check_result_size(&[batch], 0).is_ok());
    }

    #[test]
    fn test_check_result_size_within_limit() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema, vec![std::sync::Arc::new(Int64Array::from(vec![1])) as arrow::array::ArrayRef],
        ).unwrap();
        assert!(super::check_result_size(&[batch], 1_000_000).is_ok());
    }

    #[test]
    fn test_check_result_size_exceeds_limit() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema, vec![std::sync::Arc::new(Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef],
        ).unwrap();
        // Set limit to 1 byte — any result should exceed
        assert!(super::check_result_size(&[batch], 1).is_err());
    }
}

/// GET /api/fuse/autotune — analyze slow query history and suggest tuning changes.
pub async fn autotune_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let threshold: u64 = params.get("threshold_ms").and_then(|v| v.parse().ok()).unwrap_or(1000);
    let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(100);

    let history = state.history.recent(limit);
    let samples: Vec<crate::query_autotuner::QuerySample> = history
        .into_iter()
        .filter_map(|h| {
            let refs = parse_sql_sources(&h.query).ok()?;
            let (ds, table) = refs.into_iter().next()?;
            Some(crate::query_autotuner::QuerySample {
                query: h.query,
                datasource: ds,
                table,
                latency_ms: h.latency_ms,
            })
        })
        .collect();

    let recs = crate::query_autotuner::analyze(&samples, threshold);
    axum::Json(serde_json::json!({
        "recommendations": recs,
        "analyzed_queries": samples.len(),
        "threshold_ms": threshold,
    }))
    .into_response()
}

/// Try to generate a JOIN query from natural language.
/// Detects patterns like "join logs with users", "combine orders and products",
/// "correlate metrics with alerts", "enrich logs with user info".
fn try_generate_join(q: &str, schemas: &[DatasourceSchema]) -> Option<String> {
    use fuse_core::sql::quote_ident;

    let join_keywords = ["join", "combine", "correlate", "enrich", "merge", "link"];
    let has_join_intent = join_keywords.iter().any(|kw| q.contains(kw));
    if !has_join_intent || schemas.len() < 2 {
        return None;
    }

    // Find two table references mentioned in the question
    let (left, right) = find_two_tables(q, schemas)?;

    // Guess a join column: look for common column name hints
    let join_col = guess_join_column(q).unwrap_or_else(|| "id".to_string());
    let join_col = sanitize_nl_identifier(&join_col);

    let left_alias = "a";
    let right_alias = "b";
    let left_src = format!("{}.{}", quote_ident(&left.0), quote_ident(&left.1));
    let right_src = format!("{}.{}", quote_ident(&right.0), quote_ident(&right.1));

    Some(format!(
        "SELECT {la}.*, {ra}.* FROM {left} {la} JOIN {right} {ra} ON {la}.{col} = {ra}.{col} LIMIT 100",
        la = left_alias,
        ra = right_alias,
        left = left_src,
        right = right_src,
        col = quote_ident(&join_col),
    ))
}

/// Find two (datasource, table) pairs mentioned in the question text.
fn find_two_tables(q: &str, schemas: &[DatasourceSchema]) -> Option<((String, String), (String, String))> {
    let mut found = Vec::new();
    for schema in schemas {
        for table in &schema.tables {
            if q.contains(&table.to_lowercase()) {
                found.push((schema.datasource.clone(), table.clone()));
                if found.len() == 2 {
                    return Some((found[0].clone(), found[1].clone()));
                }
            }
        }
        // Also match datasource name
        if q.contains(&schema.datasource.to_lowercase()) && !schema.tables.is_empty() {
            let entry = (schema.datasource.clone(), schema.tables[0].clone());
            if !found.contains(&entry) {
                found.push(entry);
                if found.len() == 2 {
                    return Some((found[0].clone(), found[1].clone()));
                }
            }
        }
    }
    // Fallback: use first two datasources
    if schemas.len() >= 2 {
        let ds1 = &schemas[0];
        let ds2 = &schemas[1];
        let t1 = ds1.tables.first().cloned().unwrap_or_else(|| "table1".into());
        let t2 = ds2.tables.first().cloned().unwrap_or_else(|| "table2".into());
        return Some(((ds1.datasource.clone(), t1), (ds2.datasource.clone(), t2)));
    }
    None
}

/// Guess the join column from natural language hints.
fn guess_join_column(q: &str) -> Option<String> {
    // "on user_id", "by user_id", "using trace_id"
    for prefix in ["on ", "by ", "using ", "where "] {
        if let Some(col) = extract_keyword_after(q, prefix) {
            let clean = sanitize_nl_identifier(&col);
            if clean.len() >= 2 && clean.contains('_') {
                return Some(clean);
            }
        }
    }
    // Common join column patterns in the question
    let common = ["user_id", "trace_id", "order_id", "id", "customer_id", "session_id"];
    for col in common {
        if q.contains(col) {
            return Some(col.to_string());
        }
    }
    None
}

/// GET /api/fuse/similarity — find duplicate/similar query patterns.
pub async fn similarity_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(500);
    let min_group: usize = params.get("min_group").and_then(|v| v.parse().ok()).unwrap_or(2);

    let history = state.history.recent(limit);
    let entries: Vec<crate::query_similarity::QueryEntry> = history
        .into_iter()
        .map(|h| crate::query_similarity::QueryEntry {
            query: h.query,
            latency_ms: h.latency_ms,
            tenant: None,
        })
        .collect();

    let groups = crate::query_similarity::find_similar(&entries, min_group);
    axum::Json(serde_json::json!({
        "groups": groups,
        "total_groups": groups.len(),
        "analyzed_queries": entries.len(),
    }))
    .into_response()
}
