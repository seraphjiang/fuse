// SPDX-License-Identifier: Apache-2.0

pub mod adaptive_parallelism;
pub mod adaptive_timeout;
pub mod access_log;
pub mod alert_api;
pub mod alert_monitor;
pub mod agg_functions;
pub mod aggregator;
pub mod alias;
pub mod arrow_export;
pub mod api;
pub mod anomaly;
pub mod anomaly_alert;
pub mod arrow_ipc;
pub mod async_query;
pub mod audit;
pub mod audit_meta;
pub mod auth;
pub mod bookmarks;
pub mod auto_suggest;
pub mod autocomplete;
pub mod complexity;
pub mod coercer;
pub mod column_stats;
pub mod cache_key;
pub mod capability_summary;
pub mod case_when;
pub mod cdc;
pub mod circuit_breaker;
pub mod config_watch;
pub mod cost_estimator;
pub mod cors;
pub mod data_quality;
pub mod cost_tracker;
pub mod dedup;
pub mod date_fn;
pub mod delivery;
pub mod distinct;
pub mod export;
pub mod explain_cache;
pub mod federation;
pub mod fingerprint;
pub mod flattener;
pub mod formatter;
pub mod graphql;
pub mod grouper;
pub mod having;
pub mod health;
pub mod intersect;
pub mod joiner;
pub mod health_monitor;
pub mod history;
pub mod history_analytics;
pub mod lineage;
pub mod adaptive_cache;
pub mod usage_metering;
pub mod math_fn;
pub mod metrics;
pub mod nl_query;
pub mod notifications;
pub mod otel_ingest;
pub mod pagination;
pub mod null_handler;
pub mod offset_pagination;
pub mod pivot;
pub mod plan_cache;
pub mod pipeline;pub mod pool_stats;
pub mod query_policy;
pub mod query_replay;
pub mod profiler;
pub mod query_parser;
pub mod query_predictor;
pub mod projector;
pub mod registry_snapshot;pub mod query_context;
pub mod prepared;
pub mod query_advisor;
pub mod query_compilation;
pub mod query_diff;
pub mod query_explain;
pub mod rate_limit;
pub mod rate_monitor;pub mod redis_cache;
pub mod refresh_scheduler;
pub mod request_id;
pub mod renamer;
pub mod reorder;
pub mod response_builder;
pub mod result_filter;pub mod retry;
pub mod rewrite;
pub mod row_limit;
pub mod shutdown;
pub mod slow_query;
pub mod set_ops;
pub mod sorter;
pub mod string_fn;
pub mod wasm_plugin;
pub mod webhook;
pub mod ws_streaming;
mod server_integration_tests;pub mod window_fn;pub mod saved_queries;
pub mod sanitize;
pub mod sampling;
pub mod scheduler;
pub mod schema_discovery;
pub mod security_headers;
pub mod shared_state;
pub mod sse_stream;
pub mod streaming;
pub mod telemetry;
pub mod tenant;
pub mod templates;pub mod tags;pub mod transaction;
pub mod type_infer;
pub mod top_n;
pub mod transpose;
pub mod timeout_tracker;
pub mod union_typed;
pub mod validate;
pub mod tracing_ctx;

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::http::header;
use axum::middleware;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Router;
use tower_http::trace::TraceLayer;
use tracing::Level;

use api::AppState;

const PLAYGROUND_HTML: &str = include_str!("../../../playground/index.html");
const DASHBOARD_HTML: &str = include_str!("../../../playground/dashboard.html");
const EXPLORE_HTML: &str = include_str!("../../../playground/explore.html");
const SETTINGS_HTML: &str = include_str!("../../../playground/settings.html");
const STATUS_HTML: &str = include_str!("../../../playground/status.html");
const HELP_HTML: &str = include_str!("../../../playground/help.html");
const ADMIN_HTML: &str = include_str!("../../../playground/admin.html");
const CHANGELOG_HTML: &str = include_str!("../../../playground/changelog.html");
const FEEDBACK_WIDGET_HTML: &str = include_str!("../../../playground/feedback-widget.html");
const VIEWS_HTML: &str = include_str!("../../../playground/views.html");
const PLUGINS_HTML: &str = include_str!("../../../playground/plugins.html");
const ALERTS_HTML: &str = include_str!("../../../playground/alerts.html");
const TERMINAL_HTML: &str = include_str!("../../../playground/terminal.html");
const FEDERATION_HTML: &str = include_str!("../../../playground/federation.html");
const SCHEDULES_HTML: &str = include_str!("../../../playground/schedules.html");
const QUALITY_HTML: &str = include_str!("../../../playground/quality.html");
const LINEAGE_HTML: &str = include_str!("../../../playground/lineage.html");
const REPLAY_HTML: &str = include_str!("../../../playground/replay.html");
const COST_HTML: &str = include_str!("../../../playground/cost.html");
const GRAPHQL_HTML: &str = include_str!("../../../playground/graphql.html");

async fn playground() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(PLAYGROUND_HTML))
}

async fn dashboard() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(DASHBOARD_HTML))
}

async fn explore() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(EXPLORE_HTML))
}

async fn settings() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(SETTINGS_HTML))
}

async fn status() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(STATUS_HTML))
}

async fn help() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(HELP_HTML))
}

async fn admin() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(ADMIN_HTML))
}

async fn changelog() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(CHANGELOG_HTML))
}

async fn feedback_widget() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(FEEDBACK_WIDGET_HTML))
}

async fn views_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(VIEWS_HTML))
}

async fn plugins_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(PLUGINS_HTML))
}

async fn alerts_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(ALERTS_HTML))
}

async fn terminal_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(TERMINAL_HTML))
}

async fn federation_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(FEDERATION_HTML))
}

async fn schedules_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(SCHEDULES_HTML))
}

async fn quality_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(QUALITY_HTML))
}

async fn lineage_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(LINEAGE_HTML))
}

async fn replay_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(REPLAY_HTML))
}

async fn cost_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(COST_HTML))
}

async fn graphql_page() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(GRAPHQL_HTML))
}

/// Build the Fuse API router with the given shared state.
/// Build the Fuse API router with the given shared state and default rate limits.
pub fn build_router(state: Arc<AppState>) -> Router {
    build_router_with_limits(state, rate_limit::RateLimitState::default())
}

/// Build the Fuse API router with custom rate limits (useful for testing).
/// Build alert rules CRUD routes with AlertMonitor state.
fn build_alert_routes(_state: Arc<api::AppState>) -> Router<Arc<api::AppState>> {
    let monitor = Arc::new(alert_monitor::AlertMonitor::new());
    Router::new()
        .route("/", get(alert_api::list_rules).post(alert_api::create_rule))
        .route("/{id}", axum::routing::delete(alert_api::delete_rule))
        .route("/{id}/acknowledge", post(alert_api::acknowledge_alert))
        .route("/active", get(alert_api::list_active_alerts))
        .route("/history", get(alert_api::list_alert_history))
        .with_state(monitor)
}

fn build_otel_routes(state: Arc<api::AppState>) -> Router<Arc<api::AppState>> {
    match &state.otel_store {
        Some(store) => {
            let otel_state = otel_ingest::OtelIngestState { store: store.clone() };
            Router::new()
                .route("/traces", post(otel_ingest::ingest_traces))
                .route("/metrics", post(otel_ingest::ingest_metrics))
                .route("/logs", post(otel_ingest::ingest_logs))
                .route("/health", get(otel_ingest::otel_health))
                .with_state(otel_state)
        }
        None => Router::new(),
    }
}

pub fn build_router_with_limits(state: Arc<AppState>, rl: rate_limit::RateLimitState) -> Router {
    Router::new()
        .route("/", get(playground))
        .route("/playground", get(playground))
        .route("/dashboard", get(dashboard))
        .route("/explore", get(explore))
        .route("/settings", get(settings))
        .route("/status", get(status))
        .route("/help", get(help))
        .route("/admin", get(admin))
        .route("/changelog", get(changelog))
        .route("/feedback-widget", get(feedback_widget))
        .route("/views", get(views_page))
        .route("/plugins", get(plugins_page))
        .route("/alerts", get(alerts_page))
        .route("/terminal", get(terminal_page))
        .route("/federation", get(federation_page))
        .route("/schedules", get(schedules_page))
        .route("/quality", get(quality_page))
        .route("/lineage", get(lineage_page))
        .route("/replay", get(replay_page))
        .route("/cost", get(cost_page))
        .route("/graphql", get(graphql_page))
        .route("/api/fuse/query", post(api::query_handler))
        .route("/api/fuse/query/stream", post(streaming::stream_handler))
        .route("/api/fuse/datasources", get(api::list_datasources))
        .route(
            "/api/fuse/datasources/{id}/schemas",
            get(api::get_schemas),
        )
        .route(
            "/api/fuse/datasources/{id}/schemas/{table}/fields",
            get(api::get_fields),
        )
        .route("/api/fuse/query/explain", post(api::explain_handler))
        .route("/api/fuse/query/validate", post(api::validate_handler))
        .route("/api/fuse/health", get(api::health_handler))
        .route("/api/fuse/info", get(api::info_handler))
        .route("/api/fuse/history", get(api::history_handler))
        .route("/api/fuse/stats", get(api::stats_handler))
        .route("/api/fuse/audit", get(api::audit_handler))
        .route("/api/fuse/cache", axum::routing::delete(api::clear_cache_handler))
        .route("/api/fuse/saved", get(api::list_saved_queries).post(api::save_query))
        .route("/api/fuse/saved/{name}", get(api::get_saved_query).delete(api::delete_saved_query))
        .route("/api/fuse/queries/running", get(api::list_running_queries))
        .route("/api/fuse/query/{id}/cancel", axum::routing::delete(api::cancel_query))
        .route("/api/fuse/alerts", get(api::list_alerts))
        .route("/api/fuse/alerts/evaluate", post(api::evaluate_alerts))
        .route("/api/fuse/views", get(api::list_views).post(api::create_view))
        .route("/api/fuse/multi", post(api::multi_query_handler))
        .route("/api/fuse/nl", post(api::nl_to_sql_handler))
        .route("/api/fuse/advisor", get(api::query_advisor_handler))
        .route("/api/fuse/views/{name}", get(api::get_view).delete(api::delete_view))
        .route("/api/fuse/views/{name}/refresh", post(api::refresh_view))
        .route("/api/fuse/trace/{trace_id}", get(api::trace_handler))
        .route("/api/fuse/federation", get(api::federation_handler))
        .route("/api/fuse/relationships", get(schema_discovery::relationships_handler))
        .route("/api/fuse/predict", get(query_predictor::predict_handler))
        .route("/api/fuse/lineage", post(api::lineage_handler))
        .route("/api/fuse/replay/recordings", get(api::list_recordings).delete(api::clear_recordings))
        .route("/api/fuse/replay/record", post(api::record_query))
        .route("/api/fuse/graphql", get(graphql::graphiql_handler).post(graphql::graphql_handler))
        .route("/metrics", get(metrics::metrics_handler))
        // OTLP ingestion routes — active when otel connector is configured
        .nest("/v1", build_otel_routes(state.clone()))
        // Alert rules CRUD — nested with AlertMonitor state
        .nest("/api/fuse/alert-rules", build_alert_routes(state.clone()))
        // Webhook subscriptions — event-driven query monitoring
        .nest("/api/fuse/webhooks", webhook::webhook_routes())
        // CDC — change data capture for materialized view refresh
        .nest("/api/fuse/cdc", cdc::cdc_routes())
        .layer(middleware::from_fn(rate_limit::rate_limit_middleware))
        .layer(axum::Extension(rl))
        .layer(middleware::from_fn(auth::auth_middleware))
        .layer(axum::Extension(auth::AuthState::default()))
        .layer(TraceLayer::new_for_http()
            .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(Level::INFO))
            .on_response(tower_http::trace::DefaultOnResponse::new().level(Level::INFO)))
        .layer(middleware::from_fn(security_headers::security_headers_middleware))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024)) // 10MB request body limit
        .with_state(state)
}
