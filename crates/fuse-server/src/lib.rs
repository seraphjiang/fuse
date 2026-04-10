// SPDX-License-Identifier: Apache-2.0

pub mod api;
pub mod audit;
pub mod auth;
pub mod health;
pub mod history;
pub mod metrics;
pub mod plan_cache;
pub mod rate_limit;
pub mod saved_queries;
pub mod streaming;
pub mod tenant;

use std::sync::Arc;

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

/// Build the Fuse API router with the given shared state.
/// Build the Fuse API router with the given shared state and default rate limits.
pub fn build_router(state: Arc<AppState>) -> Router {
    build_router_with_limits(state, rate_limit::RateLimitState::default())
}

/// Build the Fuse API router with custom rate limits (useful for testing).
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
        .route("/api/fuse/history", get(api::history_handler))
        .route("/api/fuse/stats", get(api::stats_handler))
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
        .route("/metrics", get(metrics::metrics_handler))
        .layer(middleware::from_fn(rate_limit::rate_limit_middleware))
        .layer(axum::Extension(rl))
        .layer(middleware::from_fn(auth::auth_middleware))
        .layer(axum::Extension(auth::AuthState::default()))
        .layer(TraceLayer::new_for_http()
            .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(Level::INFO))
            .on_response(tower_http::trace::DefaultOnResponse::new().level(Level::INFO)))
        .with_state(state)
}
