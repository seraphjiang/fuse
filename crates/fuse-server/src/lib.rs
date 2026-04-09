// SPDX-License-Identifier: Apache-2.0

pub mod api;
pub mod health;
pub mod streaming;

use std::sync::Arc;

use axum::http::header;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Router;
use tower_http::trace::TraceLayer;
use tracing::Level;

use api::AppState;

const PLAYGROUND_HTML: &str = include_str!("../../../playground/index.html");

async fn playground() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(PLAYGROUND_HTML))
}

/// Build the Fuse API router with the given shared state.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(playground))
        .route("/playground", get(playground))
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
        .layer(TraceLayer::new_for_http()
            .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(Level::INFO))
            .on_response(tower_http::trace::DefaultOnResponse::new().level(Level::INFO)))
        .with_state(state)
}
