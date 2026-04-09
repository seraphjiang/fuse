// SPDX-License-Identifier: Apache-2.0

pub mod api;
pub mod health;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use api::AppState;

/// Build the Fuse API router with the given shared state.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/fuse/query", post(api::query_handler))
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
        .with_state(state)
}
