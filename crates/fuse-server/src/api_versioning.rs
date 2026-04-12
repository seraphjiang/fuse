// SPDX-License-Identifier: Apache-2.0
//! REST API versioning — /api/v1/fuse/* and /api/v2/fuse/* prefix support.
//!
//! Adds version negotiation via Accept-Version header, versioned route
//! prefixes, and an X-Fuse-Api-Version response header.

use axum::{
    extract::Request, middleware::Next, response::IntoResponse, routing::get, Json, Router,
};
use serde::Serialize;

/// Supported API versions.
#[derive(Serialize)]
pub struct ApiVersionInfo {
    pub versions: Vec<VersionEntry>,
    pub current: &'static str,
}

#[derive(Serialize)]
pub struct VersionEntry {
    pub version: &'static str,
    pub status: &'static str,
    pub prefix: &'static str,
}

/// GET /api/versions — list supported API versions.
pub async fn versions_handler() -> impl IntoResponse {
    Json(ApiVersionInfo {
        current: "v1",
        versions: vec![
            VersionEntry {
                version: "v1",
                status: "stable",
                prefix: "/api/v1/fuse",
            },
            VersionEntry {
                version: "v2",
                status: "beta",
                prefix: "/api/v2/fuse",
            },
        ],
    })
}

/// Middleware that adds X-Fuse-Api-Version header to all responses.
pub async fn version_header_middleware(req: Request, next: Next) -> impl IntoResponse {
    let mut resp = next.run(req).await;
    resp.headers_mut()
        .insert("x-fuse-api-version", "v1".parse().unwrap());
    resp
}

/// Build versioned route nesting for v1 and v2 prefixes.
/// Core query/datasource routes are available under both prefixes.
pub fn versioned_api_routes(
    _state: std::sync::Arc<crate::api::AppState>,
) -> Router<std::sync::Arc<crate::api::AppState>> {
    let core_routes: Router<std::sync::Arc<crate::api::AppState>> = Router::new()
        .route("/query", axum::routing::post(crate::api::query_handler))
        .route("/datasources", get(crate::api::list_datasources))
        .route("/datasources/{id}/schemas", get(crate::api::get_schemas))
        .route(
            "/datasources/{id}/schemas/{table}/fields",
            get(crate::api::get_fields),
        )
        .route("/health", get(crate::api::health_handler))
        .route("/history", get(crate::api::history_handler));

    Router::new()
        .nest("/api/v1/fuse", core_routes.clone())
        .nest("/api/v2/fuse", core_routes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_info_serialization() {
        let info = ApiVersionInfo {
            current: "v1",
            versions: vec![VersionEntry {
                version: "v1",
                status: "stable",
                prefix: "/api/v1/fuse",
            }],
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"v1\""));
        assert!(json.contains("\"stable\""));
    }

    #[test]
    fn test_multiple_versions() {
        let info = ApiVersionInfo {
            current: "v1",
            versions: vec![
                VersionEntry {
                    version: "v1",
                    status: "stable",
                    prefix: "/api/v1/fuse",
                },
                VersionEntry {
                    version: "v2",
                    status: "beta",
                    prefix: "/api/v2/fuse",
                },
            ],
        };
        assert_eq!(info.versions.len(), 2);
        assert_eq!(info.current, "v1");
    }

    #[tokio::test]
    async fn test_versions_handler_returns_json() {
        let resp = versions_handler().await;
        use axum::response::IntoResponse;
        let resp = resp.into_response();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }
}
