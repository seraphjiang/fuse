// SPDX-License-Identifier: Apache-2.0

//! Security response headers middleware.
//!
//! Adds standard security headers to every response:
//! - X-Content-Type-Options: nosniff
//! - X-Frame-Options: DENY
//! - X-XSS-Protection: 0 (modern browsers use CSP instead)
//! - Referrer-Policy: strict-origin-when-cross-origin
//! - Permissions-Policy: (empty — deny all)
//! - Content-Security-Policy: default-src 'self' (API responses)

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Response};
use axum::middleware::Next;

pub async fn security_headers_middleware(req: Request, next: Next) -> Response<Body> {
    let mut resp = next.run(req).await;
    let h = resp.headers_mut();
    h.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    h.insert("x-frame-options", HeaderValue::from_static("DENY"));
    h.insert("x-xss-protection", HeaderValue::from_static("0"));
    h.insert(
        "referrer-policy",
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    h.insert("permissions-policy", HeaderValue::from_static(""));
    h.insert(
        "content-security-policy",
        HeaderValue::from_static("default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'"),
    );
    resp
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::middleware;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    fn app() -> Router {
        Router::new()
            .route("/", get(|| async { "ok" }))
            .layer(middleware::from_fn(security_headers_middleware))
    }

    #[tokio::test]
    async fn test_headers_present() {
        let resp = app()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(resp.headers().get("x-frame-options").unwrap(), "DENY");
        assert_eq!(resp.headers().get("x-xss-protection").unwrap(), "0");
        assert_eq!(
            resp.headers().get("referrer-policy").unwrap(),
            "strict-origin-when-cross-origin"
        );
        assert!(resp.headers().contains_key("permissions-policy"));
        assert!(resp.headers().contains_key("content-security-policy"));
    }
}
