// SPDX-License-Identifier: Apache-2.0
//! Rate limiting middleware using governor (token bucket).
//!
//! Two limiters:
//! - Per-IP: 100 req/min (configurable via `rate_limit_per_ip`)
//! - Global:  1000 req/min (configurable via `rate_limit_global`)
//!
//! Returns 429 Too Many Requests with `Retry-After: 60` on violation.

use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Response, StatusCode};
use axum::middleware::Next;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use governor::state::keyed::DefaultKeyedStateStore;

pub type GlobalLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;
pub type PerIpLimiter = RateLimiter<IpAddr, DefaultKeyedStateStore<IpAddr>, DefaultClock>;

/// Rate limiter state shared via axum Extension.
#[derive(Clone)]
pub struct RateLimitState {
    pub global: Arc<GlobalLimiter>,
    pub per_ip: Arc<PerIpLimiter>,
}

impl RateLimitState {
    /// Create with requests-per-minute limits.
    pub fn new(global_rpm: u32, per_ip_rpm: u32) -> Self {
        let global_quota = Quota::per_minute(NonZeroU32::new(global_rpm).unwrap());
        let per_ip_quota = Quota::per_minute(NonZeroU32::new(per_ip_rpm).unwrap());
        Self {
            global: Arc::new(RateLimiter::direct(global_quota)),
            per_ip: Arc::new(RateLimiter::keyed(per_ip_quota)),
        }
    }
}

impl Default for RateLimitState {
    fn default() -> Self {
        Self::new(1000, 100)
    }
}

fn too_many_requests() -> Response<Body> {
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header("Retry-After", HeaderValue::from_static("60"))
        .header("Content-Type", HeaderValue::from_static("application/json"))
        .body(Body::from(r#"{"error":"rate limit exceeded"}"#))
        .unwrap()
}

/// Axum middleware that enforces global + per-IP rate limits.
pub async fn rate_limit_middleware(
    axum::Extension(state): axum::Extension<RateLimitState>,
    req: Request,
    next: Next,
) -> Response<Body> {
    // Global limit
    if state.global.check().is_err() {
        return too_many_requests();
    }

    // Per-IP limit — extract from X-Forwarded-For or connection info
    let ip = extract_ip(&req).unwrap_or(IpAddr::from([0, 0, 0, 0]));
    if state.per_ip.check_key(&ip).is_err() {
        return too_many_requests();
    }

    next.run(req).await
}

fn extract_ip(req: &Request) -> Option<IpAddr> {
    // Try X-Forwarded-For first (behind ALB/proxy)
    if let Some(xff) = req.headers().get("x-forwarded-for") {
        if let Ok(s) = xff.to_str() {
            if let Some(first) = s.split(',').next() {
                if let Ok(ip) = first.trim().parse() {
                    return Some(ip);
                }
            }
        }
    }
    // Fall back to X-Real-IP
    if let Some(xri) = req.headers().get("x-real-ip") {
        if let Ok(s) = xri.to_str() {
            if let Ok(ip) = s.trim().parse() {
                return Some(ip);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits() {
        let s = RateLimitState::default();
        // Should allow first request
        assert!(s.global.check().is_ok());
        assert!(s.per_ip.check_key(&IpAddr::from([127, 0, 0, 1])).is_ok());
    }

    #[test]
    fn test_global_limit_exceeded() {
        // 1 req/min limit
        let s = RateLimitState::new(1, 100);
        assert!(s.global.check().is_ok());
        assert!(s.global.check().is_err());
    }

    #[test]
    fn test_per_ip_limit_exceeded() {
        let s = RateLimitState::new(1000, 1);
        let ip = IpAddr::from([10, 0, 0, 1]);
        assert!(s.per_ip.check_key(&ip).is_ok());
        assert!(s.per_ip.check_key(&ip).is_err());
    }

    #[test]
    fn test_per_ip_different_ips_independent() {
        let s = RateLimitState::new(1000, 1);
        let ip1 = IpAddr::from([10, 0, 0, 1]);
        let ip2 = IpAddr::from([10, 0, 0, 2]);
        assert!(s.per_ip.check_key(&ip1).is_ok());
        assert!(s.per_ip.check_key(&ip1).is_err()); // ip1 exhausted
        assert!(s.per_ip.check_key(&ip2).is_ok()); // ip2 unaffected
    }

    #[tokio::test]
    async fn test_429_after_global_limit() {
        use axum::middleware;
        use axum::routing::get;
        use axum::Router;
        use tower::ServiceExt;

        let state = RateLimitState::new(1, 100); // 1 global req/min
        let app = Router::new()
            .route("/", get(|| async { "ok" }))
            .layer(middleware::from_fn(rate_limit_middleware))
            .layer(axum::Extension(state));

        // First request succeeds
        let r1 = app.clone()
            .oneshot(axum::http::Request::builder().uri("/").body(Body::empty()).unwrap())
            .await.unwrap();
        assert_eq!(r1.status(), StatusCode::OK);

        // Second request hits limit
        let r2 = app
            .oneshot(axum::http::Request::builder().uri("/").body(Body::empty()).unwrap())
            .await.unwrap();
        assert_eq!(r2.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(r2.headers().get("Retry-After").unwrap(), "60");
    }
}
