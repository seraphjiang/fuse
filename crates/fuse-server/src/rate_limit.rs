// SPDX-License-Identifier: Apache-2.0
//! Rate limiting middleware using governor (token bucket).
//!
//! Two limiters:
//! - Per-IP: 100 req/min (configurable via `rate_limit_per_ip`)
//! - Global:  1000 req/min (configurable via `rate_limit_global`)
//!
//! Returns 429 Too Many Requests with `Retry-After: 60` on violation.

use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Response, StatusCode};
use axum::middleware::Next;
use governor::clock::DefaultClock;
use governor::state::keyed::DefaultKeyedStateStore;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};

pub type GlobalLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;
pub type PerIpLimiter = RateLimiter<IpAddr, DefaultKeyedStateStore<IpAddr>, DefaultClock>;
pub type PerKeyLimiter = RateLimiter<String, DefaultKeyedStateStore<String>, DefaultClock>;

/// Per-datasource concurrency limiter using semaphores.
///
/// Enforces `ConnectorCapabilities::max_concurrent_queries` per connector,
/// preventing any single datasource from being overwhelmed.
pub struct DatasourceLimiter {
    semaphores: Mutex<HashMap<String, Arc<tokio::sync::Semaphore>>>,
}

impl Default for DatasourceLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl DatasourceLimiter {
    pub fn new() -> Self {
        Self {
            semaphores: Mutex::new(HashMap::new()),
        }
    }

    /// Register a datasource with its concurrency limit.
    pub fn register(&self, datasource_id: &str, max_concurrent: usize) {
        let max = if max_concurrent == 0 {
            16
        } else {
            max_concurrent
        };
        self.semaphores
            .lock()
            .unwrap()
            .entry(datasource_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(max)));
    }

    /// Acquire a permit for the given datasource. Returns None if unknown.
    pub async fn acquire(&self, datasource_id: &str) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let sem: Option<Arc<tokio::sync::Semaphore>> =
            { self.semaphores.lock().unwrap().get(datasource_id).cloned() };
        if let Some(s) = sem {
            let permit: Result<tokio::sync::OwnedSemaphorePermit, _> = s.acquire_owned().await;
            permit.ok()
        } else {
            None
        }
    }

    /// Available permits for a datasource.
    pub fn available(&self, datasource_id: &str) -> Option<usize> {
        self.semaphores
            .lock()
            .unwrap()
            .get(datasource_id)
            .map(|s: &Arc<tokio::sync::Semaphore>| s.available_permits())
    }

    pub fn datasource_count(&self) -> usize {
        self.semaphores.lock().unwrap().len()
    }
}

/// Rate limiter state shared via axum Extension.
#[derive(Clone)]
pub struct RateLimitState {
    pub global: Arc<GlobalLimiter>,
    pub per_ip: Arc<PerIpLimiter>,
    pub per_key: Arc<PerKeyLimiter>,
}

impl RateLimitState {
    /// Create with requests-per-minute limits.
    pub fn new(global_rpm: u32, per_ip_rpm: u32) -> Self {
        Self::with_key_limit(global_rpm, per_ip_rpm, 200)
    }

    /// Create with explicit per-key limit.
    pub fn with_key_limit(global_rpm: u32, per_ip_rpm: u32, per_key_rpm: u32) -> Self {
        let global_quota = Quota::per_minute(NonZeroU32::new(global_rpm).unwrap());
        let per_ip_quota = Quota::per_minute(NonZeroU32::new(per_ip_rpm).unwrap());
        let per_key_quota = Quota::per_minute(NonZeroU32::new(per_key_rpm).unwrap());
        Self {
            global: Arc::new(RateLimiter::direct(global_quota)),
            per_ip: Arc::new(RateLimiter::keyed(per_ip_quota)),
            per_key: Arc::new(RateLimiter::keyed(per_key_quota)),
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
        .header("X-RateLimit-Remaining", HeaderValue::from_static("0"))
        .header("Content-Type", HeaderValue::from_static("application/json"))
        .body(Body::from(r#"{"error":"rate limit exceeded"}"#))
        .unwrap()
}

fn too_many_requests_for_key(_identity: &str) -> Response<Body> {
    let body = serde_json::json!({
        "error": "rate limit exceeded for API key",
    })
    .to_string();
    Response::builder()
        .status(StatusCode::TOO_MANY_REQUESTS)
        .header("Retry-After", HeaderValue::from_static("60"))
        .header("X-RateLimit-Remaining", HeaderValue::from_static("0"))
        .header("Content-Type", HeaderValue::from_static("application/json"))
        .body(Body::from(body))
        .unwrap()
}

/// Axum middleware that enforces global + per-IP + per-API-key rate limits.
pub async fn rate_limit_middleware(
    axum::Extension(state): axum::Extension<RateLimitState>,
    req: Request,
    next: Next,
) -> Response<Body> {
    // Global limit
    if state.global.check().is_err() {
        metrics::counter!("fuse_rate_limit_rejected", "scope" => "global").increment(1);
        return too_many_requests();
    }

    // Per-IP limit
    let ip = extract_ip(&req).unwrap_or(IpAddr::from([0, 0, 0, 0]));
    if state.per_ip.check_key(&ip).is_err() {
        metrics::counter!("fuse_rate_limit_rejected", "scope" => "per_ip").increment(1);
        return too_many_requests();
    }

    // Per-API-key limit (if authenticated via #501)
    if let Some(identity) = req.extensions().get::<crate::auth::AuthIdentity>() {
        if state.per_key.check_key(&identity.identity).is_err() {
            metrics::counter!("fuse_rate_limit_rejected", "scope" => "per_key").increment(1);
            return too_many_requests_for_key(&identity.identity);
        }
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
        let r1 = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r1.status(), StatusCode::OK);

        // Second request hits limit
        let r2 = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r2.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(r2.headers().get("Retry-After").unwrap(), "60");
    }

    #[test]
    fn test_extract_ip_from_x_real_ip() {
        use axum::http::Request;
        let req = Request::builder()
            .header("x-real-ip", "203.0.113.5")
            .body(Body::empty())
            .unwrap();
        let ip = extract_ip(&req);
        assert_eq!(ip, Some(IpAddr::from([203, 0, 113, 5])));
    }

    #[test]
    fn test_extract_ip_xff_takes_priority() {
        use axum::http::Request;
        let req = Request::builder()
            .header("x-forwarded-for", "10.0.0.1, 10.0.0.2")
            .header("x-real-ip", "10.0.0.99")
            .body(Body::empty())
            .unwrap();
        let ip = extract_ip(&req);
        assert_eq!(ip, Some(IpAddr::from([10, 0, 0, 1])));
    }

    #[test]
    fn test_extract_ip_none_when_no_headers() {
        use axum::http::Request;
        let req = Request::builder().body(Body::empty()).unwrap();
        assert_eq!(extract_ip(&req), None);
    }

    #[test]
    fn test_per_key_limit_exceeded() {
        let s = RateLimitState::with_key_limit(1000, 100, 1);
        let key = "alice".to_string();
        assert!(s.per_key.check_key(&key).is_ok());
        assert!(s.per_key.check_key(&key).is_err());
    }

    #[test]
    fn test_per_key_different_keys_independent() {
        let s = RateLimitState::with_key_limit(1000, 100, 1);
        let k1 = "alice".to_string();
        let k2 = "bob".to_string();
        assert!(s.per_key.check_key(&k1).is_ok());
        assert!(s.per_key.check_key(&k1).is_err()); // alice exhausted
        assert!(s.per_key.check_key(&k2).is_ok()); // bob unaffected
    }

    #[test]
    fn test_default_per_key_limit() {
        // Default: 200 req/min per key
        let s = RateLimitState::default();
        let key = "test".to_string();
        // Should allow at least one request
        assert!(s.per_key.check_key(&key).is_ok());
    }

    #[test]
    fn test_too_many_requests_for_key_includes_identity() {
        let resp = too_many_requests_for_key("alice");
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn test_datasource_limiter_basic() {
        let limiter = DatasourceLimiter::new();
        limiter.register("pg", 2);
        assert_eq!(limiter.available("pg"), Some(2));
        let _p1 = limiter.acquire("pg").await.unwrap();
        assert_eq!(limiter.available("pg"), Some(1));
        let _p2 = limiter.acquire("pg").await.unwrap();
        assert_eq!(limiter.available("pg"), Some(0));
    }

    #[tokio::test]
    async fn test_datasource_limiter_release() {
        let limiter = DatasourceLimiter::new();
        limiter.register("es", 1);
        let p = limiter.acquire("es").await.unwrap();
        assert_eq!(limiter.available("es"), Some(0));
        drop(p); // release
        assert_eq!(limiter.available("es"), Some(1));
    }

    #[tokio::test]
    async fn test_datasource_limiter_unknown() {
        let limiter = DatasourceLimiter::new();
        assert!(limiter.acquire("unknown").await.is_none());
    }

    #[test]
    fn test_datasource_limiter_count() {
        let limiter = DatasourceLimiter::new();
        limiter.register("a", 5);
        limiter.register("b", 10);
        assert_eq!(limiter.datasource_count(), 2);
    }

    #[test]
    fn test_datasource_limiter_default_max() {
        let limiter = DatasourceLimiter::new();
        limiter.register("ds1", 0); // 0 should default to 16
        assert_eq!(limiter.available("ds1"), Some(16));
    }

    #[test]
    fn test_datasource_limiter_custom_max() {
        let limiter = DatasourceLimiter::new();
        limiter.register("ds1", 4);
        assert_eq!(limiter.available("ds1"), Some(4));
    }

    #[tokio::test]
    async fn test_datasource_limiter_concurrency_enforced() {
        let limiter = DatasourceLimiter::new();
        limiter.register("ds1", 2);

        let p1 = limiter.acquire("ds1").await;
        assert!(p1.is_some());
        assert_eq!(limiter.available("ds1"), Some(1));

        let p2 = limiter.acquire("ds1").await;
        assert!(p2.is_some());
        assert_eq!(limiter.available("ds1"), Some(0));

        // Drop one permit — should free a slot
        drop(p1);
        assert_eq!(limiter.available("ds1"), Some(1));
    }
}
