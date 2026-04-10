// SPDX-License-Identifier: Apache-2.0

//! API key authentication middleware.
//!
//! Checks `x-api-key` header or `Authorization: Bearer <key>`.
//! When enabled, rejects unauthenticated requests with 401.
//! Health and metrics endpoints are always public.
//!
//! Keys are loaded from config. Each key has an identity (name) and
//! optional role for future RBAC.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Response, StatusCode};
use axum::middleware::Next;

/// A registered API key with identity and role.
#[derive(Debug, Clone)]
pub struct ApiKeyEntry {
    pub key: String,
    pub identity: String,
    pub role: Role,
}

/// Role for RBAC. Viewer can read, Editor can write, Admin can manage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Viewer,
    Editor,
    Admin,
}

/// Auth state shared via axum Extension.
#[derive(Clone)]
pub struct AuthState {
    /// Map from key string → entry. Empty = auth disabled.
    keys: Arc<HashMap<String, ApiKeyEntry>>,
}

impl AuthState {
    /// Create with no keys (auth disabled — all requests pass).
    pub fn disabled() -> Self {
        Self { keys: Arc::new(HashMap::new()) }
    }

    /// Create with a set of API keys (auth enabled).
    pub fn new(entries: Vec<ApiKeyEntry>) -> Self {
        let keys = entries.into_iter().map(|e| (e.key.clone(), e)).collect();
        Self { keys: Arc::new(keys) }
    }

    /// Whether auth is enabled (at least one key configured).
    pub fn is_enabled(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Validate a key, returning the entry if valid.
    pub fn validate(&self, key: &str) -> Option<&ApiKeyEntry> {
        self.keys.get(key)
    }
}

impl Default for AuthState {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Paths that bypass authentication.
fn is_public_path(path: &str) -> bool {
    matches!(path, "/api/fuse/health" | "/metrics" | "/" | "/playground")
}

/// Extract API key from request headers.
fn extract_api_key(req: &Request) -> Option<String> {
    // Try x-api-key header first
    if let Some(val) = req.headers().get("x-api-key") {
        return val.to_str().ok().map(|s| s.to_string());
    }
    // Try Authorization: Bearer <key>
    if let Some(val) = req.headers().get("authorization") {
        if let Ok(s) = val.to_str() {
            if let Some(key) = s.strip_prefix("Bearer ") {
                return Some(key.trim().to_string());
            }
        }
    }
    None
}

/// Auth middleware. Insert as layer before route handlers.
pub async fn auth_middleware(
    req: Request,
    next: Next,
) -> Response<Body> {
    let auth_state = req.extensions().get::<AuthState>().cloned();

    let auth = match auth_state {
        Some(a) => a,
        None => return next.run(req).await, // No auth state = disabled
    };

    // Auth disabled = pass through
    if !auth.is_enabled() {
        return next.run(req).await;
    }

    // Public paths bypass auth
    if is_public_path(req.uri().path()) {
        return next.run(req).await;
    }

    // Extract and validate key
    let key = match extract_api_key(&req) {
        Some(k) => k,
        None => return unauthorized("missing API key — set x-api-key header"),
    };

    match auth.validate(&key) {
        Some(entry) => {
            let mut req = req;
            // Attach identity for downstream use (rate limiting per key, audit)
            req.extensions_mut().insert(AuthIdentity {
                identity: entry.identity.clone(),
                role: entry.role,
            });
            next.run(req).await
        }
        None => unauthorized("invalid API key"),
    }
}

/// Identity attached to authenticated requests.
#[derive(Debug, Clone)]
pub struct AuthIdentity {
    pub identity: String,
    pub role: Role,
}

fn unauthorized(msg: &str) -> Response<Body> {
    let body = serde_json::json!({ "error": msg }).to_string();
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("content-type", "application/json")
        .header("www-authenticate", HeaderValue::from_static("Bearer"))
        .body(Body::from(body))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_keys() -> AuthState {
        AuthState::new(vec![
            ApiKeyEntry { key: "key-abc".into(), identity: "alice".into(), role: Role::Admin },
            ApiKeyEntry { key: "key-xyz".into(), identity: "bob".into(), role: Role::Viewer },
        ])
    }

    #[test]
    fn test_disabled_by_default() {
        let auth = AuthState::disabled();
        assert!(!auth.is_enabled());
    }

    #[test]
    fn test_enabled_with_keys() {
        let auth = test_keys();
        assert!(auth.is_enabled());
    }

    #[test]
    fn test_validate_valid_key() {
        let auth = test_keys();
        let entry = auth.validate("key-abc").unwrap();
        assert_eq!(entry.identity, "alice");
        assert_eq!(entry.role, Role::Admin);
    }

    #[test]
    fn test_validate_invalid_key() {
        let auth = test_keys();
        assert!(auth.validate("bad-key").is_none());
    }

    #[test]
    fn test_validate_second_key() {
        let auth = test_keys();
        let entry = auth.validate("key-xyz").unwrap();
        assert_eq!(entry.identity, "bob");
        assert_eq!(entry.role, Role::Viewer);
    }

    #[test]
    fn test_public_paths() {
        assert!(is_public_path("/api/fuse/health"));
        assert!(is_public_path("/metrics"));
        assert!(is_public_path("/"));
        assert!(is_public_path("/playground"));
        assert!(!is_public_path("/api/fuse/query"));
        assert!(!is_public_path("/api/fuse/datasources"));
    }

    #[test]
    fn test_extract_api_key_header() {
        let req = Request::builder()
            .header("x-api-key", "my-key")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_api_key(&req), Some("my-key".into()));
    }

    #[test]
    fn test_extract_bearer_token() {
        let req = Request::builder()
            .header("authorization", "Bearer my-token")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_api_key(&req), Some("my-token".into()));
    }

    #[test]
    fn test_extract_no_key() {
        let req = Request::builder().body(Body::empty()).unwrap();
        assert_eq!(extract_api_key(&req), None);
    }

    #[test]
    fn test_role_equality() {
        assert_eq!(Role::Admin, Role::Admin);
        assert_ne!(Role::Viewer, Role::Editor);
    }

    #[test]
    fn test_auth_identity_clone() {
        let id = AuthIdentity { identity: "test".into(), role: Role::Editor };
        let id2 = id.clone();
        assert_eq!(id2.identity, "test");
    }
}
