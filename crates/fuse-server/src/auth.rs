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
/// Hierarchy: Admin > Editor > Viewer (higher roles inherit lower permissions).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    Viewer = 0,
    Editor = 1,
    Admin = 2,
}

impl Role {
    /// Check if this role has at least the given permission level.
    pub fn has(self, required: Role) -> bool {
        self >= required
    }
}

/// Check if the request has at least the required role.
/// Returns Ok(()) if auth is disabled or role is sufficient, Err(Response) otherwise.
#[allow(clippy::result_large_err)]
pub fn require_role(
    identity: Option<&AuthIdentity>,
    required: Role,
    auth_enabled: bool,
) -> Result<(), Response<Body>> {
    if !auth_enabled {
        return Ok(());
    }
    match identity {
        Some(id) if id.role.has(required) => Ok(()),
        Some(id) => {
            let body = serde_json::json!({
                "error": format!(
                    "insufficient permissions: role {:?} required, you have {:?}",
                    required, id.role
                )
            })
            .to_string();
            Err(Response::builder()
                .status(StatusCode::FORBIDDEN)
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap())
        }
        None => Err(unauthorized("authentication required")),
    }
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
        Self {
            keys: Arc::new(HashMap::new()),
        }
    }

    /// Create with a set of API keys (auth enabled).
    pub fn new(entries: Vec<ApiKeyEntry>) -> Self {
        let keys = entries.into_iter().map(|e| (e.key.clone(), e)).collect();
        Self {
            keys: Arc::new(keys),
        }
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

// ── API Key Rotation ──

/// Manages API key rotation with grace periods.
/// During rotation, both old and new keys are valid until the grace period expires.
pub struct KeyRotationManager {
    state: std::sync::RwLock<RotationState>,
}

struct RotationState {
    /// Active keys (current).
    active: HashMap<String, ApiKeyEntry>,
    /// Keys in grace period (old keys still valid until expiry).
    grace: Vec<GracePeriodKey>,
}

struct GracePeriodKey {
    entry: ApiKeyEntry,
    expires_at: std::time::Instant,
}

/// Result of a key rotation operation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RotationResult {
    pub identity: String,
    pub new_key: String,
    pub grace_period_secs: u64,
    pub old_key_expires_at_unix: u64,
}

impl KeyRotationManager {
    pub fn new(initial_keys: Vec<ApiKeyEntry>) -> Self {
        let active = initial_keys
            .into_iter()
            .map(|e| (e.key.clone(), e))
            .collect();
        Self {
            state: std::sync::RwLock::new(RotationState {
                active,
                grace: Vec::new(),
            }),
        }
    }

    /// Rotate a key for the given identity. Returns the new key.
    /// The old key enters a grace period and remains valid for `grace_secs`.
    pub fn rotate(&self, identity: &str, grace_secs: u64) -> Option<RotationResult> {
        let mut state = self.state.write().ok()?;

        // Find the current key for this identity
        let old_entry = state
            .active
            .values()
            .find(|e| e.identity == identity)?
            .clone();
        let old_key = old_entry.key.clone();

        // Generate new key
        let new_key = generate_api_key();
        let new_entry = ApiKeyEntry {
            key: new_key.clone(),
            identity: old_entry.identity.clone(),
            role: old_entry.role,
        };

        // Move old key to grace period
        state.grace.push(GracePeriodKey {
            entry: old_entry,
            expires_at: std::time::Instant::now() + std::time::Duration::from_secs(grace_secs),
        });

        // Replace active key
        state.active.remove(&old_key);
        state.active.insert(new_key.clone(), new_entry);

        let expires_unix = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + grace_secs;

        Some(RotationResult {
            identity: identity.to_string(),
            new_key,
            grace_period_secs: grace_secs,
            old_key_expires_at_unix: expires_unix,
        })
    }

    /// Validate a key against active keys and grace-period keys.
    /// Expired grace keys are pruned on each call.
    pub fn validate(&self, key: &str) -> Option<ApiKeyEntry> {
        let mut state = self.state.write().ok()?;

        // Prune expired grace keys
        let now = std::time::Instant::now();
        state.grace.retain(|g| g.expires_at > now);

        // Check active keys first
        if let Some(entry) = state.active.get(key) {
            return Some(entry.clone());
        }

        // Check grace period keys
        state
            .grace
            .iter()
            .find(|g| g.entry.key == key)
            .map(|g| g.entry.clone())
    }

    /// List all identities with active keys (no secrets exposed).
    pub fn list_identities(&self) -> Vec<String> {
        let state = self.state.read().unwrap();
        state.active.values().map(|e| e.identity.clone()).collect()
    }

    /// Number of keys currently in grace period.
    pub fn grace_count(&self) -> usize {
        let state = self.state.read().unwrap();
        state.grace.len()
    }
}

/// Generate a random API key (32 hex chars).
fn generate_api_key() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let random_part: u64 = (ts as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    format!("fuse-{:016x}{:016x}", ts as u64, random_part)
}

#[cfg(test)]
mod rotation_tests {
    use super::*;

    fn test_entries() -> Vec<ApiKeyEntry> {
        vec![
            ApiKeyEntry {
                key: "key-alice".into(),
                identity: "alice".into(),
                role: Role::Admin,
            },
            ApiKeyEntry {
                key: "key-bob".into(),
                identity: "bob".into(),
                role: Role::Viewer,
            },
        ]
    }

    #[test]
    fn test_rotate_returns_new_key() {
        let mgr = KeyRotationManager::new(test_entries());
        let result = mgr.rotate("alice", 300).unwrap();
        assert_eq!(result.identity, "alice");
        assert!(result.new_key.starts_with("fuse-"));
        assert_eq!(result.grace_period_secs, 300);
    }

    #[test]
    fn test_old_key_in_grace_period() {
        let mgr = KeyRotationManager::new(test_entries());
        // Old key works before rotation
        assert!(mgr.validate("key-alice").is_some());
        // Rotate
        let result = mgr.rotate("alice", 300).unwrap();
        // New key works
        assert!(mgr.validate(&result.new_key).is_some());
        // Old key still works (grace period)
        assert!(mgr.validate("key-alice").is_some());
    }

    #[test]
    fn test_other_keys_unaffected() {
        let mgr = KeyRotationManager::new(test_entries());
        mgr.rotate("alice", 300);
        assert!(mgr.validate("key-bob").is_some());
    }

    #[test]
    fn test_rotate_unknown_identity() {
        let mgr = KeyRotationManager::new(test_entries());
        assert!(mgr.rotate("unknown", 300).is_none());
    }

    #[test]
    fn test_grace_count() {
        let mgr = KeyRotationManager::new(test_entries());
        assert_eq!(mgr.grace_count(), 0);
        mgr.rotate("alice", 300);
        assert_eq!(mgr.grace_count(), 1);
        mgr.rotate("bob", 300);
        assert_eq!(mgr.grace_count(), 2);
    }

    #[test]
    fn test_list_identities() {
        let mgr = KeyRotationManager::new(test_entries());
        let ids = mgr.list_identities();
        assert!(ids.contains(&"alice".to_string()));
        assert!(ids.contains(&"bob".to_string()));
    }

    #[test]
    fn test_generate_api_key_format() {
        let key = generate_api_key();
        assert!(key.starts_with("fuse-"));
        assert!(key.len() > 20);
    }

    #[test]
    fn test_double_rotation() {
        let mgr = KeyRotationManager::new(test_entries());
        let r1 = mgr.rotate("alice", 300).unwrap();
        let r2 = mgr.rotate("alice", 300).unwrap();
        // Both old keys in grace
        assert!(mgr.validate("key-alice").is_some());
        assert!(mgr.validate(&r1.new_key).is_some());
        // Latest key is active
        assert!(mgr.validate(&r2.new_key).is_some());
        assert_eq!(mgr.grace_count(), 2);
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
pub async fn auth_middleware(req: Request, next: Next) -> Response<Body> {
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
            ApiKeyEntry {
                key: "key-abc".into(),
                identity: "alice".into(),
                role: Role::Admin,
            },
            ApiKeyEntry {
                key: "key-xyz".into(),
                identity: "bob".into(),
                role: Role::Viewer,
            },
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
    fn test_role_hierarchy() {
        assert!(Role::Admin.has(Role::Admin));
        assert!(Role::Admin.has(Role::Editor));
        assert!(Role::Admin.has(Role::Viewer));
        assert!(Role::Editor.has(Role::Editor));
        assert!(Role::Editor.has(Role::Viewer));
        assert!(!Role::Editor.has(Role::Admin));
        assert!(Role::Viewer.has(Role::Viewer));
        assert!(!Role::Viewer.has(Role::Editor));
        assert!(!Role::Viewer.has(Role::Admin));
    }

    #[test]
    fn test_role_ordering() {
        assert!(Role::Admin > Role::Editor);
        assert!(Role::Editor > Role::Viewer);
        assert!(Role::Admin > Role::Viewer);
    }

    #[test]
    fn test_require_role_auth_disabled() {
        assert!(require_role(None, Role::Admin, false).is_ok());
    }

    #[test]
    fn test_require_role_sufficient() {
        let id = AuthIdentity {
            identity: "alice".into(),
            role: Role::Admin,
        };
        assert!(require_role(Some(&id), Role::Editor, true).is_ok());
    }

    #[test]
    fn test_require_role_insufficient() {
        let id = AuthIdentity {
            identity: "bob".into(),
            role: Role::Viewer,
        };
        let result = require_role(Some(&id), Role::Editor, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_require_role_no_identity() {
        let result = require_role(None, Role::Viewer, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_auth_identity_clone() {
        let id = AuthIdentity {
            identity: "test".into(),
            role: Role::Editor,
        };
        let id2 = id.clone();
        assert_eq!(id2.identity, "test");
    }
}
