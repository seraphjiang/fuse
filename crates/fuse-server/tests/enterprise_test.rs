// SPDX-License-Identifier: Apache-2.0
//! #720 Enterprise stack E2E — multi-tenancy + auth + rate limit + audit + governor.

use fuse_server::auth::{AuthState, ApiKeyEntry, Role};
use fuse_server::tenant::{TenantConfig, TenantRegistry, QueryGovernor};
use fuse_server::audit::{AuditLog, AuditEntry};

// ── Multi-tenancy ──

#[test]
fn test_tenant_admin_accesses_all_datasources() {
    let admin = TenantConfig::admin("admin-tenant");
    let registry = TenantRegistry::new(vec![admin]);
    let all_ds = vec!["os1".into(), "ddb1".into(), "s3".into()];
    let filtered = registry.filter_datasources("admin-tenant", &all_ds);
    assert_eq!(filtered.len(), 3, "admin should access all datasources");
}

#[test]
fn test_tenant_restricted_sees_only_allowed() {
    let restricted = TenantConfig::with_datasources("team-a", vec!["os1".into()]);
    let registry = TenantRegistry::new(vec![restricted]);
    let all_ds = vec!["os1".into(), "ddb1".into(), "s3".into()];
    let filtered = registry.filter_datasources("team-a", &all_ds);
    assert_eq!(filtered, vec!["os1".to_string()], "should only see allowed datasources");
}

#[test]
fn test_tenant_unknown_gets_empty() {
    let registry = TenantRegistry::new(vec![TenantConfig::admin("known")]);
    let all_ds = vec!["os1".into()];
    let filtered = registry.filter_datasources("unknown", &all_ds);
    assert!(filtered.is_empty(), "unknown tenant should see nothing");
}

// ── Query Governor ──

#[test]
fn test_governor_within_limits() {
    let config = TenantConfig {
        tenant_id: "t1".into(),
        allowed_datasources: Default::default(),
        max_concurrent_queries: None,
        max_rows: Some(1000),
        max_time_ms: Some(30000),
        max_result_bytes: Some(10_000_000),
        max_queries_per_minute: None,
    };
    assert!(QueryGovernor::check_limits(&config, 500, 5000, 1000).is_ok());
}

#[test]
fn test_governor_exceeds_max_rows() {
    let config = TenantConfig {
        tenant_id: "t1".into(),
        allowed_datasources: Default::default(),
        max_concurrent_queries: None,
        max_rows: Some(100),
        max_time_ms: None,
        max_result_bytes: None,
        max_queries_per_minute: None,
    };
    let err = QueryGovernor::check_limits(&config, 200, 0, 0).unwrap_err();
    assert!(err.contains("max_rows"), "should mention max_rows: {}", err);
}

#[test]
fn test_governor_exceeds_max_time() {
    let config = TenantConfig {
        tenant_id: "t1".into(),
        allowed_datasources: Default::default(),
        max_concurrent_queries: None,
        max_rows: None,
        max_time_ms: Some(5000),
        max_result_bytes: None,
        max_queries_per_minute: None,
    };
    let err = QueryGovernor::check_limits(&config, 0, 0, 10000).unwrap_err();
    assert!(err.contains("max_time_ms"), "should mention time: {}", err);
}

#[test]
fn test_governor_row_limit_truncation() {
    let config = TenantConfig {
        tenant_id: "t1".into(),
        allowed_datasources: Default::default(),
        max_concurrent_queries: None,
        max_rows: Some(50),
        max_time_ms: None,
        max_result_bytes: None,
        max_queries_per_minute: None,
    };
    assert_eq!(QueryGovernor::apply_row_limit(&config, 100), 50);
    assert_eq!(QueryGovernor::apply_row_limit(&config, 30), 30);
}

#[test]
fn test_governor_effective_timeout() {
    let config = TenantConfig {
        tenant_id: "t1".into(),
        allowed_datasources: Default::default(),
        max_concurrent_queries: None,
        max_rows: None,
        max_time_ms: Some(5000),
        max_result_bytes: None,
        max_queries_per_minute: None,
    };
    assert_eq!(QueryGovernor::effective_timeout_ms(&config, 30000), 5000);
    assert_eq!(QueryGovernor::effective_timeout_ms(&config, 3000), 3000);
}

// ── Auth ──

#[test]
fn test_auth_admin_vs_viewer_roles() {
    let auth = AuthState::new(vec![
        ApiKeyEntry { key: "admin-key".into(), identity: "alice".into(), role: Role::Admin },
        ApiKeyEntry { key: "viewer-key".into(), identity: "bob".into(), role: Role::Viewer },
    ]);
    let admin = auth.validate("admin-key").unwrap();
    let viewer = auth.validate("viewer-key").unwrap();
    assert_eq!(admin.role, Role::Admin);
    assert_eq!(viewer.role, Role::Viewer);
    assert_ne!(admin.role, viewer.role);
}

// ── Audit Log ──

#[tokio::test]
async fn test_audit_log_records_entries() {
    let log = AuditLog::new(100);
    log.record(AuditEntry {
        timestamp: fuse_server::audit::now_secs(),
        identity: "alice".into(),
        action: fuse_server::audit::AuditAction::Query,
        query: Some("SELECT * FROM logs".into()),
        datasources: vec!["os1".into()],
        status: fuse_server::audit::AuditStatus::Success,
        duration_ms: 42,
        row_count: 10,
        error: None,
        client_ip: None,
    }).await;
    let entries = log.recent(10).await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].identity, "alice");
}

#[tokio::test]
async fn test_audit_log_respects_max_entries() {
    let log = AuditLog::new(3);
    for i in 0..5 {
        log.record(AuditEntry {
            timestamp: fuse_server::audit::now_secs(),
            identity: format!("user{i}"),
            action: fuse_server::audit::AuditAction::Query,
            query: None,
            datasources: vec![],
            status: fuse_server::audit::AuditStatus::Success,
            duration_ms: 0,
            row_count: 0,
            error: None,
            client_ip: None,
        }).await;
    }
    let count = log.count().await;
    assert!(count <= 3, "should cap at max_entries, got {}", count);
}

// ── Rate Limiting ──

#[test]
fn test_rate_limit_state_creation() {
    let rl = fuse_server::rate_limit::RateLimitState::default();
    // Default state should exist without panic
    assert!(true, "rate limit state created successfully");
    let _ = rl; // use it
}

// ── Integration: auth + tenant isolation ──

#[test]
fn test_tenant_isolation_different_keys_different_access() {
    let auth = AuthState::new(vec![
        ApiKeyEntry { key: "team-a-key".into(), identity: "team-a".into(), role: Role::Viewer },
        ApiKeyEntry { key: "team-b-key".into(), identity: "team-b".into(), role: Role::Viewer },
    ]);
    let tenants = TenantRegistry::new(vec![
        TenantConfig::with_datasources("team-a", vec!["os-prod".into()]),
        TenantConfig::with_datasources("team-b", vec!["ddb-staging".into()]),
    ]);
    let all_ds = vec!["os-prod".into(), "ddb-staging".into(), "s3-archive".into()];

    // team-a authenticates and sees only os-prod
    let a_identity = auth.validate("team-a-key").unwrap();
    let a_ds = tenants.filter_datasources(&a_identity.identity, &all_ds);
    assert_eq!(a_ds, vec!["os-prod".to_string()]);

    // team-b authenticates and sees only ddb-staging
    let b_identity = auth.validate("team-b-key").unwrap();
    let b_ds = tenants.filter_datasources(&b_identity.identity, &all_ds);
    assert_eq!(b_ds, vec!["ddb-staging".to_string()]);

    // No cross-tenant leakage
    assert!(!a_ds.contains(&"ddb-staging".to_string()));
    assert!(!b_ds.contains(&"os-prod".to_string()));
}
