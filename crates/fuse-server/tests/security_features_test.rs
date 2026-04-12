// SPDX-License-Identifier: Apache-2.0

//! Integration tests for security features:
//! - Audit logging endpoint (GET /api/fuse/audit)
//! - API key rotation (POST /api/fuse/keys/rotate)
//! - HMAC request signing (verify_signature / sign_request)
//! - URL validator (SSRF protection)

use fuse_server::audit::{AuditAction, AuditEntry, AuditLog, AuditStatus};
use fuse_server::auth::{KeyRotationManager, ApiKeyEntry, Role};
use fuse_server::request_signing::{sign_request, verify_signature, SigningConfig, SigningKey};
use fuse_server::url_validator::validate_callback_url;

// ── Audit Logging ──

#[tokio::test]
async fn test_audit_log_records_all_actions() {
    let log = AuditLog::new(100);
    let actions = [AuditAction::Query,
        AuditAction::Explain,
        AuditAction::Validate];
    for (i, action) in actions.iter().enumerate() {
        log.record(AuditEntry {
            timestamp: 1000 + i as u64,
            identity: format!("user-{}", i),
            action: action.clone(),
            query: Some("SELECT 1".into()),
            datasources: vec!["ds".into()],
            duration_ms: 10,
            row_count: 1,
            status: AuditStatus::Success,
            error: None,
            client_ip: Some("10.0.0.1".into()),
        }).await;
    }
    assert_eq!(log.count().await, 3);
    let recent = log.recent(10).await;
    assert_eq!(recent.len(), 3);
    // Most recent first
    assert_eq!(recent[0].identity, "user-2");
}

#[tokio::test]
async fn test_audit_log_error_entries_preserved() {
    let log = AuditLog::new(100);
    log.record(AuditEntry {
        timestamp: 1000,
        identity: "alice".into(),
        action: AuditAction::Query,
        query: Some("SELECT bad".into()),
        datasources: vec![],
        duration_ms: 5,
        row_count: 0,
        status: AuditStatus::Error,
        error: Some("parse error".into()),
        client_ip: None,
    }).await;
    let entries = log.recent(1).await;
    assert!(matches!(entries[0].status, AuditStatus::Error));
    assert_eq!(entries[0].error.as_deref(), Some("parse error"));
}

#[tokio::test]
async fn test_audit_log_identity_filter() {
    let log = AuditLog::new(100);
    for name in &["alice", "bob", "alice", "carol", "alice"] {
        log.record(AuditEntry {
            timestamp: 1000,
            identity: name.to_string(),
            action: AuditAction::Query,
            query: None,
            datasources: vec![],
            duration_ms: 1,
            row_count: 0,
            status: AuditStatus::Success,
            error: None,
            client_ip: None,
        }).await;
    }
    let alice = log.for_identity("alice", 10).await;
    assert_eq!(alice.len(), 3);
    let bob = log.for_identity("bob", 10).await;
    assert_eq!(bob.len(), 1);
}

#[tokio::test]
async fn test_audit_ndjson_export() {
    let log = AuditLog::new(100);
    log.record(AuditEntry {
        timestamp: 1000,
        identity: "test".into(),
        action: AuditAction::Query,
        query: Some("SELECT 1".into()),
        datasources: vec!["ds".into()],
        duration_ms: 10,
        row_count: 1,
        status: AuditStatus::Success,
        error: None,
        client_ip: None,
    }).await;
    let ndjson = log.export_ndjson().await;
    let parsed: serde_json::Value = serde_json::from_str(ndjson.lines().next().unwrap()).unwrap();
    assert_eq!(parsed["identity"], "test");
    assert_eq!(parsed["row_count"], 1);
}

// ── API Key Rotation ──

#[test]
fn test_key_rotation_basic_flow() {
    let mgr = KeyRotationManager::new(vec![
        ApiKeyEntry { key: "k-alice".into(), identity: "alice".into(), role: Role::Admin },
    ]);
    // Old key works
    assert!(mgr.validate("k-alice").is_some());
    // Rotate
    let result = mgr.rotate("alice", 3600).unwrap();
    assert!(result.new_key.starts_with("fuse-"));
    // New key works
    assert!(mgr.validate(&result.new_key).is_some());
    // Old key still works (grace period)
    assert!(mgr.validate("k-alice").is_some());
    assert_eq!(mgr.grace_count(), 1);
}

#[test]
fn test_key_rotation_unknown_identity_fails() {
    let mgr = KeyRotationManager::new(vec![
        ApiKeyEntry { key: "k-1".into(), identity: "alice".into(), role: Role::Admin },
    ]);
    assert!(mgr.rotate("nobody", 300).is_none());
}

#[test]
fn test_key_rotation_preserves_role() {
    let mgr = KeyRotationManager::new(vec![
        ApiKeyEntry { key: "k-v".into(), identity: "viewer".into(), role: Role::Viewer },
    ]);
    let result = mgr.rotate("viewer", 300).unwrap();
    let entry = mgr.validate(&result.new_key).unwrap();
    assert_eq!(entry.role, Role::Viewer);
}

#[test]
fn test_key_rotation_multiple_rotations() {
    let mgr = KeyRotationManager::new(vec![
        ApiKeyEntry { key: "k-1".into(), identity: "alice".into(), role: Role::Admin },
    ]);
    let r1 = mgr.rotate("alice", 3600).unwrap();
    let r2 = mgr.rotate("alice", 3600).unwrap();
    // All three keys work during grace
    assert!(mgr.validate("k-1").is_some());
    assert!(mgr.validate(&r1.new_key).is_some());
    assert!(mgr.validate(&r2.new_key).is_some());
    assert_eq!(mgr.grace_count(), 2);
}

// ── HMAC Request Signing ──

#[test]
fn test_hmac_sign_verify_roundtrip() {
    let secret = b"test-secret-key-32bytes-long!!!!";
    let body = b"{\"query\": \"SELECT 1\"}";
    let sig = sign_request(secret, None, body);
    let config = SigningConfig::new(
        vec![SigningKey { key_id: "k1".into(), secret: secret.to_vec() }],
        300,
    );
    assert!(verify_signature(&config, &sig, None, body).is_ok());
}

#[test]
fn test_hmac_with_timestamp_replay_protection() {
    let secret = b"my-secret";
    let now = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs();
    let body = b"test body";
    let sig = sign_request(secret, Some(now), body);
    let config = SigningConfig::new(
        vec![SigningKey { key_id: "k1".into(), secret: secret.to_vec() }],
        300,
    );
    // Current timestamp passes
    assert!(verify_signature(&config, &sig, Some(now), body).is_ok());
    // Old timestamp fails
    let old_sig = sign_request(secret, Some(1000), body);
    assert!(verify_signature(&config, &old_sig, Some(1000), body).is_err());
}

#[test]
fn test_hmac_tampered_body_rejected() {
    let secret = b"secret";
    let sig = sign_request(secret, None, b"original body");
    let config = SigningConfig::new(
        vec![SigningKey { key_id: "k1".into(), secret: secret.to_vec() }],
        300,
    );
    assert!(verify_signature(&config, &sig, None, b"tampered body").is_err());
}

#[test]
fn test_hmac_wrong_key_rejected() {
    let sig = sign_request(b"correct-key", None, b"body");
    let config = SigningConfig::new(
        vec![SigningKey { key_id: "k1".into(), secret: b"wrong-key".to_vec() }],
        300,
    );
    assert!(verify_signature(&config, &sig, None, b"body").is_err());
}

#[test]
fn test_hmac_multi_key_accepts_any() {
    let body = b"payload";
    let sig = sign_request(b"key-b", None, body);
    let config = SigningConfig::new(vec![
        SigningKey { key_id: "a".into(), secret: b"key-a".to_vec() },
        SigningKey { key_id: "b".into(), secret: b"key-b".to_vec() },
    ], 300);
    assert!(verify_signature(&config, &sig, None, body).is_ok());
}

#[test]
fn test_hmac_disabled_passes_anything() {
    let config = SigningConfig::disabled();
    assert!(verify_signature(&config, "garbage", None, b"anything").is_ok());
}

// ── URL Validator (SSRF Protection) ──

#[test]
fn test_ssrf_blocks_localhost() {
    assert!(validate_callback_url("http://localhost/hook").is_err());
    assert!(validate_callback_url("http://127.0.0.1/hook").is_err());
}

#[test]
fn test_ssrf_blocks_private_networks() {
    assert!(validate_callback_url("http://10.0.0.1/hook").is_err());
    assert!(validate_callback_url("http://172.16.0.1/hook").is_err());
    assert!(validate_callback_url("http://192.168.1.1/hook").is_err());
}

#[test]
fn test_ssrf_blocks_cloud_metadata() {
    assert!(validate_callback_url("http://169.254.169.254/latest/meta-data/").is_err());
    assert!(validate_callback_url("http://metadata.google.internal/").is_err());
}

#[test]
fn test_ssrf_blocks_non_http() {
    assert!(validate_callback_url("ftp://example.com/hook").is_err());
    assert!(validate_callback_url("file:///etc/passwd").is_err());
}

#[test]
fn test_ssrf_allows_public_urls() {
    assert!(validate_callback_url("https://hooks.slack.com/services/T/B/x").is_ok());
    assert!(validate_callback_url("https://example.com/webhook").is_ok());
}
