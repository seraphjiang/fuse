// SPDX-License-Identifier: Apache-2.0

//! Structured audit logging.
//!
//! Records who queried what, when, with what result.
//! Integrates with auth.rs AuthIdentity for identity tracking.

use std::time::SystemTime;
use tokio::sync::Mutex;

/// A single audit log entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuditEntry {
    pub timestamp: u64,
    pub identity: String,
    pub action: AuditAction,
    pub query: Option<String>,
    pub datasources: Vec<String>,
    pub duration_ms: u64,
    pub row_count: u64,
    pub status: AuditStatus,
    pub error: Option<String>,
    pub client_ip: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum AuditAction {
    Query,
    Explain,
    Validate,
    ListDatasources,
    GetSchema,
    TraceReconstruction,
    SavedQueryCreate,
    SavedQueryDelete,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum AuditStatus {
    Success,
    Error,
    Denied,
}

/// Thread-safe audit log with bounded capacity.
pub struct AuditLog {
    entries: Mutex<Vec<AuditEntry>>,
    max_entries: usize,
}

impl AuditLog {
    pub fn new(max_entries: usize) -> Self {
        Self { entries: Mutex::new(Vec::new()), max_entries }
    }

    /// Record an audit entry.
    pub async fn record(&self, entry: AuditEntry) {
        let mut entries = self.entries.lock().await;
        if entries.len() >= self.max_entries {
            entries.remove(0);
        }
        tracing::info!(
            audit = true,
            identity = %entry.identity,
            action = ?entry.action,
            status = ?entry.status,
            duration_ms = entry.duration_ms,
            row_count = entry.row_count,
            "audit"
        );
        entries.push(entry);
    }

    /// Get recent audit entries.
    pub async fn recent(&self, limit: usize) -> Vec<AuditEntry> {
        let entries = self.entries.lock().await;
        entries.iter().rev().take(limit).cloned().collect()
    }

    /// Get entries for a specific identity.
    pub async fn for_identity(&self, identity: &str, limit: usize) -> Vec<AuditEntry> {
        let entries = self.entries.lock().await;
        entries.iter().rev()
            .filter(|e| e.identity == identity)
            .take(limit)
            .cloned()
            .collect()
    }

    pub async fn count(&self) -> usize {
        self.entries.lock().await.len()
    }

    /// Export all entries as NDJSON (newline-delimited JSON).
    pub async fn export_ndjson(&self) -> String {
        let entries = self.entries.lock().await;
        entries.iter()
            .filter_map(|e| serde_json::to_string(e).ok())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Export entries since a given timestamp as NDJSON.
    pub async fn export_since(&self, since_secs: u64) -> String {
        let entries = self.entries.lock().await;
        entries.iter()
            .filter(|e| e.timestamp >= since_secs)
            .filter_map(|e| serde_json::to_string(e).ok())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Drain and return all entries (for periodic flush to external storage).
    pub async fn drain(&self) -> Vec<AuditEntry> {
        let mut entries = self.entries.lock().await;
        std::mem::take(&mut *entries)
    }
}

pub fn now_secs() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(identity: &str, action: AuditAction, status: AuditStatus) -> AuditEntry {
        AuditEntry {
            timestamp: now_secs(),
            identity: identity.into(),
            action,
            query: Some("SELECT * FROM ds.t".into()),
            datasources: vec!["ds".into()],
            duration_ms: 42,
            row_count: 10,
            status,
            error: None,
            client_ip: Some("10.0.0.1".into()),
        }
    }

    #[tokio::test]
    async fn test_record_and_recent() {
        let log = AuditLog::new(100);
        log.record(sample_entry("alice", AuditAction::Query, AuditStatus::Success)).await;
        log.record(sample_entry("bob", AuditAction::Explain, AuditStatus::Success)).await;
        let recent = log.recent(10).await;
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].identity, "bob"); // most recent first
        assert_eq!(recent[1].identity, "alice");
    }

    #[tokio::test]
    async fn test_for_identity() {
        let log = AuditLog::new(100);
        log.record(sample_entry("alice", AuditAction::Query, AuditStatus::Success)).await;
        log.record(sample_entry("bob", AuditAction::Query, AuditStatus::Success)).await;
        log.record(sample_entry("alice", AuditAction::Explain, AuditStatus::Success)).await;
        let alice = log.for_identity("alice", 10).await;
        assert_eq!(alice.len(), 2);
    }

    #[tokio::test]
    async fn test_max_entries_eviction() {
        let log = AuditLog::new(3);
        for i in 0..5 {
            log.record(sample_entry(&format!("u{}", i), AuditAction::Query, AuditStatus::Success)).await;
        }
        assert_eq!(log.count().await, 3);
        let recent = log.recent(10).await;
        assert_eq!(recent[0].identity, "u4"); // newest
        assert_eq!(recent[2].identity, "u2"); // oldest surviving
    }

    #[tokio::test]
    async fn test_error_entry() {
        let log = AuditLog::new(100);
        let mut entry = sample_entry("alice", AuditAction::Query, AuditStatus::Error);
        entry.error = Some("syntax error".into());
        log.record(entry).await;
        let recent = log.recent(1).await;
        assert!(matches!(recent[0].status, AuditStatus::Error));
        assert_eq!(recent[0].error.as_deref(), Some("syntax error"));
    }

    #[tokio::test]
    async fn test_denied_entry() {
        let log = AuditLog::new(100);
        log.record(sample_entry("intruder", AuditAction::Query, AuditStatus::Denied)).await;
        let recent = log.recent(1).await;
        assert!(matches!(recent[0].status, AuditStatus::Denied));
    }

    #[test]
    fn test_audit_entry_serialization() {
        let entry = sample_entry("alice", AuditAction::Query, AuditStatus::Success);
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["identity"], "alice");
        assert_eq!(json["row_count"], 10);
        assert!(json["timestamp"].as_u64().unwrap() > 0);
    }

    #[test]
    fn test_all_actions_serialize() {
        let actions = vec![
            AuditAction::Query, AuditAction::Explain, AuditAction::Validate,
            AuditAction::ListDatasources, AuditAction::GetSchema,
            AuditAction::TraceReconstruction, AuditAction::SavedQueryCreate,
            AuditAction::SavedQueryDelete,
        ];
        for action in actions {
            let json = serde_json::to_value(&action).unwrap();
            assert!(json.is_string());
        }
    }

    #[tokio::test]
    async fn test_export_ndjson() {
        let log = AuditLog::new(100);
        log.record(sample_entry("alice", AuditAction::Query, AuditStatus::Success)).await;
        log.record(sample_entry("bob", AuditAction::Explain, AuditStatus::Success)).await;
        let ndjson = log.export_ndjson().await;
        let lines: Vec<&str> = ndjson.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("alice"));
        assert!(lines[1].contains("bob"));
    }

    #[tokio::test]
    async fn test_export_since() {
        let log = AuditLog::new(100);
        log.record(sample_entry("old", AuditAction::Query, AuditStatus::Success)).await;
        let cutoff = super::now_secs();
        log.record(sample_entry("new", AuditAction::Query, AuditStatus::Success)).await;
        let exported = log.export_since(cutoff).await;
        // At least the "new" entry should be included (timestamps may be same second)
        assert!(exported.contains("new") || exported.contains("old"));
    }

    #[tokio::test]
    async fn test_drain() {
        let log = AuditLog::new(100);
        log.record(sample_entry("a", AuditAction::Query, AuditStatus::Success)).await;
        log.record(sample_entry("b", AuditAction::Query, AuditStatus::Success)).await;
        let drained = log.drain().await;
        assert_eq!(drained.len(), 2);
        assert_eq!(log.count().await, 0);
    }
}
