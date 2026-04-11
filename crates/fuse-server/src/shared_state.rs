// SPDX-License-Identifier: Apache-2.0
//! Redis-backed shared state for stateless horizontal scaling.
//!
//! Each store uses Redis when `FUSE_REDIS_URL` is set, falling back to
//! the existing in-memory implementation otherwise.

use std::sync::Arc;
use tracing::{debug, warn};

use crate::history::{HistoryEntry, QueryHistory};
use crate::audit::{AuditEntry, AuditLog};
use crate::saved_queries::{SavedQuery, SavedQueryRegistry};
use crate::tenant::TenantConfig;

fn redis_client() -> Option<redis::Client> {
    let url = std::env::var("FUSE_REDIS_URL").ok()?;
    match redis::Client::open(url.as_str()) {
        Ok(c) => Some(c),
        Err(e) => {
            warn!("Redis connect failed ({}), falling back to in-memory", e);
            None
        }
    }
}

// ── Shared Saved Query Store ──

#[derive(Clone)]
pub enum SharedSavedQueries {
    Redis { client: redis::Client, key: String },
    InMemory(Arc<SavedQueryRegistry>),
}

impl SharedSavedQueries {
    pub fn from_env() -> Self {
        match redis_client() {
            Some(client) => {
                debug!("Saved queries: Redis");
                Self::Redis { client, key: "fuse:saved_queries".into() }
            }
            None => Self::InMemory(Arc::new(SavedQueryRegistry::new())),
        }
    }

    pub fn is_redis(&self) -> bool { matches!(self, Self::Redis { .. }) }

    pub async fn save(&self, sq: SavedQuery) {
        match self {
            Self::Redis { client, key } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return };
                let Ok(json) = serde_json::to_string(&sq) else { return };
                let _: Result<(), _> = redis::cmd("HSET").arg(key).arg(&sq.name).arg(json).query_async(&mut conn).await;
            }
            Self::InMemory(reg) => reg.save(sq),
        }
    }

    pub async fn get(&self, name: &str) -> Option<SavedQuery> {
        match self {
            Self::Redis { client, key } => {
                let mut conn = client.get_multiplexed_async_connection().await.ok()?;
                let data: Option<String> = redis::cmd("HGET").arg(key).arg(name).query_async(&mut conn).await.ok()?;
                data.and_then(|s| serde_json::from_str(&s).ok())
            }
            Self::InMemory(reg) => reg.get(name),
        }
    }

    pub async fn delete(&self, name: &str) -> bool {
        match self {
            Self::Redis { client, key } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return false };
                let removed: i64 = redis::cmd("HDEL").arg(key).arg(name).query_async(&mut conn).await.unwrap_or(0);
                removed > 0
            }
            Self::InMemory(reg) => reg.delete(name),
        }
    }

    pub async fn list(&self) -> Vec<SavedQuery> {
        match self {
            Self::Redis { client, key } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return vec![] };
                let vals: Vec<String> = redis::cmd("HVALS").arg(key).query_async(&mut conn).await.unwrap_or_default();
                vals.iter().filter_map(|s| serde_json::from_str(s).ok()).collect()
            }
            Self::InMemory(reg) => reg.list(),
        }
    }
}

// ── Shared Query History ──

const HISTORY_KEY: &str = "fuse:history";
const MAX_HISTORY: usize = 50;

#[derive(Clone)]
pub enum SharedQueryHistory {
    Redis { client: redis::Client },
    InMemory(Arc<QueryHistory>),
}

impl SharedQueryHistory {
    pub fn from_env() -> Self {
        match redis_client() {
            Some(client) => {
                debug!("Query history: Redis");
                Self::Redis { client }
            }
            None => Self::InMemory(Arc::new(QueryHistory::new())),
        }
    }

    pub fn is_redis(&self) -> bool { matches!(self, Self::Redis { .. }) }

    pub async fn push(&self, entry: HistoryEntry) {
        match self {
            Self::Redis { client } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return };
                let Ok(json) = serde_json::to_string(&entry) else { return };
                let _: Result<(), _> = redis::cmd("LPUSH").arg(HISTORY_KEY).arg(json).query_async(&mut conn).await;
                let _: Result<(), _> = redis::cmd("LTRIM").arg(HISTORY_KEY).arg(0i64).arg((MAX_HISTORY - 1) as i64).query_async(&mut conn).await;
            }
            Self::InMemory(h) => h.push(entry),
        }
    }

    pub async fn list(&self) -> Vec<HistoryEntry> {
        match self {
            Self::Redis { client } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return vec![] };
                let vals: Vec<String> = redis::cmd("LRANGE").arg(HISTORY_KEY).arg(0i64).arg(-1i64).query_async(&mut conn).await.unwrap_or_default();
                vals.iter().filter_map(|s| serde_json::from_str(s).ok()).collect()
            }
            Self::InMemory(h) => h.list(),
        }
    }

    pub async fn recent(&self, max: usize) -> Vec<HistoryEntry> {
        match self {
            Self::Redis { client } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return vec![] };
                let vals: Vec<String> = redis::cmd("LRANGE").arg(HISTORY_KEY).arg(0i64).arg((max - 1) as i64).query_async(&mut conn).await.unwrap_or_default();
                vals.iter().filter_map(|s| serde_json::from_str(s).ok()).collect()
            }
            Self::InMemory(h) => h.recent(max),
        }
    }
}

// ── Shared Audit Log ──

const AUDIT_KEY: &str = "fuse:audit";
const MAX_AUDIT: usize = 500;

#[derive(Clone)]
pub enum SharedAuditLog {
    Redis { client: redis::Client },
    InMemory(Arc<AuditLog>),
}

impl SharedAuditLog {
    pub fn from_env() -> Self {
        match redis_client() {
            Some(client) => {
                debug!("Audit log: Redis");
                Self::Redis { client }
            }
            None => Self::InMemory(Arc::new(AuditLog::new(MAX_AUDIT))),
        }
    }

    pub fn is_redis(&self) -> bool { matches!(self, Self::Redis { .. }) }

    pub async fn record(&self, entry: AuditEntry) {
        match self {
            Self::Redis { client } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return };
                let Ok(json) = serde_json::to_string(&entry) else { return };
                let _: Result<(), _> = redis::cmd("LPUSH").arg(AUDIT_KEY).arg(json).query_async(&mut conn).await;
                let _: Result<(), _> = redis::cmd("LTRIM").arg(AUDIT_KEY).arg(0i64).arg((MAX_AUDIT - 1) as i64).query_async(&mut conn).await;
            }
            Self::InMemory(log) => log.record(entry).await,
        }
    }

    pub async fn recent(&self, limit: usize) -> Vec<AuditEntry> {
        match self {
            Self::Redis { client } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return vec![] };
                let vals: Vec<String> = redis::cmd("LRANGE").arg(AUDIT_KEY).arg(0i64).arg((limit - 1) as i64).query_async(&mut conn).await.unwrap_or_default();
                vals.iter().filter_map(|s| serde_json::from_str(s).ok()).collect()
            }
            Self::InMemory(log) => log.recent(limit).await,
        }
    }
}

// ── Shared Tenant Registry ──

const TENANT_KEY: &str = "fuse:tenants";

/// Tenant registry — Redis hash or in-memory, with hot-reload support.
#[derive(Clone)]
pub enum SharedTenantRegistry {
    Redis { client: redis::Client },
    InMemory(Arc<std::sync::RwLock<Vec<TenantConfig>>>),
}

impl SharedTenantRegistry {
    pub fn from_env(initial: Vec<TenantConfig>) -> Self {
        match redis_client() {
            Some(client) => {
                debug!("Tenant registry: Redis");
                // Initial configs will be seeded on first reload() call
                Self::Redis { client }
            }
            None => Self::InMemory(Arc::new(std::sync::RwLock::new(initial))),
        }
    }

    pub fn is_redis(&self) -> bool { matches!(self, Self::Redis { .. }) }

    pub async fn list(&self) -> Vec<TenantConfig> {
        match self {
            Self::Redis { client } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return vec![] };
                let vals: Vec<String> = redis::cmd("HVALS").arg(TENANT_KEY).query_async(&mut conn).await.unwrap_or_default();
                vals.iter().filter_map(|s| serde_json::from_str(s).ok()).collect()
            }
            Self::InMemory(configs) => configs.read().unwrap().clone(),
        }
    }

    pub async fn get(&self, tenant_id: &str) -> Option<TenantConfig> {
        match self {
            Self::Redis { client } => {
                let mut conn = client.get_multiplexed_async_connection().await.ok()?;
                let data: Option<String> = redis::cmd("HGET").arg(TENANT_KEY).arg(tenant_id).query_async(&mut conn).await.ok()?;
                data.and_then(|s| serde_json::from_str(&s).ok())
            }
            Self::InMemory(configs) => configs.read().unwrap().iter().find(|c| c.tenant_id == tenant_id).cloned(),
        }
    }

    pub async fn put(&self, config: TenantConfig) {
        match self {
            Self::Redis { client } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return };
                let Ok(json) = serde_json::to_string(&config) else { return };
                let _: Result<(), _> = redis::cmd("HSET").arg(TENANT_KEY).arg(&config.tenant_id).arg(json).query_async(&mut conn).await;
            }
            Self::InMemory(configs) => {
                let mut w = configs.write().unwrap();
                if let Some(existing) = w.iter_mut().find(|c| c.tenant_id == config.tenant_id) {
                    *existing = config;
                } else {
                    w.push(config);
                }
            }
        }
    }

    pub async fn remove(&self, tenant_id: &str) -> bool {
        match self {
            Self::Redis { client } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return false };
                let removed: i64 = redis::cmd("HDEL").arg(TENANT_KEY).arg(tenant_id).query_async(&mut conn).await.unwrap_or(0);
                removed > 0
            }
            Self::InMemory(configs) => {
                let mut w = configs.write().unwrap();
                let before = w.len();
                w.retain(|c| c.tenant_id != tenant_id);
                w.len() < before
            }
        }
    }

    /// Reload from a list of configs (e.g. re-read from config file).
    pub async fn reload(&self, configs: Vec<TenantConfig>) {
        match self {
            Self::Redis { client } => {
                let Ok(mut conn) = client.get_multiplexed_async_connection().await else { return };
                let _: Result<(), _> = redis::cmd("DEL").arg(TENANT_KEY).query_async(&mut conn).await;
                for tc in &configs {
                    let Ok(json) = serde_json::to_string(tc) else { continue };
                    let _: Result<(), _> = redis::cmd("HSET").arg(TENANT_KEY).arg(&tc.tenant_id).arg(json).query_async(&mut conn).await;
                }
            }
            Self::InMemory(store) => {
                *store.write().unwrap() = configs;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sq(name: &str, query: &str) -> SavedQuery {
        SavedQuery { name: name.into(), query: query.into(), format: "sql".into(), description: String::new() }
    }

    fn entry(q: &str, rows: u64) -> HistoryEntry {
        HistoryEntry { query: q.into(), format: "sql".into(), timestamp: 0, latency_ms: 10, row_count: rows, error: None }
    }

    #[test]
    fn test_saved_queries_no_redis() {
        std::env::remove_var("FUSE_REDIS_URL");
        assert!(!SharedSavedQueries::from_env().is_redis());
    }

    #[test]
    fn test_saved_queries_with_redis_url() {
        std::env::set_var("FUSE_REDIS_URL", "redis://127.0.0.1:6379");
        assert!(SharedSavedQueries::from_env().is_redis());
        std::env::remove_var("FUSE_REDIS_URL");
    }

    #[tokio::test]
    async fn test_saved_queries_save_get() {
        std::env::remove_var("FUSE_REDIS_URL");
        let s = SharedSavedQueries::from_env();
        s.save(sq("q1", "SELECT 1")).await;
        assert_eq!(s.get("q1").await.unwrap().query, "SELECT 1");
    }

    #[tokio::test]
    async fn test_saved_queries_get_miss() {
        std::env::remove_var("FUSE_REDIS_URL");
        assert!(SharedSavedQueries::from_env().get("nope").await.is_none());
    }

    #[tokio::test]
    async fn test_saved_queries_delete() {
        std::env::remove_var("FUSE_REDIS_URL");
        let s = SharedSavedQueries::from_env();
        s.save(sq("tmp", "SELECT 1")).await;
        assert!(s.delete("tmp").await);
        assert!(s.get("tmp").await.is_none());
        assert!(!s.delete("tmp").await);
    }

    #[tokio::test]
    async fn test_saved_queries_list() {
        std::env::remove_var("FUSE_REDIS_URL");
        let s = SharedSavedQueries::from_env();
        s.save(sq("a", "SELECT 1")).await;
        s.save(sq("b", "SELECT 2")).await;
        assert_eq!(s.list().await.len(), 2);
    }

    #[tokio::test]
    async fn test_saved_queries_overwrite() {
        std::env::remove_var("FUSE_REDIS_URL");
        let s = SharedSavedQueries::from_env();
        s.save(sq("q", "SELECT 1")).await;
        s.save(sq("q", "SELECT 2")).await;
        assert_eq!(s.get("q").await.unwrap().query, "SELECT 2");
    }

    #[test]
    fn test_history_no_redis() {
        std::env::remove_var("FUSE_REDIS_URL");
        assert!(!SharedQueryHistory::from_env().is_redis());
    }

    #[test]
    fn test_history_with_redis_url() {
        std::env::set_var("FUSE_REDIS_URL", "redis://127.0.0.1:6379");
        assert!(SharedQueryHistory::from_env().is_redis());
        std::env::remove_var("FUSE_REDIS_URL");
    }

    #[tokio::test]
    async fn test_history_push_and_list() {
        std::env::remove_var("FUSE_REDIS_URL");
        let h = SharedQueryHistory::from_env();
        h.push(entry("SELECT 1", 1)).await;
        h.push(entry("SELECT 2", 2)).await;
        assert_eq!(h.list().await.len(), 2);
    }

    #[tokio::test]
    async fn test_history_recent() {
        std::env::remove_var("FUSE_REDIS_URL");
        let h = SharedQueryHistory::from_env();
        h.push(entry("a", 1)).await;
        h.push(entry("b", 2)).await;
        h.push(entry("c", 3)).await;
        assert_eq!(h.recent(2).await.len(), 2);
    }

    #[tokio::test]
    async fn test_history_empty() {
        std::env::remove_var("FUSE_REDIS_URL");
        assert!(SharedQueryHistory::from_env().list().await.is_empty());
    }

    // ── SharedAuditLog ──

    fn audit_entry(identity: &str) -> AuditEntry {
        AuditEntry {
            timestamp: 0, identity: identity.into(),
            action: crate::audit::AuditAction::Query, query: Some("SELECT 1".into()),
            datasources: vec![], duration_ms: 5, row_count: 1,
            status: crate::audit::AuditStatus::Success, error: None, client_ip: None,
        }
    }

    #[test]
    fn test_audit_no_redis() {
        std::env::remove_var("FUSE_REDIS_URL");
        assert!(!SharedAuditLog::from_env().is_redis());
    }

    #[test]
    fn test_audit_with_redis_url() {
        std::env::set_var("FUSE_REDIS_URL", "redis://127.0.0.1:6379");
        assert!(SharedAuditLog::from_env().is_redis());
        std::env::remove_var("FUSE_REDIS_URL");
    }

    #[tokio::test]
    async fn test_audit_record_and_recent() {
        std::env::remove_var("FUSE_REDIS_URL");
        let log = SharedAuditLog::from_env();
        log.record(audit_entry("user1")).await;
        log.record(audit_entry("user2")).await;
        let recent = log.recent(10).await;
        assert_eq!(recent.len(), 2);
    }

    #[tokio::test]
    async fn test_audit_empty() {
        std::env::remove_var("FUSE_REDIS_URL");
        assert!(SharedAuditLog::from_env().recent(10).await.is_empty());
    }

    // ── SharedTenantRegistry ──

    fn tenant(id: &str) -> TenantConfig {
        TenantConfig::with_datasources(id, vec!["ds1".into()])
    }

    #[test]
    fn test_tenant_registry_no_redis() {
        std::env::remove_var("FUSE_REDIS_URL");
        assert!(!SharedTenantRegistry::from_env(vec![]).is_redis());
    }

    #[test]
    fn test_tenant_registry_with_redis_url() {
        std::env::set_var("FUSE_REDIS_URL", "redis://127.0.0.1:6379");
        assert!(SharedTenantRegistry::from_env(vec![]).is_redis());
        std::env::remove_var("FUSE_REDIS_URL");
    }

    #[tokio::test]
    async fn test_tenant_registry_put_and_get() {
        std::env::remove_var("FUSE_REDIS_URL");
        let r = SharedTenantRegistry::from_env(vec![]);
        r.put(tenant("t1")).await;
        assert!(r.get("t1").await.is_some());
        assert!(r.get("t2").await.is_none());
    }

    #[tokio::test]
    async fn test_tenant_registry_list() {
        std::env::remove_var("FUSE_REDIS_URL");
        let r = SharedTenantRegistry::from_env(vec![tenant("a"), tenant("b")]);
        assert_eq!(r.list().await.len(), 2);
    }

    #[tokio::test]
    async fn test_tenant_registry_remove() {
        std::env::remove_var("FUSE_REDIS_URL");
        let r = SharedTenantRegistry::from_env(vec![tenant("x")]);
        assert!(r.remove("x").await);
        assert!(r.list().await.is_empty());
        assert!(!r.remove("x").await);
    }

    #[tokio::test]
    async fn test_tenant_registry_reload() {
        std::env::remove_var("FUSE_REDIS_URL");
        let r = SharedTenantRegistry::from_env(vec![tenant("old")]);
        r.reload(vec![tenant("new1"), tenant("new2")]).await;
        assert_eq!(r.list().await.len(), 2);
        assert!(r.get("old").await.is_none());
        assert!(r.get("new1").await.is_some());
    }
}
