// SPDX-License-Identifier: Apache-2.0

//! Multi-tenant isolation.
//!
//! Maps API key identities to datasource allowlists. When enabled,
//! tenants can only query datasources they have access to.
//!
//! Integrates with auth.rs AuthIdentity — the tenant_id is the identity string.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Tenant configuration: which datasources a tenant can access.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TenantConfig {
    pub tenant_id: String,
    /// Allowed datasource IDs. Empty = access to all (admin).
    pub allowed_datasources: HashSet<String>,
    /// Max concurrent queries for this tenant.
    pub max_concurrent_queries: Option<usize>,
    /// Max rows returned per query.
    pub max_rows: Option<u64>,
    /// Max query execution time in milliseconds.
    pub max_time_ms: Option<u64>,
    /// Max result size in bytes.
    pub max_result_bytes: Option<u64>,
    /// Max queries per minute (rate limit).
    #[serde(default)]
    pub max_queries_per_minute: Option<u32>,
}

impl TenantConfig {
    /// Admin tenant with access to everything.
    pub fn admin(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            allowed_datasources: HashSet::new(),
            max_concurrent_queries: None,
            max_rows: None,
            max_time_ms: None,
            max_result_bytes: None,
            max_queries_per_minute: None,
        }
    }

    /// Tenant with specific datasource access.
    pub fn with_datasources(tenant_id: impl Into<String>, datasources: Vec<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            allowed_datasources: datasources.into_iter().collect(),
            max_concurrent_queries: None,
            max_rows: None,
            max_time_ms: None,
            max_result_bytes: None,
            max_queries_per_minute: None,
        }
    }

    /// Set resource limits. Builder pattern.
    pub fn with_limits(mut self, max_rows: u64, max_time_ms: u64, max_result_bytes: u64) -> Self {
        self.max_rows = Some(max_rows);
        self.max_time_ms = Some(max_time_ms);
        self.max_result_bytes = Some(max_result_bytes);
        self
    }

    /// Whether this tenant can access a given datasource.
    pub fn can_access(&self, datasource_id: &str) -> bool {
        self.allowed_datasources.is_empty() || self.allowed_datasources.contains(datasource_id)
    }
}

/// Tenant registry shared via axum Extension.
#[derive(Clone)]
pub struct TenantRegistry {
    tenants: Arc<HashMap<String, TenantConfig>>,
}

impl TenantRegistry {
    /// No tenants configured = isolation disabled (all access).
    pub fn disabled() -> Self {
        Self { tenants: Arc::new(HashMap::new()) }
    }

    /// Create with tenant configs.
    pub fn new(configs: Vec<TenantConfig>) -> Self {
        let tenants = configs.into_iter().map(|c| (c.tenant_id.clone(), c)).collect();
        Self { tenants: Arc::new(tenants) }
    }

    pub fn is_enabled(&self) -> bool {
        !self.tenants.is_empty()
    }

    /// Get tenant config by identity. Returns None if not found.
    pub fn get(&self, tenant_id: &str) -> Option<&TenantConfig> {
        self.tenants.get(tenant_id)
    }

    /// Filter a list of datasource IDs to only those the tenant can access.
    pub fn filter_datasources(&self, tenant_id: &str, datasources: &[String]) -> Vec<String> {
        match self.get(tenant_id) {
            Some(config) => datasources.iter()
                .filter(|ds| config.can_access(ds))
                .cloned()
                .collect(),
            None if self.is_enabled() => vec![], // Unknown tenant = no access
            None => datasources.to_vec(), // Isolation disabled = all access
        }
    }
}

impl Default for TenantRegistry {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Query governor: enforces per-tenant resource limits on query results.
pub struct QueryGovernor;

impl QueryGovernor {
    /// Check if a tenant can start a new query (rate limit check).
    pub fn check_rate_limit(
        config: &TenantConfig,
        queries_this_minute: u32,
    ) -> Result<(), String> {
        if let Some(max) = config.max_queries_per_minute {
            if queries_this_minute >= max {
                return Err(format!(
                    "tenant '{}' exceeded rate limit: {} queries/min (max {})",
                    config.tenant_id, queries_this_minute, max
                ));
            }
        }
        Ok(())
    }

    /// Check if result exceeds tenant limits. Returns Err with reason if violated.
    pub fn check_limits(
        config: &TenantConfig,
        row_count: u64,
        result_bytes: u64,
        elapsed_ms: u64,
    ) -> Result<(), String> {
        if let Some(max) = config.max_rows {
            if row_count > max {
                return Err(format!(
                    "tenant '{}' exceeded max_rows limit: {} > {}",
                    config.tenant_id, row_count, max
                ));
            }
        }
        if let Some(max) = config.max_result_bytes {
            if result_bytes > max {
                return Err(format!(
                    "tenant '{}' exceeded max_result_bytes limit: {} > {}",
                    config.tenant_id, result_bytes, max
                ));
            }
        }
        if let Some(max) = config.max_time_ms {
            if elapsed_ms > max {
                return Err(format!(
                    "tenant '{}' exceeded max_time_ms limit: {}ms > {}ms",
                    config.tenant_id, elapsed_ms, max
                ));
            }
        }
        Ok(())
    }

    /// Apply row limit: truncate results to tenant's max_rows.
    pub fn apply_row_limit(config: &TenantConfig, total_rows: u64) -> u64 {
        match config.max_rows {
            Some(max) => total_rows.min(max),
            None => total_rows,
        }
    }

    /// Get effective timeout for tenant (min of request timeout and tenant limit).
    pub fn effective_timeout_ms(config: &TenantConfig, request_timeout_ms: u64) -> u64 {
        match config.max_time_ms {
            Some(max) => request_timeout_ms.min(max),
            None => request_timeout_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_allows_all() {
        let reg = TenantRegistry::disabled();
        assert!(!reg.is_enabled());
        let ds = vec!["a".into(), "b".into()];
        assert_eq!(reg.filter_datasources("anyone", &ds), ds);
    }

    #[test]
    fn test_admin_tenant_sees_all() {
        let reg = TenantRegistry::new(vec![TenantConfig::admin("admin")]);
        assert!(reg.is_enabled());
        let config = reg.get("admin").unwrap();
        assert!(config.can_access("anything"));
        assert!(config.can_access("cluster_a"));
    }

    #[test]
    fn test_restricted_tenant() {
        let reg = TenantRegistry::new(vec![
            TenantConfig::with_datasources("team_a", vec!["cluster_a".into(), "s3_logs".into()]),
        ]);
        let config = reg.get("team_a").unwrap();
        assert!(config.can_access("cluster_a"));
        assert!(config.can_access("s3_logs"));
        assert!(!config.can_access("cluster_b"));
    }

    #[test]
    fn test_filter_datasources() {
        let reg = TenantRegistry::new(vec![
            TenantConfig::with_datasources("team_a", vec!["ds1".into(), "ds2".into()]),
        ]);
        let all = vec!["ds1".into(), "ds2".into(), "ds3".into()];
        let filtered = reg.filter_datasources("team_a", &all);
        assert_eq!(filtered, vec!["ds1".to_string(), "ds2".to_string()]);
    }

    #[test]
    fn test_unknown_tenant_no_access() {
        let reg = TenantRegistry::new(vec![TenantConfig::admin("admin")]);
        let all = vec!["ds1".into()];
        let filtered = reg.filter_datasources("unknown", &all);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_admin_filter_keeps_all() {
        let reg = TenantRegistry::new(vec![TenantConfig::admin("admin")]);
        let all = vec!["ds1".into(), "ds2".into(), "ds3".into()];
        let filtered = reg.filter_datasources("admin", &all);
        assert_eq!(filtered, all);
    }

    #[test]
    fn test_max_concurrent_queries() {
        let config = TenantConfig::with_datasources("t", vec!["ds1".into()])
            .with_limits(1000, 30_000, 10_000_000);
        assert_eq!(config.max_rows, Some(1000));
        assert_eq!(config.max_time_ms, Some(30_000));
        assert_eq!(config.max_result_bytes, Some(10_000_000));
    }

    #[test]
    fn test_governor_within_limits() {
        let config = TenantConfig::with_datasources("t", vec!["ds1".into()])
            .with_limits(1000, 30_000, 10_000_000);
        assert!(QueryGovernor::check_limits(&config, 500, 5_000_000, 15_000).is_ok());
    }

    #[test]
    fn test_governor_exceeds_rows() {
        let config = TenantConfig::with_datasources("t", vec!["ds1".into()])
            .with_limits(100, 30_000, 10_000_000);
        let err = QueryGovernor::check_limits(&config, 200, 0, 0).unwrap_err();
        assert!(err.contains("max_rows"));
    }

    #[test]
    fn test_governor_exceeds_time() {
        let config = TenantConfig::with_datasources("t", vec!["ds1".into()])
            .with_limits(1000, 5_000, 10_000_000);
        let err = QueryGovernor::check_limits(&config, 10, 0, 10_000).unwrap_err();
        assert!(err.contains("max_time_ms"));
    }

    #[test]
    fn test_governor_exceeds_bytes() {
        let config = TenantConfig::with_datasources("t", vec!["ds1".into()])
            .with_limits(1000, 30_000, 1_000);
        let err = QueryGovernor::check_limits(&config, 10, 5_000, 100).unwrap_err();
        assert!(err.contains("max_result_bytes"));
    }

    #[test]
    fn test_governor_no_limits() {
        let config = TenantConfig::admin("admin");
        assert!(QueryGovernor::check_limits(&config, 1_000_000, 1_000_000_000, 60_000).is_ok());
    }

    #[test]
    fn test_effective_timeout() {
        let config = TenantConfig::with_datasources("t", vec![])
            .with_limits(1000, 5_000, 10_000_000);
        assert_eq!(QueryGovernor::effective_timeout_ms(&config, 30_000), 5_000);
        assert_eq!(QueryGovernor::effective_timeout_ms(&config, 3_000), 3_000);
    }

    #[test]
    fn test_apply_row_limit() {
        let config = TenantConfig::with_datasources("t", vec![])
            .with_limits(100, 30_000, 10_000_000);
        assert_eq!(QueryGovernor::apply_row_limit(&config, 500), 100);
        assert_eq!(QueryGovernor::apply_row_limit(&config, 50), 50);
    }

    #[test]
    fn test_multiple_tenants() {
        let reg = TenantRegistry::new(vec![
            TenantConfig::with_datasources("team_a", vec!["ds1".into()]),
            TenantConfig::with_datasources("team_b", vec!["ds2".into()]),
            TenantConfig::admin("ops"),
        ]);
        assert_eq!(reg.filter_datasources("team_a", &["ds1".into(), "ds2".into()]), vec!["ds1".to_string()]);
        assert_eq!(reg.filter_datasources("team_b", &["ds1".into(), "ds2".into()]), vec!["ds2".to_string()]);
        assert_eq!(reg.filter_datasources("ops", &["ds1".into(), "ds2".into()]).len(), 2);
    }

    #[test]
    fn test_rate_limit_within() {
        let mut config = TenantConfig::admin("t1");
        config.max_queries_per_minute = Some(100);
        assert!(QueryGovernor::check_rate_limit(&config, 50).is_ok());
    }

    #[test]
    fn test_rate_limit_exceeded() {
        let mut config = TenantConfig::admin("t1");
        config.max_queries_per_minute = Some(10);
        let err = QueryGovernor::check_rate_limit(&config, 10).unwrap_err();
        assert!(err.contains("rate limit"));
    }

    #[test]
    fn test_rate_limit_none_allows_all() {
        let config = TenantConfig::admin("t1");
        assert!(QueryGovernor::check_rate_limit(&config, 999_999).is_ok());
    }
}
