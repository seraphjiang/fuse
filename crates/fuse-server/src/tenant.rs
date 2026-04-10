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
#[derive(Debug, Clone)]
pub struct TenantConfig {
    pub tenant_id: String,
    /// Allowed datasource IDs. Empty = access to all (admin).
    pub allowed_datasources: HashSet<String>,
    /// Max concurrent queries for this tenant.
    pub max_concurrent_queries: Option<usize>,
}

impl TenantConfig {
    /// Admin tenant with access to everything.
    pub fn admin(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            allowed_datasources: HashSet::new(),
            max_concurrent_queries: None,
        }
    }

    /// Tenant with specific datasource access.
    pub fn with_datasources(tenant_id: impl Into<String>, datasources: Vec<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            allowed_datasources: datasources.into_iter().collect(),
            max_concurrent_queries: None,
        }
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
        let mut config = TenantConfig::with_datasources("t", vec!["ds1".into()]);
        config.max_concurrent_queries = Some(5);
        assert_eq!(config.max_concurrent_queries, Some(5));
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
}
