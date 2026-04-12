// SPDX-License-Identifier: Apache-2.0
//! Usage Metering for Multi-tenant SaaS Mode (#1841)
//!
//! Track per-tenant query usage: count, rows scanned, bytes processed,
//! compute time. Exposes usage reports for billing integration.

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Default, Serialize)]
pub struct TenantUsage {
    pub tenant_id: String,
    pub query_count: u64,
    pub total_rows: u64,
    pub total_bytes: u64,
    pub total_duration_ms: u64,
    pub datasource_breakdown: HashMap<String, DatasourceUsage>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DatasourceUsage {
    pub query_count: u64,
    pub rows: u64,
    pub bytes: u64,
}

pub struct UsageMeter {
    usage: Mutex<HashMap<String, TenantUsage>>,
}

impl Default for UsageMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl UsageMeter {
    pub fn new() -> Self {
        Self {
            usage: Mutex::new(HashMap::new()),
        }
    }

    /// Record a query execution for a tenant.
    pub fn record(
        &self,
        tenant_id: &str,
        datasources: &[String],
        rows: u64,
        bytes: u64,
        duration_ms: u64,
    ) {
        let mut usage = self.usage.lock().unwrap();
        let entry = usage
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantUsage {
                tenant_id: tenant_id.to_string(),
                ..Default::default()
            });
        entry.query_count += 1;
        entry.total_rows += rows;
        entry.total_bytes += bytes;
        entry.total_duration_ms += duration_ms;

        let per_ds_rows = if datasources.is_empty() {
            0
        } else {
            rows / datasources.len() as u64
        };
        let per_ds_bytes = if datasources.is_empty() {
            0
        } else {
            bytes / datasources.len() as u64
        };
        for ds in datasources {
            let ds_usage = entry.datasource_breakdown.entry(ds.clone()).or_default();
            ds_usage.query_count += 1;
            ds_usage.rows += per_ds_rows;
            ds_usage.bytes += per_ds_bytes;
        }
    }

    /// Get usage for a specific tenant.
    pub fn get(&self, tenant_id: &str) -> Option<TenantUsage> {
        self.usage.lock().unwrap().get(tenant_id).cloned()
    }

    /// Get usage for all tenants.
    pub fn all(&self) -> Vec<TenantUsage> {
        self.usage.lock().unwrap().values().cloned().collect()
    }

    /// Reset usage for a tenant (e.g., after billing cycle).
    pub fn reset_tenant(&self, tenant_id: &str) {
        self.usage.lock().unwrap().remove(tenant_id);
    }

    /// Reset all usage.
    pub fn reset_all(&self) {
        self.usage.lock().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_get() {
        let m = UsageMeter::new();
        m.record("t1", &["ds1".into()], 100, 1024, 50);
        let u = m.get("t1").unwrap();
        assert_eq!(u.query_count, 1);
        assert_eq!(u.total_rows, 100);
        assert_eq!(u.total_bytes, 1024);
        assert_eq!(u.total_duration_ms, 50);
    }

    #[test]
    fn test_accumulates() {
        let m = UsageMeter::new();
        m.record("t1", &["ds1".into()], 100, 1000, 10);
        m.record("t1", &["ds1".into()], 200, 2000, 20);
        let u = m.get("t1").unwrap();
        assert_eq!(u.query_count, 2);
        assert_eq!(u.total_rows, 300);
        assert_eq!(u.total_bytes, 3000);
    }

    #[test]
    fn test_datasource_breakdown() {
        let m = UsageMeter::new();
        m.record("t1", &["ds1".into(), "ds2".into()], 100, 1000, 10);
        let u = m.get("t1").unwrap();
        assert_eq!(u.datasource_breakdown.len(), 2);
        assert_eq!(u.datasource_breakdown["ds1"].query_count, 1);
    }

    #[test]
    fn test_multiple_tenants() {
        let m = UsageMeter::new();
        m.record("t1", &["ds1".into()], 10, 100, 5);
        m.record("t2", &["ds1".into()], 20, 200, 10);
        assert_eq!(m.all().len(), 2);
    }

    #[test]
    fn test_unknown_tenant() {
        let m = UsageMeter::new();
        assert!(m.get("unknown").is_none());
    }

    #[test]
    fn test_reset_tenant() {
        let m = UsageMeter::new();
        m.record("t1", &["ds1".into()], 10, 100, 5);
        m.reset_tenant("t1");
        assert!(m.get("t1").is_none());
    }

    #[test]
    fn test_reset_all() {
        let m = UsageMeter::new();
        m.record("t1", &["ds1".into()], 10, 100, 5);
        m.record("t2", &["ds1".into()], 20, 200, 10);
        m.reset_all();
        assert!(m.all().is_empty());
    }

    #[test]
    fn test_empty_datasources() {
        let m = UsageMeter::new();
        m.record("t1", &[], 100, 1000, 10);
        let u = m.get("t1").unwrap();
        assert_eq!(u.query_count, 1);
        assert!(u.datasource_breakdown.is_empty());
    }
}
