// SPDX-License-Identifier: Apache-2.0
//! Query cost tracking for billing and chargeback.

use std::collections::HashMap;
use std::sync::Mutex;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize)]
pub struct CostEntry {
    pub query_count: u64,
    pub total_rows: u64,
    pub total_bytes: u64,
    pub total_duration_ms: u64,
}

pub struct CostTracker {
    /// Per-datasource costs, keyed by (tenant, datasource).
    costs: Mutex<HashMap<(String, String), CostEntry>>,
}

impl Default for CostTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CostTracker {
    pub fn new() -> Self {
        Self { costs: Mutex::new(HashMap::new()) }
    }

    pub fn record(&self, tenant: &str, datasource: &str, rows: u64, bytes: u64, duration_ms: u64) {
        let mut map = self.costs.lock().unwrap();
        let entry = map.entry((tenant.to_string(), datasource.to_string())).or_default();
        entry.query_count += 1;
        entry.total_rows += rows;
        entry.total_bytes += bytes;
        entry.total_duration_ms += duration_ms;
    }

    pub fn for_tenant(&self, tenant: &str) -> HashMap<String, CostEntry> {
        self.costs.lock().unwrap()
            .iter()
            .filter(|((t, _), _)| t == tenant)
            .map(|((_, ds), e)| (ds.clone(), e.clone()))
            .collect()
    }

    /// Get aggregated costs across all tenants.
    /// SECURITY: This exposes cross-tenant data — restrict to admin role in API handler.
    pub fn all(&self) -> HashMap<String, CostEntry> {
        let mut merged: HashMap<String, CostEntry> = HashMap::new();
        for ((_, ds), e) in self.costs.lock().unwrap().iter() {
            let m = merged.entry(ds.clone()).or_default();
            m.query_count += e.query_count;
            m.total_rows += e.total_rows;
            m.total_bytes += e.total_bytes;
            m.total_duration_ms += e.total_duration_ms;
        }
        merged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_query() {
        let t = CostTracker::new();
        t.record("team_a", "pg", 100, 5000, 50);
        t.record("team_a", "pg", 200, 8000, 80);
        let costs = t.for_tenant("team_a");
        assert_eq!(costs["pg"].query_count, 2);
        assert_eq!(costs["pg"].total_rows, 300);
    }

    #[test]
    fn test_multi_tenant() {
        let t = CostTracker::new();
        t.record("a", "pg", 10, 100, 5);
        t.record("b", "pg", 20, 200, 10);
        assert_eq!(t.for_tenant("a")["pg"].total_rows, 10);
        assert_eq!(t.for_tenant("b")["pg"].total_rows, 20);
        assert_eq!(t.all()["pg"].total_rows, 30);
    }

    #[test]
    fn test_empty_tenant() {
        let t = CostTracker::new();
        assert!(t.for_tenant("nobody").is_empty());
    }
}
