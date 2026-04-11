// SPDX-License-Identifier: Apache-2.0
//! Query execution context — carries per-query state through the pipeline.

use std::time::Instant;

/// Per-query execution context.
#[derive(Debug, Clone)]
pub struct QueryContext {
    pub query_id: String,
    pub format: String,
    pub tenant_id: Option<String>,
    pub timeout_ms: u64,
    pub started_at: Instant,
    pub datasources: Vec<String>,
    pub cached: bool,
    pub cancelled: bool,
}

impl QueryContext {
    pub fn new(query_id: &str, format: &str, timeout_ms: u64) -> Self {
        Self {
            query_id: query_id.to_string(),
            format: format.to_string(),
            tenant_id: None,
            timeout_ms,
            started_at: Instant::now(),
            datasources: Vec::new(),
            cached: false,
            cancelled: false,
        }
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    pub fn remaining_ms(&self) -> u64 {
        self.timeout_ms.saturating_sub(self.elapsed_ms())
    }

    pub fn is_timed_out(&self) -> bool {
        self.elapsed_ms() >= self.timeout_ms
    }

    pub fn with_tenant(mut self, tenant: &str) -> Self {
        self.tenant_id = Some(tenant.to_string());
        self
    }

    pub fn add_datasource(&mut self, ds: &str) {
        if !self.datasources.contains(&ds.to_string()) {
            self.datasources.push(ds.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_context() {
        let ctx = QueryContext::new("q-1", "sql", 30000);
        assert_eq!(ctx.query_id, "q-1");
        assert!(!ctx.is_timed_out());
        assert!(ctx.remaining_ms() > 0);
    }

    #[test]
    fn test_with_tenant() {
        let ctx = QueryContext::new("q-2", "ppl", 5000).with_tenant("team_a");
        assert_eq!(ctx.tenant_id.as_deref(), Some("team_a"));
    }

    #[test]
    fn test_add_datasource_dedup() {
        let mut ctx = QueryContext::new("q-3", "sql", 1000);
        ctx.add_datasource("pg");
        ctx.add_datasource("pg");
        ctx.add_datasource("es");
        assert_eq!(ctx.datasources.len(), 2);
    }

    #[test]
    fn test_timeout_zero() {
        let ctx = QueryContext::new("q-4", "sql", 0);
        assert!(ctx.is_timed_out());
        assert_eq!(ctx.remaining_ms(), 0);
    }
}
