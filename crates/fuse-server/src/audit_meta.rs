// SPDX-License-Identifier: Apache-2.0
//! Audit enrichment — attach metadata to audit entries.

use serde::Serialize;

/// Enriched audit metadata for a query execution.
#[derive(Debug, Clone, Serialize)]
pub struct AuditMeta {
    pub query_id: String,
    pub datasources: Vec<String>,
    pub row_count: u64,
    pub duration_ms: u64,
    pub cached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complexity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
}

impl AuditMeta {
    pub fn new(query_id: &str, datasources: Vec<String>, row_count: u64, duration_ms: u64) -> Self {
        Self {
            query_id: query_id.to_string(),
            datasources,
            row_count,
            duration_ms,
            cached: false,
            complexity: None,
            fingerprint: None,
        }
    }

    pub fn with_cache(mut self, cached: bool) -> Self {
        self.cached = cached;
        self
    }

    pub fn with_complexity(mut self, level: &str) -> Self {
        self.complexity = Some(level.to_string());
        self
    }

    pub fn with_fingerprint(mut self, fp: &str) -> Self {
        self.fingerprint = Some(fp.to_string());
        self
    }

    pub fn is_cross_source(&self) -> bool {
        self.datasources.len() > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let m = AuditMeta::new("q-1", vec!["pg".into()], 100, 50);
        assert!(!m.is_cross_source());
        assert!(!m.cached);
    }

    #[test]
    fn test_cross_source() {
        let m = AuditMeta::new("q-2", vec!["pg".into(), "es".into()], 200, 100);
        assert!(m.is_cross_source());
    }

    #[test]
    fn test_builder() {
        let m = AuditMeta::new("q-3", vec![], 0, 0)
            .with_cache(true)
            .with_complexity("complex")
            .with_fingerprint("SELECT * FROM ?");
        assert!(m.cached);
        assert_eq!(m.complexity.as_deref(), Some("complex"));
        assert_eq!(m.fingerprint.as_deref(), Some("SELECT * FROM ?"));
    }

    #[test]
    fn test_serialization_skips_none() {
        let m = AuditMeta::new("q-4", vec!["ds".into()], 10, 5);
        let json = serde_json::to_string(&m).unwrap();
        assert!(!json.contains("complexity"));
        assert!(!json.contains("fingerprint"));
    }
}
