// SPDX-License-Identifier: Apache-2.0
//! Data lineage tracking — record which datasources contributed to each query.

use serde::Serialize;

/// Lineage record for a single query execution.
#[derive(Debug, Clone, Serialize)]
pub struct QueryLineage {
    pub query_id: String,
    pub sources: Vec<LineageSource>,
    pub join_type: Option<String>,
    pub timestamp: u64,
}

/// A datasource that contributed to a query result.
#[derive(Debug, Clone, Serialize)]
pub struct LineageSource {
    pub datasource: String,
    pub table: String,
    pub rows_scanned: Option<u64>,
    pub bytes_read: Option<u64>,
    pub push_down_applied: bool,
}

impl QueryLineage {
    pub fn new(query_id: &str, sources: Vec<(&str, &str)>) -> Self {
        Self {
            query_id: query_id.to_string(),
            sources: sources.into_iter().map(|(ds, tbl)| LineageSource {
                datasource: ds.to_string(),
                table: tbl.to_string(),
                rows_scanned: None,
                bytes_read: None,
                push_down_applied: false,
            }).collect(),
            join_type: None,
            timestamp: crate::audit::now_secs(),
        }
    }

    pub fn with_join(mut self, join_type: &str) -> Self {
        self.join_type = Some(join_type.to_string());
        self
    }

    pub fn datasource_ids(&self) -> Vec<&str> {
        self.sources.iter().map(|s| s.datasource.as_str()).collect()
    }

    pub fn is_cross_source(&self) -> bool {
        let mut ds: Vec<&str> = self.sources.iter().map(|s| s.datasource.as_str()).collect();
        ds.sort();
        ds.dedup();
        ds.len() > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_source() {
        let l = QueryLineage::new("q-1", vec![("cluster_a", "logs")]);
        assert_eq!(l.sources.len(), 1);
        assert!(!l.is_cross_source());
    }

    #[test]
    fn test_cross_source() {
        let l = QueryLineage::new("q-2", vec![("cluster_a", "logs"), ("dynamodb", "users")]);
        assert!(l.is_cross_source());
        assert_eq!(l.datasource_ids(), vec!["cluster_a", "dynamodb"]);
    }

    #[test]
    fn test_with_join() {
        let l = QueryLineage::new("q-3", vec![("a", "t1"), ("b", "t2")])
            .with_join("hash_join");
        assert_eq!(l.join_type.as_deref(), Some("hash_join"));
    }

    #[test]
    fn test_same_source_not_cross() {
        let l = QueryLineage::new("q-4", vec![("ds", "t1"), ("ds", "t2")]);
        assert!(!l.is_cross_source());
    }
}
