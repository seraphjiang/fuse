// SPDX-License-Identifier: Apache-2.0
//! Datasource dependency graph — track co-usage patterns.

use std::collections::HashMap;
use std::sync::Mutex;

/// Tracks which datasources are queried together.
pub struct DependencyGraph {
    /// (ds_a, ds_b) -> co-occurrence count (sorted pair)
    edges: Mutex<HashMap<(String, String), u64>>,
}

impl DependencyGraph {
    pub fn new() -> Self {
        Self { edges: Mutex::new(HashMap::new()) }
    }

    /// Record that these datasources were queried together.
    pub fn record(&self, datasources: &[String]) {
        let mut sorted: Vec<&String> = datasources.iter().collect();
        sorted.sort();
        sorted.dedup();
        let mut edges = self.edges.lock().unwrap();
        for i in 0..sorted.len() {
            for j in (i + 1)..sorted.len() {
                let key = (sorted[i].clone(), sorted[j].clone());
                *edges.entry(key).or_insert(0) += 1;
            }
        }
    }

    /// Get the most common datasource pairs.
    pub fn top_pairs(&self, limit: usize) -> Vec<((String, String), u64)> {
        let edges = self.edges.lock().unwrap();
        let mut pairs: Vec<_> = edges.iter().map(|(k, v)| (k.clone(), *v)).collect();
        pairs.sort_by(|a, b| b.1.cmp(&a.1));
        pairs.truncate(limit);
        pairs
    }

    /// Get datasources commonly paired with a given one.
    pub fn neighbors(&self, ds: &str) -> Vec<(String, u64)> {
        let edges = self.edges.lock().unwrap();
        let mut result: Vec<(String, u64)> = edges.iter()
            .filter_map(|((a, b), count)| {
                if a == ds { Some((b.clone(), *count)) }
                else if b == ds { Some((a.clone(), *count)) }
                else { None }
            })
            .collect();
        result.sort_by(|a, b| b.1.cmp(&a.1));
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_pair() {
        let g = DependencyGraph::new();
        g.record(&["pg".into(), "es".into()]);
        g.record(&["pg".into(), "es".into()]);
        let pairs = g.top_pairs(10);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1, 2);
    }

    #[test]
    fn test_single_datasource_no_edges() {
        let g = DependencyGraph::new();
        g.record(&["pg".into()]);
        assert!(g.top_pairs(10).is_empty());
    }

    #[test]
    fn test_neighbors() {
        let g = DependencyGraph::new();
        g.record(&["pg".into(), "es".into()]);
        g.record(&["pg".into(), "ddb".into()]);
        let n = g.neighbors("pg");
        assert_eq!(n.len(), 2);
    }

    #[test]
    fn test_three_datasources() {
        let g = DependencyGraph::new();
        g.record(&["a".into(), "b".into(), "c".into()]);
        // 3 pairs: (a,b), (a,c), (b,c)
        assert_eq!(g.top_pairs(10).len(), 3);
    }
}
