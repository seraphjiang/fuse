// SPDX-License-Identifier: Apache-2.0
//! Plan execution statistics — track per-node metrics for EXPLAIN ANALYZE.

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Default, Serialize)]
pub struct NodeStats {
    pub rows_in: u64,
    pub rows_out: u64,
    pub duration_ms: u64,
    pub bytes_processed: u64,
}

pub struct PlanStats {
    nodes: Mutex<HashMap<String, NodeStats>>,
}

impl Default for PlanStats {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanStats {
    pub fn new() -> Self {
        Self { nodes: Mutex::new(HashMap::new()) }
    }

    pub fn record(&self, node_id: &str, rows_in: u64, rows_out: u64, duration_ms: u64, bytes: u64) {
        self.nodes.lock().unwrap().insert(node_id.to_string(), NodeStats {
            rows_in, rows_out, duration_ms, bytes_processed: bytes,
        });
    }

    pub fn get(&self, node_id: &str) -> Option<NodeStats> {
        self.nodes.lock().unwrap().get(node_id).cloned()
    }

    pub fn total_duration(&self) -> u64 {
        self.nodes.lock().unwrap().values().map(|s| s.duration_ms).sum()
    }

    pub fn total_rows_out(&self) -> u64 {
        self.nodes.lock().unwrap().values().map(|s| s.rows_out).max().unwrap_or(0)
    }

    pub fn all(&self) -> HashMap<String, NodeStats> {
        self.nodes.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_get() {
        let s = PlanStats::new();
        s.record("scan_1", 0, 1000, 50, 8000);
        let n = s.get("scan_1").unwrap();
        assert_eq!(n.rows_out, 1000);
        assert_eq!(n.duration_ms, 50);
    }

    #[test]
    fn test_total_duration() {
        let s = PlanStats::new();
        s.record("a", 0, 100, 30, 0);
        s.record("b", 100, 50, 20, 0);
        assert_eq!(s.total_duration(), 50);
    }

    #[test]
    fn test_empty() {
        let s = PlanStats::new();
        assert_eq!(s.total_duration(), 0);
        assert_eq!(s.total_rows_out(), 0);
    }

    #[test]
    fn test_all() {
        let s = PlanStats::new();
        s.record("x", 0, 10, 5, 100);
        assert_eq!(s.all().len(), 1);
    }
}
