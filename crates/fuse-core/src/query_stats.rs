// SPDX-License-Identifier: Apache-2.0
//! Query statistics collector — per-datasource query patterns.

use std::collections::HashMap;
use std::sync::Mutex;
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryStats {
    pub total: u64,
    pub success: u64,
    pub errors: u64,
    pub total_rows: u64,
    pub total_duration_ms: u64,
    pub min_duration_ms: u64,
    pub max_duration_ms: u64,
}

pub struct StatsCollector {
    stats: Mutex<HashMap<String, QueryStats>>,
}

impl StatsCollector {
    pub fn new() -> Self {
        Self { stats: Mutex::new(HashMap::new()) }
    }

    pub fn record(&self, datasource: &str, success: bool, rows: u64, duration_ms: u64) {
        let mut map = self.stats.lock().unwrap();
        let s = map.entry(datasource.to_string()).or_default();
        s.total += 1;
        if success { s.success += 1; } else { s.errors += 1; }
        s.total_rows += rows;
        s.total_duration_ms += duration_ms;
        if s.total == 1 || duration_ms < s.min_duration_ms { s.min_duration_ms = duration_ms; }
        if duration_ms > s.max_duration_ms { s.max_duration_ms = duration_ms; }
    }

    pub fn get(&self, datasource: &str) -> Option<QueryStats> {
        self.stats.lock().unwrap().get(datasource).cloned()
    }

    pub fn all(&self) -> HashMap<String, QueryStats> {
        self.stats.lock().unwrap().clone()
    }

    pub fn avg_duration(&self, datasource: &str) -> Option<u64> {
        let map = self.stats.lock().unwrap();
        map.get(datasource).filter(|s| s.total > 0).map(|s| s.total_duration_ms / s.total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_get() {
        let c = StatsCollector::new();
        c.record("pg", true, 100, 50);
        c.record("pg", true, 200, 80);
        c.record("pg", false, 0, 5000);
        let s = c.get("pg").unwrap();
        assert_eq!(s.total, 3);
        assert_eq!(s.success, 2);
        assert_eq!(s.errors, 1);
        assert_eq!(s.min_duration_ms, 50);
        assert_eq!(s.max_duration_ms, 5000);
    }

    #[test]
    fn test_avg_duration() {
        let c = StatsCollector::new();
        c.record("ds", true, 0, 100);
        c.record("ds", true, 0, 200);
        assert_eq!(c.avg_duration("ds"), Some(150));
    }

    #[test]
    fn test_unknown() {
        let c = StatsCollector::new();
        assert!(c.get("missing").is_none());
        assert!(c.avg_duration("missing").is_none());
    }

    #[test]
    fn test_all() {
        let c = StatsCollector::new();
        c.record("a", true, 10, 5);
        c.record("b", true, 20, 10);
        assert_eq!(c.all().len(), 2);
    }
}
