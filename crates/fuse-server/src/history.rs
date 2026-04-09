// SPDX-License-Identifier: Apache-2.0
//! In-memory query history — stores the last N queries with stats.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::SystemTime;

use serde::Serialize;

const MAX_HISTORY: usize = 50;

#[derive(Debug, Clone, Serialize)]
pub struct HistoryEntry {
    pub query: String,
    pub format: String,
    /// Unix timestamp (seconds)
    pub timestamp: u64,
    pub latency_ms: u64,
    pub row_count: u64,
    pub error: Option<String>,
}

#[derive(Debug, Default)]
pub struct QueryHistory {
    entries: Mutex<VecDeque<HistoryEntry>>,
}

impl QueryHistory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, entry: HistoryEntry) {
        let mut q = self.entries.lock().unwrap();
        if q.len() >= MAX_HISTORY {
            q.pop_front();
        }
        q.push_back(entry);
    }

    /// Returns entries newest-first.
    pub fn list(&self) -> Vec<HistoryEntry> {
        let q = self.entries.lock().unwrap();
        q.iter().cloned().rev().collect()
    }

    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    /// Compute aggregate stats from history.
    pub fn stats(&self) -> QueryStats {
        let q = self.entries.lock().unwrap();
        let total = q.len() as u64;
        if total == 0 {
            return QueryStats { total_queries: 0, error_count: 0, avg_latency_ms: 0, p95_latency_ms: 0, total_rows_returned: 0 };
        }
        let error_count = q.iter().filter(|e| e.error.is_some()).count() as u64;
        let total_rows_returned: u64 = q.iter().map(|e| e.row_count).sum();
        let avg_latency_ms = q.iter().map(|e| e.latency_ms).sum::<u64>() / total;
        let mut latencies: Vec<u64> = q.iter().map(|e| e.latency_ms).collect();
        latencies.sort_unstable();
        let p95_idx = ((latencies.len() as f64) * 0.95).ceil() as usize;
        let p95_latency_ms = latencies[p95_idx.min(latencies.len() - 1)];
        QueryStats { total_queries: total, error_count, avg_latency_ms, p95_latency_ms, total_rows_returned }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryStats {
    pub total_queries: u64,
    pub error_count: u64,
    pub avg_latency_ms: u64,
    pub p95_latency_ms: u64,
    pub total_rows_returned: u64,
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(q: &str, rows: u64) -> HistoryEntry {
        HistoryEntry {
            query: q.into(),
            format: "sql".into(),
            timestamp: 0,
            latency_ms: 10,
            row_count: rows,
            error: None,
        }
    }

    #[test]
    fn test_push_and_list() {
        let h = QueryHistory::new();
        h.push(entry("SELECT 1", 1));
        h.push(entry("SELECT 2", 2));
        let list = h.list();
        assert_eq!(list.len(), 2);
        // newest first
        assert_eq!(list[0].query, "SELECT 2");
        assert_eq!(list[1].query, "SELECT 1");
    }

    #[test]
    fn test_max_capacity() {
        let h = QueryHistory::new();
        for i in 0..60 {
            h.push(entry(&format!("SELECT {i}"), i as u64));
        }
        assert_eq!(h.len(), 50);
        // oldest (SELECT 0..9) evicted, newest is SELECT 59
        let list = h.list();
        assert_eq!(list[0].query, "SELECT 59");
        assert_eq!(list[49].query, "SELECT 10");
    }

    #[test]
    fn test_empty_list() {
        let h = QueryHistory::new();
        assert!(h.list().is_empty());
    }

    #[test]
    fn test_error_entry() {
        let h = QueryHistory::new();
        h.push(HistoryEntry {
            query: "bad query".into(),
            format: "sql".into(),
            timestamp: 0,
            latency_ms: 5,
            row_count: 0,
            error: Some("parse error".into()),
        });
        let list = h.list();
        assert_eq!(list[0].error.as_deref(), Some("parse error"));
    }

    #[test]
    fn test_stats_empty() {
        let h = QueryHistory::new();
        let s = h.stats();
        assert_eq!(s.total_queries, 0);
        assert_eq!(s.error_count, 0);
        assert_eq!(s.avg_latency_ms, 0);
    }

    #[test]
    fn test_stats_counts() {
        let h = QueryHistory::new();
        h.push(HistoryEntry { query: "q1".into(), format: "sql".into(), timestamp: 0, latency_ms: 10, row_count: 5, error: None });
        h.push(HistoryEntry { query: "q2".into(), format: "sql".into(), timestamp: 0, latency_ms: 30, row_count: 0, error: Some("err".into()) });
        let s = h.stats();
        assert_eq!(s.total_queries, 2);
        assert_eq!(s.error_count, 1);
        assert_eq!(s.avg_latency_ms, 20);
        assert_eq!(s.total_rows_returned, 5);
    }

    #[test]
    fn test_now_secs_is_recent() {
        let t = now_secs();
        // Should be after 2024-01-01 (unix 1704067200)
        assert!(t > 1_704_067_200);
    }
}
