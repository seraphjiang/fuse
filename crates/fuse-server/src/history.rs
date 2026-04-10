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

    pub fn recent(&self, max: usize) -> Vec<HistoryEntry> {
        let q = self.entries.lock().unwrap();
        q.iter().rev().take(max).cloned().collect()
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

// ── Query Advisor: learn from history ──

/// Optimization suggestion from query history analysis.
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryAdvice {
    pub category: String,
    pub message: String,
    pub affected_queries: usize,
}

/// Analyze query history and produce optimization suggestions.
pub struct QueryAdvisor;

impl QueryAdvisor {
    /// Analyze history entries and return suggestions.
    pub fn analyze(entries: &[HistoryEntry]) -> Vec<QueryAdvice> {
        let mut advice = Vec::new();
        if entries.is_empty() { return advice; }

        // 1. Identify slow queries (above p95 latency)
        let mut latencies: Vec<u64> = entries.iter().map(|e| e.latency_ms).collect();
        latencies.sort_unstable();
        let p95_idx = (latencies.len() as f64 * 0.95) as usize;
        let p95 = latencies.get(p95_idx.min(latencies.len() - 1)).copied().unwrap_or(0);
        let slow: Vec<&HistoryEntry> = entries.iter().filter(|e| e.latency_ms > p95 && e.error.is_none()).collect();
        if !slow.is_empty() && p95 > 100 {
            let no_limit = slow.iter().filter(|e| {
                let lower = e.query.to_lowercase();
                !lower.contains("limit ")
            }).count();
            if no_limit > 0 {
                advice.push(QueryAdvice {
                    category: "missing_limit".into(),
                    message: format!("{} slow queries (>{}ms) have no LIMIT clause. Add LIMIT to reduce data transfer.", no_limit, p95),
                    affected_queries: no_limit,
                });
            }
        }

        // 2. Identify high-error-rate patterns
        let total = entries.len();
        let errors = entries.iter().filter(|e| e.error.is_some()).count();
        let error_rate = errors as f64 / total as f64;
        if error_rate > 0.1 && errors > 2 {
            advice.push(QueryAdvice {
                category: "high_error_rate".into(),
                message: format!("{:.0}% error rate ({}/{} queries). Check connector health and query syntax.", error_rate * 100.0, errors, total),
                affected_queries: errors,
            });
        }

        // 3. Identify repeated queries (caching opportunity)
        let mut query_counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for e in entries {
            *query_counts.entry(&e.query).or_default() += 1;
        }
        let repeated: usize = query_counts.values().filter(|&&c| c > 3).sum();
        if repeated > 0 {
            let unique_repeated = query_counts.values().filter(|&&c| c > 3).count();
            advice.push(QueryAdvice {
                category: "cache_opportunity".into(),
                message: format!("{} queries repeated >3 times ({} unique patterns). Consider CREATE VIEW or increasing result cache TTL.", repeated, unique_repeated),
                affected_queries: repeated,
            });
        }

        // 4. Identify queries without filters (full table scans)
        let no_filter = entries.iter().filter(|e| {
            let lower = e.query.to_lowercase();
            e.error.is_none() && !lower.contains("where ") && !lower.contains("| where") && e.row_count > 1000
        }).count();
        if no_filter > 0 {
            advice.push(QueryAdvice {
                category: "missing_filter".into(),
                message: format!("{} queries return >1000 rows without WHERE clause. Add filters to reduce scan size.", no_filter),
                affected_queries: no_filter,
            });
        }

        advice
    }
}

#[cfg(test)]
mod advisor_tests {
    use super::*;

    fn entry(query: &str, latency_ms: u64, row_count: u64, error: Option<&str>) -> HistoryEntry {
        HistoryEntry {
            query: query.into(), format: "sql".into(), timestamp: now_secs(),
            latency_ms, row_count, error: error.map(|s| s.into()),
        }
    }

    #[test]
    fn test_missing_limit_advice() {
        let mut entries: Vec<HistoryEntry> = (0..20).map(|_| {
            entry("SELECT * FROM a.t LIMIT 10", 50, 10, None)
        }).collect();
        // Add outliers well above p95
        entries.push(entry("SELECT * FROM a.t", 2000, 10000, None));
        entries.push(entry("SELECT * FROM a.t", 3000, 10000, None));
        let advice = QueryAdvisor::analyze(&entries);
        assert!(advice.iter().any(|a| a.category == "missing_limit"), "advice: {:?}", advice);
    }

    #[test]
    fn test_high_error_rate_advice() {
        let mut entries: Vec<HistoryEntry> = (0..8).map(|_| entry("SELECT 1", 10, 1, None)).collect();
        entries.extend((0..3).map(|_| entry("SELECT bad", 10, 0, Some("parse error"))));
        let advice = QueryAdvisor::analyze(&entries);
        assert!(advice.iter().any(|a| a.category == "high_error_rate"));
    }

    #[test]
    fn test_cache_opportunity_advice() {
        let entries: Vec<HistoryEntry> = (0..5).map(|_| entry("SELECT * FROM a.t WHERE x = 1", 50, 10, None)).collect();
        let advice = QueryAdvisor::analyze(&entries);
        assert!(advice.iter().any(|a| a.category == "cache_opportunity"));
    }

    #[test]
    fn test_no_advice_for_healthy_history() {
        let entries: Vec<HistoryEntry> = (0..5).map(|i| {
            entry(&format!("SELECT * FROM a.t WHERE id = {} LIMIT 10", i), 30, 10, None)
        }).collect();
        let advice = QueryAdvisor::analyze(&entries);
        assert!(advice.is_empty(), "expected no advice, got: {:?}", advice);
    }
}
