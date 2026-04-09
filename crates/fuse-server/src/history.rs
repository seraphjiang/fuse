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
}
