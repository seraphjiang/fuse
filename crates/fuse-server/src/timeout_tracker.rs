// SPDX-License-Identifier: Apache-2.0
//! Timeout tracker — record timed-out queries for debugging.

use std::collections::VecDeque;
use std::sync::Mutex;
use serde::Serialize;

const MAX_ENTRIES: usize = 100;

#[derive(Debug, Clone, Serialize)]
pub struct TimeoutEntry {
    pub query_id: String,
    pub datasource: String,
    pub timeout_ms: u64,
    pub elapsed_ms: u64,
    pub timestamp: u64,
}

pub struct TimeoutTracker {
    entries: Mutex<VecDeque<TimeoutEntry>>,
}

impl Default for TimeoutTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeoutTracker {
    pub fn new() -> Self {
        Self { entries: Mutex::new(VecDeque::new()) }
    }

    pub fn record(&self, query_id: &str, datasource: &str, timeout_ms: u64, elapsed_ms: u64) {
        let entry = TimeoutEntry {
            query_id: query_id.to_string(),
            datasource: datasource.to_string(),
            timeout_ms,
            elapsed_ms,
            timestamp: crate::audit::now_secs(),
        };
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= MAX_ENTRIES {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    pub fn recent(&self, limit: usize) -> Vec<TimeoutEntry> {
        self.entries.lock().unwrap().iter().rev().take(limit).cloned().collect()
    }

    pub fn count(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn count_for_datasource(&self, datasource: &str) -> usize {
        self.entries.lock().unwrap().iter().filter(|e| e.datasource == datasource).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_recent() {
        let t = TimeoutTracker::new();
        t.record("q-1", "pg", 5000, 5100);
        t.record("q-2", "es", 3000, 3200);
        assert_eq!(t.count(), 2);
        let recent = t.recent(1);
        assert_eq!(recent[0].query_id, "q-2");
    }

    #[test]
    fn test_count_for_datasource() {
        let t = TimeoutTracker::new();
        t.record("q-1", "pg", 5000, 5100);
        t.record("q-2", "pg", 5000, 6000);
        t.record("q-3", "es", 3000, 3200);
        assert_eq!(t.count_for_datasource("pg"), 2);
        assert_eq!(t.count_for_datasource("es"), 1);
    }

    #[test]
    fn test_max_entries() {
        let t = TimeoutTracker::new();
        for i in 0..150 {
            t.record(&format!("q-{}", i), "ds", 1000, 1100);
        }
        assert_eq!(t.count(), MAX_ENTRIES);
    }
}
