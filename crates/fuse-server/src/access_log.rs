// SPDX-License-Identifier: Apache-2.0
//! API access log — lightweight HTTP request logging.

use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Mutex;

const MAX_ENTRIES: usize = 1000;

#[derive(Debug, Clone, Serialize)]
pub struct AccessEntry {
    pub method: String,
    pub path: String,
    pub status: u16,
    pub duration_ms: u64,
    pub timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
}

pub struct AccessLog {
    entries: Mutex<VecDeque<AccessEntry>>,
}

impl Default for AccessLog {
    fn default() -> Self {
        Self::new()
    }
}

impl AccessLog {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(VecDeque::new()),
        }
    }

    pub fn record(&self, entry: AccessEntry) {
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= MAX_ENTRIES {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    pub fn recent(&self, limit: usize) -> Vec<AccessEntry> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn count_by_status(&self) -> std::collections::HashMap<u16, usize> {
        let mut counts = std::collections::HashMap::new();
        for e in self.entries.lock().unwrap().iter() {
            *counts.entry(e.status).or_insert(0) += 1;
        }
        counts
    }

    pub fn count(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(status: u16) -> AccessEntry {
        AccessEntry {
            method: "POST".into(),
            path: "/api/fuse/query".into(),
            status,
            duration_ms: 50,
            timestamp: 0,
            client_ip: None,
        }
    }

    #[test]
    fn test_record_and_recent() {
        let log = AccessLog::new();
        log.record(sample(200));
        log.record(sample(400));
        assert_eq!(log.count(), 2);
        assert_eq!(log.recent(1)[0].status, 400);
    }

    #[test]
    fn test_count_by_status() {
        let log = AccessLog::new();
        log.record(sample(200));
        log.record(sample(200));
        log.record(sample(500));
        let counts = log.count_by_status();
        assert_eq!(counts[&200], 2);
        assert_eq!(counts[&500], 1);
    }

    #[test]
    fn test_max_entries() {
        let log = AccessLog::new();
        for _ in 0..1500 {
            log.record(sample(200));
        }
        assert_eq!(log.count(), MAX_ENTRIES);
    }
}
