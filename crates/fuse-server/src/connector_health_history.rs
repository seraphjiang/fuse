// SPDX-License-Identifier: Apache-2.0
//! Connector health history — tracks uptime and latency over time.

use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Instant;

const MAX_ENTRIES: usize = 1000;

#[derive(Debug, Clone, Serialize)]
pub struct HealthRecord {
    pub timestamp_secs: u64,
    pub healthy: bool,
    pub latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectorHealthSummary {
    pub connector_id: String,
    pub total_checks: usize,
    pub healthy_checks: usize,
    pub uptime_pct: f64,
    pub avg_latency_ms: f64,
    pub p99_latency_ms: u64,
    pub recent: Vec<HealthRecord>,
}

pub struct HealthHistory {
    records: Mutex<HashMap<String, VecDeque<HealthRecord>>>,
}

impl HealthHistory {
    pub fn new() -> Self {
        Self { records: Mutex::new(HashMap::new()) }
    }

    pub fn record(&self, connector_id: &str, healthy: bool, latency_ms: u64, message: Option<String>) {
        let mut map = self.records.lock().unwrap();
        let entries = map.entry(connector_id.to_string()).or_insert_with(VecDeque::new);
        entries.push_back(HealthRecord {
            timestamp_secs: crate::history::now_secs(),
            healthy,
            latency_ms,
            message,
        });
        while entries.len() > MAX_ENTRIES {
            entries.pop_front();
        }
    }

    pub fn summary(&self, connector_id: &str, limit: usize) -> Option<ConnectorHealthSummary> {
        let map = self.records.lock().unwrap();
        let entries = map.get(connector_id)?;
        let total = entries.len();
        let healthy = entries.iter().filter(|r| r.healthy).count();
        let avg_latency = if total > 0 {
            entries.iter().map(|r| r.latency_ms).sum::<u64>() as f64 / total as f64
        } else { 0.0 };
        let mut latencies: Vec<u64> = entries.iter().map(|r| r.latency_ms).collect();
        latencies.sort_unstable();
        let p99 = latencies.get((latencies.len() * 99 / 100).min(latencies.len().saturating_sub(1))).copied().unwrap_or(0);
        let recent: Vec<HealthRecord> = entries.iter().rev().take(limit).cloned().collect();
        Some(ConnectorHealthSummary {
            connector_id: connector_id.to_string(),
            total_checks: total,
            healthy_checks: healthy,
            uptime_pct: if total > 0 { healthy as f64 / total as f64 * 100.0 } else { 0.0 },
            avg_latency_ms: avg_latency,
            p99_latency_ms: p99,
            recent,
        })
    }

    pub fn all_summaries(&self, limit: usize) -> Vec<ConnectorHealthSummary> {
        let ids = self.list_connectors();
        ids.iter().filter_map(|id| self.summary(id, limit)).collect()
    }

    pub fn list_connectors(&self) -> Vec<String> {
        self.records.lock().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_summary() {
        let h = HealthHistory::new();
        h.record("os1", true, 50, None);
        h.record("os1", true, 30, None);
        h.record("os1", false, 500, Some("timeout".into()));
        let s = h.summary("os1", 10).unwrap();
        assert_eq!(s.total_checks, 3);
        assert_eq!(s.healthy_checks, 2);
        assert!(s.uptime_pct > 60.0);
        assert_eq!(s.recent.len(), 3);
    }

    #[test]
    fn test_max_entries() {
        let h = HealthHistory::new();
        for i in 0..1100 {
            h.record("os1", true, i as u64, None);
        }
        let s = h.summary("os1", 5).unwrap();
        assert_eq!(s.total_checks, 1000);
        assert_eq!(s.recent.len(), 5);
    }

    #[test]
    fn test_missing_connector() {
        let h = HealthHistory::new();
        assert!(h.summary("nonexistent", 10).is_none());
    }
}
