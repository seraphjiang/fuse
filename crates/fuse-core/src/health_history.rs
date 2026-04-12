// SPDX-License-Identifier: Apache-2.0
//! Connector health history — track health status over time.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use serde::Serialize;

const DEFAULT_MAX_ENTRIES: usize = 60;

#[derive(Debug, Clone, Serialize)]
pub struct HealthSnapshot {
    pub timestamp: u64,
    pub healthy: bool,
    pub latency_ms: Option<u64>,
}

/// Tracks health history per connector.
pub struct HealthHistory {
    entries: Mutex<HashMap<String, VecDeque<HealthSnapshot>>>,
    max_entries: usize,
}

impl HealthHistory {
    pub fn new(max_entries: usize) -> Self {
        Self { entries: Mutex::new(HashMap::new()), max_entries }
    }

    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES)
    }

    pub fn record(&self, connector: &str, healthy: bool, latency_ms: Option<u64>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut map = self.entries.lock().unwrap();
        let history = map.entry(connector.to_string()).or_default();
        if history.len() >= self.max_entries {
            history.pop_front();
        }
        history.push_back(HealthSnapshot { timestamp: now, healthy, latency_ms });
    }

    pub fn get(&self, connector: &str) -> Vec<HealthSnapshot> {
        self.entries.lock().unwrap()
            .get(connector)
            .map(|h| h.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Uptime percentage for a connector (0.0 - 1.0).
    pub fn uptime(&self, connector: &str) -> f64 {
        let map = self.entries.lock().unwrap();
        match map.get(connector) {
            Some(h) if !h.is_empty() => {
                let healthy = h.iter().filter(|s| s.healthy).count();
                healthy as f64 / h.len() as f64
            }
            _ => 1.0,
        }
    }

    /// Average latency for a connector.
    pub fn avg_latency(&self, connector: &str) -> Option<u64> {
        let map = self.entries.lock().unwrap();
        let h = map.get(connector)?;
        let latencies: Vec<u64> = h.iter().filter_map(|s| s.latency_ms).collect();
        if latencies.is_empty() { return None; }
        Some(latencies.iter().sum::<u64>() / latencies.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_get() {
        let h = HealthHistory::new(10);
        h.record("pg", true, Some(5));
        h.record("pg", true, Some(8));
        assert_eq!(h.get("pg").len(), 2);
    }

    #[test]
    fn test_max_entries() {
        let h = HealthHistory::new(3);
        for i in 0..5 {
            h.record("ds", true, Some(i));
        }
        assert_eq!(h.get("ds").len(), 3);
    }

    #[test]
    fn test_uptime() {
        let h = HealthHistory::new(10);
        h.record("ds", true, None);
        h.record("ds", true, None);
        h.record("ds", false, None);
        assert!((h.uptime("ds") - 0.6667).abs() < 0.01);
    }

    #[test]
    fn test_avg_latency() {
        let h = HealthHistory::new(10);
        h.record("ds", true, Some(10));
        h.record("ds", true, Some(20));
        assert_eq!(h.avg_latency("ds"), Some(15));
    }

    #[test]
    fn test_unknown_connector() {
        let h = HealthHistory::new(10);
        assert!(h.get("unknown").is_empty());
        assert_eq!(h.uptime("unknown"), 1.0);
        assert!(h.avg_latency("unknown").is_none());
    }
}
