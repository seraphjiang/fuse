// SPDX-License-Identifier: Apache-2.0
//! Smart query routing — route to fastest connector based on historical latency.

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Mutex;

const MAX_SAMPLES: usize = 100;

#[derive(Debug, Clone, Serialize)]
pub struct ConnectorLatencyStats {
    pub connector_id: String,
    pub avg_ms: f64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub sample_count: usize,
}

pub struct SmartRouter {
    samples: Mutex<HashMap<String, Vec<u64>>>,
}

impl Default for SmartRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl SmartRouter {
    pub fn new() -> Self {
        Self {
            samples: Mutex::new(HashMap::new()),
        }
    }

    /// Record a query latency for a connector.
    pub fn record(&self, connector_id: &str, latency_ms: u64) {
        let mut map = self.samples.lock().unwrap();
        let v = map.entry(connector_id.to_string()).or_default();
        v.push(latency_ms);
        if v.len() > MAX_SAMPLES {
            v.remove(0);
        }
    }

    /// Get latency stats for a connector.
    pub fn stats(&self, connector_id: &str) -> Option<ConnectorLatencyStats> {
        let map = self.samples.lock().unwrap();
        let v = map.get(connector_id)?;
        if v.is_empty() {
            return None;
        }
        let mut sorted = v.clone();
        sorted.sort_unstable();
        let n = sorted.len();
        Some(ConnectorLatencyStats {
            connector_id: connector_id.to_string(),
            avg_ms: v.iter().sum::<u64>() as f64 / n as f64,
            p50_ms: sorted[n / 2],
            p95_ms: sorted[(n * 95 / 100).min(n - 1)],
            sample_count: n,
        })
    }

    /// Pick the fastest connector from candidates based on historical p50 latency.
    pub fn fastest(&self, candidates: &[&str]) -> Option<String> {
        let map = self.samples.lock().unwrap();
        candidates
            .iter()
            .filter_map(|id| {
                let v = map.get(*id)?;
                if v.is_empty() {
                    return None;
                }
                let mut sorted = v.clone();
                sorted.sort_unstable();
                Some((id.to_string(), sorted[sorted.len() / 2]))
            })
            .min_by_key(|(_, p50)| *p50)
            .map(|(id, _)| id)
    }

    /// Get all connector stats.
    pub fn all_stats(&self) -> Vec<ConnectorLatencyStats> {
        let ids: Vec<String> = self.samples.lock().unwrap().keys().cloned().collect();
        ids.iter().filter_map(|id| self.stats(id)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_stats() {
        let r = SmartRouter::new();
        r.record("os1", 10);
        r.record("os1", 20);
        r.record("os1", 30);
        let s = r.stats("os1").unwrap();
        assert_eq!(s.sample_count, 3);
        assert!((s.avg_ms - 20.0).abs() < 0.1);
        assert_eq!(s.p50_ms, 20);
    }

    #[test]
    fn test_fastest() {
        let r = SmartRouter::new();
        r.record("slow", 100);
        r.record("slow", 200);
        r.record("fast", 5);
        r.record("fast", 10);
        assert_eq!(r.fastest(&["slow", "fast"]).unwrap(), "fast");
    }

    #[test]
    fn test_max_samples() {
        let r = SmartRouter::new();
        for i in 0..150 {
            r.record("os1", i);
        }
        assert_eq!(r.stats("os1").unwrap().sample_count, MAX_SAMPLES);
    }

    #[test]
    fn test_missing_connector() {
        let r = SmartRouter::new();
        assert!(r.stats("nonexistent").is_none());
        assert!(r.fastest(&["nonexistent"]).is_none());
    }
}
