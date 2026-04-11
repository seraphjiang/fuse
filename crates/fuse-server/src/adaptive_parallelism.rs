// SPDX-License-Identifier: Apache-2.0
//! #1441 Adaptive parallelism — auto-tune fan-out concurrency per datasource.
//!
//! Tracks success/failure rates and latencies per datasource to determine
//! optimal concurrency. Slow or failing datasources get throttled.

use std::collections::HashMap;
use std::sync::Mutex;

const DEFAULT_CONCURRENCY: usize = 4;
const MIN_CONCURRENCY: usize = 1;
const MAX_CONCURRENCY: usize = 16;
const SAMPLES_FOR_ADJUSTMENT: usize = 10;

pub struct AdaptiveParallelism {
    stats: Mutex<HashMap<String, DatasourceStats>>,
}

struct DatasourceStats {
    concurrency: usize,
    recent_latencies: Vec<u64>,
    recent_errors: usize,
    recent_successes: usize,
}

impl Default for DatasourceStats {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_CONCURRENCY,
            recent_latencies: Vec::new(),
            recent_errors: 0,
            recent_successes: 0,
        }
    }
}

impl AdaptiveParallelism {
    pub fn new() -> Self {
        Self { stats: Mutex::new(HashMap::new()) }
    }

    /// Get the current concurrency limit for a datasource.
    pub fn concurrency_for(&self, datasource: &str) -> usize {
        self.stats.lock().unwrap()
            .get(datasource)
            .map(|s| s.concurrency)
            .unwrap_or(DEFAULT_CONCURRENCY)
    }

    /// Record a successful query execution.
    pub fn record_success(&self, datasource: &str, latency_ms: u64) {
        let mut map = self.stats.lock().unwrap();
        let stats = map.entry(datasource.to_string()).or_default();
        stats.recent_successes += 1;
        if stats.recent_latencies.len() >= 100 {
            stats.recent_latencies.remove(0);
        }
        stats.recent_latencies.push(latency_ms);
        self.maybe_adjust(stats);
    }

    /// Record a failed query execution.
    pub fn record_failure(&self, datasource: &str) {
        let mut map = self.stats.lock().unwrap();
        let stats = map.entry(datasource.to_string()).or_default();
        stats.recent_errors += 1;
        self.maybe_adjust(stats);
    }

    fn maybe_adjust(&self, stats: &mut DatasourceStats) {
        let total = stats.recent_successes + stats.recent_errors;
        if total < SAMPLES_FOR_ADJUSTMENT {
            return;
        }

        let error_rate = stats.recent_errors as f64 / total as f64;

        if error_rate > 0.3 {
            // High error rate — reduce concurrency
            stats.concurrency = (stats.concurrency / 2).max(MIN_CONCURRENCY);
        } else if error_rate < 0.05 && !stats.recent_latencies.is_empty() {
            let avg_latency: u64 = stats.recent_latencies.iter().sum::<u64>()
                / stats.recent_latencies.len() as u64;
            if avg_latency < 500 {
                // Fast + low errors — increase concurrency
                stats.concurrency = (stats.concurrency + 1).min(MAX_CONCURRENCY);
            }
        }

        // Reset counters for next window
        stats.recent_successes = 0;
        stats.recent_errors = 0;
    }

    /// Get stats for monitoring.
    pub fn stats(&self) -> HashMap<String, (usize, usize)> {
        self.stats.lock().unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), (v.concurrency, v.recent_latencies.len())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_concurrency() {
        let ap = AdaptiveParallelism::new();
        assert_eq!(ap.concurrency_for("unknown"), DEFAULT_CONCURRENCY);
    }

    #[test]
    fn test_increases_on_fast_success() {
        let ap = AdaptiveParallelism::new();
        for _ in 0..SAMPLES_FOR_ADJUSTMENT {
            ap.record_success("fast_ds", 100);
        }
        assert!(ap.concurrency_for("fast_ds") > DEFAULT_CONCURRENCY);
    }

    #[test]
    fn test_decreases_on_high_errors() {
        let ap = AdaptiveParallelism::new();
        for _ in 0..4 {
            ap.record_failure("bad_ds");
        }
        for _ in 0..6 {
            ap.record_success("bad_ds", 1000);
        }
        // 40% error rate > 30% threshold — should decrease
        assert!(ap.concurrency_for("bad_ds") < DEFAULT_CONCURRENCY);
    }

    #[test]
    fn test_stays_stable_moderate_latency() {
        let ap = AdaptiveParallelism::new();
        for _ in 0..SAMPLES_FOR_ADJUSTMENT {
            ap.record_success("moderate", 2000);
        }
        // High latency but no errors — stays at default
        assert_eq!(ap.concurrency_for("moderate"), DEFAULT_CONCURRENCY);
    }

    #[test]
    fn test_clamped_to_min() {
        let ap = AdaptiveParallelism::new();
        // Force many error cycles
        for _ in 0..5 {
            for _ in 0..SAMPLES_FOR_ADJUSTMENT {
                ap.record_failure("terrible");
            }
        }
        assert_eq!(ap.concurrency_for("terrible"), MIN_CONCURRENCY);
    }

    #[test]
    fn test_independent_datasources() {
        let ap = AdaptiveParallelism::new();
        for _ in 0..SAMPLES_FOR_ADJUSTMENT {
            ap.record_success("fast", 50);
            ap.record_failure("slow");
        }
        assert!(ap.concurrency_for("fast") > ap.concurrency_for("slow"));
    }

    #[test]
    fn test_stats_output() {
        let ap = AdaptiveParallelism::new();
        ap.record_success("ds_a", 100);
        let s = ap.stats();
        assert!(s.contains_key("ds_a"));
    }
}
