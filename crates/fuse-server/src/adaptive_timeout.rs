// SPDX-License-Identifier: Apache-2.0
//! #1113 Adaptive query timeout — learns from query history per datasource.
//!
//! Tracks recent latencies per datasource and computes a timeout as:
//!   p95_latency * multiplier (default 3x), clamped to [min, max].

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

const DEFAULT_MIN_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_MAX_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_MULTIPLIER: f64 = 3.0;
const DEFAULT_FALLBACK_MS: u64 = 30_000;
const MAX_SAMPLES: usize = 100;

pub struct AdaptiveTimeout {
    latencies: Mutex<HashMap<String, Vec<u64>>>,
    multiplier: f64,
    min_ms: u64,
    max_ms: u64,
    fallback_ms: u64,
}

impl Default for AdaptiveTimeout {
    fn default() -> Self {
        Self::new()
    }
}

impl AdaptiveTimeout {
    pub fn new() -> Self {
        Self {
            latencies: Mutex::new(HashMap::new()),
            multiplier: DEFAULT_MULTIPLIER,
            min_ms: DEFAULT_MIN_TIMEOUT_MS,
            max_ms: DEFAULT_MAX_TIMEOUT_MS,
            fallback_ms: DEFAULT_FALLBACK_MS,
        }
    }

    /// Record a completed query latency for a datasource.
    pub fn record(&self, datasource: &str, latency_ms: u64) {
        let mut map = self.latencies.lock().unwrap();
        let samples = map.entry(datasource.to_string()).or_default();
        if samples.len() >= MAX_SAMPLES {
            samples.remove(0);
        }
        samples.push(latency_ms);
    }

    /// Get the adaptive timeout for a datasource.
    /// Returns the fallback if no history exists.
    pub fn timeout_for(&self, datasource: &str) -> Duration {
        let ms = self.timeout_ms_for(datasource);
        Duration::from_millis(ms)
    }

    /// Get the adaptive timeout in milliseconds.
    pub fn timeout_ms_for(&self, datasource: &str) -> u64 {
        let map = self.latencies.lock().unwrap();
        match map.get(datasource) {
            Some(samples) if !samples.is_empty() => {
                let p95 = percentile(samples, 95);
                let adaptive = (p95 as f64 * self.multiplier) as u64;
                adaptive.clamp(self.min_ms, self.max_ms)
            }
            _ => self.fallback_ms,
        }
    }

    /// Get stats for all tracked datasources.
    pub fn stats(&self) -> HashMap<String, DatasourceTimeoutStats> {
        let map = self.latencies.lock().unwrap();
        map.iter()
            .map(|(ds, samples)| {
                let p95 = percentile(samples, 95);
                (
                    ds.clone(),
                    DatasourceTimeoutStats {
                        sample_count: samples.len(),
                        p95_latency_ms: p95,
                        adaptive_timeout_ms: ((p95 as f64 * self.multiplier) as u64)
                            .clamp(self.min_ms, self.max_ms),
                    },
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DatasourceTimeoutStats {
    pub sample_count: usize,
    pub p95_latency_ms: u64,
    pub adaptive_timeout_ms: u64,
}

fn percentile(samples: &[u64], pct: u8) -> u64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let idx = ((pct as f64 / 100.0) * (sorted.len() - 1) as f64).ceil() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fallback_when_no_history() {
        let at = AdaptiveTimeout::new();
        assert_eq!(at.timeout_ms_for("unknown"), DEFAULT_FALLBACK_MS);
    }

    #[test]
    fn test_adaptive_from_samples() {
        let at = AdaptiveTimeout::new();
        // Record 20 samples: 100ms each
        for _ in 0..20 {
            at.record("fast_ds", 100);
        }
        // p95 = 100, adaptive = 100 * 3 = 300, clamped to min 5000
        assert_eq!(at.timeout_ms_for("fast_ds"), DEFAULT_MIN_TIMEOUT_MS);
    }

    #[test]
    fn test_adaptive_scales_with_latency() {
        let at = AdaptiveTimeout::new();
        for _ in 0..20 {
            at.record("slow_ds", 10_000);
        }
        // p95 = 10000, adaptive = 10000 * 3 = 30000
        assert_eq!(at.timeout_ms_for("slow_ds"), 30_000);
    }

    #[test]
    fn test_adaptive_clamped_to_max() {
        let at = AdaptiveTimeout::new();
        for _ in 0..20 {
            at.record("very_slow", 50_000);
        }
        // p95 = 50000, adaptive = 150000, clamped to max 120000
        assert_eq!(at.timeout_ms_for("very_slow"), DEFAULT_MAX_TIMEOUT_MS);
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let at = AdaptiveTimeout::new();
        // Fill beyond MAX_SAMPLES
        for i in 0..150 {
            at.record("ds", i);
        }
        let map = at.latencies.lock().unwrap();
        assert_eq!(map["ds"].len(), MAX_SAMPLES);
        // Oldest samples (0..49) should be evicted, keeping 50..149
        assert_eq!(map["ds"][0], 50);
    }

    #[test]
    fn test_stats_output() {
        let at = AdaptiveTimeout::new();
        for _ in 0..10 {
            at.record("ds_a", 200);
        }
        let stats = at.stats();
        assert_eq!(stats["ds_a"].sample_count, 10);
        assert_eq!(stats["ds_a"].p95_latency_ms, 200);
    }

    #[test]
    fn test_percentile_single_sample() {
        assert_eq!(percentile(&[42], 95), 42);
    }

    #[test]
    fn test_percentile_varied() {
        let samples: Vec<u64> = (1..=100).collect();
        let p95 = percentile(&samples, 95);
        // p95 of 1..=100 should be around 95-96
        assert!((95..=96).contains(&p95), "p95 was {p95}");
    }

    #[test]
    fn test_timeout_for_returns_duration() {
        let at = AdaptiveTimeout::new();
        assert_eq!(at.timeout_for("x"), Duration::from_millis(DEFAULT_FALLBACK_MS));
    }

    #[test]
    fn test_independent_datasources() {
        let at = AdaptiveTimeout::new();
        for _ in 0..20 {
            at.record("fast", 100);
            at.record("slow", 20_000);
        }
        // They should have different timeouts
        assert_ne!(at.timeout_ms_for("fast"), at.timeout_ms_for("slow"));
    }
}
