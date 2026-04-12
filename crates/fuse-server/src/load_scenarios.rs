// SPDX-License-Identifier: Apache-2.0
//! Load test scenario definitions — spike, soak, stress patterns.
//!
//! Provides configurable load profiles for testing Fuse under different
//! traffic patterns. Used by integration tests and the load test script.

use serde::{Deserialize, Serialize};

/// A load test scenario definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadScenario {
    pub name: String,
    pub pattern: LoadPattern,
    /// Total duration in seconds.
    pub duration_secs: u64,
    /// Queries to execute per second at peak.
    pub peak_qps: u32,
}

/// Traffic pattern for load generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LoadPattern {
    /// Sudden burst: ramp from 0 to peak in ramp_secs, hold, then drop.
    Spike {
        ramp_up_secs: u64,
        hold_secs: u64,
        ramp_down_secs: u64,
    },
    /// Sustained constant load for the full duration.
    Soak,
    /// Linearly increasing load from base_qps to peak over duration.
    Stress {
        base_qps: u32,
    },
}

/// Result of a load test run.
#[derive(Debug, Clone, Serialize)]
pub struct LoadResult {
    pub scenario: String,
    pub total_requests: u64,
    pub successful: u64,
    pub failed: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub max_ms: u64,
    pub avg_ms: f64,
    pub actual_qps: f64,
    pub error_rate_pct: f64,
}

impl LoadResult {
    /// Compute stats from a list of latencies (in ms).
    pub fn from_latencies(scenario: &str, latencies: &mut [u64], failed: u64, duration_secs: f64) -> Self {
        latencies.sort_unstable();
        let n = latencies.len();
        let total = n as u64 + failed;
        Self {
            scenario: scenario.to_string(),
            total_requests: total,
            successful: n as u64,
            failed,
            p50_ms: if n > 0 { latencies[n / 2] } else { 0 },
            p95_ms: if n > 0 { latencies[(n * 95 / 100).min(n - 1)] } else { 0 },
            p99_ms: if n > 0 { latencies[(n * 99 / 100).min(n - 1)] } else { 0 },
            max_ms: latencies.last().copied().unwrap_or(0),
            avg_ms: if n > 0 { latencies.iter().sum::<u64>() as f64 / n as f64 } else { 0.0 },
            actual_qps: if duration_secs > 0.0 { total as f64 / duration_secs } else { 0.0 },
            error_rate_pct: if total > 0 { failed as f64 / total as f64 * 100.0 } else { 0.0 },
        }
    }
}

/// Compute the target QPS at a given elapsed second for a scenario.
pub fn qps_at(scenario: &LoadScenario, elapsed_secs: u64) -> u32 {
    match &scenario.pattern {
        LoadPattern::Spike { ramp_up_secs, hold_secs, ramp_down_secs } => {
            if elapsed_secs < *ramp_up_secs {
                // Linear ramp up
                if *ramp_up_secs == 0 { scenario.peak_qps }
                else { (scenario.peak_qps as u64 * elapsed_secs / ramp_up_secs) as u32 }
            } else if elapsed_secs < ramp_up_secs + hold_secs {
                scenario.peak_qps
            } else {
                let down_elapsed = elapsed_secs - ramp_up_secs - hold_secs;
                if *ramp_down_secs == 0 || down_elapsed >= *ramp_down_secs { 0 }
                else { (scenario.peak_qps as u64 * (ramp_down_secs - down_elapsed) / ramp_down_secs) as u32 }
            }
        }
        LoadPattern::Soak => scenario.peak_qps,
        LoadPattern::Stress { base_qps } => {
            if scenario.duration_secs == 0 { scenario.peak_qps }
            else {
                let range = scenario.peak_qps.saturating_sub(*base_qps);
                base_qps + (range as u64 * elapsed_secs / scenario.duration_secs) as u32
            }
        }
    }
}

/// Built-in scenario presets.
pub fn preset_spike() -> LoadScenario {
    LoadScenario {
        name: "spike".into(),
        pattern: LoadPattern::Spike { ramp_up_secs: 2, hold_secs: 5, ramp_down_secs: 2 },
        duration_secs: 9,
        peak_qps: 100,
    }
}

pub fn preset_soak() -> LoadScenario {
    LoadScenario {
        name: "soak".into(),
        pattern: LoadPattern::Soak,
        duration_secs: 30,
        peak_qps: 50,
    }
}

pub fn preset_stress() -> LoadScenario {
    LoadScenario {
        name: "stress".into(),
        pattern: LoadPattern::Stress { base_qps: 10 },
        duration_secs: 20,
        peak_qps: 200,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spike_qps_ramp_up() {
        let s = preset_spike();
        assert_eq!(qps_at(&s, 0), 0);
        assert_eq!(qps_at(&s, 1), 50); // halfway through 2s ramp
    }

    #[test]
    fn test_spike_qps_hold() {
        let s = preset_spike();
        assert_eq!(qps_at(&s, 2), 100); // start of hold
        assert_eq!(qps_at(&s, 5), 100); // during hold
    }

    #[test]
    fn test_spike_qps_ramp_down() {
        let s = preset_spike();
        assert_eq!(qps_at(&s, 7), 100); // start of ramp down
        assert_eq!(qps_at(&s, 8), 50);  // halfway down
    }

    #[test]
    fn test_soak_constant() {
        let s = preset_soak();
        assert_eq!(qps_at(&s, 0), 50);
        assert_eq!(qps_at(&s, 15), 50);
        assert_eq!(qps_at(&s, 29), 50);
    }

    #[test]
    fn test_stress_linear_increase() {
        let s = preset_stress();
        assert_eq!(qps_at(&s, 0), 10);  // base
        assert_eq!(qps_at(&s, 10), 105); // halfway
        assert_eq!(qps_at(&s, 20), 200); // peak
    }

    #[test]
    fn test_load_result_from_latencies() {
        let mut lats = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        let result = LoadResult::from_latencies("test", &mut lats, 2, 5.0);
        assert_eq!(result.total_requests, 12);
        assert_eq!(result.successful, 10);
        assert_eq!(result.failed, 2);
        assert_eq!(result.p50_ms, 60);
        assert_eq!(result.max_ms, 100);
        assert!((result.avg_ms - 55.0).abs() < 0.1);
        assert!((result.actual_qps - 2.4).abs() < 0.1);
        assert!((result.error_rate_pct - 16.67).abs() < 0.1);
    }

    #[test]
    fn test_load_result_empty() {
        let mut lats = vec![];
        let result = LoadResult::from_latencies("empty", &mut lats, 0, 1.0);
        assert_eq!(result.total_requests, 0);
        assert_eq!(result.p50_ms, 0);
        assert!((result.avg_ms).abs() < 0.01);
    }

    #[test]
    fn test_scenario_serialization() {
        let s = preset_spike();
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"spike\""));
        assert!(json.contains("ramp_up_secs"));
    }

    #[test]
    fn test_stress_zero_duration() {
        let s = LoadScenario {
            name: "instant".into(),
            pattern: LoadPattern::Stress { base_qps: 0 },
            duration_secs: 0,
            peak_qps: 100,
        };
        assert_eq!(qps_at(&s, 0), 100);
    }
}
