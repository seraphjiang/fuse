// SPDX-License-Identifier: Apache-2.0
//! Performance regression detection — compare benchmark runs.
//!
//! Compares two sets of benchmark results and flags regressions where
//! latency increased beyond a configurable threshold.

use serde::Serialize;

/// A single benchmark measurement.
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub name: String,
    pub avg_ms: f64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub throughput_qps: f64,
}

/// Comparison result for a single benchmark.
#[derive(Debug, Clone, Serialize)]
pub struct RegressionResult {
    pub name: String,
    pub baseline_avg_ms: f64,
    pub current_avg_ms: f64,
    pub change_pct: f64,
    pub status: RegressionStatus,
    pub p95_change_pct: f64,
    pub throughput_change_pct: f64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RegressionStatus {
    Improved,
    Stable,
    Regressed,
}

/// Compare baseline and current benchmark runs.
/// `threshold_pct` is the percentage increase that triggers a regression flag.
pub fn detect_regressions(
    baseline: &[BenchmarkResult],
    current: &[BenchmarkResult],
    threshold_pct: f64,
) -> Vec<RegressionResult> {
    let mut results = Vec::new();
    for curr in current {
        let base = baseline.iter().find(|b| b.name == curr.name);
        let (base_avg, base_p95, base_qps) = match base {
            Some(b) => (b.avg_ms, b.p95_ms, b.throughput_qps),
            None => continue,
        };
        if base_avg < f64::EPSILON {
            continue;
        }
        let change_pct = ((curr.avg_ms - base_avg) / base_avg) * 100.0;
        let p95_change = if base_p95 > 0 {
            ((curr.p95_ms as f64 - base_p95 as f64) / base_p95 as f64) * 100.0
        } else {
            0.0
        };
        let tp_change = if base_qps > f64::EPSILON {
            ((curr.throughput_qps - base_qps) / base_qps) * 100.0
        } else {
            0.0
        };
        let status = if change_pct > threshold_pct {
            RegressionStatus::Regressed
        } else if change_pct < -threshold_pct {
            RegressionStatus::Improved
        } else {
            RegressionStatus::Stable
        };
        results.push(RegressionResult {
            name: curr.name.clone(),
            baseline_avg_ms: base_avg,
            current_avg_ms: curr.avg_ms,
            change_pct,
            status,
            p95_change_pct: p95_change,
            throughput_change_pct: tp_change,
        });
    }
    results
}

/// Summary of a regression comparison.
#[derive(Debug, Serialize)]
pub struct RegressionSummary {
    pub total: usize,
    pub regressed: usize,
    pub improved: usize,
    pub stable: usize,
    pub has_regressions: bool,
}

pub fn summarize(results: &[RegressionResult]) -> RegressionSummary {
    let regressed = results
        .iter()
        .filter(|r| r.status == RegressionStatus::Regressed)
        .count();
    let improved = results
        .iter()
        .filter(|r| r.status == RegressionStatus::Improved)
        .count();
    RegressionSummary {
        total: results.len(),
        regressed,
        improved,
        stable: results.len() - regressed - improved,
        has_regressions: regressed > 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bench(name: &str, avg: f64, p95: u64, qps: f64) -> BenchmarkResult {
        BenchmarkResult {
            name: name.into(),
            avg_ms: avg,
            p50_ms: (avg * 0.9) as u64,
            p95_ms: p95,
            p99_ms: p95 + 10,
            throughput_qps: qps,
        }
    }

    #[test]
    fn test_detect_regression() {
        let baseline = vec![bench("query_a", 100.0, 200, 50.0)];
        let current = vec![bench("query_a", 130.0, 260, 40.0)];
        let results = detect_regressions(&baseline, &current, 10.0);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, RegressionStatus::Regressed);
        assert!(results[0].change_pct > 20.0);
    }

    #[test]
    fn test_detect_improvement() {
        let baseline = vec![bench("query_a", 100.0, 200, 50.0)];
        let current = vec![bench("query_a", 70.0, 140, 70.0)];
        let results = detect_regressions(&baseline, &current, 10.0);
        assert_eq!(results[0].status, RegressionStatus::Improved);
    }

    #[test]
    fn test_detect_stable() {
        let baseline = vec![bench("query_a", 100.0, 200, 50.0)];
        let current = vec![bench("query_a", 105.0, 210, 48.0)];
        let results = detect_regressions(&baseline, &current, 10.0);
        assert_eq!(results[0].status, RegressionStatus::Stable);
    }

    #[test]
    fn test_summary() {
        let baseline = vec![
            bench("a", 100.0, 200, 50.0),
            bench("b", 100.0, 200, 50.0),
            bench("c", 100.0, 200, 50.0),
        ];
        let current = vec![
            bench("a", 150.0, 300, 30.0), // regressed
            bench("b", 60.0, 120, 80.0),  // improved
            bench("c", 102.0, 204, 49.0), // stable
        ];
        let results = detect_regressions(&baseline, &current, 10.0);
        let summary = summarize(&results);
        assert_eq!(summary.regressed, 1);
        assert_eq!(summary.improved, 1);
        assert_eq!(summary.stable, 1);
        assert!(summary.has_regressions);
    }

    #[test]
    fn test_missing_baseline_skipped() {
        let baseline = vec![bench("a", 100.0, 200, 50.0)];
        let current = vec![bench("b", 100.0, 200, 50.0)];
        let results = detect_regressions(&baseline, &current, 10.0);
        assert!(results.is_empty());
    }
}
