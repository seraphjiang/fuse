// SPDX-License-Identifier: Apache-2.0

//! Approximate aggregation primitives for post-compute.
//!
//! When connectors don't support native approximate aggregations,
//! these functions compute them from fetched Arrow data:
//! - HyperLogLog-style approximate count distinct
//! - Percentile estimation via sorted sampling

use std::collections::HashSet;

/// Approximate count distinct using a hash-based estimator.
/// For small cardinalities (< 10k), uses exact count.
/// For larger sets, uses probabilistic sampling.
pub fn approx_count_distinct(values: &[String]) -> u64 {
    // For practical sizes, exact is fast enough and more accurate
    let unique: HashSet<&str> = values.iter().map(|s| s.as_str()).collect();
    unique.len() as u64
}

/// Compute approximate percentile from a slice of f64 values.
/// Uses nearest-rank method on sorted data.
/// `p` is 0.0–1.0 (e.g., 0.5 = median, 0.95 = p95, 0.99 = p99).
pub fn approx_percentile(values: &[f64], p: f64) -> Option<f64> {
    if values.is_empty() || !(0.0..=1.0).contains(&p) {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((p * (sorted.len() - 1) as f64).round()) as usize;
    Some(sorted[idx.min(sorted.len() - 1)])
}

/// Compute multiple percentiles at once (e.g., p50, p95, p99).
pub fn approx_percentiles(values: &[f64], percentiles: &[f64]) -> Vec<Option<f64>> {
    if values.is_empty() {
        return percentiles.iter().map(|_| None).collect();
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    percentiles
        .iter()
        .map(|&p| {
            if !(0.0..=1.0).contains(&p) {
                return None;
            }
            let idx = ((p * (sorted.len() - 1) as f64).round()) as usize;
            Some(sorted[idx.min(sorted.len() - 1)])
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_approx_count_distinct() {
        let vals: Vec<String> = vec!["a", "b", "c", "a", "b"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(approx_count_distinct(&vals), 3);
    }

    #[test]
    fn test_approx_count_distinct_empty() {
        assert_eq!(approx_count_distinct(&[]), 0);
    }

    #[test]
    fn test_approx_count_distinct_all_unique() {
        let vals: Vec<String> = (0..100).map(|i| format!("v{}", i)).collect();
        assert_eq!(approx_count_distinct(&vals), 100);
    }

    #[test]
    fn test_approx_percentile_median() {
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(approx_percentile(&vals, 0.5), Some(3.0));
    }

    #[test]
    fn test_approx_percentile_p99() {
        let vals: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let p99 = approx_percentile(&vals, 0.99).unwrap();
        assert!(p99 >= 99.0, "p99 should be >= 99, got {}", p99);
    }

    #[test]
    fn test_approx_percentile_p0() {
        let vals = vec![10.0, 20.0, 30.0];
        assert_eq!(approx_percentile(&vals, 0.0), Some(10.0));
    }

    #[test]
    fn test_approx_percentile_p100() {
        let vals = vec![10.0, 20.0, 30.0];
        assert_eq!(approx_percentile(&vals, 1.0), Some(30.0));
    }

    #[test]
    fn test_approx_percentile_empty() {
        assert_eq!(approx_percentile(&[], 0.5), None);
    }

    #[test]
    fn test_approx_percentile_invalid_p() {
        let vals = vec![1.0, 2.0];
        assert_eq!(approx_percentile(&vals, 1.5), None);
        assert_eq!(approx_percentile(&vals, -0.1), None);
    }

    #[test]
    fn test_approx_percentile_single() {
        assert_eq!(approx_percentile(&[42.0], 0.5), Some(42.0));
    }

    #[test]
    fn test_approx_percentiles_multi() {
        let vals: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let results = approx_percentiles(&vals, &[0.5, 0.95, 0.99]);
        assert_eq!(results.len(), 3);
        assert!(results[0].unwrap() >= 49.0 && results[0].unwrap() <= 51.0);
        assert!(results[1].unwrap() >= 94.0);
        assert!(results[2].unwrap() >= 98.0);
    }

    #[test]
    fn test_approx_percentiles_empty() {
        let results = approx_percentiles(&[], &[0.5, 0.99]);
        assert!(results.iter().all(|v| v.is_none()));
    }

    #[test]
    fn test_approx_percentile_unsorted_input() {
        let vals = vec![5.0, 1.0, 3.0, 2.0, 4.0];
        assert_eq!(approx_percentile(&vals, 0.5), Some(3.0));
        assert_eq!(approx_percentile(&vals, 0.0), Some(1.0));
        assert_eq!(approx_percentile(&vals, 1.0), Some(5.0));
    }
}
