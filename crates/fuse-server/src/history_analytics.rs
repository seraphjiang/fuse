// SPDX-License-Identifier: Apache-2.0
//! Query history analytics — compute stats from query history.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct HistoryAnalytics {
    pub total_queries: u64,
    pub success_count: u64,
    pub error_count: u64,
    pub success_rate: f64,
    pub avg_duration_ms: u64,
    pub p95_duration_ms: u64,
    pub top_datasources: Vec<(String, u64)>,
}

/// Compute analytics from a list of query durations and statuses.
pub fn compute_analytics(
    entries: &[(bool, u64, Vec<String>)], // (success, duration_ms, datasources)
) -> HistoryAnalytics {
    if entries.is_empty() {
        return HistoryAnalytics {
            total_queries: 0, success_count: 0, error_count: 0,
            success_rate: 0.0, avg_duration_ms: 0, p95_duration_ms: 0,
            top_datasources: vec![],
        };
    }

    let total = entries.len() as u64;
    let success = entries.iter().filter(|(s, _, _)| *s).count() as u64;
    let error = total - success;
    let rate = success as f64 / total as f64;

    let durations: Vec<u64> = entries.iter().map(|(_, d, _)| *d).collect();
    let avg = durations.iter().sum::<u64>() / total;

    let mut sorted = durations.clone();
    sorted.sort();
    let p95_idx = (sorted.len() as f64 * 0.95) as usize;
    let p95 = sorted.get(p95_idx.min(sorted.len() - 1)).copied().unwrap_or(0);

    let mut ds_counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for (_, _, ds_list) in entries {
        for ds in ds_list {
            *ds_counts.entry(ds.clone()).or_default() += 1;
        }
    }
    let mut top: Vec<(String, u64)> = ds_counts.into_iter().collect();
    top.sort_by(|a, b| b.1.cmp(&a.1));
    top.truncate(10);

    HistoryAnalytics {
        total_queries: total, success_count: success, error_count: error,
        success_rate: rate, avg_duration_ms: avg, p95_duration_ms: p95,
        top_datasources: top,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let a = compute_analytics(&[]);
        assert_eq!(a.total_queries, 0);
    }

    #[test]
    fn test_basic() {
        let entries = vec![
            (true, 100, vec!["pg".into()]),
            (true, 200, vec!["pg".into(), "es".into()]),
            (false, 5000, vec!["es".into()]),
        ];
        let a = compute_analytics(&entries);
        assert_eq!(a.total_queries, 3);
        assert_eq!(a.success_count, 2);
        assert_eq!(a.error_count, 1);
        assert!(a.avg_duration_ms > 0);
        assert!(a.top_datasources.len() == 2);
    }

    #[test]
    fn test_all_success() {
        let entries = vec![(true, 50, vec![]), (true, 60, vec![])];
        let a = compute_analytics(&entries);
        assert_eq!(a.success_rate, 1.0);
    }
}
