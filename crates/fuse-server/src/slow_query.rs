// SPDX-License-Identifier: Apache-2.0
//! Slow query detection and logging.

use std::time::Duration;

const DEFAULT_SLOW_THRESHOLD_MS: u64 = 5000;

/// Check if a query is slow and log it.
pub fn check_slow_query(
    query_id: &str,
    format: &str,
    duration: Duration,
    datasources: &[String],
    row_count: u64,
    threshold_ms: Option<u64>,
) -> bool {
    let threshold = threshold_ms.unwrap_or(DEFAULT_SLOW_THRESHOLD_MS);
    let elapsed_ms = duration.as_millis() as u64;

    if elapsed_ms >= threshold {
        tracing::warn!(
            query_id,
            format,
            elapsed_ms,
            threshold_ms = threshold,
            datasources = ?datasources,
            row_count,
            "Slow query detected"
        );
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slow_query_detected() {
        let is_slow = check_slow_query(
            "q-1",
            "sql",
            Duration::from_secs(10),
            &["cluster_a".into()],
            1000,
            None,
        );
        assert!(is_slow);
    }

    #[test]
    fn test_fast_query_not_slow() {
        let is_slow = check_slow_query(
            "q-2",
            "sql",
            Duration::from_millis(100),
            &["cluster_a".into()],
            50,
            None,
        );
        assert!(!is_slow);
    }

    #[test]
    fn test_custom_threshold() {
        let is_slow = check_slow_query("q-3", "sql", Duration::from_millis(200), &[], 0, Some(100));
        assert!(is_slow);
    }
}
