// SPDX-License-Identifier: Apache-2.0
//! Result size estimator — predict result size before execution.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SizeEstimate {
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
    pub confidence: f64, // 0.0 - 1.0
}

/// Estimate result size from table stats and query characteristics.
pub fn estimate(
    table_rows: u64,
    avg_row_bytes: u64,
    selectivity: f64,
    limit: Option<u64>,
) -> SizeEstimate {
    let filtered_rows = (table_rows as f64 * selectivity) as u64;
    let rows = match limit {
        Some(l) => filtered_rows.min(l),
        None => filtered_rows,
    };
    SizeEstimate {
        estimated_rows: rows,
        estimated_bytes: rows * avg_row_bytes,
        confidence: if table_rows > 0 { 0.7 } else { 0.1 },
    }
}

/// Estimate join result size.
pub fn estimate_join(left_rows: u64, right_rows: u64, selectivity: f64) -> SizeEstimate {
    let rows = (left_rows as f64 * right_rows as f64 * selectivity) as u64;
    SizeEstimate {
        estimated_rows: rows.min(left_rows.max(right_rows) * 2),
        estimated_bytes: rows * 100, // assume 100 bytes/row
        confidence: 0.5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_with_limit() {
        let e = estimate(10000, 100, 0.1, Some(50));
        assert_eq!(e.estimated_rows, 50);
    }

    #[test]
    fn test_estimate_no_limit() {
        let e = estimate(10000, 100, 0.1, None);
        assert_eq!(e.estimated_rows, 1000);
    }

    #[test]
    fn test_estimate_join() {
        let e = estimate_join(1000, 500, 0.01);
        assert!(e.estimated_rows > 0);
    }

    #[test]
    fn test_zero_rows() {
        let e = estimate(0, 100, 1.0, None);
        assert_eq!(e.estimated_rows, 0);
        assert_eq!(e.confidence, 0.1);
    }
}
