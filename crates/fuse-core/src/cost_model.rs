// SPDX-License-Identifier: Apache-2.0
//! Query plan cost model — estimate execution cost.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct CostEstimate {
    pub estimated_rows: u64,
    pub estimated_cost: f64,
    pub io_cost: f64,
    pub cpu_cost: f64,
}

/// Estimate cost for a table scan.
pub fn scan_cost(estimated_rows: u64, columns: usize) -> CostEstimate {
    let io = estimated_rows as f64 * columns as f64 * 0.01;
    let cpu = estimated_rows as f64 * 0.001;
    CostEstimate {
        estimated_rows,
        estimated_cost: io + cpu,
        io_cost: io,
        cpu_cost: cpu,
    }
}

/// Estimate cost for a hash join.
pub fn join_cost(left_rows: u64, right_rows: u64) -> CostEstimate {
    let build = right_rows as f64 * 0.02; // build hash table
    let probe = left_rows as f64 * 0.01; // probe
    let estimated = (left_rows as f64 * right_rows as f64 * 0.1) as u64; // selectivity
    CostEstimate {
        estimated_rows: estimated.min(left_rows.max(right_rows)),
        estimated_cost: build + probe,
        io_cost: build,
        cpu_cost: probe,
    }
}

/// Estimate cost for sort.
pub fn sort_cost(rows: u64) -> CostEstimate {
    let n = rows as f64;
    let cost = if n > 0.0 { n * n.log2() * 0.005 } else { 0.0 };
    CostEstimate {
        estimated_rows: rows,
        estimated_cost: cost,
        io_cost: 0.0,
        cpu_cost: cost,
    }
}

/// Combine costs from multiple plan nodes.
pub fn total_cost(estimates: &[CostEstimate]) -> CostEstimate {
    let rows = estimates.last().map(|e| e.estimated_rows).unwrap_or(0);
    let cost: f64 = estimates.iter().map(|e| e.estimated_cost).sum();
    let io: f64 = estimates.iter().map(|e| e.io_cost).sum();
    let cpu: f64 = estimates.iter().map(|e| e.cpu_cost).sum();
    CostEstimate {
        estimated_rows: rows,
        estimated_cost: cost,
        io_cost: io,
        cpu_cost: cpu,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_cost() {
        let c = scan_cost(1000, 5);
        assert!(c.estimated_cost > 0.0);
        assert_eq!(c.estimated_rows, 1000);
    }

    #[test]
    fn test_join_cost() {
        let c = join_cost(1000, 500);
        assert!(c.estimated_cost > 0.0);
        assert!(c.estimated_rows > 0);
    }

    #[test]
    fn test_sort_cost() {
        let c = sort_cost(1000);
        assert!(c.cpu_cost > 0.0);
        assert_eq!(c.io_cost, 0.0);
    }

    #[test]
    fn test_total_cost() {
        let estimates = vec![scan_cost(1000, 3), sort_cost(1000)];
        let total = total_cost(&estimates);
        assert!(total.estimated_cost > estimates[0].estimated_cost);
    }

    #[test]
    fn test_zero_rows() {
        let c = scan_cost(0, 5);
        assert_eq!(c.estimated_cost, 0.0);
    }
}
