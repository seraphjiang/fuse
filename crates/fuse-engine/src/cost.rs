// SPDX-License-Identifier: Apache-2.0

//! Cost-based query planning for federated execution.
//!
//! Estimates the cost of executing operations locally (in DataFusion) vs.
//! pushing them down to a remote connector. The cost model considers:
//!
//! - Estimated row counts from [`SchemaInfo`]
//! - Connector latency class
//! - Network transfer cost (rows × width)
//! - Selectivity estimates for filters and aggregations
//!
//! The optimizer uses these costs to decide push-down strategy and, in the
//! future, join ordering for cross-type federation.

use fuse_core::connector::{ConnectorCapabilities, LatencyClass};

/// Estimated cost of an execution strategy, in abstract cost units.
/// Lower is better.
#[derive(Debug, Clone, PartialEq)]
pub struct CostEstimate {
    /// CPU cost (local compute).
    pub cpu: f64,
    /// Network/IO cost (data transfer from connector).
    pub network: f64,
    /// Total = cpu + network.
    pub total: f64,
}

impl CostEstimate {
    pub fn new(cpu: f64, network: f64) -> Self {
        Self {
            cpu,
            network,
            total: cpu + network,
        }
    }

    pub fn zero() -> Self {
        Self::new(0.0, 0.0)
    }
}

/// Statistics about a table used for cost estimation.
#[derive(Debug, Clone)]
pub struct TableStats {
    /// Estimated number of rows in the table.
    pub estimated_rows: u64,
    /// Average row width in bytes (0 = unknown).
    pub avg_row_bytes: u64,
}

impl Default for TableStats {
    fn default() -> Self {
        Self {
            estimated_rows: 10_000,
            avg_row_bytes: 256,
        }
    }
}

/// Describes a query workload for cost estimation.
#[derive(Debug, Clone, Default)]
pub struct QueryWorkload {
    pub has_filter: bool,
    pub has_aggregation: bool,
    pub has_sort: bool,
    pub has_limit: bool,
    pub limit_value: Option<u64>,
    /// Number of projected columns (0 = all).
    pub projection_count: usize,
    /// Total columns in the table.
    pub total_columns: usize,
}

/// Latency multiplier per class. Higher latency = higher per-row network cost.
fn latency_multiplier(class: &LatencyClass) -> f64 {
    match class {
        LatencyClass::Low => 1.0,
        LatencyClass::Medium => 3.0,
        LatencyClass::High => 10.0,
    }
}

/// Default filter selectivity — assume filters pass 10% of rows.
const FILTER_SELECTIVITY: f64 = 0.1;

/// Aggregation reduction — assume aggregation reduces to 1% of input rows.
const AGG_REDUCTION: f64 = 0.01;

/// Estimate the cost of pushing the entire query to the remote connector.
///
/// When a connector supports the required operations, the remote side does
/// the heavy lifting and only sends back the result set.
pub fn estimate_remote_cost(
    caps: &ConnectorCapabilities,
    stats: &TableStats,
    workload: &QueryWorkload,
) -> CostEstimate {
    let base_rows = stats.estimated_rows as f64;
    let latency = latency_multiplier(&caps.latency_class);

    // Rows returned after remote-side filtering/aggregation
    let mut result_rows = base_rows;
    if workload.has_filter && caps.supports_filtering {
        result_rows *= FILTER_SELECTIVITY;
    }
    if workload.has_aggregation && caps.supports_aggregation {
        result_rows *= AGG_REDUCTION;
    }
    if let Some(limit) = workload.limit_value {
        if caps.supports_limit {
            result_rows = result_rows.min(limit as f64);
        }
    }

    // Column projection ratio
    let col_ratio = if workload.projection_count > 0 && workload.total_columns > 0 {
        workload.projection_count as f64 / workload.total_columns as f64
    } else {
        1.0
    };

    let row_bytes = stats.avg_row_bytes as f64 * col_ratio;

    // Remote CPU is "free" to us (connector does it), but there's a fixed
    // round-trip cost proportional to latency.
    let cpu = latency * 10.0; // fixed overhead per remote call
    let network = result_rows * row_bytes * latency * 0.001;

    CostEstimate::new(cpu, network)
}

/// Estimate the cost of fetching all data locally and executing in DataFusion.
///
/// This means pulling the full table (or filtered subset if the connector
/// supports filtering) and doing aggregation/sort/limit locally.
pub fn estimate_local_cost(
    caps: &ConnectorCapabilities,
    stats: &TableStats,
    workload: &QueryWorkload,
) -> CostEstimate {
    let base_rows = stats.estimated_rows as f64;
    let latency = latency_multiplier(&caps.latency_class);

    // Even in local mode, we push filters if the connector supports them
    let mut transfer_rows = base_rows;
    if workload.has_filter && caps.supports_filtering {
        transfer_rows *= FILTER_SELECTIVITY;
    }

    let row_bytes = stats.avg_row_bytes as f64;
    let network = transfer_rows * row_bytes * latency * 0.001;

    // Local CPU for aggregation, sort, limit
    let mut cpu = transfer_rows * 0.01; // base scan cost
    if workload.has_aggregation {
        cpu += transfer_rows * 0.05;
    }
    if workload.has_sort {
        // n log n
        cpu += transfer_rows * (transfer_rows.max(1.0).ln()) * 0.02;
    }

    CostEstimate::new(cpu, network)
}

/// Choose the cheaper strategy: remote push-down vs. local execution.
///
/// Returns `true` if remote execution is cheaper (push down), `false` if
/// local execution is preferred.
pub fn should_push_down(
    caps: &ConnectorCapabilities,
    stats: &TableStats,
    workload: &QueryWorkload,
) -> bool {
    // If the connector can't handle the required operations, must go local
    if workload.has_aggregation && !caps.supports_aggregation {
        return false;
    }
    if workload.has_sort && !caps.supports_sorting {
        return false;
    }

    let remote = estimate_remote_cost(caps, stats, workload);
    let local = estimate_local_cost(caps, stats, workload);

    remote.total <= local.total
}

/// Given multiple connectors serving the same table, pick the cheapest one.
///
/// Returns the index of the best connector.
pub fn pick_cheapest_connector(
    candidates: &[(ConnectorCapabilities, TableStats)],
    workload: &QueryWorkload,
) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .map(|(i, (caps, stats))| {
            let cost = estimate_remote_cost(caps, stats, workload);
            (i, cost.total)
        })
        .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn low_latency_caps() -> ConnectorCapabilities {
        ConnectorCapabilities::full()
    }

    fn high_latency_caps() -> ConnectorCapabilities {
        ConnectorCapabilities {
            latency_class: LatencyClass::High,
            ..ConnectorCapabilities::full()
        }
    }

    fn no_agg_caps() -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_aggregation: false,
            ..ConnectorCapabilities::full()
        }
    }

    fn big_table() -> TableStats {
        TableStats {
            estimated_rows: 1_000_000,
            avg_row_bytes: 512,
        }
    }

    fn agg_workload() -> QueryWorkload {
        QueryWorkload {
            has_filter: true,
            has_aggregation: true,
            has_sort: true,
            has_limit: true,
            limit_value: Some(100),
            projection_count: 3,
            total_columns: 20,
        }
    }

    #[test]
    fn test_remote_cheaper_for_agg_with_capable_connector() {
        let caps = low_latency_caps();
        let stats = big_table();
        let workload = agg_workload();
        assert!(should_push_down(&caps, &stats, &workload));
    }

    #[test]
    fn test_local_when_connector_lacks_aggregation() {
        let caps = no_agg_caps();
        let stats = big_table();
        let workload = agg_workload();
        assert!(!should_push_down(&caps, &stats, &workload));
    }

    #[test]
    fn test_pick_cheapest_prefers_low_latency() {
        let workload = agg_workload();
        let candidates = vec![
            (high_latency_caps(), big_table()),
            (low_latency_caps(), big_table()),
        ];
        assert_eq!(pick_cheapest_connector(&candidates, &workload), Some(1));
    }

    #[test]
    fn test_remote_cost_decreases_with_limit() {
        let caps = low_latency_caps();
        let stats = big_table();
        let no_limit = QueryWorkload {
            has_filter: false,
            has_limit: false,
            ..Default::default()
        };
        let with_limit = QueryWorkload {
            has_filter: false,
            has_limit: true,
            limit_value: Some(10),
            ..Default::default()
        };
        let cost_no_limit = estimate_remote_cost(&caps, &stats, &no_limit);
        let cost_with_limit = estimate_remote_cost(&caps, &stats, &with_limit);
        assert!(cost_with_limit.total < cost_no_limit.total);
    }
}
