// SPDX-License-Identifier: Apache-2.0
//! Query plan optimizer — reorder operations for performance.

use serde::Serialize;

/// A logical operation in the query plan.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum LogicalOp {
    Scan { datasource: String, table: String },
    Filter { predicate: String },
    Project { columns: Vec<String> },
    Sort { columns: Vec<String> },
    Limit { count: u64 },
    Join { join_type: String },
    Aggregate { group_by: Vec<String> },
}

/// Optimize a sequence of operations by pushing filters before joins.
pub fn optimize(ops: Vec<LogicalOp>) -> Vec<LogicalOp> {
    let mut filters = Vec::new();
    let mut others = Vec::new();

    for op in ops {
        match &op {
            LogicalOp::Filter { .. } => filters.push(op),
            _ => others.push(op),
        }
    }

    // Push filters before joins/aggregations (filter pushdown)
    let mut result = Vec::new();
    let mut filters_placed = false;
    for op in &others {
        if !filters_placed && matches!(op, LogicalOp::Join { .. } | LogicalOp::Aggregate { .. }) {
            result.append(&mut filters);
            filters_placed = true;
        }
        result.push(op.clone());
    }
    if !filters.is_empty() {
        // Place remaining filters after scans
        let scan_end = result
            .iter()
            .position(|o| !matches!(o, LogicalOp::Scan { .. }))
            .unwrap_or(result.len());
        for (i, f) in filters.into_iter().enumerate() {
            result.insert(scan_end + i, f);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_before_join() {
        let ops = vec![
            LogicalOp::Scan {
                datasource: "a".into(),
                table: "t1".into(),
            },
            LogicalOp::Join {
                join_type: "inner".into(),
            },
            LogicalOp::Filter {
                predicate: "x > 5".into(),
            },
        ];
        let optimized = optimize(ops);
        // Filter should come before Join
        let filter_pos = optimized
            .iter()
            .position(|o| matches!(o, LogicalOp::Filter { .. }))
            .unwrap();
        let join_pos = optimized
            .iter()
            .position(|o| matches!(o, LogicalOp::Join { .. }))
            .unwrap();
        assert!(filter_pos < join_pos);
    }

    #[test]
    fn test_no_reorder_needed() {
        let ops = vec![
            LogicalOp::Scan {
                datasource: "a".into(),
                table: "t".into(),
            },
            LogicalOp::Filter {
                predicate: "x = 1".into(),
            },
            LogicalOp::Limit { count: 10 },
        ];
        let optimized = optimize(ops.clone());
        assert_eq!(optimized.len(), 3);
    }

    #[test]
    fn test_empty() {
        assert!(optimize(vec![]).is_empty());
    }

    #[test]
    fn test_filter_before_aggregate() {
        let ops = vec![
            LogicalOp::Scan {
                datasource: "a".into(),
                table: "t".into(),
            },
            LogicalOp::Aggregate {
                group_by: vec!["x".into()],
            },
            LogicalOp::Filter {
                predicate: "y > 0".into(),
            },
        ];
        let optimized = optimize(ops);
        let filter_pos = optimized
            .iter()
            .position(|o| matches!(o, LogicalOp::Filter { .. }))
            .unwrap();
        let agg_pos = optimized
            .iter()
            .position(|o| matches!(o, LogicalOp::Aggregate { .. }))
            .unwrap();
        assert!(filter_pos < agg_pos);
    }
}
