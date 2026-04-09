// SPDX-License-Identifier: Apache-2.0

//! Structured query plan representation for visualization and debugging.
//!
//! Produces a tree of [`PlanNode`]s describing how a federated query will
//! execute: which connectors are involved, what gets pushed down, and how
//! results are merged.

use serde::Serialize;

use fuse_core::connector::ConnectorCapabilities;

use crate::cost::{estimate_remote_cost, QueryWorkload, TableStats};
use crate::optimizer::apply_connector_pushdown;

/// A node in the query execution plan tree.
#[derive(Debug, Clone, Serialize)]
pub struct PlanNode {
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_cost: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    pub fn leaf(op: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            op: op.into(),
            detail: Some(detail.into()),
            estimated_rows: None,
            estimated_cost: None,
            children: vec![],
        }
    }

    pub fn with_children(op: impl Into<String>, children: Vec<PlanNode>) -> Self {
        Self {
            op: op.into(),
            detail: None,
            estimated_rows: None,
            estimated_cost: None,
            children,
        }
    }

    /// Render as indented text for human-readable output.
    pub fn to_text(&self, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        let mut line = format!("{prefix}{}", self.op);
        if let Some(d) = &self.detail {
            line.push_str(&format!(" [{d}]"));
        }
        if let Some(r) = self.estimated_rows {
            line.push_str(&format!(" (est. {r} rows)"));
        }
        if let Some(c) = self.estimated_cost {
            line.push_str(&format!(" (cost: {c:.1})"));
        }
        let mut lines = vec![line];
        for child in &self.children {
            lines.push(child.to_text(indent + 1));
        }
        lines.join("\n")
    }
}

/// Build a plan for a single-source query.
pub fn plan_single(
    datasource: &str,
    table: &str,
    caps: &ConnectorCapabilities,
    workload: &QueryWorkload,
) -> PlanNode {
    let pushdown = apply_connector_pushdown(caps);
    let stats = TableStats::default();
    let cost = estimate_remote_cost(caps, &stats, workload);

    let mut pushed = Vec::new();
    if workload.has_filter && pushdown.push_filters {
        pushed.push("filter");
    }
    if workload.has_aggregation && pushdown.push_aggregations {
        pushed.push("aggregation");
    }
    if workload.has_sort && pushdown.push_sort {
        pushed.push("sort");
    }
    if workload.has_limit && pushdown.push_limit {
        pushed.push("limit");
    }

    let push_detail = if pushed.is_empty() {
        "no pushdown".to_string()
    } else {
        format!("pushdown: {}", pushed.join(", "))
    };

    let mut scan = PlanNode::leaf(
        "RemoteScan",
        format!("{datasource}.{table} ({push_detail})"),
    );
    scan.estimated_rows = Some(stats.estimated_rows);
    scan.estimated_cost = Some(cost.total);
    scan
}

/// Build a plan for a UNION ALL query across multiple sources.
pub fn plan_union(
    refs: &[(String, String)],
    caps: &[ConnectorCapabilities],
    workload: &QueryWorkload,
    limit: Option<usize>,
) -> PlanNode {
    let children: Vec<PlanNode> = refs
        .iter()
        .zip(caps.iter())
        .map(|((ds, tbl), c)| plan_single(ds, tbl, c, workload))
        .collect();

    let total_est: u64 = children.iter().filter_map(|c| c.estimated_rows).sum();

    let mut merge = PlanNode::with_children("UnionAll", children);
    merge.estimated_rows = Some(total_est);

    if let Some(n) = limit {
        let mut limit_node = PlanNode::with_children("GlobalLimit", vec![merge]);
        limit_node.detail = Some(format!("limit {n}"));
        limit_node.estimated_rows = Some((total_est).min(n as u64));
        limit_node
    } else {
        merge
    }
}

/// Build a plan for a cross-source JOIN.
pub fn plan_join(
    left: (&str, &str),
    right: (&str, &str),
    left_caps: &ConnectorCapabilities,
    right_caps: &ConnectorCapabilities,
    join_key: &str,
) -> PlanNode {
    let left_scan = plan_single(left.0, left.1, left_caps, &QueryWorkload::default());
    let right_scan = plan_single(right.0, right.1, right_caps, &QueryWorkload::default());

    let left_rows = left_scan.estimated_rows.unwrap_or(10_000);
    let right_rows = right_scan.estimated_rows.unwrap_or(10_000);

    let strategy = if left_rows.min(right_rows) <= 10_000 {
        "SemiJoin"
    } else {
        "HashJoin"
    };

    let mut join = PlanNode::with_children(strategy, vec![left_scan, right_scan]);
    join.detail = Some(format!("on {join_key}"));
    // Rough estimate: min of both sides (inner join selectivity)
    join.estimated_rows = Some(left_rows.min(right_rows));
    join
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_single_with_pushdown() {
        let caps = ConnectorCapabilities::full();
        let workload = QueryWorkload {
            has_filter: true,
            has_limit: true,
            limit_value: Some(100),
            ..Default::default()
        };
        let node = plan_single("cluster_a", "logs", &caps, &workload);
        assert_eq!(node.op, "RemoteScan");
        assert!(node.detail.unwrap().contains("pushdown: filter, limit"));
        assert!(node.estimated_rows.is_some());
        assert!(node.estimated_cost.is_some());
    }

    #[test]
    fn test_plan_single_no_pushdown() {
        let caps = ConnectorCapabilities {
            supports_filtering: false,
            supports_sorting: false,
            supports_limit: false,
            supports_aggregation: false,
            ..ConnectorCapabilities::full()
        };
        let workload = QueryWorkload {
            has_filter: true,
            ..Default::default()
        };
        let node = plan_single("s3", "data", &caps, &workload);
        assert!(node.detail.unwrap().contains("no pushdown"));
    }

    #[test]
    fn test_plan_union() {
        let refs = vec![
            ("cluster_a".into(), "logs".into()),
            ("cluster_b".into(), "logs".into()),
        ];
        let caps = vec![ConnectorCapabilities::full(), ConnectorCapabilities::full()];
        let node = plan_union(&refs, &caps, &QueryWorkload::default(), None);
        assert_eq!(node.op, "UnionAll");
        assert_eq!(node.children.len(), 2);
        assert_eq!(node.estimated_rows, Some(20_000)); // 10k default × 2
    }

    #[test]
    fn test_plan_union_with_limit() {
        let refs = vec![
            ("a".into(), "t".into()),
            ("b".into(), "t".into()),
        ];
        let caps = vec![ConnectorCapabilities::full(), ConnectorCapabilities::full()];
        let node = plan_union(&refs, &caps, &QueryWorkload::default(), Some(5));
        assert_eq!(node.op, "GlobalLimit");
        assert_eq!(node.estimated_rows, Some(5));
        assert_eq!(node.children[0].op, "UnionAll");
    }

    #[test]
    fn test_plan_join() {
        let caps = ConnectorCapabilities::full();
        let node = plan_join(
            ("cluster_a", "logs"),
            ("s3", "archive"),
            &caps,
            &caps,
            "trace_id",
        );
        assert_eq!(node.op, "SemiJoin"); // default 10k rows < threshold
        assert!(node.detail.unwrap().contains("trace_id"));
        assert_eq!(node.children.len(), 2);
    }

    #[test]
    fn test_to_text() {
        let node = PlanNode::with_children(
            "UnionAll",
            vec![
                PlanNode::leaf("RemoteScan", "cluster_a.logs"),
                PlanNode::leaf("RemoteScan", "cluster_b.logs"),
            ],
        );
        let text = node.to_text(0);
        assert!(text.contains("UnionAll"));
        assert!(text.contains("  RemoteScan [cluster_a.logs]"));
        assert!(text.contains("  RemoteScan [cluster_b.logs]"));
    }
}
