// SPDX-License-Identifier: Apache-2.0
//! Execution planner — unified planning combining optimizer, pushdown, and cost.

use crate::connector::ConnectorCapabilities;
use crate::cost_model::CostEstimate;
use serde::Serialize;

/// A complete execution plan for a query.
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionPlan {
    pub datasources: Vec<String>,
    pub pushdown_ops: Vec<String>,
    pub local_ops: Vec<String>,
    pub estimated_cost: CostEstimate,
    pub strategy: ExecutionStrategy,
}

#[derive(Debug, Clone, Serialize)]
pub enum ExecutionStrategy {
    SingleSource,
    FanOut,
    HashJoin,
    UnionAll,
}

impl ExecutionPlan {
    pub fn strategy_name(&self) -> &str {
        match self.strategy {
            ExecutionStrategy::SingleSource => "SingleSource",
            ExecutionStrategy::FanOut => "FanOut",
            ExecutionStrategy::HashJoin => "HashJoin",
            ExecutionStrategy::UnionAll => "UnionAll",
        }
    }
}

/// Plan a single-source query.
pub fn plan_single(datasource: &str, caps: &ConnectorCapabilities, estimated_rows: u64) -> ExecutionPlan {
    let mut pushdown = Vec::new();
    let mut local = Vec::new();
    if caps.supports_filtering { pushdown.push("filter".into()); } else { local.push("filter".into()); }
    if caps.supports_projection { pushdown.push("projection".into()); } else { local.push("projection".into()); }
    if caps.supports_limit { pushdown.push("limit".into()); } else { local.push("limit".into()); }

    ExecutionPlan {
        datasources: vec![datasource.to_string()],
        pushdown_ops: pushdown,
        local_ops: local,
        estimated_cost: crate::cost_model::scan_cost(estimated_rows, 5),
        strategy: ExecutionStrategy::SingleSource,
    }
}

/// Plan a two-source join query.
pub fn plan_join(left: &str, right: &str, left_rows: u64, right_rows: u64) -> ExecutionPlan {
    ExecutionPlan {
        datasources: vec![left.to_string(), right.to_string()],
        pushdown_ops: vec!["filter".into(), "projection".into()],
        local_ops: vec!["hash_join".into(), "sort".into()],
        estimated_cost: crate::cost_model::join_cost(left_rows, right_rows),
        strategy: ExecutionStrategy::HashJoin,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_single_full_pushdown() {
        let caps = ConnectorCapabilities::full();
        let plan = plan_single("pg", &caps, 1000);
        assert_eq!(plan.strategy_name(), "SingleSource");
        assert!(plan.pushdown_ops.contains(&"filter".to_string()));
        assert!(plan.local_ops.is_empty() || !plan.local_ops.contains(&"filter".to_string()));
    }

    #[test]
    fn test_plan_join() {
        let plan = plan_join("pg", "es", 1000, 500);
        assert_eq!(plan.datasources.len(), 2);
        assert!(plan.local_ops.contains(&"hash_join".to_string()));
    }

    #[test]
    fn test_plan_cost() {
        let caps = ConnectorCapabilities::full();
        let plan = plan_single("ds", &caps, 10000);
        assert!(plan.estimated_cost.estimated_cost > 0.0);
    }
}
