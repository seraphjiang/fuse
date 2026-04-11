// SPDX-License-Identifier: Apache-2.0
//! Plan rule engine — apply optimization rules to query plans.

use crate::plan_builder::{LogicalPlan, PlanOp};

/// An optimization rule that transforms a plan.
pub trait OptRule: Send + Sync {
    fn name(&self) -> &str;
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}

/// Apply a sequence of rules to a plan.
pub fn apply_rules(mut plan: LogicalPlan, rules: &[Box<dyn OptRule>]) -> LogicalPlan {
    for rule in rules {
        plan = rule.apply(plan);
    }
    plan
}

/// Rule: remove redundant Limit(MAX) nodes.
pub struct EliminateMaxLimit;
impl OptRule for EliminateMaxLimit {
    fn name(&self) -> &str { "eliminate_max_limit" }
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        LogicalPlan { root: eliminate_max_limit(plan.root) }
    }
}

fn eliminate_max_limit(op: PlanOp) -> PlanOp {
    match op {
        PlanOp::Limit { input, count } if count == u64::MAX => *input,
        PlanOp::Limit { input, count } => PlanOp::Limit { input: Box::new(eliminate_max_limit(*input)), count },
        PlanOp::Filter { input, predicate } => PlanOp::Filter { input: Box::new(eliminate_max_limit(*input)), predicate },
        PlanOp::Project { input, columns } => PlanOp::Project { input: Box::new(eliminate_max_limit(*input)), columns },
        PlanOp::Sort { input, keys } => PlanOp::Sort { input: Box::new(eliminate_max_limit(*input)), keys },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_builder::PlanBuilder;

    #[test]
    fn test_eliminate_max_limit() {
        let plan = PlanBuilder::scan("ds", "t").limit(u64::MAX).build();
        let rules: Vec<Box<dyn OptRule>> = vec![Box::new(EliminateMaxLimit)];
        let optimized = apply_rules(plan, &rules);
        assert!(matches!(optimized.root, PlanOp::Scan { .. }));
    }

    #[test]
    fn test_keep_real_limit() {
        let plan = PlanBuilder::scan("ds", "t").limit(100).build();
        let rules: Vec<Box<dyn OptRule>> = vec![Box::new(EliminateMaxLimit)];
        let optimized = apply_rules(plan, &rules);
        assert!(matches!(optimized.root, PlanOp::Limit { count: 100, .. }));
    }

    #[test]
    fn test_empty_rules() {
        let plan = PlanBuilder::scan("ds", "t").build();
        let optimized = apply_rules(plan, &[]);
        assert!(matches!(optimized.root, PlanOp::Scan { .. }));
    }
}
