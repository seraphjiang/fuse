// SPDX-License-Identifier: Apache-2.0
//! Plan merge — combine sub-plans from multiple datasources.

use crate::plan_builder::PlanOp;
use serde::Serialize;

/// A merged execution plan with multiple datasource sub-plans.
#[derive(Debug, Clone, Serialize)]
pub struct MergedPlan {
    pub sub_plans: Vec<SubPlan>,
    pub merge_strategy: MergeStrategy,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubPlan {
    pub datasource: String,
    pub plan: PlanOp,
}

#[derive(Debug, Clone, Serialize)]
pub enum MergeStrategy {
    Append,     // UNION ALL
    HashJoin,   // JOIN
    Interleave, // Round-robin merge
}

impl MergedPlan {
    pub fn union(plans: Vec<SubPlan>) -> Self {
        Self {
            sub_plans: plans,
            merge_strategy: MergeStrategy::Append,
        }
    }

    pub fn join(left: SubPlan, right: SubPlan) -> Self {
        Self {
            sub_plans: vec![left, right],
            merge_strategy: MergeStrategy::HashJoin,
        }
    }

    pub fn datasource_count(&self) -> usize {
        self.sub_plans.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(ds: &str) -> SubPlan {
        SubPlan {
            datasource: ds.into(),
            plan: PlanOp::Scan {
                datasource: ds.into(),
                table: "t".into(),
            },
        }
    }

    #[test]
    fn test_union() {
        let plan = MergedPlan::union(vec![sub("a"), sub("b"), sub("c")]);
        assert_eq!(plan.datasource_count(), 3);
        assert!(matches!(plan.merge_strategy, MergeStrategy::Append));
    }

    #[test]
    fn test_join() {
        let plan = MergedPlan::join(sub("left"), sub("right"));
        assert_eq!(plan.datasource_count(), 2);
        assert!(matches!(plan.merge_strategy, MergeStrategy::HashJoin));
    }

    #[test]
    fn test_single_source() {
        let plan = MergedPlan::union(vec![sub("only")]);
        assert_eq!(plan.datasource_count(), 1);
    }
}
