// SPDX-License-Identifier: Apache-2.0
//! Plan visitor — traverse and transform plan trees.

use crate::plan_builder::PlanOp;

/// Visit each node in a plan tree, returning collected results.
pub fn visit<T>(op: &PlanOp, f: &dyn Fn(&PlanOp) -> Option<T>) -> Vec<T> {
    let mut results = Vec::new();
    if let Some(v) = f(op) { results.push(v); }
    match op {
        PlanOp::Filter { input, .. }
        | PlanOp::Project { input, .. }
        | PlanOp::Sort { input, .. }
        | PlanOp::Limit { input, .. } => results.extend(visit(input, f)),
        PlanOp::Scan { .. } => {}
    }
    results
}

/// Count nodes in a plan tree.
pub fn node_count(op: &PlanOp) -> usize {
    visit(op, &|_| Some(())).len()
}

/// Extract all datasource names from a plan.
pub fn datasources(op: &PlanOp) -> Vec<String> {
    visit(op, &|node| match node {
        PlanOp::Scan { datasource, .. } => Some(datasource.clone()),
        _ => None,
    })
}

/// Check if plan contains a specific operation type.
pub fn has_filter(op: &PlanOp) -> bool {
    !visit(op, &|node| match node {
        PlanOp::Filter { .. } => Some(()),
        _ => None,
    }).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::Predicate;
    use crate::plan_builder::PlanBuilder;

    #[test]
    fn test_node_count() {
        let plan = PlanBuilder::scan("pg", "users")
            .filter(&Predicate::eq("x", "1"))
            .limit(10)
            .build();
        assert_eq!(node_count(&plan.root), 3);
    }

    #[test]
    fn test_datasources() {
        let plan = PlanBuilder::scan("pg", "users").build();
        assert_eq!(datasources(&plan.root), vec!["pg"]);
    }

    #[test]
    fn test_has_filter() {
        let plan = PlanBuilder::scan("pg", "t")
            .filter(&Predicate::eq("a", "b"))
            .build();
        assert!(has_filter(&plan.root));
    }

    #[test]
    fn test_no_filter() {
        let plan = PlanBuilder::scan("pg", "t").limit(10).build();
        assert!(!has_filter(&plan.root));
    }
}
