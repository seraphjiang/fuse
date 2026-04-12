// SPDX-License-Identifier: Apache-2.0
//! Query plan validator — validate plans before execution.

use crate::plan_builder::PlanOp;

/// Validation error in a query plan.
#[derive(Debug, Clone)]
pub struct PlanError {
    pub node: String,
    pub message: String,
}

/// Validate a query plan for common issues.
pub fn validate(op: &PlanOp) -> Vec<PlanError> {
    let mut errors = Vec::new();
    validate_node(op, &mut errors);
    errors
}

fn validate_node(op: &PlanOp, errors: &mut Vec<PlanError>) {
    match op {
        PlanOp::Scan { datasource, table } => {
            if datasource.is_empty() {
                errors.push(PlanError {
                    node: "Scan".into(),
                    message: "empty datasource".into(),
                });
            }
            if table.is_empty() {
                errors.push(PlanError {
                    node: "Scan".into(),
                    message: "empty table".into(),
                });
            }
        }
        PlanOp::Filter { input, predicate } => {
            if predicate.is_empty() {
                errors.push(PlanError {
                    node: "Filter".into(),
                    message: "empty predicate".into(),
                });
            }
            validate_node(input, errors);
        }
        PlanOp::Project { input, columns } => {
            if columns.is_empty() {
                errors.push(PlanError {
                    node: "Project".into(),
                    message: "empty column list".into(),
                });
            }
            validate_node(input, errors);
        }
        PlanOp::Sort { input, keys } => {
            if keys.is_empty() {
                errors.push(PlanError {
                    node: "Sort".into(),
                    message: "empty sort keys".into(),
                });
            }
            validate_node(input, errors);
        }
        PlanOp::Limit { input, count } => {
            if *count == 0 {
                errors.push(PlanError {
                    node: "Limit".into(),
                    message: "limit is zero".into(),
                });
            }
            validate_node(input, errors);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_builder::PlanBuilder;
    use crate::predicate::Predicate;

    #[test]
    fn test_valid_plan() {
        let plan = PlanBuilder::scan("ds", "t")
            .filter(&Predicate::eq("x", "1"))
            .limit(10)
            .build();
        assert!(validate(&plan.root).is_empty());
    }

    #[test]
    fn test_empty_datasource() {
        let plan = PlanBuilder::scan("", "t").build();
        assert_eq!(validate(&plan.root).len(), 1);
    }

    #[test]
    fn test_zero_limit() {
        let plan = PlanBuilder::scan("ds", "t").limit(0).build();
        assert!(validate(&plan.root)
            .iter()
            .any(|e| e.message.contains("zero")));
    }

    #[test]
    fn test_multiple_errors() {
        let plan = PlanBuilder::scan("", "").build();
        assert_eq!(validate(&plan.root).len(), 2);
    }
}
