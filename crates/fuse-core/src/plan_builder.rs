// SPDX-License-Identifier: Apache-2.0
//! Logical plan builder — compose query plans fluently.

use crate::predicate::Predicate;
use serde::Serialize;

/// A logical query plan.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalPlan {
    pub root: PlanOp,
}

#[derive(Debug, Clone, Serialize)]
pub enum PlanOp {
    Scan {
        datasource: String,
        table: String,
    },
    Filter {
        input: Box<PlanOp>,
        predicate: String,
    },
    Project {
        input: Box<PlanOp>,
        columns: Vec<String>,
    },
    Sort {
        input: Box<PlanOp>,
        keys: Vec<(String, bool)>,
    },
    Limit {
        input: Box<PlanOp>,
        count: u64,
    },
}

/// Fluent builder for logical plans.
pub struct PlanBuilder {
    current: PlanOp,
}

impl PlanBuilder {
    pub fn scan(datasource: &str, table: &str) -> Self {
        Self {
            current: PlanOp::Scan {
                datasource: datasource.into(),
                table: table.into(),
            },
        }
    }

    pub fn filter(self, pred: &Predicate) -> Self {
        Self {
            current: PlanOp::Filter {
                input: Box::new(self.current),
                predicate: pred.to_sql(),
            },
        }
    }

    pub fn project(self, columns: Vec<&str>) -> Self {
        Self {
            current: PlanOp::Project {
                input: Box::new(self.current),
                columns: columns.into_iter().map(String::from).collect(),
            },
        }
    }

    pub fn sort(self, key: &str, desc: bool) -> Self {
        Self {
            current: PlanOp::Sort {
                input: Box::new(self.current),
                keys: vec![(key.into(), desc)],
            },
        }
    }

    pub fn limit(self, count: u64) -> Self {
        Self {
            current: PlanOp::Limit {
                input: Box::new(self.current),
                count,
            },
        }
    }

    pub fn build(self) -> LogicalPlan {
        LogicalPlan { root: self.current }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_scan() {
        let plan = PlanBuilder::scan("pg", "users").build();
        assert!(matches!(plan.root, PlanOp::Scan { .. }));
    }

    #[test]
    fn test_filter_project_limit() {
        let pred = Predicate::gt("age", "18");
        let plan = PlanBuilder::scan("pg", "users")
            .filter(&pred)
            .project(vec!["name", "age"])
            .limit(10)
            .build();
        assert!(matches!(plan.root, PlanOp::Limit { .. }));
    }

    #[test]
    fn test_sort() {
        let plan = PlanBuilder::scan("ds", "t")
            .sort("created_at", true)
            .limit(100)
            .build();
        assert!(matches!(plan.root, PlanOp::Limit { .. }));
    }
}
