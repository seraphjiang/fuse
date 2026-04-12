// SPDX-License-Identifier: Apache-2.0
//! Plan printer — human-readable query plan output.

use crate::plan_builder::PlanOp;

/// Print a plan tree as indented text.
pub fn print_plan(op: &PlanOp, indent: usize) -> String {
    let prefix = "  ".repeat(indent);
    let line = match op {
        PlanOp::Scan { datasource, table } => format!("{}Scan: {}.{}", prefix, datasource, table),
        PlanOp::Filter { predicate, .. } => format!("{}Filter: {}", prefix, predicate),
        PlanOp::Project { columns, .. } => format!("{}Project: [{}]", prefix, columns.join(", ")),
        PlanOp::Sort { keys, .. } => {
            let k: Vec<String> = keys
                .iter()
                .map(|(c, d)| format!("{} {}", c, if *d { "DESC" } else { "ASC" }))
                .collect();
            format!("{}Sort: {}", prefix, k.join(", "))
        }
        PlanOp::Limit { count, .. } => format!("{}Limit: {}", prefix, count),
    };

    let child = match op {
        PlanOp::Filter { input, .. }
        | PlanOp::Project { input, .. }
        | PlanOp::Sort { input, .. }
        | PlanOp::Limit { input, .. } => Some(print_plan(input, indent + 1)),
        PlanOp::Scan { .. } => None,
    };

    match child {
        Some(c) => format!("{}\n{}", line, c),
        None => line,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_builder::PlanBuilder;
    use crate::predicate::Predicate;

    #[test]
    fn test_simple_scan() {
        let plan = PlanBuilder::scan("pg", "users").build();
        let output = print_plan(&plan.root, 0);
        assert_eq!(output, "Scan: pg.users");
    }

    #[test]
    fn test_nested_plan() {
        let plan = PlanBuilder::scan("pg", "users")
            .filter(&Predicate::eq("active", "true"))
            .project(vec!["name", "email"])
            .limit(10)
            .build();
        let output = print_plan(&plan.root, 0);
        assert!(output.contains("Limit: 10"));
        assert!(output.contains("Project: [name, email]"));
        assert!(output.contains("Filter:"));
        assert!(output.contains("Scan: pg.users"));
    }

    #[test]
    fn test_indentation() {
        let plan = PlanBuilder::scan("ds", "t").limit(5).build();
        let output = print_plan(&plan.root, 0);
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[1].starts_with("  ")); // indented child
    }
}
