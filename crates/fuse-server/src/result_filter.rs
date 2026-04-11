// SPDX-License-Identifier: Apache-2.0
//! Post-execution result filter — filter rows that couldn't be pushed down.

use serde_json::Value;

/// Filter predicate.
pub enum FilterOp {
    Eq(usize, Value),
    Neq(usize, Value),
    Gt(usize, f64),
    Gte(usize, f64),
    Lt(usize, f64),
    Lte(usize, f64),
    IsNull(usize),
    IsNotNull(usize),
}

/// Apply filters to rows.
pub fn filter_rows(rows: &[Vec<Value>], filters: &[FilterOp]) -> Vec<Vec<Value>> {
    rows.iter().filter(|row| filters.iter().all(|f| matches_filter(row, f))).cloned().collect()
}

fn matches_filter(row: &[Value], filter: &FilterOp) -> bool {
    match filter {
        FilterOp::Eq(i, v) => row.get(*i).map(|r| r == v).unwrap_or(false),
        FilterOp::Neq(i, v) => row.get(*i).map(|r| r != v).unwrap_or(true),
        FilterOp::Gt(i, n) => row.get(*i).and_then(|v| v.as_f64()).map(|v| v > *n).unwrap_or(false),
        FilterOp::Gte(i, n) => row.get(*i).and_then(|v| v.as_f64()).map(|v| v >= *n).unwrap_or(false),
        FilterOp::Lt(i, n) => row.get(*i).and_then(|v| v.as_f64()).map(|v| v < *n).unwrap_or(false),
        FilterOp::Lte(i, n) => row.get(*i).and_then(|v| v.as_f64()).map(|v| v <= *n).unwrap_or(false),
        FilterOp::IsNull(i) => row.get(*i).map(|v| v.is_null()).unwrap_or(true),
        FilterOp::IsNotNull(i) => row.get(*i).map(|v| !v.is_null()).unwrap_or(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eq_filter() {
        let rows = vec![vec![json!("a"), json!(1)], vec![json!("b"), json!(2)]];
        let result = filter_rows(&rows, &[FilterOp::Eq(0, json!("a"))]);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_gt_filter() {
        let rows = vec![vec![json!(10)], vec![json!(20)], vec![json!(5)]];
        let result = filter_rows(&rows, &[FilterOp::Gt(0, 8.0)]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_null_filter() {
        let rows = vec![vec![json!(null)], vec![json!(1)]];
        assert_eq!(filter_rows(&rows, &[FilterOp::IsNull(0)]).len(), 1);
        assert_eq!(filter_rows(&rows, &[FilterOp::IsNotNull(0)]).len(), 1);
    }

    #[test]
    fn test_combined_filters() {
        let rows = vec![
            vec![json!("a"), json!(10)],
            vec![json!("a"), json!(20)],
            vec![json!("b"), json!(15)],
        ];
        let result = filter_rows(&rows, &[
            FilterOp::Eq(0, json!("a")),
            FilterOp::Gte(1, 15.0),
        ]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][1], json!(20));
    }
}
