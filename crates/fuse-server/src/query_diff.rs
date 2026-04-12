// SPDX-License-Identifier: Apache-2.0

//! Query result diff — compare two result sets for regression detection.
//!
//! Useful for verifying query migrations, connector upgrades, and
//! data pipeline changes. Compares row counts, schema, and values.

use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DiffResult {
    pub schema_match: bool,
    pub row_count_match: bool,
    pub left_rows: usize,
    pub right_rows: usize,
    pub differences: Vec<Difference>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Difference {
    pub row: usize,
    pub column: String,
    pub left: serde_json::Value,
    pub right: serde_json::Value,
}

/// Compare two query results. Returns diff summary.
pub fn diff(
    left_cols: &[String], left_rows: &[Vec<serde_json::Value>],
    right_cols: &[String], right_rows: &[Vec<serde_json::Value>],
    max_diffs: usize,
) -> DiffResult {
    let schema_match = left_cols == right_cols;
    let row_count_match = left_rows.len() == right_rows.len();
    let mut differences = Vec::new();

    let cols = if schema_match { left_cols } else { left_cols }; // use left as reference
    let compare_rows = left_rows.len().min(right_rows.len());

    for row_idx in 0..compare_rows {
        let col_count = left_rows[row_idx].len().min(right_rows[row_idx].len()).min(cols.len());
        for col_idx in 0..col_count {
            if left_rows[row_idx][col_idx] != right_rows[row_idx][col_idx] {
                differences.push(Difference {
                    row: row_idx,
                    column: cols.get(col_idx).cloned().unwrap_or_else(|| format!("col_{}", col_idx)),
                    left: left_rows[row_idx][col_idx].clone(),
                    right: right_rows[row_idx][col_idx].clone(),
                });
                if differences.len() >= max_diffs { break; }
            }
        }
        if differences.len() >= max_diffs { break; }
    }

    DiffResult {
        schema_match,
        row_count_match,
        left_rows: left_rows.len(),
        right_rows: right_rows.len(),
        differences,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_identical_results() {
        let cols = vec!["id".into()];
        let rows = vec![vec![json!(1)], vec![json!(2)]];
        let d = diff(&cols, &rows, &cols, &rows, 100);
        assert!(d.schema_match);
        assert!(d.row_count_match);
        assert!(d.differences.is_empty());
    }

    #[test]
    fn test_value_difference() {
        let cols = vec!["x".into()];
        let left = vec![vec![json!(1)]];
        let right = vec![vec![json!(2)]];
        let d = diff(&cols, &left, &cols, &right, 100);
        assert_eq!(d.differences.len(), 1);
        assert_eq!(d.differences[0].left, json!(1));
        assert_eq!(d.differences[0].right, json!(2));
    }

    #[test]
    fn test_row_count_mismatch() {
        let cols = vec!["x".into()];
        let d = diff(&cols, &[vec![json!(1)]], &cols, &[], 100);
        assert!(!d.row_count_match);
        assert_eq!(d.left_rows, 1);
        assert_eq!(d.right_rows, 0);
    }

    #[test]
    fn test_schema_mismatch() {
        let d = diff(&["a".into()], &[], &["b".into()], &[], 100);
        assert!(!d.schema_match);
    }

    #[test]
    fn test_max_diffs_cap() {
        let cols = vec!["x".into()];
        let left: Vec<Vec<serde_json::Value>> = (0..100).map(|i| vec![json!(i)]).collect();
        let right: Vec<Vec<serde_json::Value>> = (100..200).map(|i| vec![json!(i)]).collect();
        let d = diff(&cols, &left, &cols, &right, 5);
        assert_eq!(d.differences.len(), 5);
    }
}
