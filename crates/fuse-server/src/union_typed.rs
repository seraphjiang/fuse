// SPDX-License-Identifier: Apache-2.0
//! Typed UNION — align schemas before merging result sets.

use serde_json::Value;

/// Align two result sets to a common schema, then merge.
/// Missing columns are filled with null.
pub fn union_aligned(
    left_cols: &[String],
    left_rows: &[Vec<Value>],
    right_cols: &[String],
    right_rows: &[Vec<Value>],
) -> (Vec<String>, Vec<Vec<Value>>) {
    // Build unified column list preserving left order, then right-only
    let mut all_cols = left_cols.to_vec();
    for c in right_cols {
        if !all_cols.contains(c) {
            all_cols.push(c.clone());
        }
    }

    let align = |cols: &[String], rows: &[Vec<Value>]| -> Vec<Vec<Value>> {
        rows.iter()
            .map(|row| {
                all_cols
                    .iter()
                    .map(|c| {
                        cols.iter()
                            .position(|x| x == c)
                            .and_then(|i| row.get(i).cloned())
                            .unwrap_or(Value::Null)
                    })
                    .collect()
            })
            .collect()
    };

    let mut result = align(left_cols, left_rows);
    result.extend(align(right_cols, right_rows));
    (all_cols, result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_same_schema() {
        let cols = vec!["a".into(), "b".into()];
        let left = vec![vec![json!(1), json!(2)]];
        let right = vec![vec![json!(3), json!(4)]];
        let (c, r) = union_aligned(&cols, &left, &cols, &right);
        assert_eq!(c, vec!["a", "b"]);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn test_different_schemas() {
        let left_cols = vec!["a".into(), "b".into()];
        let right_cols = vec!["b".into(), "c".into()];
        let left = vec![vec![json!(1), json!(2)]];
        let right = vec![vec![json!(3), json!(4)]];
        let (cols, rows) = union_aligned(&left_cols, &left, &right_cols, &right);
        assert_eq!(cols, vec!["a", "b", "c"]);
        assert_eq!(rows[0], vec![json!(1), json!(2), json!(null)]); // left: no c
        assert_eq!(rows[1], vec![json!(null), json!(3), json!(4)]); // right: no a
    }

    #[test]
    fn test_empty_right() {
        let cols = vec!["x".into()];
        let left = vec![vec![json!(1)]];
        let (_, rows) = union_aligned(&cols, &left, &cols, &[]);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_both_empty() {
        let (cols, rows) = union_aligned(&["a".into()], &[], &["b".into()], &[]);
        assert_eq!(cols, vec!["a", "b"]);
        assert!(rows.is_empty());
    }
}
