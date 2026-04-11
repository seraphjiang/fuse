// SPDX-License-Identifier: Apache-2.0
//! Null handler — replace nulls with defaults for cleaner output.

use serde_json::Value;

/// Replace null values in a column with a default.
pub fn coalesce(rows: &mut [Vec<Value>], col_idx: usize, default: &Value) {
    for row in rows.iter_mut() {
        if let Some(val) = row.get_mut(col_idx) {
            if val.is_null() {
                *val = default.clone();
            }
        }
    }
}

/// Replace all nulls across all columns with a single default.
pub fn fill_nulls(rows: &mut [Vec<Value>], default: &Value) {
    for row in rows.iter_mut() {
        for val in row.iter_mut() {
            if val.is_null() {
                *val = default.clone();
            }
        }
    }
}

/// Count nulls per column.
pub fn null_counts(rows: &[Vec<Value>], col_count: usize) -> Vec<usize> {
    let mut counts = vec![0usize; col_count];
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i < col_count && val.is_null() {
                counts[i] += 1;
            }
        }
    }
    counts
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_coalesce() {
        let mut rows = vec![vec![json!(null), json!(1)], vec![json!("a"), json!(null)]];
        coalesce(&mut rows, 0, &json!("N/A"));
        assert_eq!(rows[0][0], json!("N/A"));
        assert_eq!(rows[1][0], json!("a")); // unchanged
    }

    #[test]
    fn test_fill_nulls() {
        let mut rows = vec![vec![json!(null), json!(null)], vec![json!(1), json!(null)]];
        fill_nulls(&mut rows, &json!(0));
        assert_eq!(rows[0], vec![json!(0), json!(0)]);
        assert_eq!(rows[1], vec![json!(1), json!(0)]);
    }

    #[test]
    fn test_null_counts() {
        let rows = vec![
            vec![json!(null), json!(1)],
            vec![json!(null), json!(null)],
            vec![json!("a"), json!(2)],
        ];
        assert_eq!(null_counts(&rows, 2), vec![2, 1]);
    }

    #[test]
    fn test_no_nulls() {
        let mut rows = vec![vec![json!(1), json!(2)]];
        coalesce(&mut rows, 0, &json!(0));
        assert_eq!(rows[0][0], json!(1)); // unchanged
    }
}
