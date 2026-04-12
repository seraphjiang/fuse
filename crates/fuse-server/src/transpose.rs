// SPDX-License-Identifier: Apache-2.0
//! Result transpose — swap rows and columns.

use serde_json::Value;

/// Transpose a result set: rows become columns, columns become rows.
pub fn transpose(columns: &[String], rows: &[Vec<Value>]) -> (Vec<String>, Vec<Vec<Value>>) {
    if rows.is_empty() || columns.is_empty() {
        return (vec!["column".into()], vec![]);
    }
    // New columns: "column", "row_1", "row_2", ...
    let mut new_cols = vec!["column".to_string()];
    for i in 0..rows.len() {
        new_cols.push(format!("row_{}", i + 1));
    }
    // Each original column becomes a row
    let new_rows: Vec<Vec<Value>> = columns
        .iter()
        .enumerate()
        .map(|(col_idx, col_name)| {
            let mut row = vec![Value::String(col_name.clone())];
            for data_row in rows {
                row.push(data_row.get(col_idx).cloned().unwrap_or(Value::Null));
            }
            row
        })
        .collect();
    (new_cols, new_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_transpose() {
        let cols = vec!["name".into(), "age".into()];
        let rows = vec![
            vec![json!("alice"), json!(30)],
            vec![json!("bob"), json!(25)],
        ];
        let (new_cols, new_rows) = transpose(&cols, &rows);
        assert_eq!(new_cols, vec!["column", "row_1", "row_2"]);
        assert_eq!(new_rows.len(), 2); // 2 original columns
        assert_eq!(new_rows[0][0], json!("name"));
        assert_eq!(new_rows[0][1], json!("alice"));
        assert_eq!(new_rows[0][2], json!("bob"));
    }

    #[test]
    fn test_transpose_single_row() {
        let cols = vec!["x".into(), "y".into()];
        let rows = vec![vec![json!(1), json!(2)]];
        let (new_cols, new_rows) = transpose(&cols, &rows);
        assert_eq!(new_cols, vec!["column", "row_1"]);
        assert_eq!(new_rows[0][1], json!(1));
        assert_eq!(new_rows[1][1], json!(2));
    }

    #[test]
    fn test_transpose_empty() {
        let (_, rows) = transpose(&["a".into()], &[]);
        assert!(rows.is_empty());
    }
}
