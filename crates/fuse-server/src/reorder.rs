// SPDX-License-Identifier: Apache-2.0
//! Column reorder — rearrange columns in query results.

use serde_json::Value;

/// Reorder columns to match a desired order.
pub fn reorder(
    columns: &[String],
    rows: &[Vec<Value>],
    desired_order: &[String],
) -> (Vec<String>, Vec<Vec<Value>>) {
    let indices: Vec<Option<usize>> = desired_order.iter()
        .map(|d| columns.iter().position(|c| c == d))
        .collect();

    let new_cols: Vec<String> = indices.iter().enumerate()
        .filter_map(|(i, idx)| idx.map(|_| desired_order[i].clone()))
        .collect();

    let valid_indices: Vec<usize> = indices.into_iter().flatten().collect();

    let new_rows: Vec<Vec<Value>> = rows.iter().map(|row| {
        valid_indices.iter().map(|&i| row.get(i).cloned().unwrap_or(Value::Null)).collect()
    }).collect();

    (new_cols, new_rows)
}

/// Move a column to a specific position.
pub fn move_column(columns: &[String], from: &str, to_pos: usize) -> Vec<String> {
    let mut cols: Vec<String> = columns.iter().filter(|c| c.as_str() != from).cloned().collect();
    let pos = to_pos.min(cols.len());
    cols.insert(pos, from.to_string());
    cols
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_reorder() {
        let cols = vec!["a".into(), "b".into(), "c".into()];
        let rows = vec![vec![json!(1), json!(2), json!(3)]];
        let (new_cols, new_rows) = reorder(&cols, &rows, &["c".into(), "a".into()]);
        assert_eq!(new_cols, vec!["c", "a"]);
        assert_eq!(new_rows[0], vec![json!(3), json!(1)]);
    }

    #[test]
    fn test_reorder_missing() {
        let cols = vec!["a".into(), "b".into()];
        let rows = vec![vec![json!(1), json!(2)]];
        let (new_cols, _) = reorder(&cols, &rows, &["b".into(), "missing".into(), "a".into()]);
        assert_eq!(new_cols, vec!["b", "a"]);
    }

    #[test]
    fn test_move_column() {
        let cols = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(move_column(&cols, "c", 0), vec!["c", "a", "b"]);
    }

    #[test]
    fn test_move_column_end() {
        let cols = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(move_column(&cols, "a", 99), vec!["b", "c", "a"]);
    }
}
