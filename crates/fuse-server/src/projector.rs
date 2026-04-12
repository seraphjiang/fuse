// SPDX-License-Identifier: Apache-2.0
//! Column projector — select specific columns from results.

use serde_json::Value;

/// Project (select) specific columns from rows.
pub fn project(
    rows: &[Vec<Value>],
    columns: &[String],
    selected: &[String],
) -> (Vec<String>, Vec<Vec<Value>>) {
    let indices: Vec<usize> = selected
        .iter()
        .filter_map(|s| columns.iter().position(|c| c == s))
        .collect();
    let new_cols: Vec<String> = indices.iter().map(|&i| columns[i].clone()).collect();
    let new_rows: Vec<Vec<Value>> = rows
        .iter()
        .map(|row| {
            indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    (new_cols, new_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_project_subset() {
        let cols = vec!["id".into(), "name".into(), "age".into()];
        let rows = vec![vec![json!(1), json!("alice"), json!(30)]];
        let (new_cols, new_rows) = project(&rows, &cols, &["name".into(), "age".into()]);
        assert_eq!(new_cols, vec!["name", "age"]);
        assert_eq!(new_rows[0], vec![json!("alice"), json!(30)]);
    }

    #[test]
    fn test_project_reorder() {
        let cols = vec!["a".into(), "b".into()];
        let rows = vec![vec![json!(1), json!(2)]];
        let (new_cols, _) = project(&rows, &cols, &["b".into(), "a".into()]);
        assert_eq!(new_cols, vec!["b", "a"]);
    }

    #[test]
    fn test_project_missing_column() {
        let cols = vec!["a".into()];
        let rows = vec![vec![json!(1)]];
        let (new_cols, _) = project(&rows, &cols, &["a".into(), "missing".into()]);
        assert_eq!(new_cols, vec!["a"]); // missing column skipped
    }
}
