// SPDX-License-Identifier: Apache-2.0
//! Result pivot — transform rows into columns (cross-tab).

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Pivot rows: group by `row_col`, spread `pivot_col` values as columns,
/// aggregate `value_col` values.
pub fn pivot(
    rows: &[Vec<Value>],
    row_col: usize,
    pivot_col: usize,
    value_col: usize,
) -> (Vec<String>, Vec<Vec<Value>>) {
    // Collect unique pivot values (column headers)
    let mut pivot_values = BTreeSet::new();
    for row in rows {
        if let Some(v) = row.get(pivot_col) {
            let s = match v {
                Value::String(s) => s.clone(),
                _ => v.to_string(),
            };
            pivot_values.insert(s);
        }
    }
    let pivot_cols: Vec<String> = pivot_values.into_iter().collect();

    // Group by row key
    let mut groups: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
    for row in rows {
        let key = row.get(row_col).map(|v| v.to_string()).unwrap_or_default();
        let pv = row
            .get(pivot_col)
            .map(|v| match v {
                Value::String(s) => s.clone(),
                _ => v.to_string(),
            })
            .unwrap_or_default();
        let val = row.get(value_col).cloned().unwrap_or(Value::Null);
        groups.entry(key).or_default().insert(pv, val);
    }

    // Build output
    let mut columns = vec!["key".to_string()];
    columns.extend(pivot_cols.iter().cloned());

    let result_rows: Vec<Vec<Value>> = groups
        .into_iter()
        .map(|(key, vals)| {
            let mut row = vec![Value::String(key)];
            for pc in &pivot_cols {
                row.push(vals.get(pc).cloned().unwrap_or(Value::Null));
            }
            row
        })
        .collect();

    (columns, result_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_pivot() {
        let rows = vec![
            vec![json!("2024-01"), json!("web"), json!(100)],
            vec![json!("2024-01"), json!("api"), json!(200)],
            vec![json!("2024-02"), json!("web"), json!(150)],
        ];
        let (cols, result) = pivot(&rows, 0, 1, 2);
        assert!(cols.contains(&"web".to_string()));
        assert!(cols.contains(&"api".to_string()));
        assert_eq!(result.len(), 2); // 2 months
    }

    #[test]
    fn test_pivot_missing_values() {
        let rows = vec![
            vec![json!("a"), json!("x"), json!(1)],
            vec![json!("b"), json!("y"), json!(2)],
        ];
        let (_, result) = pivot(&rows, 0, 1, 2);
        // "a" has no "y" value, "b" has no "x" value
        assert_eq!(result[0].len(), 3); // key + x + y
    }

    #[test]
    fn test_empty_pivot() {
        let (cols, rows) = pivot(&[], 0, 1, 2);
        assert_eq!(cols, vec!["key"]);
        assert!(rows.is_empty());
    }
}
