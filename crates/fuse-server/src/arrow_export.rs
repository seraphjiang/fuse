// SPDX-License-Identifier: Apache-2.0
//! Arrow export — convert JSON results to Arrow-compatible format.

use serde::Serialize;
use serde_json::Value;

/// Arrow-compatible column representation.
#[derive(Debug, Clone, Serialize)]
pub struct ArrowColumn {
    pub name: String,
    pub data_type: String,
    pub values: Vec<Value>,
    pub null_count: usize,
}

/// Convert row-oriented results to columnar Arrow-like format.
pub fn to_columnar(columns: &[String], rows: &[Vec<Value>]) -> Vec<ArrowColumn> {
    columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let values: Vec<Value> = rows
                .iter()
                .map(|r| r.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            let null_count = values.iter().filter(|v| v.is_null()).count();
            let data_type = values
                .iter()
                .find(|v| !v.is_null())
                .map(|v| match v {
                    Value::String(_) => "utf8",
                    Value::Number(n) if n.is_i64() => "int64",
                    Value::Number(_) => "float64",
                    Value::Bool(_) => "boolean",
                    _ => "utf8",
                })
                .unwrap_or("null")
                .to_string();

            ArrowColumn {
                name: name.clone(),
                data_type,
                values,
                null_count,
            }
        })
        .collect()
}

/// Convert columnar back to row-oriented.
pub fn to_rows(columns: &[ArrowColumn]) -> Vec<Vec<Value>> {
    if columns.is_empty() {
        return vec![];
    }
    let row_count = columns[0].values.len();
    (0..row_count)
        .map(|i| {
            columns
                .iter()
                .map(|c| c.values.get(i).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_to_columnar() {
        let cols = vec!["id".into(), "name".into()];
        let rows = vec![vec![json!(1), json!("alice")], vec![json!(2), json!("bob")]];
        let arrow = to_columnar(&cols, &rows);
        assert_eq!(arrow.len(), 2);
        assert_eq!(arrow[0].data_type, "int64");
        assert_eq!(arrow[1].data_type, "utf8");
        assert_eq!(arrow[0].null_count, 0);
    }

    #[test]
    fn test_roundtrip() {
        let cols = vec!["x".into()];
        let rows = vec![vec![json!(1)], vec![json!(2)]];
        let columnar = to_columnar(&cols, &rows);
        let back = to_rows(&columnar);
        assert_eq!(back, rows);
    }

    #[test]
    fn test_with_nulls() {
        let cols = vec!["a".into()];
        let rows = vec![vec![json!(1)], vec![json!(null)]];
        let arrow = to_columnar(&cols, &rows);
        assert_eq!(arrow[0].null_count, 1);
    }

    #[test]
    fn test_empty() {
        assert!(to_columnar(&[], &[]).is_empty());
        assert!(to_rows(&[]).is_empty());
    }
}
