// SPDX-License-Identifier: Apache-2.0
//! String functions — UPPER, LOWER, TRIM, CONCAT for result columns.

use serde_json::Value;

/// Apply UPPER to a string column.
pub fn upper(rows: &mut [Vec<Value>], col: usize) {
    for row in rows.iter_mut() {
        if let Some(Value::String(s)) = row.get_mut(col) {
            *s = s.to_uppercase();
        }
    }
}

/// Apply LOWER to a string column.
pub fn lower(rows: &mut [Vec<Value>], col: usize) {
    for row in rows.iter_mut() {
        if let Some(Value::String(s)) = row.get_mut(col) {
            *s = s.to_lowercase();
        }
    }
}

/// Apply TRIM to a string column.
pub fn trim(rows: &mut [Vec<Value>], col: usize) {
    for row in rows.iter_mut() {
        if let Some(Value::String(s)) = row.get_mut(col) {
            *s = s.trim().to_string();
        }
    }
}

/// Concatenate two columns into a new value.
pub fn concat_columns(rows: &[Vec<Value>], col_a: usize, col_b: usize, sep: &str) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            let a = row
                .get(col_a)
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    _ => v.to_string(),
                })
                .unwrap_or_default();
            let b = row
                .get(col_b)
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    _ => v.to_string(),
                })
                .unwrap_or_default();
            Value::String(format!("{}{}{}", a, sep, b))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_upper() {
        let mut rows = vec![vec![json!("hello")], vec![json!("world")]];
        upper(&mut rows, 0);
        assert_eq!(rows[0][0], json!("HELLO"));
    }

    #[test]
    fn test_lower() {
        let mut rows = vec![vec![json!("HELLO")]];
        lower(&mut rows, 0);
        assert_eq!(rows[0][0], json!("hello"));
    }

    #[test]
    fn test_trim() {
        let mut rows = vec![vec![json!("  hello  ")]];
        trim(&mut rows, 0);
        assert_eq!(rows[0][0], json!("hello"));
    }

    #[test]
    fn test_concat() {
        let rows = vec![vec![json!("John"), json!("Doe")]];
        let result = concat_columns(&rows, 0, 1, " ");
        assert_eq!(result[0], json!("John Doe"));
    }

    #[test]
    fn test_non_string_unchanged() {
        let mut rows = vec![vec![json!(42)]];
        upper(&mut rows, 0);
        assert_eq!(rows[0][0], json!(42)); // unchanged
    }
}
