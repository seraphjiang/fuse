// SPDX-License-Identifier: Apache-2.0
//! HAVING filter — post-aggregation row filtering.

use serde_json::Value;

/// Filter aggregated rows where a numeric column meets a threshold.
pub fn having_gte(rows: &[Vec<Value>], col_idx: usize, threshold: f64) -> Vec<Vec<Value>> {
    rows.iter()
        .filter(|row| {
            row.get(col_idx)
                .and_then(|v| v.as_f64())
                .map(|n| n >= threshold)
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

/// Filter aggregated rows where a numeric column is less than threshold.
pub fn having_lt(rows: &[Vec<Value>], col_idx: usize, threshold: f64) -> Vec<Vec<Value>> {
    rows.iter()
        .filter(|row| {
            row.get(col_idx)
                .and_then(|v| v.as_f64())
                .map(|n| n < threshold)
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

/// Filter rows where a column equals a specific value.
pub fn having_eq(rows: &[Vec<Value>], col_idx: usize, value: &Value) -> Vec<Vec<Value>> {
    rows.iter()
        .filter(|row| row.get(col_idx) == Some(value))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_having_gte() {
        let rows = vec![
            vec![json!("a"), json!(10)],
            vec![json!("b"), json!(5)],
            vec![json!("c"), json!(15)],
        ];
        let result = having_gte(&rows, 1, 10.0);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_having_lt() {
        let rows = vec![vec![json!(1)], vec![json!(5)], vec![json!(10)]];
        assert_eq!(having_lt(&rows, 0, 6.0).len(), 2);
    }

    #[test]
    fn test_having_eq() {
        let rows = vec![vec![json!("x")], vec![json!("y")], vec![json!("x")]];
        assert_eq!(having_eq(&rows, 0, &json!("x")).len(), 2);
    }

    #[test]
    fn test_having_empty() {
        assert!(having_gte(&[], 0, 0.0).is_empty());
    }
}
