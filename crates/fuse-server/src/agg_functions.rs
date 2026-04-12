// SPDX-License-Identifier: Apache-2.0
//! Aggregate functions — SUM, AVG, MIN, MAX, COUNT for result columns.

use serde_json::Value;

/// SUM of a numeric column.
pub fn sum(rows: &[Vec<Value>], col: usize) -> f64 {
    rows.iter().filter_map(|r| r.get(col)?.as_f64()).sum()
}

/// AVG of a numeric column.
pub fn avg(rows: &[Vec<Value>], col: usize) -> Option<f64> {
    let vals: Vec<f64> = rows.iter().filter_map(|r| r.get(col)?.as_f64()).collect();
    if vals.is_empty() {
        None
    } else {
        Some(vals.iter().sum::<f64>() / vals.len() as f64)
    }
}

/// MIN of a numeric column.
pub fn min(rows: &[Vec<Value>], col: usize) -> Option<f64> {
    rows.iter()
        .filter_map(|r| r.get(col)?.as_f64())
        .reduce(f64::min)
}

/// MAX of a numeric column.
pub fn max(rows: &[Vec<Value>], col: usize) -> Option<f64> {
    rows.iter()
        .filter_map(|r| r.get(col)?.as_f64())
        .reduce(f64::max)
}

/// COUNT non-null values in a column.
pub fn count(rows: &[Vec<Value>], col: usize) -> u64 {
    rows.iter()
        .filter(|r| r.get(col).map(|v| !v.is_null()).unwrap_or(false))
        .count() as u64
}

/// COUNT DISTINCT values in a column.
pub fn count_distinct(rows: &[Vec<Value>], col: usize) -> u64 {
    let mut seen = std::collections::HashSet::new();
    for r in rows {
        if let Some(v) = r.get(col) {
            if !v.is_null() {
                seen.insert(v.to_string());
            }
        }
    }
    seen.len() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rows() -> Vec<Vec<Value>> {
        vec![
            vec![json!(10)],
            vec![json!(20)],
            vec![json!(30)],
            vec![json!(null)],
        ]
    }

    #[test]
    fn test_sum() {
        assert_eq!(sum(&rows(), 0), 60.0);
    }

    #[test]
    fn test_avg() {
        assert_eq!(avg(&rows(), 0), Some(20.0));
    }

    #[test]
    fn test_min() {
        assert_eq!(min(&rows(), 0), Some(10.0));
    }

    #[test]
    fn test_max() {
        assert_eq!(max(&rows(), 0), Some(30.0));
    }

    #[test]
    fn test_count() {
        assert_eq!(count(&rows(), 0), 3);
    }

    #[test]
    fn test_count_distinct() {
        let r = vec![
            vec![json!("a")],
            vec![json!("b")],
            vec![json!("a")],
            vec![json!(null)],
        ];
        assert_eq!(count_distinct(&r, 0), 2);
    }

    #[test]
    fn test_empty() {
        assert_eq!(sum(&[], 0), 0.0);
        assert_eq!(avg(&[], 0), None);
        assert_eq!(min(&[], 0), None);
        assert_eq!(max(&[], 0), None);
        assert_eq!(count(&[], 0), 0);
    }

    #[test]
    fn test_all_null() {
        let r = vec![vec![json!(null)], vec![json!(null)]];
        assert_eq!(count(&r, 0), 0);
        assert_eq!(sum(&r, 0), 0.0);
    }

    #[test]
    fn test_single_value() {
        let r = vec![vec![json!(42)]];
        assert_eq!(sum(&r, 0), 42.0);
        assert_eq!(avg(&r, 0), Some(42.0));
        assert_eq!(min(&r, 0), Some(42.0));
        assert_eq!(max(&r, 0), Some(42.0));
    }

    #[test]
    fn test_negative_values() {
        let r = vec![vec![json!(-5)], vec![json!(10)], vec![json!(-3)]];
        assert_eq!(min(&r, 0), Some(-5.0));
        assert_eq!(max(&r, 0), Some(10.0));
        assert_eq!(sum(&r, 0), 2.0);
    }

    #[test]
    fn test_float_precision() {
        let r = vec![vec![json!(0.1)], vec![json!(0.2)]];
        assert!((sum(&r, 0) - 0.3).abs() < 1e-10);
    }
}
