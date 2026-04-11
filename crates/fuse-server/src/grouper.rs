// SPDX-License-Identifier: Apache-2.0
//! Post-execution GROUP BY — group and aggregate results.

use serde_json::Value;
use std::collections::HashMap;

/// Group rows by a column and count per group.
pub fn group_count(rows: &[Vec<Value>], group_col: usize) -> Vec<(Value, u64)> {
    let mut counts: HashMap<String, (Value, u64)> = HashMap::new();
    for row in rows {
        if let Some(val) = row.get(group_col) {
            let key = val.to_string();
            counts.entry(key).or_insert_with(|| (val.clone(), 0)).1 += 1;
        }
    }
    let mut result: Vec<(Value, u64)> = counts.into_values().collect();
    result.sort_by(|a, b| b.1.cmp(&a.1));
    result
}

/// Group rows by a column and sum a numeric column.
pub fn group_sum(rows: &[Vec<Value>], group_col: usize, sum_col: usize) -> Vec<(Value, f64)> {
    let mut sums: HashMap<String, (Value, f64)> = HashMap::new();
    for row in rows {
        if let (Some(key_val), Some(sum_val)) = (row.get(group_col), row.get(sum_col)) {
            let key = key_val.to_string();
            let num = sum_val.as_f64().unwrap_or(0.0);
            let entry = sums.entry(key).or_insert_with(|| (key_val.clone(), 0.0));
            entry.1 += num;
        }
    }
    let mut result: Vec<(Value, f64)> = sums.into_values().collect();
    result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_group_count() {
        let rows = vec![
            vec![json!("a"), json!(1)],
            vec![json!("b"), json!(2)],
            vec![json!("a"), json!(3)],
        ];
        let groups = group_count(&rows, 0);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, json!("a"));
        assert_eq!(groups[0].1, 2);
    }

    #[test]
    fn test_group_sum() {
        let rows = vec![
            vec![json!("x"), json!(10)],
            vec![json!("y"), json!(20)],
            vec![json!("x"), json!(30)],
        ];
        let groups = group_sum(&rows, 0, 1);
        assert_eq!(groups[0].0, json!("x"));
        assert_eq!(groups[0].1, 40.0);
    }

    #[test]
    fn test_empty() {
        assert!(group_count(&[], 0).is_empty());
        assert!(group_sum(&[], 0, 1).is_empty());
    }

    #[test]
    fn test_single_group() {
        let rows = vec![vec![json!("a")], vec![json!("a")], vec![json!("a")]];
        let groups = group_count(&rows, 0);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].1, 3);
    }
}
