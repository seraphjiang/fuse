// SPDX-License-Identifier: Apache-2.0
//! Post-execution hash join — join two result sets by key column.

use serde_json::Value;
use std::collections::HashMap;

/// Inner hash join on key columns.
pub fn hash_join(
    left: &[Vec<Value>],
    left_key: usize,
    right: &[Vec<Value>],
    right_key: usize,
) -> Vec<Vec<Value>> {
    // Build hash table from right side
    let mut table: HashMap<String, Vec<&Vec<Value>>> = HashMap::new();
    for row in right {
        if let Some(key) = row.get(right_key) {
            table.entry(key.to_string()).or_default().push(row);
        }
    }
    // Probe with left side
    let mut result = Vec::new();
    for lrow in left {
        if let Some(key) = lrow.get(left_key) {
            if let Some(matches) = table.get(&key.to_string()) {
                for rrow in matches {
                    let mut combined = lrow.clone();
                    for (i, val) in rrow.iter().enumerate() {
                        if i != right_key {
                            combined.push(val.clone());
                        }
                    }
                    result.push(combined);
                }
            }
        }
    }
    result
}

/// Left outer join — includes unmatched left rows with nulls.
pub fn left_join(
    left: &[Vec<Value>],
    left_key: usize,
    right: &[Vec<Value>],
    right_key: usize,
    right_col_count: usize,
) -> Vec<Vec<Value>> {
    let mut table: HashMap<String, Vec<&Vec<Value>>> = HashMap::new();
    for row in right {
        if let Some(key) = row.get(right_key) {
            table.entry(key.to_string()).or_default().push(row);
        }
    }
    let null_cols = right_col_count.saturating_sub(1); // exclude join key
    let mut result = Vec::new();
    for lrow in left {
        if let Some(key) = lrow.get(left_key) {
            if let Some(matches) = table.get(&key.to_string()) {
                for rrow in matches {
                    let mut combined = lrow.clone();
                    for (i, val) in rrow.iter().enumerate() {
                        if i != right_key {
                            combined.push(val.clone());
                        }
                    }
                    result.push(combined);
                }
            } else {
                let mut combined = lrow.clone();
                combined.extend(std::iter::repeat_n(Value::Null, null_cols));
                result.push(combined);
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_inner_join() {
        let left = vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
        let right = vec![vec![json!(1), json!("x")], vec![json!(3), json!("z")]];
        let result = hash_join(&left, 0, &right, 0);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], json!(1));
        assert_eq!(result[0][2], json!("x"));
    }

    #[test]
    fn test_inner_join_multiple_matches() {
        let left = vec![vec![json!(1), json!("a")]];
        let right = vec![vec![json!(1), json!("x")], vec![json!(1), json!("y")]];
        let result = hash_join(&left, 0, &right, 0);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_left_join() {
        let left = vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
        let right = vec![vec![json!(1), json!("x")]];
        let result = left_join(&left, 0, &right, 0, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[1][2], json!(null)); // unmatched
    }

    #[test]
    fn test_empty_join() {
        let result = hash_join(&[], 0, &[], 0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_no_matches() {
        let left = vec![vec![json!(1)]];
        let right = vec![vec![json!(2)]];
        assert!(hash_join(&left, 0, &right, 0).is_empty());
    }
}
