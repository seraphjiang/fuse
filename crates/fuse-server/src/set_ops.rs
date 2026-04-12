// SPDX-License-Identifier: Apache-2.0
//! Anti-join and semi-join — set operations on result sets.

use serde_json::Value;
use std::collections::HashSet;

/// Anti-join: rows in left where key doesn't exist in right.
pub fn anti_join(
    left: &[Vec<Value>],
    left_key: usize,
    right: &[Vec<Value>],
    right_key: usize,
) -> Vec<Vec<Value>> {
    let right_keys: HashSet<String> = right
        .iter()
        .filter_map(|r| r.get(right_key).map(|v| v.to_string()))
        .collect();
    left.iter()
        .filter(|row| {
            row.get(left_key)
                .map(|v| !right_keys.contains(&v.to_string()))
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

/// Semi-join: rows in left where key exists in right (no right columns added).
pub fn semi_join(
    left: &[Vec<Value>],
    left_key: usize,
    right: &[Vec<Value>],
    right_key: usize,
) -> Vec<Vec<Value>> {
    let right_keys: HashSet<String> = right
        .iter()
        .filter_map(|r| r.get(right_key).map(|v| v.to_string()))
        .collect();
    left.iter()
        .filter(|row| {
            row.get(left_key)
                .map(|v| right_keys.contains(&v.to_string()))
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_anti_join() {
        let left = vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]];
        let right = vec![vec![json!(2)], vec![json!(4)]];
        let result = anti_join(&left, 0, &right, 0);
        assert_eq!(result.len(), 2); // 1 and 3
    }

    #[test]
    fn test_semi_join() {
        let left = vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]];
        let right = vec![vec![json!(2)], vec![json!(3)]];
        let result = semi_join(&left, 0, &right, 0);
        assert_eq!(result.len(), 2); // 2 and 3
    }

    #[test]
    fn test_anti_join_no_matches() {
        let left = vec![vec![json!(1)]];
        let right = vec![vec![json!(2)]];
        assert_eq!(anti_join(&left, 0, &right, 0).len(), 1);
    }

    #[test]
    fn test_semi_join_all_match() {
        let left = vec![vec![json!(1)], vec![json!(2)]];
        let right = vec![vec![json!(1)], vec![json!(2)]];
        assert_eq!(semi_join(&left, 0, &right, 0).len(), 2);
    }

    #[test]
    fn test_empty() {
        assert!(anti_join(&[], 0, &[], 0).is_empty());
        assert!(semi_join(&[], 0, &[], 0).is_empty());
    }
}
