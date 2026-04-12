// SPDX-License-Identifier: Apache-2.0
//! Result intersect — find common rows between result sets.

use serde_json::Value;
use std::collections::HashSet;

/// Return rows that exist in both left and right (INTERSECT).
pub fn intersect(left: &[Vec<Value>], right: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let right_keys: HashSet<String> = right
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("\x00")
        })
        .collect();
    left.iter()
        .filter(|row| {
            let key = row
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("\x00");
            right_keys.contains(&key)
        })
        .cloned()
        .collect()
}

/// Return rows in left that don't exist in right (EXCEPT/MINUS).
pub fn except(left: &[Vec<Value>], right: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let right_keys: HashSet<String> = right
        .iter()
        .map(|r| {
            r.iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("\x00")
        })
        .collect();
    left.iter()
        .filter(|row| {
            let key = row
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("\x00");
            !right_keys.contains(&key)
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_intersect() {
        let left = vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]];
        let right = vec![vec![json!(2)], vec![json!(3)], vec![json!(4)]];
        let result = intersect(&left, &right);
        assert_eq!(result.len(), 2); // 2 and 3
    }

    #[test]
    fn test_except() {
        let left = vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]];
        let right = vec![vec![json!(2)]];
        let result = except(&left, &right);
        assert_eq!(result.len(), 2); // 1 and 3
    }

    #[test]
    fn test_no_overlap() {
        let left = vec![vec![json!(1)]];
        let right = vec![vec![json!(2)]];
        assert!(intersect(&left, &right).is_empty());
        assert_eq!(except(&left, &right).len(), 1);
    }

    #[test]
    fn test_empty() {
        assert!(intersect(&[], &[]).is_empty());
        assert!(except(&[], &[]).is_empty());
    }
}
