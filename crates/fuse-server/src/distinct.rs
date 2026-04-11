// SPDX-License-Identifier: Apache-2.0
//! Result distinct — deduplicate rows.

use serde_json::Value;
use std::collections::HashSet;

/// Remove duplicate rows.
pub fn distinct(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    let mut seen = HashSet::new();
    rows.into_iter().filter(|row| {
        let key = row.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("\x00");
        seen.insert(key)
    }).collect()
}

/// Count distinct values in a specific column.
pub fn count_distinct(rows: &[Vec<Value>], col_idx: usize) -> usize {
    let mut seen = HashSet::new();
    for row in rows {
        if let Some(v) = row.get(col_idx) {
            seen.insert(v.to_string());
        }
    }
    seen.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_distinct() {
        let rows = vec![
            vec![json!(1), json!("a")],
            vec![json!(2), json!("b")],
            vec![json!(1), json!("a")], // duplicate
        ];
        assert_eq!(distinct(rows).len(), 2);
    }

    #[test]
    fn test_no_duplicates() {
        let rows = vec![vec![json!(1)], vec![json!(2)]];
        assert_eq!(distinct(rows).len(), 2);
    }

    #[test]
    fn test_count_distinct() {
        let rows = vec![vec![json!("a")], vec![json!("b")], vec![json!("a")]];
        assert_eq!(count_distinct(&rows, 0), 2);
    }

    #[test]
    fn test_empty() {
        assert!(distinct(vec![]).is_empty());
        assert_eq!(count_distinct(&[], 0), 0);
    }
}
