// SPDX-License-Identifier: Apache-2.0
//! Result aggregator — merge partial results from multiple datasources.

use serde_json::Value;

/// Merge multiple result sets by appending rows (UNION ALL semantics).
pub fn merge_results(results: Vec<(Vec<String>, Vec<Vec<Value>>)>) -> (Vec<String>, Vec<Vec<Value>>) {
    if results.is_empty() {
        return (vec![], vec![]);
    }
    let columns = results[0].0.clone();
    let rows: Vec<Vec<Value>> = results.into_iter().flat_map(|(_, r)| r).collect();
    (columns, rows)
}

/// Merge and deduplicate results (UNION semantics).
pub fn merge_distinct(results: Vec<(Vec<String>, Vec<Vec<Value>>)>) -> (Vec<String>, Vec<Vec<Value>>) {
    let (columns, rows) = merge_results(results);
    let mut seen = std::collections::HashSet::new();
    let deduped: Vec<Vec<Value>> = rows.into_iter().filter(|row| {
        let key = row.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("|");
        seen.insert(key)
    }).collect();
    (columns, deduped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_merge_results() {
        let r1 = (vec!["id".into()], vec![vec![json!(1)], vec![json!(2)]]);
        let r2 = (vec!["id".into()], vec![vec![json!(3)]]);
        let (cols, rows) = merge_results(vec![r1, r2]);
        assert_eq!(cols, vec!["id"]);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_merge_distinct() {
        let r1 = (vec!["id".into()], vec![vec![json!(1)], vec![json!(2)]]);
        let r2 = (vec!["id".into()], vec![vec![json!(2)], vec![json!(3)]]);
        let (_, rows) = merge_distinct(vec![r1, r2]);
        assert_eq!(rows.len(), 3); // 1, 2, 3 (deduped)
    }

    #[test]
    fn test_empty() {
        let (cols, rows) = merge_results(vec![]);
        assert!(cols.is_empty());
        assert!(rows.is_empty());
    }
}
