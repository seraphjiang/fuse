// SPDX-License-Identifier: Apache-2.0
//! Row limit enforcer — truncate results with warning metadata.

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct TruncatedResult {
    pub rows: Vec<Vec<Value>>,
    pub total_before_truncation: usize,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// Enforce a row limit on query results.
pub fn enforce_limit(rows: Vec<Vec<Value>>, max_rows: usize) -> TruncatedResult {
    let total = rows.len();
    if total <= max_rows {
        TruncatedResult {
            rows,
            total_before_truncation: total,
            truncated: false,
            warning: None,
        }
    } else {
        let truncated_rows: Vec<Vec<Value>> = rows.into_iter().take(max_rows).collect();
        TruncatedResult {
            rows: truncated_rows,
            total_before_truncation: total,
            truncated: true,
            warning: Some(format!(
                "Result truncated: {} rows returned of {} total (limit: {})",
                max_rows, total, max_rows
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_within_limit() {
        let rows = vec![vec![json!(1)], vec![json!(2)]];
        let r = enforce_limit(rows, 10);
        assert!(!r.truncated);
        assert_eq!(r.rows.len(), 2);
        assert!(r.warning.is_none());
    }

    #[test]
    fn test_exceeds_limit() {
        let rows: Vec<Vec<Value>> = (0..100).map(|i| vec![json!(i)]).collect();
        let r = enforce_limit(rows, 10);
        assert!(r.truncated);
        assert_eq!(r.rows.len(), 10);
        assert_eq!(r.total_before_truncation, 100);
        assert!(r.warning.unwrap().contains("truncated"));
    }

    #[test]
    fn test_exact_limit() {
        let rows = vec![vec![json!(1)]; 5];
        let r = enforce_limit(rows, 5);
        assert!(!r.truncated);
        assert_eq!(r.rows.len(), 5);
    }
}
