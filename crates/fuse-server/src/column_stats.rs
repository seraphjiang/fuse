// SPDX-License-Identifier: Apache-2.0
//! Column statistics — compute min/max/count/nulls per column.

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct ColumnStats {
    pub name: String,
    pub count: usize,
    pub null_count: usize,
    pub distinct_approx: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max: Option<Value>,
}

/// Compute statistics for each column.
pub fn compute_stats(columns: &[String], rows: &[Vec<Value>]) -> Vec<ColumnStats> {
    columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let values: Vec<&Value> = rows.iter().filter_map(|r| r.get(i)).collect();
            let null_count = values.iter().filter(|v| v.is_null()).count();
            let non_null: Vec<&Value> = values.iter().filter(|v| !v.is_null()).copied().collect();

            let mut seen = std::collections::HashSet::new();
            for v in &non_null {
                seen.insert(v.to_string());
            }

            let (min, max) = if non_null.is_empty() {
                (None, None)
            } else {
                let strs: Vec<String> = non_null
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => s.clone(),
                        _ => v.to_string(),
                    })
                    .collect();
                let min = strs.iter().min().cloned().map(Value::String);
                let max = strs.iter().max().cloned().map(Value::String);
                (min, max)
            };

            ColumnStats {
                name: name.clone(),
                count: values.len(),
                null_count,
                distinct_approx: seen.len(),
                min,
                max,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_stats() {
        let cols = vec!["name".into(), "age".into()];
        let rows = vec![
            vec![json!("alice"), json!(30)],
            vec![json!("bob"), json!(25)],
            vec![json!("alice"), json!(null)],
        ];
        let stats = compute_stats(&cols, &rows);
        assert_eq!(stats[0].count, 3);
        assert_eq!(stats[0].null_count, 0);
        assert_eq!(stats[0].distinct_approx, 2);
        assert_eq!(stats[1].null_count, 1);
    }

    #[test]
    fn test_empty_rows() {
        let cols = vec!["x".into()];
        let stats = compute_stats(&cols, &[]);
        assert_eq!(stats[0].count, 0);
        assert!(stats[0].min.is_none());
    }

    #[test]
    fn test_all_nulls() {
        let cols = vec!["x".into()];
        let rows = vec![vec![json!(null)], vec![json!(null)]];
        let stats = compute_stats(&cols, &rows);
        assert_eq!(stats[0].null_count, 2);
        assert_eq!(stats[0].distinct_approx, 0);
    }
}
