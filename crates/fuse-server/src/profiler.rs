// SPDX-License-Identifier: Apache-2.0
//! Data profiler — compact per-column summary for quick data understanding.

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: String,
    pub non_null_count: usize,
    pub null_pct: f64,
    pub unique_count: usize,
    pub sample_values: Vec<Value>,
}

/// Profile all columns in a result set.
pub fn profile(columns: &[String], rows: &[Vec<Value>]) -> Vec<ColumnProfile> {
    columns.iter().enumerate().map(|(i, name)| {
        let values: Vec<&Value> = rows.iter().filter_map(|r| r.get(i)).collect();
        let non_null: Vec<&Value> = values.iter().filter(|v| !v.is_null()).copied().collect();
        let null_count = values.len() - non_null.len();
        let null_pct = if values.is_empty() { 0.0 } else { null_count as f64 / values.len() as f64 * 100.0 };

        let mut unique = std::collections::HashSet::new();
        for v in &non_null { unique.insert(v.to_string()); }

        let data_type = non_null.first().map(|v| match v {
            Value::String(_) => "string",
            Value::Number(n) if n.is_i64() => "integer",
            Value::Number(_) => "float",
            Value::Bool(_) => "boolean",
            _ => "unknown",
        }).unwrap_or("null").to_string();

        let sample: Vec<Value> = non_null.iter().take(3).map(|v| (*v).clone()).collect();

        ColumnProfile { name: name.clone(), data_type, non_null_count: non_null.len(), null_pct, unique_count: unique.len(), sample_values: sample }
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_profile() {
        let cols = vec!["name".into(), "age".into()];
        let rows = vec![
            vec![json!("alice"), json!(30)],
            vec![json!("bob"), json!(null)],
            vec![json!("alice"), json!(25)],
        ];
        let profiles = profile(&cols, &rows);
        assert_eq!(profiles[0].data_type, "string");
        assert_eq!(profiles[0].unique_count, 2);
        assert_eq!(profiles[1].null_pct.round(), 33.0);
    }

    #[test]
    fn test_empty() {
        let profiles = profile(&["x".into()], &[]);
        assert_eq!(profiles[0].non_null_count, 0);
    }

    #[test]
    fn test_all_null() {
        let rows = vec![vec![json!(null)], vec![json!(null)]];
        let profiles = profile(&["x".into()], &rows);
        assert_eq!(profiles[0].data_type, "null");
        assert_eq!(profiles[0].null_pct, 100.0);
    }
}
