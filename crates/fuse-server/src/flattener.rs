// SPDX-License-Identifier: Apache-2.0
//! Result flattener — flatten nested JSON into flat columns.

use serde_json::Value;
use std::collections::BTreeMap;

/// Flatten a nested JSON value into dot-separated key-value pairs.
pub fn flatten_value(prefix: &str, value: &Value, out: &mut BTreeMap<String, Value>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let key = if prefix.is_empty() { k.clone() } else { format!("{}.{}", prefix, k) };
                flatten_value(&key, v, out);
            }
        }
        _ => { out.insert(prefix.to_string(), value.clone()); }
    }
}

/// Flatten rows of JSON objects into flat column rows.
pub fn flatten_rows(rows: &[Value]) -> (Vec<String>, Vec<Vec<Value>>) {
    // First pass: collect all column names
    let mut all_keys = BTreeMap::new();
    let mut flat_rows: Vec<BTreeMap<String, Value>> = Vec::new();
    for row in rows {
        let mut flat = BTreeMap::new();
        flatten_value("", row, &mut flat);
        for k in flat.keys() { all_keys.insert(k.clone(), ()); }
        flat_rows.push(flat);
    }
    let columns: Vec<String> = all_keys.keys().cloned().collect();
    let result: Vec<Vec<Value>> = flat_rows.iter().map(|flat| {
        columns.iter().map(|c| flat.get(c).cloned().unwrap_or(Value::Null)).collect()
    }).collect();
    (columns, result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_flatten_simple() {
        let mut out = BTreeMap::new();
        flatten_value("", &json!({"a": 1, "b": "x"}), &mut out);
        assert_eq!(out["a"], json!(1));
        assert_eq!(out["b"], json!("x"));
    }

    #[test]
    fn test_flatten_nested() {
        let mut out = BTreeMap::new();
        flatten_value("", &json!({"user": {"name": "alice", "age": 30}}), &mut out);
        assert_eq!(out["user.name"], json!("alice"));
        assert_eq!(out["user.age"], json!(30));
    }

    #[test]
    fn test_flatten_rows() {
        let rows = vec![
            json!({"id": 1, "meta": {"source": "web"}}),
            json!({"id": 2, "meta": {"source": "api"}, "extra": true}),
        ];
        let (cols, result) = flatten_rows(&rows);
        assert!(cols.contains(&"meta.source".to_string()));
        assert_eq!(result.len(), 2);
        // Row 1 has no "extra" → null
        let extra_idx = cols.iter().position(|c| c == "extra").unwrap();
        assert_eq!(result[0][extra_idx], json!(null));
    }

    #[test]
    fn test_flatten_empty() {
        let (cols, rows) = flatten_rows(&[]);
        assert!(cols.is_empty());
        assert!(rows.is_empty());
    }
}
