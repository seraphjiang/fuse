// SPDX-License-Identifier: Apache-2.0
//! Query result formatting — convert between JSON, CSV, and table formats.

use serde_json::Value;

/// Format query results as CSV.
pub fn to_csv(columns: &[String], rows: &[Vec<Value>]) -> String {
    let mut out = columns.join(",");
    out.push('\n');
    for row in rows {
        let line: Vec<String> = row.iter().map(|v| csv_escape(v)).collect();
        out.push_str(&line.join(","));
        out.push('\n');
    }
    out
}

/// Format query results as a text table.
pub fn to_table(columns: &[String], rows: &[Vec<Value>]) -> String {
    let widths: Vec<usize> = columns.iter().enumerate().map(|(i, col)| {
        let max_data = rows.iter()
            .filter_map(|r| r.get(i))
            .map(|v| value_str(v).len())
            .max()
            .unwrap_or(0);
        col.len().max(max_data).max(3)
    }).collect();

    let mut out = String::new();
    // Header
    for (i, col) in columns.iter().enumerate() {
        if i > 0 { out.push_str(" | "); }
        out.push_str(&format!("{:<width$}", col, width = widths[i]));
    }
    out.push('\n');
    // Separator
    for (i, w) in widths.iter().enumerate() {
        if i > 0 { out.push_str("-+-"); }
        out.push_str(&"-".repeat(*w));
    }
    out.push('\n');
    // Rows
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i > 0 { out.push_str(" | "); }
            out.push_str(&format!("{:<width$}", value_str(val), width = widths[i]));
        }
        out.push('\n');
    }
    out
}

fn csv_escape(v: &Value) -> String {
    let s = value_str(v);
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s
    }
}

fn value_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "NULL".to_string(),
        _ => v.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_to_csv() {
        let cols = vec!["name".into(), "age".into()];
        let rows = vec![vec![json!("alice"), json!(30)], vec![json!("bob"), json!(25)]];
        let csv = to_csv(&cols, &rows);
        assert!(csv.starts_with("name,age\n"));
        assert!(csv.contains("alice,30"));
    }

    #[test]
    fn test_csv_escape() {
        assert_eq!(csv_escape(&json!("hello, world")), "\"hello, world\"");
        assert_eq!(csv_escape(&json!("simple")), "simple");
    }

    #[test]
    fn test_to_table() {
        let cols = vec!["id".into(), "name".into()];
        let rows = vec![vec![json!(1), json!("alice")]];
        let table = to_table(&cols, &rows);
        assert!(table.contains("id"));
        assert!(table.contains("alice"));
        assert!(table.contains("---"));
    }

    #[test]
    fn test_null_values() {
        let cols = vec!["x".into()];
        let rows = vec![vec![json!(null)]];
        assert!(to_csv(&cols, &rows).contains("NULL"));
    }
}
