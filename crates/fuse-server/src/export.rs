// SPDX-License-Identifier: Apache-2.0

//! Query result export — CSV and JSON download formats.
//!
//! Converts query results (columns + rows) into downloadable formats.
//! Used by `GET /api/fuse/query/export?format=csv&job_id=...`

use serde_json;

/// Export query results as CSV.
pub fn to_csv(columns: &[String], rows: &[Vec<serde_json::Value>]) -> String {
    let mut out = columns.join(",");
    out.push('\n');
    for row in rows {
        let line: Vec<String> = row.iter().map(csv_escape).collect();
        out.push_str(&line.join(","));
        out.push('\n');
    }
    out
}

/// Export query results as NDJSON (one JSON object per line).
pub fn to_ndjson(columns: &[String], rows: &[Vec<serde_json::Value>]) -> String {
    let mut out = String::new();
    for row in rows {
        let mut map = serde_json::Map::new();
        for (i, col) in columns.iter().enumerate() {
            map.insert(col.clone(), row.get(i).cloned().unwrap_or(serde_json::Value::Null));
        }
        out.push_str(&serde_json::Value::Object(map).to_string());
        out.push('\n');
    }
    out
}

fn csv_escape(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "".into(),
        serde_json::Value::String(s) => {
            // Prefix formula-triggering characters to prevent CSV injection
            // when opened in spreadsheet applications (Excel, Sheets)
            let safe = if s.starts_with('=') || s.starts_with('+')
                || s.starts_with('-') || s.starts_with('@')
                || s.starts_with('\t') || s.starts_with('\r')
            {
                format!("'{}", s)
            } else {
                s.clone()
            };
            if safe.contains(',') || safe.contains('"') || safe.contains('\n') {
                format!("\"{}\"", safe.replace('"', "\"\""))
            } else {
                safe
            }
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_csv_simple() {
        let cols = vec!["id".into(), "name".into()];
        let rows = vec![
            vec![json!(1), json!("alice")],
            vec![json!(2), json!("bob")],
        ];
        let csv = to_csv(&cols, &rows);
        assert_eq!(csv, "id,name\n1,alice\n2,bob\n");
    }

    #[test]
    fn test_csv_escape_comma() {
        let cols = vec!["msg".into()];
        let rows = vec![vec![json!("hello, world")]];
        let csv = to_csv(&cols, &rows);
        assert!(csv.contains("\"hello, world\""));
    }

    #[test]
    fn test_csv_null() {
        let cols = vec!["x".into()];
        let rows = vec![vec![json!(null)]];
        assert_eq!(to_csv(&cols, &rows), "x\n\n");
    }

    #[test]
    fn test_ndjson() {
        let cols = vec!["id".into(), "name".into()];
        let rows = vec![vec![json!(1), json!("alice")]];
        let ndjson = to_ndjson(&cols, &rows);
        let parsed: serde_json::Value = serde_json::from_str(ndjson.trim()).unwrap();
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["name"], "alice");
    }

    #[test]
    fn test_ndjson_multiple_rows() {
        let cols = vec!["x".into()];
        let rows = vec![vec![json!(1)], vec![json!(2)]];
        let ndjson = to_ndjson(&cols, &rows);
        let lines: Vec<&str> = ndjson.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_csv_empty() {
        assert_eq!(to_csv(&["a".into()], &[]), "a\n");
    }

    #[test]
    fn test_csv_formula_injection_equals() {
        let cols = vec!["x".into()];
        let rows = vec![vec![json!("=HYPERLINK(\"http://evil\")")]];
        let csv = to_csv(&cols, &rows);
        assert!(csv.contains("'="), "= formula must be quote-prefixed");
    }

    #[test]
    fn test_csv_formula_injection_plus() {
        let csv = to_csv(&["x".into()], &[vec![json!("+cmd|'/C calc'")]]);
        assert!(csv.contains("'+"), "+ formula must be quote-prefixed");
    }

    #[test]
    fn test_csv_formula_injection_minus() {
        let csv = to_csv(&["x".into()], &[vec![json!("-1+1")]]);
        assert!(csv.contains("'-"), "- formula must be quote-prefixed");
    }

    #[test]
    fn test_csv_formula_injection_at() {
        let csv = to_csv(&["x".into()], &[vec![json!("@SUM(A1)")]]);
        assert!(csv.contains("'@"), "@ formula must be quote-prefixed");
    }

    #[test]
    fn test_csv_safe_string_not_prefixed() {
        let csv = to_csv(&["x".into()], &[vec![json!("hello")]]);
        assert!(!csv.contains("'hello"), "safe strings must not be prefixed");
    }
}
