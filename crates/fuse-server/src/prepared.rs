// SPDX-License-Identifier: Apache-2.0

//! Prepared statement support: PREPARE/EXECUTE with positional parameter binding.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A prepared statement: query template with $N parameter placeholders.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub query: String,
    pub param_count: usize,
}

/// Thread-safe store for prepared statements.
pub type PreparedStatementStore = Arc<Mutex<HashMap<String, PreparedStatement>>>;

/// Create a new empty prepared statement store.
pub fn new_store() -> PreparedStatementStore {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Parse `PREPARE <name> AS <query>`. Returns (name, query_template).
pub fn parse_prepare(query: &str) -> Option<(String, String)> {
    let trimmed = query.trim();
    let len = trimmed.len();
    if len < 8 || !trimmed[..8].eq_ignore_ascii_case("prepare ") {
        return None;
    }
    let rest = trimmed[8..].trim();
    let lower_rest = rest.to_lowercase();
    let as_pos = lower_rest.find(" as ")?;
    let name = rest[..as_pos].trim().to_string();
    let template = rest[as_pos + 4..].trim().to_string();
    if name.is_empty() || template.is_empty() {
        return None;
    }
    Some((name, template))
}

/// Parse `EXECUTE <name> USING <val1>, <val2>, ...` or `EXECUTE <name>`.
pub fn parse_execute(query: &str) -> Option<(String, Vec<serde_json::Value>)> {
    let trimmed = query.trim();
    let len = trimmed.len();
    if len < 8 || !trimmed[..8].eq_ignore_ascii_case("execute ") {
        return None;
    }
    let rest = trimmed[8..].trim();
    let lower_rest = rest.to_lowercase();
    let (name, params) = if let Some(pos) = lower_rest.find(" using ") {
        let name = rest[..pos].trim().to_string();
        let params_str = rest[pos + 7..].trim();
        let params: Vec<serde_json::Value> = params_str
            .split(',')
            .map(|s| parse_param_value(s.trim()))
            .collect();
        (name, params)
    } else {
        (rest.trim_end_matches(';').trim().to_string(), vec![])
    };
    if name.is_empty() {
        return None;
    }
    Some((name, params))
}

fn parse_param_value(s: &str) -> serde_json::Value {
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        serde_json::Value::String(s[1..s.len() - 1].to_string())
    } else if s.eq_ignore_ascii_case("null") {
        serde_json::Value::Null
    } else if s.eq_ignore_ascii_case("true") {
        serde_json::Value::Bool(true)
    } else if s.eq_ignore_ascii_case("false") {
        serde_json::Value::Bool(false)
    } else if let Ok(n) = s.parse::<i64>() {
        serde_json::json!(n)
    } else if let Ok(n) = s.parse::<f64>() {
        serde_json::json!(n)
    } else {
        serde_json::Value::String(s.to_string())
    }
}

/// Count `$1`, `$2`, ... positional placeholders. Returns the highest index.
pub fn count_params(query: &str) -> usize {
    let bytes = query.as_bytes();
    let mut max_param = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start {
                if let Ok(n) = query[start..end].parse::<usize>() {
                    if n > max_param {
                        max_param = n;
                    }
                }
            }
            i = end;
        } else {
            i += 1;
        }
    }
    max_param
}

/// Bind positional parameters ($1, $2, ...) into a query template.
/// Replaces in reverse order so $1 doesn't match inside $10.
pub fn bind_positional(template: &str, params: &[serde_json::Value]) -> String {
    let mut result = template.to_string();
    for i in (0..params.len()).rev() {
        let placeholder = format!("${}", i + 1);
        let replacement = match &params[i] {
            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => "NULL".to_string(),
            other => format!("'{}'", other.to_string().replace('\'', "''")),
        };
        result = result.replace(&placeholder, &replacement);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_prepare_basic() {
        let (name, query) =
            parse_prepare("PREPARE my_stmt AS SELECT * FROM ds.t WHERE id = $1").unwrap();
        assert_eq!(name, "my_stmt");
        assert_eq!(query, "SELECT * FROM ds.t WHERE id = $1");
    }

    #[test]
    fn test_parse_prepare_case_insensitive() {
        let (name, _) = parse_prepare("prepare Foo AS SELECT 1").unwrap();
        assert_eq!(name, "Foo");
    }

    #[test]
    fn test_parse_prepare_rejects_invalid() {
        assert!(parse_prepare("PREPARE  AS ").is_none());
        assert!(parse_prepare("SELECT 1").is_none());
        assert!(parse_prepare("").is_none());
    }

    #[test]
    fn test_parse_execute_with_params() {
        let (name, params) =
            parse_execute("EXECUTE my_stmt USING 'hello', 42, true, null").unwrap();
        assert_eq!(name, "my_stmt");
        assert_eq!(params.len(), 4);
        assert_eq!(params[0], serde_json::Value::String("hello".into()));
        assert_eq!(params[1], serde_json::json!(42));
        assert_eq!(params[2], serde_json::Value::Bool(true));
        assert!(params[3].is_null());
    }

    #[test]
    fn test_parse_execute_no_params() {
        let (name, params) = parse_execute("EXECUTE my_stmt").unwrap();
        assert_eq!(name, "my_stmt");
        assert!(params.is_empty());
    }

    #[test]
    fn test_parse_execute_semicolon() {
        let (name, params) = parse_execute("EXECUTE my_stmt;").unwrap();
        assert_eq!(name, "my_stmt");
        assert!(params.is_empty());
    }

    #[test]
    fn test_parse_execute_rejects_non_execute() {
        assert!(parse_execute("SELECT 1").is_none());
        assert!(parse_execute("").is_none());
    }

    #[test]
    fn test_parse_execute_float_param() {
        let (_, params) = parse_execute("EXECUTE s USING 2.72").unwrap();
        assert_eq!(params[0], serde_json::json!(2.72));
    }

    #[test]
    fn test_count_params() {
        assert_eq!(count_params("SELECT * FROM t WHERE a = $1 AND b = $2"), 2);
        assert_eq!(count_params("SELECT * FROM t"), 0);
        assert_eq!(count_params("$3 and $1"), 3);
    }

    #[test]
    fn test_bind_positional_basic() {
        let result = bind_positional(
            "SELECT * FROM t WHERE id = $1 AND name = $2",
            &[serde_json::json!(42), serde_json::json!("alice")],
        );
        assert_eq!(result, "SELECT * FROM t WHERE id = 42 AND name = 'alice'");
    }

    #[test]
    fn test_bind_positional_escapes_quotes() {
        let result = bind_positional(
            "SELECT * FROM t WHERE name = $1",
            &[serde_json::json!("O'Brien")],
        );
        assert_eq!(result, "SELECT * FROM t WHERE name = 'O''Brien'");
    }

    #[test]
    fn test_bind_positional_null_and_bool() {
        let result = bind_positional(
            "SELECT * FROM t WHERE a = $1 AND b = $2",
            &[serde_json::Value::Null, serde_json::Value::Bool(false)],
        );
        assert_eq!(result, "SELECT * FROM t WHERE a = NULL AND b = false");
    }

    #[test]
    fn test_bind_no_collision_10_params() {
        let result = bind_positional(
            "SELECT $1, $10",
            &[
                serde_json::json!("a"),
                serde_json::json!("b"),
                serde_json::json!("c"),
                serde_json::json!("d"),
                serde_json::json!("e"),
                serde_json::json!("f"),
                serde_json::json!("g"),
                serde_json::json!("h"),
                serde_json::json!("i"),
                serde_json::json!("j"),
            ],
        );
        assert_eq!(result, "SELECT 'a', 'j'");
    }

    #[test]
    fn test_store_roundtrip() {
        let store = new_store();
        let stmt = PreparedStatement {
            query: "SELECT $1".into(),
            param_count: 1,
        };
        store.lock().unwrap().insert("test".into(), stmt);
        let retrieved = store.lock().unwrap().get("test").cloned().unwrap();
        assert_eq!(retrieved.query, "SELECT $1");
        assert_eq!(retrieved.param_count, 1);
    }
}
