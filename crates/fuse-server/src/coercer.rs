// SPDX-License-Identifier: Apache-2.0
//! Type coercion — align types across datasources for UNION/JOIN.

use serde_json::Value;

/// Coerce a value to string.
pub fn to_string(v: &Value) -> Value {
    match v {
        Value::String(_) => v.clone(),
        Value::Null => Value::Null,
        _ => Value::String(v.to_string()),
    }
}

/// Coerce a value to number (f64).
pub fn to_number(v: &Value) -> Value {
    match v {
        Value::Number(_) => v.clone(),
        Value::String(s) => s.parse::<f64>().ok()
            .map(|n| serde_json::Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),
        Value::Bool(b) => Value::Number(if *b { 1 } else { 0 }.into()),
        _ => Value::Null,
    }
}

/// Coerce an entire column to a target type.
pub fn coerce_column(rows: &mut [Vec<Value>], col_idx: usize, target: &str) {
    for row in rows.iter_mut() {
        if let Some(val) = row.get(col_idx).cloned() {
            let coerced = match target {
                "string" => to_string(&val),
                "number" => to_number(&val),
                _ => val,
            };
            if col_idx < row.len() { row[col_idx] = coerced; }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_to_string() {
        assert_eq!(to_string(&json!(42)), json!("42"));
        assert_eq!(to_string(&json!("hello")), json!("hello"));
        assert_eq!(to_string(&json!(null)), json!(null));
    }

    #[test]
    fn test_to_number() {
        assert_eq!(to_number(&json!("42")), json!(42.0));
        assert_eq!(to_number(&json!(42)), json!(42));
        assert_eq!(to_number(&json!("abc")), json!(null));
        assert_eq!(to_number(&json!(true)), json!(1));
    }

    #[test]
    fn test_coerce_column() {
        let mut rows = vec![vec![json!(1), json!(2)], vec![json!(3), json!(4)]];
        coerce_column(&mut rows, 0, "string");
        assert_eq!(rows[0][0], json!("1"));
        assert_eq!(rows[1][0], json!("3"));
    }

    #[test]
    fn test_coerce_to_number() {
        let mut rows = vec![vec![json!("10")], vec![json!("20")]];
        coerce_column(&mut rows, 0, "number");
        assert_eq!(rows[0][0], json!(10.0));
    }
}
