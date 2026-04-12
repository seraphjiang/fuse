// SPDX-License-Identifier: Apache-2.0
//! Column type inference from JSON values.

use serde_json::Value;

/// Inferred column type.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum InferredType {
    String,
    Integer,
    Float,
    Boolean,
    Null,
    Array,
    Object,
    Mixed,
}

/// Infer the type of a JSON value.
pub fn infer_type(value: &Value) -> InferredType {
    match value {
        Value::String(_) => InferredType::String,
        Value::Number(n) if n.is_i64() || n.is_u64() => InferredType::Integer,
        Value::Number(_) => InferredType::Float,
        Value::Bool(_) => InferredType::Boolean,
        Value::Null => InferredType::Null,
        Value::Array(_) => InferredType::Array,
        Value::Object(_) => InferredType::Object,
    }
}

/// Infer column types from a set of rows.
pub fn infer_column_types(rows: &[Vec<Value>], columns: &[String]) -> Vec<(String, InferredType)> {
    columns.iter().enumerate().map(|(i, name)| {
        let types: Vec<InferredType> = rows.iter()
            .filter_map(|row| row.get(i))
            .filter(|v| !v.is_null())
            .map(|v| infer_type(v))
            .collect();

        let inferred = if types.is_empty() {
            InferredType::Null
        } else if types.iter().all(|t| t == &types[0]) {
            types[0].clone()
        } else {
            InferredType::Mixed
        };

        (name.clone(), inferred)
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_infer_types() {
        assert_eq!(infer_type(&json!("hello")), InferredType::String);
        assert_eq!(infer_type(&json!(42)), InferredType::Integer);
        assert_eq!(infer_type(&json!(2.72)), InferredType::Float);
        assert_eq!(infer_type(&json!(true)), InferredType::Boolean);
        assert_eq!(infer_type(&json!(null)), InferredType::Null);
    }

    #[test]
    fn test_infer_column_types() {
        let cols = vec!["name".into(), "age".into()];
        let rows = vec![
            vec![json!("alice"), json!(30)],
            vec![json!("bob"), json!(25)],
        ];
        let types = infer_column_types(&rows, &cols);
        assert_eq!(types[0], ("name".into(), InferredType::String));
        assert_eq!(types[1], ("age".into(), InferredType::Integer));
    }

    #[test]
    fn test_mixed_types() {
        let cols = vec!["val".into()];
        let rows = vec![vec![json!("text")], vec![json!(42)]];
        let types = infer_column_types(&rows, &cols);
        assert_eq!(types[0].1, InferredType::Mixed);
    }

    #[test]
    fn test_all_null() {
        let cols = vec!["x".into()];
        let rows = vec![vec![json!(null)], vec![json!(null)]];
        let types = infer_column_types(&rows, &cols);
        assert_eq!(types[0].1, InferredType::Null);
    }
}
