// SPDX-License-Identifier: Apache-2.0
//! Expression evaluator — evaluate simple expressions on values.

use serde_json::Value;

/// Comparison operators.
#[derive(Debug, Clone)]
pub enum CompareOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    IsNull,
    IsNotNull,
}

/// Evaluate a comparison.
pub fn compare(value: &Value, op: &CompareOp, operand: &Value) -> bool {
    match op {
        CompareOp::IsNull => value.is_null(),
        CompareOp::IsNotNull => !value.is_null(),
        CompareOp::Eq => value == operand,
        CompareOp::Neq => value != operand,
        CompareOp::Gt => cmp_numeric(value, operand)
            .map(|o| o == std::cmp::Ordering::Greater)
            .unwrap_or(false),
        CompareOp::Gte => cmp_numeric(value, operand)
            .map(|o| o != std::cmp::Ordering::Less)
            .unwrap_or(false),
        CompareOp::Lt => cmp_numeric(value, operand)
            .map(|o| o == std::cmp::Ordering::Less)
            .unwrap_or(false),
        CompareOp::Lte => cmp_numeric(value, operand)
            .map(|o| o != std::cmp::Ordering::Greater)
            .unwrap_or(false),
    }
}

fn cmp_numeric(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    a.as_f64().and_then(|va| {
        b.as_f64()
            .map(|vb| va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_eq() {
        assert!(compare(&json!(42), &CompareOp::Eq, &json!(42)));
    }

    #[test]
    fn test_neq() {
        assert!(compare(&json!(1), &CompareOp::Neq, &json!(2)));
    }

    #[test]
    fn test_gt() {
        assert!(compare(&json!(10), &CompareOp::Gt, &json!(5)));
    }

    #[test]
    fn test_lte() {
        assert!(compare(&json!(5), &CompareOp::Lte, &json!(5)));
    }

    #[test]
    fn test_is_null() {
        assert!(compare(&json!(null), &CompareOp::IsNull, &json!(null)));
    }

    #[test]
    fn test_is_not_null() {
        assert!(compare(&json!(1), &CompareOp::IsNotNull, &json!(null)));
    }

    #[test]
    fn test_string_eq() {
        assert!(compare(&json!("a"), &CompareOp::Eq, &json!("a")));
    }
}
