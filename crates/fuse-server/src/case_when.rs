// SPDX-License-Identifier: Apache-2.0
//! Conditional expressions — CASE WHEN for post-execution.

use serde_json::Value;

/// A predicate function for CASE WHEN branches.
type ConditionFn = Box<dyn Fn(&Value) -> bool + Send + Sync>;

/// A CASE WHEN condition.
pub struct CaseWhen {
    pub conditions: Vec<(ConditionFn, Value)>,
    pub default: Value,
}

impl CaseWhen {
    pub fn new(default: Value) -> Self {
        Self { conditions: Vec::new(), default }
    }

    pub fn when(mut self, predicate: impl Fn(&Value) -> bool + Send + Sync + 'static, result: Value) -> Self {
        self.conditions.push((Box::new(predicate), result));
        self
    }

    pub fn evaluate(&self, value: &Value) -> Value {
        for (pred, result) in &self.conditions {
            if pred(value) { return result.clone(); }
        }
        self.default.clone()
    }
}

/// Apply CASE WHEN to a column, producing a new column.
pub fn apply_case(rows: &[Vec<Value>], col: usize, case: &CaseWhen) -> Vec<Value> {
    rows.iter().map(|row| {
        row.get(col).map(|v| case.evaluate(v)).unwrap_or_else(|| case.default.clone())
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_case_when() {
        let case = CaseWhen::new(json!("other"))
            .when(|v| v.as_f64().map(|n| n >= 500.0).unwrap_or(false), json!("error"))
            .when(|v| v.as_f64().map(|n| n >= 400.0).unwrap_or(false), json!("warning"));

        assert_eq!(case.evaluate(&json!(500)), json!("error"));
        assert_eq!(case.evaluate(&json!(404)), json!("warning"));
        assert_eq!(case.evaluate(&json!(200)), json!("other"));
    }

    #[test]
    fn test_apply_case() {
        let rows = vec![vec![json!(500)], vec![json!(200)], vec![json!(404)]];
        let case = CaseWhen::new(json!("ok"))
            .when(|v| v.as_f64().map(|n| n >= 500.0).unwrap_or(false), json!("error"));
        let result = apply_case(&rows, 0, &case);
        assert_eq!(result, vec![json!("error"), json!("ok"), json!("ok")]);
    }

    #[test]
    fn test_default_only() {
        let case = CaseWhen::new(json!("default"));
        assert_eq!(case.evaluate(&json!("anything")), json!("default"));
    }

    #[test]
    fn test_null_input() {
        let case = CaseWhen::new(json!("unknown"))
            .when(|v| v.is_null(), json!("null_value"));
        assert_eq!(case.evaluate(&json!(null)), json!("null_value"));
    }
}
