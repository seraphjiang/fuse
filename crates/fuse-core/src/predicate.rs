// SPDX-License-Identifier: Apache-2.0
//! Predicate builder — construct filter predicates programmatically.

use serde::Serialize;

/// A filter predicate for query pushdown.
#[derive(Debug, Clone, Serialize)]
pub enum Predicate {
    Eq { field: String, value: String },
    Gt { field: String, value: String },
    Lt { field: String, value: String },
    Like { field: String, pattern: String },
    In { field: String, values: Vec<String> },
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    pub fn eq(field: &str, value: &str) -> Self { Self::Eq { field: field.into(), value: value.into() } }
    pub fn gt(field: &str, value: &str) -> Self { Self::Gt { field: field.into(), value: value.into() } }
    pub fn lt(field: &str, value: &str) -> Self { Self::Lt { field: field.into(), value: value.into() } }
    pub fn like(field: &str, pattern: &str) -> Self { Self::Like { field: field.into(), pattern: pattern.into() } }
    pub fn in_list(field: &str, values: Vec<&str>) -> Self { Self::In { field: field.into(), values: values.into_iter().map(String::from).collect() } }
    pub fn and(preds: Vec<Predicate>) -> Self { Self::And(preds) }
    pub fn or(preds: Vec<Predicate>) -> Self { Self::Or(preds) }
    pub fn not(pred: Predicate) -> Self { Self::Not(Box::new(pred)) }

    /// Convert to SQL WHERE clause fragment.
    pub fn to_sql(&self) -> String {
        match self {
            Self::Eq { field, value } => format!("{} = '{}'", field, value.replace('\'', "''")),
            Self::Gt { field, value } => format!("{} > '{}'", field, value),
            Self::Lt { field, value } => format!("{} < '{}'", field, value),
            Self::Like { field, pattern } => format!("{} LIKE '{}'", field, pattern),
            Self::In { field, values } => {
                let list = values.iter().map(|v| format!("'{}'", v.replace('\'', "''"))).collect::<Vec<_>>().join(", ");
                format!("{} IN ({})", field, list)
            }
            Self::And(preds) => preds.iter().map(|p| format!("({})", p.to_sql())).collect::<Vec<_>>().join(" AND "),
            Self::Or(preds) => preds.iter().map(|p| format!("({})", p.to_sql())).collect::<Vec<_>>().join(" OR "),
            Self::Not(pred) => format!("NOT ({})", pred.to_sql()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq() {
        assert_eq!(Predicate::eq("name", "alice").to_sql(), "name = 'alice'");
    }

    #[test]
    fn test_and() {
        let p = Predicate::and(vec![Predicate::eq("a", "1"), Predicate::gt("b", "5")]);
        assert!(p.to_sql().contains("AND"));
    }

    #[test]
    fn test_or() {
        let p = Predicate::or(vec![Predicate::eq("x", "1"), Predicate::eq("x", "2")]);
        assert!(p.to_sql().contains("OR"));
    }

    #[test]
    fn test_not() {
        let p = Predicate::not(Predicate::eq("active", "false"));
        assert!(p.to_sql().starts_with("NOT"));
    }

    #[test]
    fn test_in_list() {
        let p = Predicate::in_list("status", vec!["200", "201"]);
        assert!(p.to_sql().contains("IN ('200', '201')"));
    }

    #[test]
    fn test_like() {
        assert_eq!(Predicate::like("name", "%alice%").to_sql(), "name LIKE '%alice%'");
    }

    #[test]
    fn test_escape_quotes() {
        assert_eq!(Predicate::eq("name", "O'Brien").to_sql(), "name = 'O''Brien'");
    }
}
