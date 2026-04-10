// SPDX-License-Identifier: Apache-2.0

//! Translates Fuse FilterExpr into DynamoDB expression strings.
//!
//! - `build_key_condition`: extracts partition key equality → KeyConditionExpression
//! - `build_filter_expression`: remaining predicates → FilterExpression

use aws_sdk_dynamodb::types::AttributeValue;
use fuse_core::connector::{ComparisonOp, FilterExpr, ScalarValue};

type ExprResult = Option<(String, Vec<(String, String)>, Vec<(String, AttributeValue)>)>;

/// Build a KeyConditionExpression if the filter contains a partition key equality.
/// Returns (expression, name_aliases, value_aliases).
pub fn build_key_condition(filter: &FilterExpr, pk: &str) -> ExprResult {
    match filter {
        FilterExpr::Comparison { field, op: ComparisonOp::Eq, value } if field == pk => {
            let av = scalar_to_av(value)?;
            Some((
                "#pk = :pk".to_string(),
                vec![("#pk".to_string(), pk.to_string())],
                vec![(":pk".to_string(), av)],
            ))
        }
        FilterExpr::And(l, r) => {
            build_key_condition(l, pk).or_else(|| build_key_condition(r, pk))
        }
        _ => None,
    }
}

/// Build a FilterExpression from the filter, excluding the partition key equality
/// (which is already in KeyConditionExpression).
pub fn build_filter_expression(filter: &FilterExpr, pk: Option<&str>) -> ExprResult {
    let mut names: Vec<(String, String)> = Vec::new();
    let mut values: Vec<(String, AttributeValue)> = Vec::new();
    let mut counter = Counter::default();

    let expr = translate(filter, pk, &mut names, &mut values, &mut counter)?;
    if expr.is_empty() { return None; }
    Some((expr, names, values))
}

#[derive(Default)]
struct Counter { n: usize, v: usize }
impl Counter {
    fn name(&mut self, field: &str) -> (String, String) {
        let alias = format!("#n{}", self.n);
        self.n += 1;
        (alias, field.to_string())
    }
    fn val(&mut self, av: AttributeValue) -> (String, AttributeValue) {
        let alias = format!(":v{}", self.v);
        self.v += 1;
        (alias, av)
    }
}

fn translate(
    filter: &FilterExpr,
    pk: Option<&str>,
    names: &mut Vec<(String, String)>,
    values: &mut Vec<(String, AttributeValue)>,
    c: &mut Counter,
) -> Option<String> {
    match filter {
        FilterExpr::And(l, r) => {
            let le = translate(l, pk, names, values, c);
            let re = translate(r, pk, names, values, c);
            match (le, re) {
                (Some(l), Some(r)) => Some(format!("({l} AND {r})")),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        FilterExpr::Or(l, r) => {
            let le = translate(l, pk, names, values, c)?;
            let re = translate(r, pk, names, values, c)?;
            Some(format!("({le} OR {re})"))
        }
        FilterExpr::Not(inner) => {
            let ie = translate(inner, pk, names, values, c)?;
            Some(format!("NOT ({ie})"))
        }
        FilterExpr::Comparison { field, op, value } => {
            // Skip partition key equality — it's in KeyConditionExpression
            if pk == Some(field.as_str()) && matches!(op, ComparisonOp::Eq) {
                return None;
            }
            let av = scalar_to_av(value)?;
            let (na, nv) = c.name(field);
            let (va, vv) = c.val(av);
            names.push((na.clone(), nv));
            values.push((va.clone(), vv));
            let op_str = match op {
                ComparisonOp::Eq => "=",
                ComparisonOp::Neq => "<>",
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => {
                    // DynamoDB has no LIKE — use begins_with for prefix patterns
                    return Some(format!("begins_with({na}, {va})"));
                }
            };
            Some(format!("{na} {op_str} {va}"))
        }
        FilterExpr::In { field, values: vals } => {
            let (na, nv) = c.name(field);
            names.push((na.clone(), nv));
            let mut placeholders = Vec::new();
            for v in vals {
                if let Some(av) = scalar_to_av(v) {
                    let (va, vv) = c.val(av);
                    values.push((va.clone(), vv));
                    placeholders.push(va);
                }
            }
            if placeholders.is_empty() { return None; }
            Some(format!("{na} IN ({})", placeholders.join(", ")))
        }
        FilterExpr::IsNull(field) => {
            let (na, nv) = c.name(field);
            names.push((na.clone(), nv));
            Some(format!("attribute_not_exists({na})"))
        }
        FilterExpr::IsNotNull(field) => {
            let (na, nv) = c.name(field);
            names.push((na.clone(), nv));
            Some(format!("attribute_exists({na})"))
        }
    }
}

fn scalar_to_av(v: &ScalarValue) -> Option<AttributeValue> {
    match v {
        ScalarValue::Utf8(s) => Some(AttributeValue::S(s.clone())),
        ScalarValue::Int64(n) => Some(AttributeValue::N(n.to_string())),
        ScalarValue::Float64(f) => Some(AttributeValue::N(f.to_string())),
        ScalarValue::Boolean(b) => Some(AttributeValue::Bool(*b)),
        ScalarValue::Null => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuse_core::connector::ScalarValue;

    fn eq(field: &str, val: &str) -> FilterExpr {
        FilterExpr::Comparison {
            field: field.into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8(val.into()),
        }
    }

    #[test]
    fn test_key_condition_eq() {
        let f = eq("user_id", "u123");
        let result = build_key_condition(&f, "user_id").unwrap();
        assert_eq!(result.0, "#pk = :pk");
        assert_eq!(result.1[0].1, "user_id");
        assert_eq!(result.2[0].1, AttributeValue::S("u123".into()));
    }

    #[test]
    fn test_key_condition_not_pk() {
        let f = eq("status", "active");
        assert!(build_key_condition(&f, "user_id").is_none());
    }

    #[test]
    fn test_key_condition_in_and() {
        let f = FilterExpr::And(
            Box::new(eq("user_id", "u1")),
            Box::new(eq("status", "active")),
        );
        let result = build_key_condition(&f, "user_id").unwrap();
        assert_eq!(result.0, "#pk = :pk");
    }

    #[test]
    fn test_filter_expression_skips_pk() {
        let f = FilterExpr::And(
            Box::new(eq("user_id", "u1")),
            Box::new(eq("status", "active")),
        );
        let result = build_filter_expression(&f, Some("user_id")).unwrap();
        // Only status remains
        assert!(result.0.contains("#n"));
        assert!(!result.0.contains("user_id"));
    }

    #[test]
    fn test_filter_expression_is_null() {
        let f = FilterExpr::IsNull("email".into());
        let result = build_filter_expression(&f, None).unwrap();
        assert!(result.0.starts_with("attribute_not_exists("));
    }

    #[test]
    fn test_filter_expression_is_not_null() {
        let f = FilterExpr::IsNotNull("email".into());
        let result = build_filter_expression(&f, None).unwrap();
        assert!(result.0.starts_with("attribute_exists("));
    }

    #[test]
    fn test_filter_expression_in() {
        let f = FilterExpr::In {
            field: "status".into(),
            values: vec![ScalarValue::Utf8("active".into()), ScalarValue::Utf8("pending".into())],
        };
        let result = build_filter_expression(&f, None).unwrap();
        assert!(result.0.contains(" IN ("));
        assert_eq!(result.2.len(), 2);
    }

    #[test]
    fn test_filter_expression_not() {
        let f = FilterExpr::Not(Box::new(eq("deleted", "true")));
        let result = build_filter_expression(&f, None).unwrap();
        assert!(result.0.starts_with("NOT ("));
    }

    #[test]
    fn test_filter_expression_or() {
        let f = FilterExpr::Or(Box::new(eq("a", "1")), Box::new(eq("b", "2")));
        let result = build_filter_expression(&f, None).unwrap();
        assert!(result.0.contains(" OR "));
    }

    #[test]
    fn test_scalar_to_av_int() {
        assert_eq!(scalar_to_av(&ScalarValue::Int64(42)), Some(AttributeValue::N("42".into())));
    }

    #[test]
    fn test_scalar_to_av_null() {
        assert!(scalar_to_av(&ScalarValue::Null).is_none());
    }

    // ── #300 DynamoDB verification (tester) ──

    #[test]
    fn test_key_condition_and_extracts_pk_from_left() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "user_id".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("u1".into()) }),
            Box::new(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Gte, value: ScalarValue::Int64(200) }),
        );
        let kc = build_key_condition(&f, "user_id");
        assert!(kc.is_some(), "should extract PK from left side of AND");
    }

    #[test]
    fn test_key_condition_and_extracts_pk_from_right() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Gte, value: ScalarValue::Int64(200) }),
            Box::new(FilterExpr::Comparison { field: "user_id".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("u1".into()) }),
        );
        let kc = build_key_condition(&f, "user_id");
        assert!(kc.is_some(), "should extract PK from right side of AND");
    }

    #[test]
    fn test_filter_expression_excludes_pk() {
        // AND(pk=val, status>=200) → filter should only have status, not pk
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "user_id".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("u1".into()) }),
            Box::new(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Gte, value: ScalarValue::Int64(200) }),
        );
        let fe = build_filter_expression(&f, Some("user_id"));
        assert!(fe.is_some());
        let (expr, _, _) = fe.unwrap();
        assert!(!expr.contains("pk"), "filter should exclude PK: {}", expr);
    }

    #[test]
    fn test_key_condition_non_eq_returns_none() {
        // PK with >= (not =) should not produce a key condition
        let f = FilterExpr::Comparison { field: "user_id".into(), op: ComparisonOp::Gte, value: ScalarValue::Utf8("u1".into()) };
        assert!(build_key_condition(&f, "user_id").is_none());
    }

    #[test]
    fn test_filter_expression_complex_and_or() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Or(
                Box::new(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(200) }),
                Box::new(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(500) }),
            )),
            Box::new(FilterExpr::Comparison { field: "host".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("h1".into()) }),
        );
        let fe = build_filter_expression(&f, None);
        assert!(fe.is_some());
        let (expr, _, _) = fe.unwrap();
        assert!(expr.contains("OR"), "should contain OR: {}", expr);
        assert!(expr.contains("AND"), "should contain AND: {}", expr);
    }
}
