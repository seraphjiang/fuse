// SPDX-License-Identifier: Apache-2.0

//! Translates a SubQuery into a SQL string for passthrough to PostgreSQL/MySQL.

use fuse_core::connector::{AggFunction, ComparisonOp, FilterExpr, ScalarValue, SubQuery};

/// Build a SQL SELECT statement from a SubQuery.
pub fn subquery_to_sql(q: &SubQuery) -> String {
    let select = if !q.aggregations.is_empty() {
        let aggs: Vec<String> = q.aggregations.iter().map(|a| {
            let field = a.field.as_deref().unwrap_or("*");
            let expr = match a.function {
                AggFunction::Count => format!("COUNT({field})"),
                AggFunction::Sum => format!("SUM({field})"),
                AggFunction::Avg => format!("AVG({field})"),
                AggFunction::Min => format!("MIN({field})"),
                AggFunction::Max => format!("MAX({field})"),
                AggFunction::CountDistinct => format!("COUNT(DISTINCT {field})"),
            };
            format!("{expr} AS {}", a.alias)
        }).collect();
        let mut parts = Vec::new();
        if !q.group_by.is_empty() {
            parts.extend(q.group_by.iter().cloned());
        }
        parts.extend(aggs);
        parts.join(", ")
    } else if !q.projections.is_empty() {
        q.projections.join(", ")
    } else {
        "*".to_string()
    };

    let mut sql = format!("SELECT {select} FROM {}", q.table);

    if let Some(filter) = &q.filter {
        sql.push_str(&format!(" WHERE {}", filter_to_sql(filter)));
    }

    if !q.group_by.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", q.group_by.join(", ")));
    }

    if let Some(having) = &q.having {
        sql.push_str(&format!(" HAVING {}", filter_to_sql(having)));
    }

    if !q.sort.is_empty() {
        let order: Vec<String> = q.sort.iter().map(|s| {
            if s.descending { format!("{} DESC", s.field) } else { s.field.clone() }
        }).collect();
        sql.push_str(&format!(" ORDER BY {}", order.join(", ")));
    }

    if let Some(limit) = q.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }

    sql
}

fn filter_to_sql(f: &FilterExpr) -> String {
    match f {
        FilterExpr::And(l, r) => format!("({} AND {})", filter_to_sql(l), filter_to_sql(r)),
        FilterExpr::Or(l, r) => format!("({} OR {})", filter_to_sql(l), filter_to_sql(r)),
        FilterExpr::Not(inner) => format!("NOT ({})", filter_to_sql(inner)),
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=",
                ComparisonOp::Neq => "!=",
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
                ComparisonOp::Like => "LIKE",
                ComparisonOp::ILike => "ILIKE",
            };
            format!("{field} {op_str} {}", scalar_to_sql(value))
        }
        FilterExpr::In { field, values } => {
            let vals: Vec<String> = values.iter().map(scalar_to_sql).collect();
            format!("{field} IN ({})", vals.join(", "))
        }
        FilterExpr::IsNull(field) => format!("{field} IS NULL"),
        FilterExpr::IsNotNull(field) => format!("{field} IS NOT NULL"),
    }
}

fn scalar_to_sql(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(f) => f.to_string(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Null => "NULL".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuse_core::connector::{AggFunction, AggregationExpr, FilterExpr, ScalarValue, SortExpr, SubQuery};

    fn base() -> SubQuery {
        SubQuery {
            table: "users".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            passthrough: None,
        }
    }

    #[test]
    fn test_select_star() {
        assert_eq!(subquery_to_sql(&base()), "SELECT * FROM users");
    }

    #[test]
    fn test_select_projections() {
        let q = SubQuery { projections: vec!["id".into(), "name".into()], ..base() };
        assert_eq!(subquery_to_sql(&q), "SELECT id, name FROM users");
    }

    #[test]
    fn test_where_eq() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "status".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Utf8("active".into()),
            }),
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users WHERE status = 'active'");
    }

    #[test]
    fn test_where_int_gt() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "age".into(),
                op: ComparisonOp::Gt,
                value: ScalarValue::Int64(18),
            }),
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users WHERE age > 18");
    }

    #[test]
    fn test_limit() {
        let q = SubQuery { limit: Some(10), ..base() };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users LIMIT 10");
    }

    #[test]
    fn test_order_by() {
        let q = SubQuery {
            sort: vec![SortExpr { field: "name".into(), descending: false }],
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users ORDER BY name");
    }

    #[test]
    fn test_order_by_desc() {
        let q = SubQuery {
            sort: vec![SortExpr { field: "created_at".into(), descending: true }],
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users ORDER BY created_at DESC");
    }

    #[test]
    fn test_aggregation_count() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr { function: AggFunction::Count, field: None, alias: "cnt".into() }],
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT COUNT(*) AS cnt FROM users");
    }

    #[test]
    fn test_aggregation_with_group_by() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr { function: AggFunction::Sum, field: Some("amount".into()), alias: "total".into() }],
            group_by: vec!["region".into()],
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT region, SUM(amount) AS total FROM users GROUP BY region");
    }

    #[test]
    fn test_is_null() {
        let q = SubQuery {
            filter: Some(FilterExpr::IsNull("email".into())),
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users WHERE email IS NULL");
    }

    #[test]
    fn test_in_filter() {
        let q = SubQuery {
            filter: Some(FilterExpr::In {
                field: "status".into(),
                values: vec![ScalarValue::Utf8("a".into()), ScalarValue::Utf8("b".into())],
            }),
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users WHERE status IN ('a', 'b')");
    }

    #[test]
    fn test_sql_injection_escaped() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "name".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Utf8("O'Brien".into()),
            }),
            ..base()
        };
        assert!(subquery_to_sql(&q).contains("O''Brien"));
    }

    #[test]
    fn test_having() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr { function: AggFunction::Count, field: None, alias: "cnt".into() }],
            group_by: vec!["region".into()],
            having: Some(FilterExpr::Comparison {
                field: "cnt".into(),
                op: ComparisonOp::Gt,
                value: ScalarValue::Int64(5),
            }),
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("HAVING cnt > 5"));
    }

    // ── #301 Verification tests (tester) ──

    #[test]
    fn test_ilike_pushdown() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "name".into(),
                op: ComparisonOp::ILike,
                value: ScalarValue::Utf8("%alice%".into()),
            }),
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users WHERE name ILIKE '%alice%'");
    }

    #[test]
    fn test_count_distinct_pushdown() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr {
                function: AggFunction::CountDistinct,
                field: Some("email".into()),
                alias: "uniq".into(),
            }],
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT COUNT(DISTINCT email) AS uniq FROM users");
    }

    #[test]
    fn test_not_filter() {
        let q = SubQuery {
            filter: Some(FilterExpr::Not(Box::new(FilterExpr::Comparison {
                field: "active".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Boolean(true),
            }))),
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users WHERE NOT (active = true)");
    }

    #[test]
    fn test_compound_and_or() {
        let q = SubQuery {
            filter: Some(FilterExpr::Or(
                Box::new(FilterExpr::And(
                    Box::new(FilterExpr::Comparison { field: "a".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(1) }),
                    Box::new(FilterExpr::Comparison { field: "b".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(2) }),
                )),
                Box::new(FilterExpr::Comparison { field: "c".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(3) }),
            )),
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("(a = 1 AND b = 2)"));
        assert!(sql.contains("OR c = 3"));
    }

    #[test]
    fn test_multi_sort() {
        let q = SubQuery {
            sort: vec![
                SortExpr { field: "region".into(), descending: false },
                SortExpr { field: "created_at".into(), descending: true },
            ],
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users ORDER BY region, created_at DESC");
    }

    #[test]
    fn test_full_pushdown_combined() {
        let q = SubQuery {
            projections: vec![],
            aggregations: vec![AggregationExpr { function: AggFunction::Count, field: None, alias: "cnt".into() }],
            group_by: vec!["region".into()],
            filter: Some(FilterExpr::Comparison { field: "active".into(), op: ComparisonOp::Eq, value: ScalarValue::Boolean(true) }),
            having: Some(FilterExpr::Comparison { field: "cnt".into(), op: ComparisonOp::Gte, value: ScalarValue::Int64(10) }),
            sort: vec![SortExpr { field: "cnt".into(), descending: true }],
            limit: Some(5),
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.starts_with("SELECT region, COUNT(*) AS cnt FROM users"));
        assert!(sql.contains("WHERE active = true"));
        assert!(sql.contains("GROUP BY region"));
        assert!(sql.contains("HAVING cnt >= 10"));
        assert!(sql.contains("ORDER BY cnt DESC"));
        assert!(sql.ends_with("LIMIT 5"));
    }

    #[test]
    fn test_single_quote_double_escape() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "name".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Utf8("it's a 'test'".into()),
            }),
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("it''s a ''test''"), "multiple quotes should all be escaped: {}", sql);
    }

    #[test]
    fn test_is_not_null() {
        let q = SubQuery {
            filter: Some(FilterExpr::IsNotNull("email".into())),
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM users WHERE email IS NOT NULL");
    }

    #[test]
    fn test_null_scalar_value() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "x".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Null,
            }),
            ..base()
        };
        assert!(subquery_to_sql(&q).contains("x = NULL"));
    }
}
