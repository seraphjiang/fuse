// SPDX-License-Identifier: Apache-2.0

//! SQL generation for ClickHouse — same as standard SQL with minor differences.
//! ClickHouse uses backtick-quoted identifiers and supports all standard operators.

use fuse_core::connector::{AggFunction, ComparisonOp, FilterExpr, ScalarValue, SubQuery};

pub fn subquery_to_sql(q: &SubQuery) -> String {
    let select = if !q.aggregations.is_empty() {
        let aggs: Vec<String> = q.aggregations.iter().map(|a| {
            let field = a.field.as_deref().unwrap_or("*");
            let expr = match a.function {
                AggFunction::Count => format!("count({field})"),
                AggFunction::Sum => format!("sum({field})"),
                AggFunction::Avg => format!("avg({field})"),
                AggFunction::Min => format!("min({field})"),
                AggFunction::Max => format!("max({field})"),
                AggFunction::CountDistinct => format!("uniq({field})"), // ClickHouse uses uniq()
            };
            format!("{expr} AS {}", a.alias)
        }).collect();
        let mut parts = q.group_by.clone();
        parts.extend(aggs);
        parts.join(", ")
    } else if !q.projections.is_empty() {
        q.projections.join(", ")
    } else {
        "*".to_string()
    };

    let mut sql = format!("SELECT {select} FROM {}", q.table);

    if let Some(f) = &q.filter {
        sql.push_str(&format!(" WHERE {}", filter_to_sql(f)));
    }
    if !q.group_by.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", q.group_by.join(", ")));
    }
    if let Some(h) = &q.having {
        sql.push_str(&format!(" HAVING {}", filter_to_sql(h)));
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
                ComparisonOp::Eq => "=", ComparisonOp::Neq => "!=",
                ComparisonOp::Lt => "<", ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">", ComparisonOp::Gte => ">=",
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => "LIKE",
            };
            format!("{field} {op_str} {}", scalar_to_sql(value))
        }
        FilterExpr::In { field, values } => {
            let vals: Vec<String> = values.iter().map(scalar_to_sql).collect();
            format!("{field} IN ({})", vals.join(", "))
        }
        FilterExpr::IsNull(field) => format!("isNull({field})"),
        FilterExpr::IsNotNull(field) => format!("isNotNull({field})"),
    }
}

fn scalar_to_sql(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(f) => f.to_string(),
        ScalarValue::Boolean(b) => if *b { "1".to_string() } else { "0".to_string() },
        ScalarValue::Null => "NULL".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuse_core::connector::{AggFunction, AggregationExpr, SubQuery};

    fn base() -> SubQuery {
        SubQuery { table: "events".into(), projections: vec![], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: None, passthrough: None }
    }

    #[test]
    fn test_select_star() {
        assert_eq!(subquery_to_sql(&base()), "SELECT * FROM events");
    }

    #[test]
    fn test_count_distinct_uses_uniq() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr { function: AggFunction::CountDistinct, field: Some("user_id".into()), alias: "unique_users".into() }],
            ..base()
        };
        assert!(subquery_to_sql(&q).contains("uniq(user_id)"));
    }

    #[test]
    fn test_is_null_uses_clickhouse_syntax() {
        let q = SubQuery {
            filter: Some(FilterExpr::IsNull("email".into())),
            ..base()
        };
        assert!(subquery_to_sql(&q).contains("isNull(email)"));
    }

    #[test]
    fn test_is_not_null() {
        let q = SubQuery {
            filter: Some(FilterExpr::IsNotNull("email".into())),
            ..base()
        };
        assert!(subquery_to_sql(&q).contains("isNotNull(email)"));
    }

    #[test]
    fn test_boolean_as_int() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison { field: "active".into(), op: ComparisonOp::Eq, value: ScalarValue::Boolean(true) }),
            ..base()
        };
        assert!(subquery_to_sql(&q).contains("active = 1"));
    }

    // ── #452 Verification tests (tester) ──

    #[test]
    fn test_uniq_with_group_by() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr { function: AggFunction::CountDistinct, field: Some("user_id".into()), alias: "uniq_users".into() }],
            group_by: vec!["region".into()],
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("uniq(user_id) AS uniq_users"));
        assert!(sql.contains("GROUP BY region"));
    }

    #[test]
    fn test_having_clause() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr { function: AggFunction::Count, field: None, alias: "cnt".into() }],
            group_by: vec!["host".into()],
            having: Some(FilterExpr::Comparison { field: "cnt".into(), op: ComparisonOp::Gt, value: ScalarValue::Int64(100) }),
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("HAVING cnt > 100"));
    }

    #[test]
    fn test_false_boolean_as_zero() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison { field: "active".into(), op: ComparisonOp::Eq, value: ScalarValue::Boolean(false) }),
            ..base()
        };
        assert!(subquery_to_sql(&q).contains("active = 0"));
    }

    #[test]
    fn test_full_pushdown() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr { function: AggFunction::Sum, field: Some("amount".into()), alias: "total".into() }],
            group_by: vec!["region".into()],
            filter: Some(FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("active".into()) }),
            sort: vec![fuse_core::connector::SortExpr { field: "total".into(), descending: true }],
            limit: Some(10),
            having: None, ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("WHERE status = 'active'"));
        assert!(sql.contains("GROUP BY region"));
        assert!(sql.contains("ORDER BY total DESC"));
        assert!(sql.contains("LIMIT 10"));
    }
}
