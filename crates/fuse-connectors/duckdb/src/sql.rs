// SPDX-License-Identifier: Apache-2.0

//! SQL generation for DuckDB — standard SQL with DuckDB-specific functions.

use fuse_core::connector::{AggFunction, ComparisonOp, FilterExpr, ScalarValue, SubQuery};
use fuse_core::sql::{quote_ident, quote_table};

pub fn subquery_to_sql(q: &SubQuery) -> String {
    let select = if !q.aggregations.is_empty() {
        let aggs: Vec<String> = q
            .aggregations
            .iter()
            .map(|a| {
                let field = a
                    .field
                    .as_deref()
                    .map(quote_ident)
                    .unwrap_or_else(|| "*".to_string());
                let expr = match a.function {
                    AggFunction::Count => format!("count({field})"),
                    AggFunction::Sum => format!("sum({field})"),
                    AggFunction::Avg => format!("avg({field})"),
                    AggFunction::Min => format!("min({field})"),
                    AggFunction::Max => format!("max({field})"),
                    AggFunction::ApproxCountDistinct => format!("approx_count_distinct({field})"),
                    AggFunction::ApproxPercentile(p) => format!("approx_quantile({field}, {p})"),
                    AggFunction::CountDistinct => format!("count(DISTINCT {field})"),
                };
                format!("{expr} AS {}", quote_ident(&a.alias))
            })
            .collect();
        let mut parts: Vec<String> = q.group_by.iter().map(|g| quote_ident(g)).collect();
        parts.extend(aggs);
        parts.join(", ")
    } else if !q.projections.is_empty() {
        q.projections
            .iter()
            .map(|p| {
                if p == "*" {
                    "*".to_string()
                } else {
                    quote_ident(p)
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        "*".to_string()
    };

    let mut sql = format!("SELECT {select} FROM {}", quote_table(&q.table));
    if let Some(f) = &q.filter {
        sql.push_str(&format!(" WHERE {}", filter_to_sql(f)));
    }
    if !q.group_by.is_empty() {
        let groups: Vec<String> = q.group_by.iter().map(|g| quote_ident(g)).collect();
        sql.push_str(&format!(" GROUP BY {}", groups.join(", ")));
    }
    if let Some(h) = &q.having {
        sql.push_str(&format!(" HAVING {}", filter_to_sql(h)));
    }
    if !q.sort.is_empty() {
        let order: Vec<String> = q
            .sort
            .iter()
            .map(|s| {
                if s.descending {
                    format!("{} DESC", quote_ident(&s.field))
                } else {
                    quote_ident(&s.field)
                }
            })
            .collect();
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
                ComparisonOp::Like | ComparisonOp::Contains => "LIKE",
                ComparisonOp::ILike => "ILIKE",
            };
            format!("{} {op_str} {}", quote_ident(field), scalar_to_sql(value))
        }
        FilterExpr::In { field, values } => {
            let vals: Vec<String> = values.iter().map(scalar_to_sql).collect();
            format!("{} IN ({})", quote_ident(field), vals.join(", "))
        }
        FilterExpr::IsNull(field) => format!("{} IS NULL", quote_ident(field)),
        FilterExpr::IsNotNull(field) => format!("{} IS NOT NULL", quote_ident(field)),
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
    use fuse_core::connector::{FilterExpr, ScalarValue, SubQuery};

    fn base() -> SubQuery {
        SubQuery {
            table: "events".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            passthrough: None,
            offset: None,
        }
    }

    #[test]
    fn test_select_star() {
        assert_eq!(subquery_to_sql(&base()), "SELECT * FROM \"events\"");
    }

    #[test]
    fn test_limit() {
        let q = SubQuery {
            limit: Some(5),
            ..base()
        };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM \"events\" LIMIT 5");
    }

    #[test]
    fn test_ilike() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "name".into(),
                op: ComparisonOp::ILike,
                value: ScalarValue::Utf8("%alice%".into()),
            }),
            ..base()
        };
        assert!(subquery_to_sql(&q).contains("ILIKE"));
        assert!(subquery_to_sql(&q).contains("\"name\""));
    }

    #[test]
    fn test_identifier_with_special_chars() {
        let q = SubQuery {
            table: "my table".into(),
            filter: Some(FilterExpr::Comparison {
                field: "col\"name".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Int64(1),
            }),
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("FROM \"my table\""));
        assert!(sql.contains("\"col\"\"name\""));
    }

    #[test]
    fn test_filter_and_or() {
        let q = SubQuery {
            filter: Some(FilterExpr::Or(
                Box::new(FilterExpr::Comparison {
                    field: "a".into(),
                    op: ComparisonOp::Eq,
                    value: ScalarValue::Int64(1),
                }),
                Box::new(FilterExpr::Comparison {
                    field: "b".into(),
                    op: ComparisonOp::Eq,
                    value: ScalarValue::Int64(2),
                }),
            )),
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("OR"));
    }

    #[test]
    fn test_filter_not() {
        let q = SubQuery {
            filter: Some(FilterExpr::Not(Box::new(FilterExpr::Comparison {
                field: "x".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Null,
            }))),
            ..base()
        };
        assert!(subquery_to_sql(&q).contains("NOT"));
    }

    #[test]
    fn test_filter_in() {
        let q = SubQuery {
            filter: Some(FilterExpr::In {
                field: "status".into(),
                values: vec![
                    ScalarValue::Utf8("ok".into()),
                    ScalarValue::Utf8("warn".into()),
                ],
            }),
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("IN"));
        assert!(sql.contains("'ok'"));
        assert!(sql.contains("'warn'"));
    }

    #[test]
    fn test_is_null_is_not_null() {
        let q1 = SubQuery {
            filter: Some(FilterExpr::IsNull("x".into())),
            ..base()
        };
        assert!(subquery_to_sql(&q1).contains("IS NULL"));

        let q2 = SubQuery {
            filter: Some(FilterExpr::IsNotNull("x".into())),
            ..base()
        };
        assert!(subquery_to_sql(&q2).contains("IS NOT NULL"));
    }

    #[test]
    fn test_projections() {
        let q = SubQuery {
            projections: vec!["id".into(), "name".into()],
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("id") && sql.contains("name"));
        assert!(!sql.contains("*"));
    }

    #[test]
    fn test_sort_asc_desc() {
        use fuse_core::connector::SortExpr;
        let q = SubQuery {
            sort: vec![
                SortExpr {
                    field: "a".into(),
                    descending: false,
                },
                SortExpr {
                    field: "b".into(),
                    descending: true,
                },
            ],
            ..base()
        };
        let sql = subquery_to_sql(&q);
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("DESC"));
    }
}
