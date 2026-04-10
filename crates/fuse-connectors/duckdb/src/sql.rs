// SPDX-License-Identifier: Apache-2.0

//! SQL generation for DuckDB — standard SQL with DuckDB-specific functions.

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
                AggFunction::ApproxCountDistinct => format!("approx_count_distinct({field})"),
                AggFunction::ApproxPercentile(p) => format!("approx_quantile({field}, {p})"),
                AggFunction::CountDistinct => format!("count(DISTINCT {field})"),
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
    if let Some(f) = &q.filter { sql.push_str(&format!(" WHERE {}", filter_to_sql(f))); }
    if !q.group_by.is_empty() { sql.push_str(&format!(" GROUP BY {}", q.group_by.join(", "))); }
    if let Some(h) = &q.having { sql.push_str(&format!(" HAVING {}", filter_to_sql(h))); }
    if !q.sort.is_empty() {
        let order: Vec<String> = q.sort.iter().map(|s| if s.descending { format!("{} DESC", s.field) } else { s.field.clone() }).collect();
        sql.push_str(&format!(" ORDER BY {}", order.join(", ")));
    }
    if let Some(limit) = q.limit { sql.push_str(&format!(" LIMIT {limit}")); }
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
                ComparisonOp::Like | ComparisonOp::Contains => "LIKE", ComparisonOp::ILike => "ILIKE",
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
    use fuse_core::connector::SubQuery;

    fn base() -> SubQuery {
        SubQuery { table: "events".into(), projections: vec![], filter: None, aggregations: vec![], group_by: vec![], having: None, sort: vec![], limit: None, passthrough: None, offset: None }
    }

    #[test]
    fn test_select_star() { assert_eq!(subquery_to_sql(&base()), "SELECT * FROM events"); }

    #[test]
    fn test_limit() {
        let q = SubQuery { limit: Some(5), ..base() };
        assert_eq!(subquery_to_sql(&q), "SELECT * FROM events LIMIT 5");
    }

    #[test]
    fn test_ilike() {
        let q = SubQuery {
            filter: Some(FilterExpr::Comparison { field: "name".into(), op: ComparisonOp::ILike, value: ScalarValue::Utf8("%alice%".into()) }),
            ..base()
        };
        assert!(subquery_to_sql(&q).contains("ILIKE"));
    }
}
