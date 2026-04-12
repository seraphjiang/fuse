// SPDX-License-Identifier: Apache-2.0

//! S3 Select integration for filter push-down on Parquet/CSV files.

use fuse_core::connector::{ComparisonOp, FilterExpr, ScalarValue};

/// Build an S3 Select SQL expression from a FilterExpr.
/// Returns None if the filter can't be expressed in S3 Select SQL.
pub fn filter_to_s3_select_where(filter: &FilterExpr) -> Option<String> {
    match filter {
        FilterExpr::And(left, right) => {
            let l = filter_to_s3_select_where(left)?;
            let r = filter_to_s3_select_where(right)?;
            Some(format!("({l}) AND ({r})"))
        }
        FilterExpr::Or(left, right) => {
            let l = filter_to_s3_select_where(left)?;
            let r = filter_to_s3_select_where(right)?;
            Some(format!("({l}) OR ({r})"))
        }
        FilterExpr::Not(inner) => {
            let expr = filter_to_s3_select_where(inner)?;
            Some(format!("NOT ({expr})"))
        }
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=",
                ComparisonOp::Neq => "!=",
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
                ComparisonOp::Like => "LIKE",
                ComparisonOp::ILike | ComparisonOp::Contains => "LIKE",  // S3 Select is case-insensitive by default
            };
            let val = scalar_to_sql(value);
            Some(format!("s.\"{field}\" {op_str} {val}"))
        }
        FilterExpr::In { field, values } => {
            let vals: Vec<String> = values.iter().map(scalar_to_sql).collect();
            Some(format!("s.\"{field}\" IN ({})", vals.join(", ")))
        }
        FilterExpr::IsNull(field) => Some(format!("s.\"{field}\" IS NULL")),
        FilterExpr::IsNotNull(field) => Some(format!("s.\"{field}\" IS NOT NULL")),
    }
}

/// Build a full S3 Select query for Parquet.
pub fn build_s3_select_query(
    projections: &[String],
    filter: Option<&FilterExpr>,
    limit: Option<u64>,
) -> String {
    let select_cols = if projections.is_empty() {
        "*".to_string()
    } else {
        projections
            .iter()
            .map(|c| format!("s.\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let where_clause = filter
        .and_then(filter_to_s3_select_where)
        .map(|w| format!(" WHERE {w}"))
        .unwrap_or_default();

    let limit_clause = limit
        .map(|n| format!(" LIMIT {n}"))
        .unwrap_or_default();

    format!("SELECT {select_cols} FROM s3object s{where_clause}{limit_clause}")
}

fn scalar_to_sql(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Null => "NULL".to_string(),
        ScalarValue::Boolean(b) => b.to_string().to_uppercase(),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(f) => f.to_string(),
        ScalarValue::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_eq() {
        let f = FilterExpr::Comparison {
            field: "status".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Int64(200),
        };
        assert_eq!(
            filter_to_s3_select_where(&f).unwrap(),
            r#"s."status" = 200"#
        );
    }

    #[test]
    fn test_filter_like() {
        let f = FilterExpr::Comparison {
            field: "msg".into(),
            op: ComparisonOp::Like,
            value: ScalarValue::Utf8("%error%".into()),
        };
        assert_eq!(
            filter_to_s3_select_where(&f).unwrap(),
            r#"s."msg" LIKE '%error%'"#
        );
    }

    #[test]
    fn test_filter_and_or() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison {
                field: "a".into(),
                op: ComparisonOp::Gt,
                value: ScalarValue::Int64(1),
            }),
            Box::new(FilterExpr::Or(
                Box::new(FilterExpr::IsNull("b".into())),
                Box::new(FilterExpr::IsNotNull("c".into())),
            )),
        );
        let sql = filter_to_s3_select_where(&f).unwrap();
        assert!(sql.contains("AND"));
        assert!(sql.contains("OR"));
        assert!(sql.contains("IS NULL"));
        assert!(sql.contains("IS NOT NULL"));
    }

    #[test]
    fn test_filter_not() {
        let f = FilterExpr::Not(Box::new(FilterExpr::Comparison {
            field: "x".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Boolean(true),
        }));
        assert_eq!(
            filter_to_s3_select_where(&f).unwrap(),
            r#"NOT (s."x" = TRUE)"#
        );
    }

    #[test]
    fn test_filter_in_list() {
        let f = FilterExpr::In {
            field: "id".into(),
            values: vec![
                ScalarValue::Utf8("a".into()),
                ScalarValue::Utf8("b".into()),
            ],
        };
        assert_eq!(
            filter_to_s3_select_where(&f).unwrap(),
            r#"s."id" IN ('a', 'b')"#
        );
    }

    #[test]
    fn test_build_s3_select_all_columns() {
        let sql = build_s3_select_query(&[], None, None);
        assert_eq!(sql, "SELECT * FROM s3object s");
    }

    #[test]
    fn test_build_s3_select_with_projections() {
        let sql = build_s3_select_query(&["a".into(), "b".into()], None, None);
        assert_eq!(sql, r#"SELECT s."a", s."b" FROM s3object s"#);
    }

    #[test]
    fn test_build_s3_select_with_filter_and_limit() {
        let f = FilterExpr::Comparison {
            field: "status".into(),
            op: ComparisonOp::Gte,
            value: ScalarValue::Int64(500),
        };
        let sql = build_s3_select_query(&["status".into()], Some(&f), Some(10));
        assert_eq!(
            sql,
            r#"SELECT s."status" FROM s3object s WHERE s."status" >= 500 LIMIT 10"#
        );
    }

    #[test]
    fn test_scalar_to_sql_string_escaping() {
        let val = ScalarValue::Utf8("it's".into());
        assert_eq!(scalar_to_sql(&val), "'it''s'");
    }

    #[test]
    fn test_scalar_to_sql_null() {
        assert_eq!(scalar_to_sql(&ScalarValue::Null), "NULL");
    }

    #[test]
    fn test_scalar_to_sql_float() {
        assert_eq!(scalar_to_sql(&ScalarValue::Float64(2.72)), "2.72");
    }
}
