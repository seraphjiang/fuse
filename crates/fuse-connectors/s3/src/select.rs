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
