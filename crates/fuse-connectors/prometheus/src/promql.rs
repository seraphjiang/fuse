// SPDX-License-Identifier: Apache-2.0

//! Translates SubQuery filters into PromQL label matchers and query expressions.

use fuse_core::connector::{ComparisonOp, FilterExpr, ScalarValue, SubQuery};

/// Build a PromQL query string from a SubQuery.
///
/// The table name is the metric name. Filters on non-`__name__` fields become
/// label matchers. Aggregations map to PromQL aggregation operators.
pub fn build_promql(query: &SubQuery) -> String {
    let metric = &query.table;
    let matchers = query
        .filter
        .as_ref()
        .map(|f| extract_label_matchers(f))
        .unwrap_or_default();

    let matcher_str = if matchers.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", matchers.join(", "))
    };

    let base = format!("{metric}{matcher_str}");

    // Check for rate/irate passthrough
    if let Some(pt) = &query.passthrough {
        if let Some(func) = pt.get("function").and_then(|v| v.as_str()) {
            let range = pt
                .get("range")
                .and_then(|v| v.as_str())
                .unwrap_or("5m");
            return format!("{func}({base}[{range}])");
        }
    }

    // Wrap with aggregation if present
    if let Some(agg) = query.aggregations.first() {
        let func = match agg.function {
            fuse_core::connector::AggFunction::Count => "count",
            fuse_core::connector::AggFunction::Sum => "sum",
            fuse_core::connector::AggFunction::Avg => "avg",
            fuse_core::connector::AggFunction::Min => "min",
            fuse_core::connector::AggFunction::Max => "max",
        };
        if query.group_by.is_empty() {
            format!("{func}({base})")
        } else {
            let by = query.group_by.join(", ");
            format!("{func} by ({by}) ({base})")
        }
    } else {
        base
    }
}

/// Extract PromQL label matchers from a FilterExpr tree.
fn extract_label_matchers(expr: &FilterExpr) -> Vec<String> {
    match expr {
        FilterExpr::And(left, right) => {
            let mut m = extract_label_matchers(left);
            m.extend(extract_label_matchers(right));
            m
        }
        FilterExpr::Comparison { field, op, value } => {
            if let Some(val_str) = scalar_to_string(value) {
                let op_str = match op {
                    ComparisonOp::Eq => "=",
                    ComparisonOp::Neq => "!=",
                    ComparisonOp::Like => "=~",
                    _ => return vec![], // PromQL only supports =, !=, =~, !~
                };
                vec![format!("{field}{op_str}\"{val_str}\"")]
            } else {
                vec![]
            }
        }
        FilterExpr::Not(inner) => {
            // NOT(field = value) → field != value
            if let FilterExpr::Comparison { field, op: ComparisonOp::Eq, value } = inner.as_ref() {
                if let Some(val_str) = scalar_to_string(value) {
                    return vec![format!("{field}!=\"{val_str}\"")];
                }
            }
            if let FilterExpr::Comparison { field, op: ComparisonOp::Like, value } = inner.as_ref() {
                if let Some(val_str) = scalar_to_string(value) {
                    return vec![format!("{field}!~\"{val_str}\"")];
                }
            }
            vec![]
        }
        // OR and other complex filters can't be expressed as PromQL label matchers
        _ => vec![],
    }
}

fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(s) => Some(s.clone()),
        ScalarValue::Int64(n) => Some(n.to_string()),
        ScalarValue::Float64(f) => Some(f.to_string()),
        ScalarValue::Boolean(b) => Some(b.to_string()),
        ScalarValue::Null => None,
    }
}
