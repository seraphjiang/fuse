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
            fuse_core::connector::AggFunction::ApproxCountDistinct => "count",
            fuse_core::connector::AggFunction::ApproxPercentile(_) => "quantile",
            fuse_core::connector::AggFunction::CountDistinct => "count",
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
                let safe_field = sanitize_label_name(field);
                let op_str = match op {
                    ComparisonOp::Eq => "=",
                    ComparisonOp::Neq => "!=",
                    ComparisonOp::Like => "=~",
                    _ => return vec![],
                };
                vec![format!("{safe_field}{op_str}\"{val_str}\"")]
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
        ScalarValue::Utf8(s) => Some(sanitize_promql_value(s)),
        ScalarValue::Int64(n) => Some(n.to_string()),
        ScalarValue::Float64(f) => Some(f.to_string()),
        ScalarValue::Boolean(b) => Some(b.to_string()),
        ScalarValue::Null => None,
    }
}

/// Sanitize a string for PromQL label values (double-quoted).
fn sanitize_promql_value(s: &str) -> String {
    s.replace('\\', "\\\\")
     .replace('"', "\\\"")
     .replace('\n', "\\n")
     .replace('\r', "\\r")
     .replace('\0', "")
}

/// Sanitize a PromQL label name — allow only alphanumeric + underscore.
fn sanitize_label_name(s: &str) -> String {
    let clean: String = s.chars().filter(|c| c.is_alphanumeric() || *c == '_').collect();
    if clean.is_empty() { "_".to_string() } else { clean }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuse_core::connector::{AggFunction, AggregationExpr};

    fn simple_query(table: &str) -> SubQuery {
        SubQuery {
            table: table.into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None, passthrough: None, offset: None,
        }
    }

    #[test]
    fn test_build_promql_simple_metric() {
        let q = simple_query("http_requests_total");
        assert_eq!(build_promql(&q), "http_requests_total");
    }

    #[test]
    fn test_build_promql_with_label_matchers() {
        let q = SubQuery {
            filter: Some(FilterExpr::And(
                Box::new(FilterExpr::Comparison {
                    field: "job".into(),
                    op: ComparisonOp::Eq,
                    value: ScalarValue::Utf8("api".into()),
                }),
                Box::new(FilterExpr::Comparison {
                    field: "status".into(),
                    op: ComparisonOp::Neq,
                    value: ScalarValue::Utf8("200".into()),
                }),
            )),
            ..simple_query("http_requests_total")
        };
        let pql = build_promql(&q);
        assert!(pql.starts_with("http_requests_total{"));
        assert!(pql.contains(r#"job="api""#));
        assert!(pql.contains(r#"status!="200""#));
    }

    #[test]
    fn test_build_promql_with_aggregation() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr {
                function: AggFunction::Sum,
                field: None,
                alias: "total".into(),
            }],
            ..simple_query("http_requests_total")
        };
        assert_eq!(build_promql(&q), "sum(http_requests_total)");
    }

    #[test]
    fn test_build_promql_with_aggregation_and_group_by() {
        let q = SubQuery {
            aggregations: vec![AggregationExpr {
                function: AggFunction::Count,
                field: None,
                alias: "cnt".into(),
            }],
            group_by: vec!["job".into(), "instance".into()],
            ..simple_query("up")
        };
        assert_eq!(build_promql(&q), "count by (job, instance) (up)");
    }

    #[test]
    fn test_build_promql_with_rate_passthrough() {
        let q = SubQuery {
            passthrough: Some(serde_json::json!({"function": "rate", "range": "5m"})),
            ..simple_query("http_requests_total")
        };
        assert_eq!(build_promql(&q), "rate(http_requests_total[5m])");
    }

    #[test]
    fn test_extract_label_matchers_eq() {
        let f = FilterExpr::Comparison {
            field: "env".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8("prod".into()),
        };
        let matchers = extract_label_matchers(&f);
        assert_eq!(matchers, vec![r#"env="prod""#]);
    }

    #[test]
    fn test_extract_label_matchers_like_becomes_regex() {
        let f = FilterExpr::Comparison {
            field: "path".into(),
            op: ComparisonOp::Like,
            value: ScalarValue::Utf8("/api/.*".into()),
        };
        let matchers = extract_label_matchers(&f);
        assert_eq!(matchers, vec![r#"path=~"/api/.*""#]);
    }

    #[test]
    fn test_extract_label_matchers_not_eq() {
        let f = FilterExpr::Not(Box::new(FilterExpr::Comparison {
            field: "env".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8("test".into()),
        }));
        let matchers = extract_label_matchers(&f);
        assert_eq!(matchers, vec![r#"env!="test""#]);
    }

    #[test]
    fn test_extract_label_matchers_unsupported_op_returns_empty() {
        let f = FilterExpr::Comparison {
            field: "val".into(),
            op: ComparisonOp::Gt,
            value: ScalarValue::Int64(100),
        };
        let matchers = extract_label_matchers(&f);
        assert!(matchers.is_empty());
    }

    #[test]
    fn test_extract_label_matchers_and_combines() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison {
                field: "a".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Utf8("1".into()),
            }),
            Box::new(FilterExpr::Comparison {
                field: "b".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Utf8("2".into()),
            }),
        );
        let matchers = extract_label_matchers(&f);
        assert_eq!(matchers.len(), 2);
    }

    // ── #601 Prometheus injection fix verification (tester) ──

    #[test]
    fn test_scalar_escapes_double_quotes() {
        let val = scalar_to_string(&ScalarValue::Utf8("test\"inject".into()));
        assert!(val.as_ref().unwrap().contains("\\\""), "should escape double quotes: {:?}", val);
    }

    #[test]
    fn test_scalar_escapes_backslash() {
        let val = scalar_to_string(&ScalarValue::Utf8("test\\path".into()));
        assert!(val.unwrap().contains("\\\\"), "should escape backslash");
    }

    #[test]
    fn test_scalar_preserves_single_quotes() {
        // PromQL uses double-quoted label values — single quotes are safe
        let val = scalar_to_string(&ScalarValue::Utf8("it's fine".into()));
        assert!(val.unwrap().contains("it's fine"));
    }

    #[test]
    fn test_sanitize_promql_newline_injection() {
        let result = sanitize_promql_value("val\n}[5m])\nup{");
        assert!(!result.contains('\n'), "should escape newlines: {}", result);
        assert!(result.contains("\\n"));
    }

    #[test]
    fn test_sanitize_promql_null_byte() {
        let result = sanitize_promql_value("val\0bad");
        assert!(!result.contains('\0'));
    }

    #[test]
    fn test_sanitize_label_name() {
        assert_eq!(sanitize_label_name("host"), "host");
        assert_eq!(sanitize_label_name("host}[5m])"), "host5m");
        assert_eq!(sanitize_label_name(""), "_");
    }

    #[test]
    fn test_promql_matcher_uses_sanitized_label() {
        let f = FilterExpr::Comparison {
            field: "job\"}[5m])".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8("api".into()),
        };
        let matchers = extract_label_matchers(&f);
        assert!(!matchers[0].contains('}'), "should sanitize label name: {}", matchers[0]);
    }
}
