// SPDX-License-Identifier: Apache-2.0

//! Hive-style partition pruning for S3 Parquet paths.
//!
//! Paths like `prefix/year=2024/month=01/data.parquet` encode partition
//! key=value pairs. Given a WHERE filter, we can skip files whose partition
//! values are known to not match — without downloading them.

use fuse_core::connector::{ComparisonOp, FilterExpr, ScalarValue};
use std::collections::HashMap;

/// Extract Hive partition key=value pairs from an S3 key path.
///
/// Example: `logs/year=2024/month=01/part-0.parquet`
/// → `{"year": "2024", "month": "01"}`
pub fn extract_partitions(key: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for segment in key.split('/') {
        if let Some((k, v)) = segment.split_once('=') {
            if !k.is_empty() && !v.is_empty() {
                map.insert(k.to_string(), v.to_string());
            }
        }
    }
    map
}

/// Returns `false` if the filter is provably unsatisfied by the partition values.
/// Returns `true` (keep) when uncertain or when the filter references non-partition columns.
pub fn partition_matches(filter: &FilterExpr, partitions: &HashMap<String, String>) -> bool {
    match filter {
        FilterExpr::And(l, r) => {
            partition_matches(l, partitions) && partition_matches(r, partitions)
        }
        FilterExpr::Or(l, r) => {
            partition_matches(l, partitions) || partition_matches(r, partitions)
        }
        FilterExpr::Not(inner) => {
            // Conservative: only prune if inner is a provable equality mismatch
            match inner.as_ref() {
                FilterExpr::Comparison { field, op: ComparisonOp::Eq, value } => {
                    !partitions.get(field).map_or(false, |pv| {
                        scalar_to_str(value).map_or(false, |fv| pv == &fv)
                    })
                }
                _ => true,
            }
        }
        FilterExpr::Comparison { field, op, value } => {
            let Some(pv) = partitions.get(field) else {
                return true; // not a partition column — can't prune
            };
            let Some(fv) = scalar_to_str(value) else {
                return true; // can't compare non-string scalar
            };
            compare_str(pv, op, &fv)
        }
        FilterExpr::In { field, values } => {
            let Some(pv) = partitions.get(field) else {
                return true;
            };
            values.iter().any(|v| scalar_to_str(v).map_or(true, |fv| fv == *pv))
        }
        FilterExpr::IsNull(field) => {
            // Partition values are never null in Hive paths
            !partitions.contains_key(field)
        }
        FilterExpr::IsNotNull(field) => partitions.contains_key(field),
    }
}

fn scalar_to_str(v: &ScalarValue) -> Option<String> {
    match v {
        ScalarValue::Utf8(s) => Some(s.clone()),
        ScalarValue::Int64(n) => Some(n.to_string()),
        ScalarValue::Float64(f) => Some(f.to_string()),
        ScalarValue::Boolean(b) => Some(b.to_string()),
        ScalarValue::Null => None,
    }
}

fn compare_str(partition_val: &str, op: &ComparisonOp, filter_val: &str) -> bool {
    match op {
        ComparisonOp::Eq => partition_val == filter_val,
        ComparisonOp::Neq => partition_val != filter_val,
        ComparisonOp::Lt => partition_val < filter_val,
        ComparisonOp::Lte => partition_val <= filter_val,
        ComparisonOp::Gt => partition_val > filter_val,
        ComparisonOp::Gte => partition_val >= filter_val,
        ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => true, // conservative
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partitions(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn test_extract_partitions_hive_path() {
        let p = extract_partitions("logs/year=2024/month=01/part-0.parquet");
        assert_eq!(p.get("year").map(|s| s.as_str()), Some("2024"));
        assert_eq!(p.get("month").map(|s| s.as_str()), Some("01"));
    }

    #[test]
    fn test_extract_partitions_no_partitions() {
        let p = extract_partitions("logs/data.parquet");
        assert!(p.is_empty());
    }

    #[test]
    fn test_extract_partitions_ignores_non_kv_segments() {
        let p = extract_partitions("prefix/year=2024/somefile.parquet");
        assert_eq!(p.len(), 1);
        assert_eq!(p.get("year").map(|s| s.as_str()), Some("2024"));
    }

    #[test]
    fn test_eq_match_keeps_file() {
        let p = partitions(&[("year", "2024")]);
        let f = FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2024".into()) };
        assert!(partition_matches(&f, &p));
    }

    #[test]
    fn test_eq_mismatch_prunes_file() {
        let p = partitions(&[("year", "2023")]);
        let f = FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2024".into()) };
        assert!(!partition_matches(&f, &p));
    }

    #[test]
    fn test_non_partition_column_keeps_file() {
        let p = partitions(&[("year", "2024")]);
        let f = FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(500) };
        assert!(partition_matches(&f, &p));
    }

    #[test]
    fn test_and_prunes_when_one_side_fails() {
        let p = partitions(&[("year", "2023"), ("month", "01")]);
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2024".into()) }),
            Box::new(FilterExpr::Comparison { field: "month".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("01".into()) }),
        );
        assert!(!partition_matches(&f, &p));
    }

    #[test]
    fn test_or_keeps_when_one_side_matches() {
        let p = partitions(&[("year", "2024")]);
        let f = FilterExpr::Or(
            Box::new(FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2024".into()) }),
            Box::new(FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2023".into()) }),
        );
        assert!(partition_matches(&f, &p));
    }

    #[test]
    fn test_in_filter_matches() {
        let p = partitions(&[("month", "03")]);
        let f = FilterExpr::In {
            field: "month".into(),
            values: vec![ScalarValue::Utf8("01".into()), ScalarValue::Utf8("03".into())],
        };
        assert!(partition_matches(&f, &p));
    }

    #[test]
    fn test_in_filter_prunes() {
        let p = partitions(&[("month", "06")]);
        let f = FilterExpr::In {
            field: "month".into(),
            values: vec![ScalarValue::Utf8("01".into()), ScalarValue::Utf8("03".into())],
        };
        assert!(!partition_matches(&f, &p));
    }

    #[test]
    fn test_gte_keeps_matching() {
        let p = partitions(&[("year", "2024")]);
        let f = FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Gte, value: ScalarValue::Utf8("2023".into()) };
        assert!(partition_matches(&f, &p));
    }

    #[test]
    fn test_gte_prunes_older() {
        let p = partitions(&[("year", "2022")]);
        let f = FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Gte, value: ScalarValue::Utf8("2023".into()) };
        assert!(!partition_matches(&f, &p));
    }

    #[test]
    fn test_is_null_prunes_when_partition_exists() {
        let p = partitions(&[("year", "2024")]);
        assert!(!partition_matches(&FilterExpr::IsNull("year".into()), &p));
    }

    #[test]
    fn test_is_not_null_keeps_when_partition_exists() {
        let p = partitions(&[("year", "2024")]);
        assert!(partition_matches(&FilterExpr::IsNotNull("year".into()), &p));
    }

    // ── Verification: no false negatives (tester) ──

    #[test]
    fn test_not_eq_keeps_non_matching() {
        // NOT(year = '2023') should KEEP year=2024
        let p = partitions(&[("year", "2024")]);
        let f = FilterExpr::Not(Box::new(
            FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2023".into()) },
        ));
        assert!(partition_matches(&f, &p));
    }

    #[test]
    fn test_not_eq_prunes_matching() {
        // NOT(year = '2024') should PRUNE year=2024
        let p = partitions(&[("year", "2024")]);
        let f = FilterExpr::Not(Box::new(
            FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2024".into()) },
        ));
        assert!(!partition_matches(&f, &p));
    }

    #[test]
    fn test_not_range_is_conservative() {
        // NOT(year > '2023') — conservative: can't safely prune, must keep
        let p = partitions(&[("year", "2024")]);
        let f = FilterExpr::Not(Box::new(
            FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Gt, value: ScalarValue::Utf8("2023".into()) },
        ));
        assert!(partition_matches(&f, &p));
    }

    #[test]
    fn test_like_is_conservative() {
        // LIKE can't be evaluated on partitions — must keep file
        let p = partitions(&[("service", "api-gateway")]);
        let f = FilterExpr::Comparison { field: "service".into(), op: ComparisonOp::Like, value: ScalarValue::Utf8("api%".into()) };
        assert!(partition_matches(&f, &p));
    }

    #[test]
    fn test_or_both_fail_prunes() {
        let p = partitions(&[("year", "2022")]);
        let f = FilterExpr::Or(
            Box::new(FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2023".into()) }),
            Box::new(FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2024".into()) }),
        );
        assert!(!partition_matches(&f, &p));
    }

    #[test]
    fn test_and_both_match_keeps() {
        let p = partitions(&[("year", "2024"), ("month", "03")]);
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2024".into()) }),
            Box::new(FilterExpr::Comparison { field: "month".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("03".into()) }),
        );
        assert!(partition_matches(&f, &p));
    }

    #[test]
    fn test_is_null_conservative_on_unknown_column() {
        // IS NULL on non-partition column — must keep (conservative)
        let p = partitions(&[("year", "2024")]);
        assert!(partition_matches(&FilterExpr::IsNull("status".into()), &p));
    }

    #[test]
    fn test_empty_partitions_always_keeps() {
        // No partition info — can never prune
        let p = HashMap::new();
        let f = FilterExpr::Comparison { field: "year".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("2024".into()) };
        assert!(partition_matches(&f, &p));
    }
}
