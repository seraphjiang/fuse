// SPDX-License-Identifier: Apache-2.0

//! Query rewrite optimizations applied before fan-out to connectors.
//!
//! - **Predicate pushdown through UNION**: pushes WHERE filters to each
//!   sub-query in a UNION ALL so connectors filter remotely.
//! - **Projection pruning**: propagates column lists to sub-queries so
//!   connectors only return needed columns.
//! - **Limit pushdown**: pushes LIMIT to each sub-query (each side gets
//!   the full limit; the merge layer applies the global limit after).

use fuse_core::connector::SubQuery;

/// Apply all rewrites to a set of per-source sub-queries.
///
/// `base` is the parsed sub-query from the user's original query.
/// `per_source` are the sub-queries that will be sent to each connector.
/// The rewriter copies filters, projections, and limits from `base` into each.
pub fn push_down_to_sources(base: &SubQuery, per_source: &mut [SubQuery]) {
    for sq in per_source.iter_mut() {
        // Push filter
        if sq.filter.is_none() {
            sq.filter = base.filter.clone();
        }
        // Push projections
        if sq.projections.is_empty() && !base.projections.is_empty() {
            sq.projections = base.projections.clone();
        }
        // Push sort
        if sq.sort.is_empty() && !base.sort.is_empty() {
            sq.sort = base.sort.clone();
        }
        // Push limit — for Top-N (sort + limit), use the smaller of base and existing
        if let Some(base_limit) = base.limit {
            match sq.limit {
                None => sq.limit = Some(base_limit),
                Some(existing) => sq.limit = Some(existing.min(base_limit)),
            }
        }
        // Push aggregations + group_by + having
        if sq.aggregations.is_empty() && !base.aggregations.is_empty() {
            sq.aggregations = base.aggregations.clone();
            sq.group_by = base.group_by.clone();
            if sq.having.is_none() {
                sq.having = base.having.clone();
            }
        }
    }
}

/// Remove columns from projections that don't exist in the target schema fields.
pub fn prune_projections(projections: &[String], available_fields: &[String]) -> Vec<String> {
    if projections.is_empty() {
        return vec![]; // empty = all columns
    }
    projections
        .iter()
        .filter(|p| available_fields.contains(p))
        .cloned()
        .collect()
}

// ── Query Plan Optimizer Rules ──

/// Apply all optimizer rules to a SubQuery in place.
pub fn optimize(sq: &mut SubQuery) {
    eliminate_redundant_sort(sq);
    merge_adjacent_limits(sq);
}

/// Rule 1: Eliminate redundant sorts — if no LIMIT and no explicit ORDER BY need,
/// remove sort to avoid unnecessary work at the connector.
/// Keeps sort if there's a limit (Top-N) or if sort is explicitly requested.
pub fn eliminate_redundant_sort(sq: &mut SubQuery) {
    // Sort is only useful with LIMIT (Top-N) or when results need ordering.
    // If there's a GROUP BY with aggregations but no LIMIT, sort is redundant
    // because reaggregation will reorder anyway.
    if !sq.sort.is_empty() && sq.limit.is_none() && !sq.group_by.is_empty() {
        sq.sort.clear();
    }
}

/// Rule 2: Merge adjacent limits — if both the query and a subquery specify limits,
/// use the smaller one.
pub fn merge_adjacent_limits(sq: &mut SubQuery) {
    // Already handled in push_down_to_sources via min(base, existing).
    // This rule handles the case where offset + limit can be tightened:
    // LIMIT 10 OFFSET 5 → connector needs at most 15 rows.
    if let (Some(limit), Some(offset)) = (sq.limit, sq.offset) {
        let needed = limit.saturating_add(offset);
        sq.limit = Some(needed);
        // Offset applied post-fetch, not at connector
        sq.offset = None;
    }
}

/// Rule 3: Extract join-side filters from a WHERE clause.
/// Given a filter and two table aliases, split into left-only, right-only, and shared filters.
pub fn split_join_filters(
    filter: &fuse_core::connector::FilterExpr,
    left_columns: &[String],
    right_columns: &[String],
) -> (Option<fuse_core::connector::FilterExpr>, Option<fuse_core::connector::FilterExpr>) {
    use fuse_core::connector::FilterExpr;
    match filter {
        FilterExpr::And(l, r) => {
            let (ll, lr) = split_join_filters(l, left_columns, right_columns);
            let (rl, rr) = split_join_filters(r, left_columns, right_columns);
            let left = match (ll, rl) {
                (Some(a), Some(b)) => Some(FilterExpr::And(Box::new(a), Box::new(b))),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            };
            let right = match (lr, rr) {
                (Some(a), Some(b)) => Some(FilterExpr::And(Box::new(a), Box::new(b))),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            };
            (left, right)
        }
        FilterExpr::Comparison { field, .. } => {
            let bare = field.rsplit('.').next().unwrap_or(field);
            if left_columns.iter().any(|c| c == bare) {
                (Some(filter.clone()), None)
            } else if right_columns.iter().any(|c| c == bare) {
                (None, Some(filter.clone()))
            } else {
                // Shared — push to both sides
                (Some(filter.clone()), Some(filter.clone()))
            }
        }
        _ => (Some(filter.clone()), Some(filter.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuse_core::connector::{ComparisonOp, FilterExpr, ScalarValue, SortExpr};

    fn empty_sq(table: &str) -> SubQuery {
        SubQuery {
            table: table.into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None, offset: None, passthrough: None,
        }
    }

    fn base_with_filter() -> SubQuery {
        SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "status".into(),
                op: ComparisonOp::Gte,
                value: ScalarValue::Int64(500),
            }),
            limit: Some(100),
            projections: vec!["host".into(), "status".into()],
            sort: vec![SortExpr {
                field: "status".into(),
                descending: true,
            }],
            ..empty_sq("ignored")
        }
    }

    #[test]
    fn test_push_down_filter_to_all_sources() {
        let base = base_with_filter();
        let mut sources = vec![empty_sq("logs_a"), empty_sq("logs_b")];
        push_down_to_sources(&base, &mut sources);

        for sq in &sources {
            assert!(sq.filter.is_some());
            assert_eq!(sq.limit, Some(100));
            assert_eq!(sq.projections, vec!["host", "status"]);
            assert_eq!(sq.sort.len(), 1);
        }
        // Table names preserved
        assert_eq!(sources[0].table, "logs_a");
        assert_eq!(sources[1].table, "logs_b");
    }

    #[test]
    fn test_push_down_does_not_overwrite_existing() {
        let base = base_with_filter();
        let existing_filter = FilterExpr::Comparison {
            field: "level".into(),
            op: ComparisonOp::Eq,
            value: ScalarValue::Utf8("ERROR".into()),
        };
        let mut sources = vec![SubQuery {
            filter: Some(existing_filter),
            limit: Some(10),
            projections: vec!["level".into()],
            ..empty_sq("logs_a")
        }];
        push_down_to_sources(&base, &mut sources);

        // Existing values not overwritten
        assert_eq!(sources[0].limit, Some(10));
        assert_eq!(sources[0].projections, vec!["level"]);
        // Filter kept (not overwritten)
        match &sources[0].filter {
            Some(FilterExpr::Comparison { field, .. }) => assert_eq!(field, "level"),
            _ => panic!("expected existing filter"),
        }
    }

    #[test]
    fn test_push_down_empty_base_is_noop() {
        let base = empty_sq("x");
        let mut sources = vec![empty_sq("a"), empty_sq("b")];
        push_down_to_sources(&base, &mut sources);
        for sq in &sources {
            assert!(sq.filter.is_none());
            assert!(sq.projections.is_empty());
            assert!(sq.limit.is_none());
        }
    }

    #[test]
    fn test_prune_projections() {
        let available = vec!["host".into(), "status".into(), "message".into()];
        let pruned = prune_projections(
            &["host".into(), "nonexistent".into(), "status".into()],
            &available,
        );
        assert_eq!(pruned, vec!["host", "status"]);
    }

    #[test]
    fn test_prune_projections_empty_means_all() {
        let available = vec!["a".into(), "b".into()];
        let pruned = prune_projections(&[], &available);
        assert!(pruned.is_empty()); // empty = all columns
    }

    #[test]
    fn test_push_down_having() {
        use fuse_core::connector::{AggFunction, AggregationExpr};
        let base = SubQuery {
            aggregations: vec![AggregationExpr {
                function: AggFunction::Count,
                field: None,
                alias: "cnt".into(),
            }],
            group_by: vec!["service".into()],
            having: Some(FilterExpr::Comparison {
                field: "cnt".into(),
                op: ComparisonOp::Gt,
                value: ScalarValue::Int64(5),
            }),
            ..empty_sq("x")
        };
        let mut sources = vec![empty_sq("a"), empty_sq("b")];
        push_down_to_sources(&base, &mut sources);
        for sq in &sources {
            assert_eq!(sq.aggregations.len(), 1);
            assert_eq!(sq.group_by, vec!["service"]);
            assert!(sq.having.is_some());
        }
    }

    #[test]
    fn test_push_down_having_not_overwritten() {
        use fuse_core::connector::{AggFunction, AggregationExpr};
        let base = SubQuery {
            aggregations: vec![AggregationExpr {
                function: AggFunction::Count,
                field: None,
                alias: "cnt".into(),
            }],
            group_by: vec!["service".into()],
            having: Some(FilterExpr::Comparison {
                field: "cnt".into(),
                op: ComparisonOp::Gt,
                value: ScalarValue::Int64(5),
            }),
            ..empty_sq("x")
        };
        let existing_having = FilterExpr::Comparison {
            field: "cnt".into(),
            op: ComparisonOp::Gt,
            value: ScalarValue::Int64(10),
        };
        let mut sources = vec![SubQuery {
            aggregations: vec![AggregationExpr {
                function: AggFunction::Count,
                field: None,
                alias: "cnt".into(),
            }],
            group_by: vec!["service".into()],
            having: Some(existing_having),
            ..empty_sq("a")
        }];
        push_down_to_sources(&base, &mut sources);
        // Existing having not overwritten (aggregations already present, so no push)
        match &sources[0].having {
            Some(FilterExpr::Comparison { value: ScalarValue::Int64(10), .. }) => {}
            _ => panic!("expected existing having with value 10"),
        }
    }

    #[test]
    fn test_top_n_pushdown_uses_min_limit() {
        // Source has default 10k limit, base has LIMIT 10 → should push 10
        let base = SubQuery {
            table: "t".into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec![],
            sort: vec![SortExpr { field: "count".into(), descending: true }],
            limit: Some(10), having: None, offset: None, passthrough: None,
        };
        let mut sources = vec![SubQuery {
            table: "t".into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec![], sort: vec![],
            limit: Some(10_000), having: None, offset: None, passthrough: None,
        }];
        push_down_to_sources(&base, &mut sources);
        assert_eq!(sources[0].limit, Some(10));
        assert_eq!(sources[0].sort.len(), 1);
    }

    #[test]
    fn test_pushdown_limit_no_existing() {
        // Source has no limit, base has LIMIT 50 → should push 50
        let base = SubQuery {
            table: "t".into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec![], sort: vec![],
            limit: Some(50), having: None, offset: None, passthrough: None,
        };
        let mut sources = vec![SubQuery {
            table: "t".into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec![], sort: vec![],
            limit: None, having: None, offset: None, passthrough: None,
        }];
        push_down_to_sources(&base, &mut sources);
        assert_eq!(sources[0].limit, Some(50));
    }

    #[test]
    fn test_eliminate_redundant_sort_with_group_by() {
        let mut sq = SubQuery {
            table: "t".into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec!["host".into()],
            sort: vec![SortExpr { field: "host".into(), descending: false }],
            limit: None, having: None, offset: None, passthrough: None,
        };
        eliminate_redundant_sort(&mut sq);
        assert!(sq.sort.is_empty(), "sort should be removed when GROUP BY + no LIMIT");
    }

    #[test]
    fn test_keep_sort_with_limit() {
        let mut sq = SubQuery {
            table: "t".into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec!["host".into()],
            sort: vec![SortExpr { field: "host".into(), descending: true }],
            limit: Some(10), having: None, offset: None, passthrough: None,
        };
        eliminate_redundant_sort(&mut sq);
        assert_eq!(sq.sort.len(), 1, "sort should be kept for Top-N");
    }

    #[test]
    fn test_merge_limit_offset() {
        let mut sq = SubQuery {
            table: "t".into(), projections: vec![], filter: None,
            aggregations: vec![], group_by: vec![], sort: vec![],
            limit: Some(10), having: None, offset: Some(5), passthrough: None,
        };
        merge_adjacent_limits(&mut sq);
        assert_eq!(sq.limit, Some(15), "limit should be limit+offset");
        assert_eq!(sq.offset, None, "offset cleared — applied post-fetch");
    }

    #[test]
    fn test_split_join_filters() {
        let filter = FilterExpr::And(
            Box::new(FilterExpr::Comparison {
                field: "status".into(),
                op: ComparisonOp::Gte,
                value: ScalarValue::Int64(500),
            }),
            Box::new(FilterExpr::Comparison {
                field: "region".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Utf8("us-east-1".into()),
            }),
        );
        let left_cols = vec!["status".into()];
        let right_cols = vec!["region".into()];
        let (left, right) = split_join_filters(&filter, &left_cols, &right_cols);
        assert!(left.is_some(), "status filter should go to left");
        assert!(right.is_some(), "region filter should go to right");
    }
}
