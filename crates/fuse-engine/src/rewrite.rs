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
        // Push limit (each source gets the full limit; global limit applied after merge)
        if sq.limit.is_none() && base.limit.is_some() {
            sq.limit = base.limit;
        }
        // Push aggregations + group_by
        if sq.aggregations.is_empty() && !base.aggregations.is_empty() {
            sq.aggregations = base.aggregations.clone();
            sq.group_by = base.group_by.clone();
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
            having: None, passthrough: None,
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
}
