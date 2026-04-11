// SPDX-License-Identifier: Apache-2.0
//! Pushdown negotiator — determine what to push down vs execute locally.

use crate::connector::ConnectorCapabilities;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct PushdownPlan {
    pub filter_pushdown: bool,
    pub projection_pushdown: bool,
    pub aggregation_pushdown: bool,
    pub sort_pushdown: bool,
    pub limit_pushdown: bool,
    pub local_operations: Vec<String>,
}

/// Negotiate pushdown based on connector capabilities and query needs.
pub fn negotiate(
    caps: &ConnectorCapabilities,
    needs_filter: bool,
    needs_projection: bool,
    needs_aggregation: bool,
    needs_sort: bool,
    needs_limit: bool,
) -> PushdownPlan {
    let mut local = Vec::new();

    let filter_pd = needs_filter && caps.supports_filtering;
    if needs_filter && !filter_pd { local.push("filter".into()); }

    let proj_pd = needs_projection && caps.supports_projection;
    if needs_projection && !proj_pd { local.push("projection".into()); }

    let agg_pd = needs_aggregation && caps.supports_aggregation;
    if needs_aggregation && !agg_pd { local.push("aggregation".into()); }

    let sort_pd = needs_sort && caps.supports_sorting;
    if needs_sort && !sort_pd { local.push("sort".into()); }

    let limit_pd = needs_limit && caps.supports_limit;
    if needs_limit && !limit_pd { local.push("limit".into()); }

    PushdownPlan {
        filter_pushdown: filter_pd,
        projection_pushdown: proj_pd,
        aggregation_pushdown: agg_pd,
        sort_pushdown: sort_pd,
        limit_pushdown: limit_pd,
        local_operations: local,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_pushdown() {
        let caps = ConnectorCapabilities::full();
        let plan = negotiate(&caps, true, true, true, true, true);
        assert!(plan.filter_pushdown);
        assert!(plan.sort_pushdown);
        assert!(plan.local_operations.is_empty());
    }

    #[test]
    fn test_no_pushdown() {
        let caps = ConnectorCapabilities {
            supports_filtering: false, supports_projection: false,
            supports_aggregation: false, supports_sorting: false,
            supports_limit: false, supports_join: false, supports_streaming: false,
            latency_class: crate::connector::LatencyClass::Medium, max_concurrent_queries: 16,
        };
        let plan = negotiate(&caps, true, true, true, true, true);
        assert!(!plan.filter_pushdown);
        assert_eq!(plan.local_operations.len(), 5);
    }

    #[test]
    fn test_partial_pushdown() {
        let mut caps = ConnectorCapabilities::full();
        caps.supports_aggregation = false;
        caps.supports_sorting = false;
        let plan = negotiate(&caps, true, true, true, true, true);
        assert!(plan.filter_pushdown);
        assert!(!plan.aggregation_pushdown);
        assert_eq!(plan.local_operations, vec!["aggregation", "sort"]);
    }

    #[test]
    fn test_no_needs() {
        let caps = ConnectorCapabilities::full();
        let plan = negotiate(&caps, false, false, false, false, false);
        assert!(plan.local_operations.is_empty());
    }
}
