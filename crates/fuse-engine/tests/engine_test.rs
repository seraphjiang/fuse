// SPDX-License-Identifier: Apache-2.0

//! Tests for optimizer pushdown decisions and connector capabilities.

use fuse_core::connector::{ConnectorCapabilities, LatencyClass};
use fuse_engine::apply_connector_pushdown;

#[test]
fn test_pushdown_full_capabilities() {
    let caps = ConnectorCapabilities::full();
    let decision = apply_connector_pushdown(&caps);
    assert!(decision.push_filters);
    assert!(decision.push_projections);
    assert!(decision.push_aggregations);
    assert!(decision.push_sort);
    assert!(decision.push_limit);
    assert!(decision.any_pushdown());
}

#[test]
fn test_pushdown_no_capabilities() {
    let caps = ConnectorCapabilities {
        supports_filtering: false,
        supports_projection: false,
        supports_aggregation: false,
        supports_sorting: false,
        supports_limit: false,
        supports_join: false,
        max_concurrent_queries: 1,
        supports_streaming: false,
        latency_class: LatencyClass::High,
    };
    let decision = apply_connector_pushdown(&caps);
    assert!(!decision.push_filters);
    assert!(!decision.push_projections);
    assert!(!decision.push_aggregations);
    assert!(!decision.push_sort);
    assert!(!decision.push_limit);
    assert!(!decision.any_pushdown());
}

#[test]
fn test_pushdown_partial_capabilities() {
    let caps = ConnectorCapabilities {
        supports_filtering: true,
        supports_projection: true,
        supports_aggregation: false,
        supports_sorting: false,
        supports_limit: true,
        supports_join: false,
        max_concurrent_queries: 4,
        supports_streaming: true,
        latency_class: LatencyClass::Medium,
    };
    let decision = apply_connector_pushdown(&caps);
    assert!(decision.push_filters);
    assert!(decision.push_projections);
    assert!(!decision.push_aggregations);
    assert!(!decision.push_sort);
    assert!(decision.push_limit);
    assert!(decision.any_pushdown());
}

#[test]
fn test_pushdown_filter_only() {
    let caps = ConnectorCapabilities {
        supports_filtering: true,
        supports_projection: false,
        supports_aggregation: false,
        supports_sorting: false,
        supports_limit: false,
        supports_join: false,
        max_concurrent_queries: 1,
        supports_streaming: false,
        latency_class: LatencyClass::High,
    };
    let decision = apply_connector_pushdown(&caps);
    assert!(decision.push_filters);
    assert!(decision.any_pushdown());
}
