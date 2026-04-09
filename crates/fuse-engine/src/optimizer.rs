// SPDX-License-Identifier: Apache-2.0

//! Push-down optimization utilities.
//!
//! The heavy lifting is done by `datafusion-federation`'s built-in optimizer
//! which identifies sub-plans belonging to a single `FederationProvider` and
//! replaces them with `FederatedPlanNode`s. The SQL unparser then generates
//! a query string with filters, projections, and aggregations included.
//!
//! This module provides Fuse-specific helpers that inspect
//! [`ConnectorCapabilities`] to decide what can be pushed down.

use fuse_core::ConnectorCapabilities;

/// Determines which parts of a query can be pushed to a connector.
pub fn apply_connector_pushdown(capabilities: &ConnectorCapabilities) -> PushdownDecision {
    PushdownDecision {
        push_filters: capabilities.supports_filtering,
        push_projections: capabilities.supports_projection,
        push_aggregations: capabilities.supports_aggregation,
        push_sort: capabilities.supports_sorting,
        push_limit: capabilities.supports_limit,
    }
}

/// Describes what operations should be pushed to the remote connector
/// vs. executed locally in the Fuse engine.
#[derive(Debug, Clone)]
pub struct PushdownDecision {
    pub push_filters: bool,
    pub push_projections: bool,
    pub push_aggregations: bool,
    pub push_sort: bool,
    pub push_limit: bool,
}

impl PushdownDecision {
    /// Returns true if any operation can be pushed down.
    pub fn any_pushdown(&self) -> bool {
        self.push_filters
            || self.push_projections
            || self.push_aggregations
            || self.push_sort
            || self.push_limit
    }
}
