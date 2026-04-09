// SPDX-License-Identifier: Apache-2.0

//! Fuse Engine — DataFusion-based federated query planner and execution engine.
//!
//! Integrates with `datafusion-federation` to route table scans to the
//! appropriate [`FederatedConnector`](fuse_core::FederatedConnector) via the
//! [`SQLExecutor`] trait.

mod merger;
mod optimizer;
mod planner;

pub use merger::{merge_batches, sort_batches, union_batches};
pub use optimizer::{apply_connector_pushdown, PushdownDecision};
pub use planner::{FuseEngine, FuseExecutor};
