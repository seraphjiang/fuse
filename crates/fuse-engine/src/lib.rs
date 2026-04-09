// SPDX-License-Identifier: Apache-2.0

//! Fuse Engine — DataFusion-based federated query planner and execution engine.
//!
//! Integrates with `datafusion-federation` to route table scans to the
//! appropriate [`FederatedConnector`](fuse_core::connector::FederatedConnector)
//! via the [`SQLExecutor`] trait.

mod merger;
mod optimizer;
pub mod ppl;
mod planner;
mod sql_to_subquery;

pub use merger::{
    align_batch, dedup_batches, merge_batches, sort_batches, union_batches, union_schema,
};
pub use optimizer::{apply_connector_pushdown, PushdownDecision};
pub use planner::{FuseEngine, FuseExecutor};
