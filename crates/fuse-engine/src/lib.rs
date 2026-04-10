// SPDX-License-Identifier: Apache-2.0

//! Fuse Engine — DataFusion-based federated query planner and execution engine.
//!
//! Integrates with `datafusion-federation` to route table scans to the
//! appropriate [`FederatedConnector`](fuse_core::connector::FederatedConnector)
//! via the [`SQLExecutor`] trait.

pub mod anomaly;
pub mod cache;
pub mod cache_middleware;
pub mod cost;
pub mod join;
pub mod materialized;
mod merger;
mod optimizer;
pub mod plan;
pub mod ppl;
mod planner;
pub mod rewrite;
pub mod spark;
pub mod sql_to_subquery;

pub use cost::{
    estimate_local_cost, estimate_remote_cost, pick_cheapest_connector, should_push_down,
    CostEstimate, QueryWorkload, TableStats,
};
pub use join::{
    execute_semi_join, extract_join_keys, hash_join, keys_to_in_filter, plan_join, JoinPlan,
    JoinSide, JoinStrategy, JoinType,
};
pub use merger::{
    align_batch, dedup_batches, merge_batches, sort_batches, union_batches, union_schema,
};
pub use optimizer::{apply_connector_pushdown, PushdownDecision};
pub use planner::{FuseEngine, FuseExecutor};
pub use spark::{
    plan_federated_join, should_delegate_to_spark, EmrServerlessBackend, FederatedJoinStrategy,
    LivyBackend, SparkBackend, SparkJobStatus,
};
