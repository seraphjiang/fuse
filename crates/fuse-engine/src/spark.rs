// SPDX-License-Identifier: Apache-2.0

//! Spark delegation for heavy cross-source JOINs.
//!
//! When a federated join is too large for local hash-join execution (both sides
//! > 100k rows or total > 1M rows), the join can be delegated to an external
//! > Spark cluster. This module defines:
//!
//! - [`SparkBackend`] trait — interface for submitting SQL to Spark
//! - [`should_delegate_to_spark`] — delegation rule based on table stats
//! - [`LivyBackend`] / [`EmrServerlessBackend`] — stub implementations
//!
//! Actual Spark client implementations are future work.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};

use crate::cost::TableStats;
use crate::join::JoinPlan;

/// Status of a Spark job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SparkJobStatus {
    Pending,
    Running,
    Succeeded,
    Failed(String),
    Cancelled,
}

/// Trait for submitting queries to an external Spark cluster.
#[async_trait]
pub trait SparkBackend: Send + Sync + std::fmt::Debug {
    /// Submit a SQL query for execution. Returns a job ID.
    async fn submit_query(&self, sql: &str) -> Result<String>;

    /// Check the status of a submitted job.
    async fn check_status(&self, job_id: &str) -> Result<SparkJobStatus>;

    /// Retrieve results of a completed job.
    async fn get_results(&self, job_id: &str) -> Result<Vec<RecordBatch>>;

    /// Cancel a running job.
    async fn cancel(&self, job_id: &str) -> Result<()>;
}

// ── Delegation rule ──

/// Threshold: delegate to Spark if both sides exceed this row count.
const BOTH_SIDES_THRESHOLD: u64 = 100_000;

/// Threshold: delegate if total estimated rows across both sides exceed this.
const TOTAL_ROWS_THRESHOLD: u64 = 1_000_000;

/// Decide whether a join should be delegated to Spark.
pub fn should_delegate_to_spark(left_stats: &TableStats, right_stats: &TableStats) -> bool {
    let both_large = left_stats.estimated_rows > BOTH_SIDES_THRESHOLD
        && right_stats.estimated_rows > BOTH_SIDES_THRESHOLD;
    let total_large = left_stats.estimated_rows + right_stats.estimated_rows > TOTAL_ROWS_THRESHOLD;
    both_large || total_large
}

/// Extended join strategy that includes Spark delegation.
#[derive(Debug, Clone)]
pub enum FederatedJoinStrategy {
    /// Execute locally using hash-join or semi-join.
    Local(JoinPlan),
    /// Delegate to Spark.
    Spark { sql: String },
}

/// Plan a join, considering Spark delegation if a backend is available.
///
/// If `spark_backend` is `Some` and the delegation rule triggers, returns
/// `Spark` strategy. Otherwise falls back to the local `JoinPlan`.
pub fn plan_federated_join(
    left_stats: &TableStats,
    right_stats: &TableStats,
    local_plan: JoinPlan,
    spark_backend: Option<&Arc<dyn SparkBackend>>,
) -> FederatedJoinStrategy {
    match spark_backend {
        Some(_) if should_delegate_to_spark(left_stats, right_stats) => {
            FederatedJoinStrategy::Spark {
                sql: String::new(), // Caller fills in the actual SQL
            }
        }
        _ => FederatedJoinStrategy::Local(local_plan),
    }
}

// ── Stub implementations ──

/// Apache Livy REST API backend (stub).
#[derive(Debug)]
pub struct LivyBackend {
    pub endpoint: String,
}

impl LivyBackend {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
}

#[async_trait]
impl SparkBackend for LivyBackend {
    async fn submit_query(&self, _sql: &str) -> Result<String> {
        Err(DataFusionError::NotImplemented(
            "Livy backend not yet implemented".into(),
        ))
    }

    async fn check_status(&self, _job_id: &str) -> Result<SparkJobStatus> {
        Err(DataFusionError::NotImplemented(
            "Livy backend not yet implemented".into(),
        ))
    }

    async fn get_results(&self, _job_id: &str) -> Result<Vec<RecordBatch>> {
        Err(DataFusionError::NotImplemented(
            "Livy backend not yet implemented".into(),
        ))
    }

    async fn cancel(&self, _job_id: &str) -> Result<()> {
        Err(DataFusionError::NotImplemented(
            "Livy backend not yet implemented".into(),
        ))
    }
}

/// AWS EMR Serverless backend (stub).
#[derive(Debug)]
pub struct EmrServerlessBackend {
    pub application_id: String,
    pub execution_role_arn: String,
}

impl EmrServerlessBackend {
    pub fn new(application_id: impl Into<String>, execution_role_arn: impl Into<String>) -> Self {
        Self {
            application_id: application_id.into(),
            execution_role_arn: execution_role_arn.into(),
        }
    }
}

#[async_trait]
impl SparkBackend for EmrServerlessBackend {
    async fn submit_query(&self, _sql: &str) -> Result<String> {
        Err(DataFusionError::NotImplemented(
            "EMR Serverless backend not yet implemented".into(),
        ))
    }

    async fn check_status(&self, _job_id: &str) -> Result<SparkJobStatus> {
        Err(DataFusionError::NotImplemented(
            "EMR Serverless backend not yet implemented".into(),
        ))
    }

    async fn get_results(&self, _job_id: &str) -> Result<Vec<RecordBatch>> {
        Err(DataFusionError::NotImplemented(
            "EMR Serverless backend not yet implemented".into(),
        ))
    }

    async fn cancel(&self, _job_id: &str) -> Result<()> {
        Err(DataFusionError::NotImplemented(
            "EMR Serverless backend not yet implemented".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::CostEstimate;
    use crate::join::{JoinSide, JoinStrategy};

    fn small_stats() -> TableStats {
        TableStats {
            estimated_rows: 1_000,
            avg_row_bytes: 256,
        }
    }

    fn large_stats() -> TableStats {
        TableStats {
            estimated_rows: 500_000,
            avg_row_bytes: 256,
        }
    }

    fn dummy_plan() -> JoinPlan {
        JoinPlan {
            build_side: JoinSide::Left,
            strategy: JoinStrategy::Hash,
            estimated_cost: CostEstimate::zero(),
        }
    }

    #[test]
    fn test_no_delegation_for_small_tables() {
        assert!(!should_delegate_to_spark(&small_stats(), &small_stats()));
    }

    #[test]
    fn test_delegation_when_both_large() {
        assert!(should_delegate_to_spark(&large_stats(), &large_stats()));
    }

    #[test]
    fn test_delegation_when_total_exceeds_threshold() {
        let a = TableStats {
            estimated_rows: 50_000,
            avg_row_bytes: 256,
        };
        let b = TableStats {
            estimated_rows: 960_000,
            avg_row_bytes: 256,
        };
        assert!(should_delegate_to_spark(&a, &b));
    }

    #[test]
    fn test_plan_falls_back_to_local_without_backend() {
        let plan = plan_federated_join(&large_stats(), &large_stats(), dummy_plan(), None);
        assert!(matches!(plan, FederatedJoinStrategy::Local(_)));
    }

    #[test]
    fn test_plan_delegates_with_backend_and_large_tables() {
        let backend: Arc<dyn SparkBackend> = Arc::new(LivyBackend::new("http://livy:8998"));
        let plan =
            plan_federated_join(&large_stats(), &large_stats(), dummy_plan(), Some(&backend));
        assert!(matches!(plan, FederatedJoinStrategy::Spark { .. }));
    }

    #[test]
    fn test_plan_stays_local_with_backend_but_small_tables() {
        let backend: Arc<dyn SparkBackend> = Arc::new(LivyBackend::new("http://livy:8998"));
        let plan =
            plan_federated_join(&small_stats(), &small_stats(), dummy_plan(), Some(&backend));
        assert!(matches!(plan, FederatedJoinStrategy::Local(_)));
    }
}
