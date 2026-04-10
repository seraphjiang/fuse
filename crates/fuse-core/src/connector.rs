// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::error::ConnectorError;

/// Every datasource connector implements this trait.
#[async_trait]
pub trait FederatedConnector: Send + Sync + fmt::Debug {
    /// Unique identifier for this connector instance.
    fn id(&self) -> &str;

    /// Connector type string (e.g., "opensearch", "s3", "prometheus").
    fn connector_type(&self) -> &str;

    /// Protocol version this connector implements. Defaults to current.
    fn version(&self) -> crate::version::ConnectorVersion {
        crate::version::ConnectorVersion::current()
    }

    /// Declare what this connector can do.
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Health check.
    async fn health_check(&self) -> ConnectorHealth;

    /// List available schemas (indices, tables, buckets, etc.).
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError>;

    /// Get Arrow schema for a specific table.
    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError>;

    /// List table names (convenience wrapper over discover_schemas).
    async fn table_names(&self) -> Result<Vec<String>, ConnectorError> {
        let schemas = self.discover_schemas().await?;
        Ok(schemas.into_iter().map(|s| s.name).collect())
    }

    /// Get Arrow schema ref for a specific table (for DataFusion integration).
    async fn get_table_schema(&self, table: &str) -> Result<SchemaRef, ConnectorError> {
        let schema = self.get_schema(table).await?;
        Ok(Arc::new(schema))
    }

    /// Execute a sub-query. Returns RecordBatches.
    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError>;

    /// Execute a sub-query with streaming results.
    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorType {
    OpenSearch,
    S3,
    Prometheus,
    Jdbc,
}

impl std::fmt::Display for ConnectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenSearch => write!(f, "opensearch"),
            Self::S3 => write!(f, "s3"),
            Self::Prometheus => write!(f, "prometheus"),
            Self::Jdbc => write!(f, "jdbc"),
        }
    }
}

/// Capabilities declaration used by the planner for push-down decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorCapabilities {
    pub supports_filtering: bool,
    pub supports_projection: bool,
    pub supports_aggregation: bool,
    pub supports_sorting: bool,
    pub supports_limit: bool,
    pub supports_join: bool,
    pub max_concurrent_queries: usize,
    pub supports_streaming: bool,
    pub latency_class: LatencyClass,
}

impl ConnectorCapabilities {
    /// Full push-down support (typical for OpenSearch).
    pub fn full() -> Self {
        Self {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: true,
            supports_sorting: true,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 16,
            supports_streaming: true,
            latency_class: LatencyClass::Low,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LatencyClass {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorHealth {
    pub status: HealthStatus,
    pub latency_ms: Option<u64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub name: String,
    pub schema_type: SchemaType,
    pub estimated_row_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaType {
    Index,
    Table,
    Bucket,
    MetricName,
}

// ── SubQuery and expression types ──

#[derive(Debug, Clone)]
pub struct SubQuery {
    pub table: String,
    pub projections: Vec<String>,
    pub filter: Option<FilterExpr>,
    pub aggregations: Vec<AggregationExpr>,
    pub group_by: Vec<String>,
    pub having: Option<FilterExpr>,
    pub sort: Vec<SortExpr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub passthrough: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub enum FilterExpr {
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
    Comparison {
        field: String,
        op: ComparisonOp,
        value: ScalarValue,
    },
    In {
        field: String,
        values: Vec<ScalarValue>,
    },
    IsNull(String),
    IsNotNull(String),
}

#[derive(Debug, Clone, Copy)]
pub enum ComparisonOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Like,
    ILike,
    /// Full-text search: CONTAINS or MATCH. Maps to LIKE '%term%' for generic connectors,
    /// native full-text query for OpenSearch/Elasticsearch.
    Contains,
}

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
}

#[derive(Debug, Clone)]
pub struct AggregationExpr {
    pub function: AggFunction,
    pub field: Option<String>,
    pub alias: String,
}

#[derive(Debug, Clone, Copy)]
pub enum AggFunction {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    /// Approximate count distinct via HyperLogLog (pushdown to OpenSearch cardinality).
    ApproxCountDistinct,
    /// Approximate percentile via t-digest. `percentile` is 0.0–1.0.
    ApproxPercentile(f64),
}

#[derive(Debug, Clone)]
pub struct SortExpr {
    pub field: String,
    pub descending: bool,
}

/// Serializable result set for the REST API layer.
#[derive(Debug, Clone, Serialize)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub total_rows: u64,
    pub truncated: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capabilities_full() {
        let c = ConnectorCapabilities::full();
        assert!(c.supports_filtering);
        assert!(c.supports_projection);
        assert!(c.supports_aggregation);
        assert!(c.supports_sorting);
        assert!(c.supports_limit);
        assert!(c.supports_streaming);
        assert!(!c.supports_join); // join is intentionally false for remote connectors
        assert!(matches!(c.latency_class, LatencyClass::Low));
    }

    #[test]
    fn test_health_status_variants() {
        assert_ne!(HealthStatus::Healthy, HealthStatus::Unhealthy);
        assert_ne!(HealthStatus::Healthy, HealthStatus::Degraded);
    }
}
