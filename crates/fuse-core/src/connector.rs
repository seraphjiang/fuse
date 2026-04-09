// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::error::ConnectorError;

/// Every datasource connector implements this trait.
#[async_trait]
pub trait FederatedConnector: Send + Sync {
    fn id(&self) -> &str;
    fn connector_type(&self) -> ConnectorType;
    fn capabilities(&self) -> ConnectorCapabilities;
    async fn health_check(&self) -> ConnectorHealth;
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError>;
    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError>;
    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError>;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorCapabilities {
    pub filter: PushDownSupport,
    pub projection: PushDownSupport,
    pub aggregation: PushDownSupport,
    pub sorting: PushDownSupport,
    pub limit: PushDownSupport,
    pub join: PushDownSupport,
    pub max_concurrent_queries: usize,
    pub supports_streaming: bool,
    pub latency_class: LatencyClass,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PushDownSupport {
    Full,
    Partial,
    None,
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
    pub sort: Vec<SortExpr>,
    pub limit: Option<u64>,
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
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub struct SortExpr {
    pub field: String,
    pub descending: bool,
}
