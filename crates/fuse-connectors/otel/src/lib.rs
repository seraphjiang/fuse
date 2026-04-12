// SPDX-License-Identifier: Apache-2.0
//! OpenTelemetry Collector connector — ingest OTLP traces/metrics/logs, query with SQL.
//!
//! Fuse acts as an OTel backend: applications send OTLP data via HTTP,
//! Fuse stores it in-memory, and users query it with standard SQL.
//!
//! # Tables
//!
//! - `spans` — trace spans with service, operation, duration, status, attributes
//! - `metrics` — metric data points with name, value, labels
//! - `logs` — log records with severity, body, resource attributes
//!
//! # Configuration (fuse.toml)
//!
//! ```toml
//! [[connector]]
//! id = "otel"
//! type = "otel"
//! [connector.properties]
//! max_spans = 100000
//! max_metrics = 100000
//! max_logs = 100000
//! ```

pub mod store;

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::{
    ComparisonOp, ConnectorCapabilities, ConnectorHealth, FederatedConnector, FilterExpr,
    HealthStatus, LatencyClass, SchemaInfo, SchemaType, SubQuery,
};
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

use store::{OtelFilter, OtelStore};

/// OpenTelemetry Collector connector.
#[derive(Debug)]
pub struct OtelConnector {
    id: String,
    store: Arc<OtelStore>,
}

impl OtelConnector {
    pub fn new(id: impl Into<String>, store: Arc<OtelStore>) -> Self {
        Self { id: id.into(), store }
    }

    /// Access the backing store (for ingestion routes).
    pub fn store(&self) -> &Arc<OtelStore> {
        &self.store
    }
}

/// Extract an OtelFilter from a SubQuery's filter expression.
/// Recognizes: service_name = 'x', start_time_ns >= N, end_time_ns <= N,
/// timestamp_ns >= N, timestamp_ns <= N.
fn extract_otel_filter(filter: &Option<FilterExpr>) -> OtelFilter {
    let mut out = OtelFilter::default();
    if let Some(expr) = filter {
        collect_filter(expr, &mut out);
    }
    out
}

fn collect_filter(expr: &FilterExpr, out: &mut OtelFilter) {
    match expr {
        FilterExpr::And(l, r) => {
            collect_filter(l, out);
            collect_filter(r, out);
        }
        FilterExpr::Comparison { field, op, value } => {
            let s = match value {
                fuse_core::connector::ScalarValue::Utf8(s) => Some(s.clone()),
                _ => None,
            };
            let i = match value {
                fuse_core::connector::ScalarValue::Int64(n) => Some(*n),
                _ => None,
            };
            match (field.as_str(), op) {
                ("service_name", ComparisonOp::Eq) => {
                    if let Some(v) = s { out.service_name = Some(v); }
                }
                ("start_time_ns" | "timestamp_ns", ComparisonOp::Gte | ComparisonOp::Gt) => {
                    if let Some(v) = i { out.min_time_ns = Some(v); }
                }
                ("end_time_ns" | "timestamp_ns", ComparisonOp::Lte | ComparisonOp::Lt) => {
                    if let Some(v) = i { out.max_time_ns = Some(v); }
                }
                _ => {}
            }
        }
        _ => {}
    }
}

#[async_trait]
impl FederatedConnector for OtelConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn connector_type(&self) -> &str {
        "otel"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: false,
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 16,
            supports_streaming: true,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let counts = self.store.counts();
        ConnectorHealth {
            status: HealthStatus::Healthy,
            latency_ms: Some(0),
            message: Some(format!(
                "spans={}, metrics={}, logs={}",
                counts.0, counts.1, counts.2
            )),
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let counts = self.store.counts();
        Ok(vec![
            SchemaInfo {
                name: "spans".into(),
                schema_type: SchemaType::Table,
                estimated_row_count: Some(counts.0 as u64),
            },
            SchemaInfo {
                name: "metrics".into(),
                schema_type: SchemaType::Table,
                estimated_row_count: Some(counts.1 as u64),
            },
            SchemaInfo {
                name: "logs".into(),
                schema_type: SchemaType::Table,
                estimated_row_count: Some(counts.2 as u64),
            },
        ])
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        match table {
            "spans" => Ok(store::spans_schema()),
            "metrics" => Ok(store::metrics_schema()),
            "logs" => Ok(store::logs_schema()),
            _ => Err(ConnectorError::QueryFailed(format!("table not found: {}", table))),
        }
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let filter = extract_otel_filter(&query.filter);
        let batch = match query.table.as_str() {
            "spans" => self.store.query_spans_filtered(query.limit, &filter),
            "metrics" => self.store.query_metrics_filtered(query.limit, &filter),
            "logs" => self.store.query_logs_filtered(query.limit, &filter),
            _ => return Err(ConnectorError::QueryFailed(format!("table not found: {}", query.table))),
        };
        match batch {
            Some(b) => Ok(vec![b]),
            None => Ok(vec![]),
        }
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for batch in self.execute(query).await? {
            tx.send(Ok(batch))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

/// Factory for creating OtelConnector instances.
pub struct OtelConnectorFactory;

#[async_trait]
impl ConnectorFactory for OtelConnectorFactory {
    fn connector_type(&self) -> &str {
        "otel"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let max_spans = config.properties.get("max_spans")
            .and_then(|v| v.as_integer()).unwrap_or(100_000) as usize;
        let max_metrics = config.properties.get("max_metrics")
            .and_then(|v| v.as_integer()).unwrap_or(100_000) as usize;
        let max_logs = config.properties.get("max_logs")
            .and_then(|v| v.as_integer()).unwrap_or(100_000) as usize;

        let store = Arc::new(OtelStore::new(max_spans, max_metrics, max_logs));
        Ok(Arc::new(OtelConnector::new(&config.id, store)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fuse_core::connector::ScalarValue;

    fn connector() -> OtelConnector {
        OtelConnector::new("test_otel", Arc::new(OtelStore::new(1000, 1000, 1000)))
    }

    #[tokio::test]
    async fn test_health_check() {
        let c = connector();
        let h = c.health_check().await;
        assert_eq!(h.status, HealthStatus::Healthy);
        assert!(h.message.unwrap().contains("spans=0"));
    }

    #[tokio::test]
    async fn test_discover_schemas() {
        let schemas = connector().discover_schemas().await.unwrap();
        assert_eq!(schemas.len(), 3);
        let names: Vec<&str> = schemas.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"spans"));
        assert!(names.contains(&"metrics"));
        assert!(names.contains(&"logs"));
    }

    #[tokio::test]
    async fn test_get_schema_spans() {
        let schema = connector().get_schema("spans").await.unwrap();
        assert!(schema.field_with_name("trace_id").is_ok());
        assert!(schema.field_with_name("span_id").is_ok());
        assert!(schema.field_with_name("service_name").is_ok());
        assert!(schema.field_with_name("duration_ns").is_ok());
    }

    #[tokio::test]
    async fn test_get_schema_unknown_table() {
        let err = connector().get_schema("unknown").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_execute_empty() {
        let sq = SubQuery {
            table: "spans".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None,
            passthrough: None,
            offset: None,
        };
        let batches = connector().execute(&sq).await.unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn test_execute_after_ingest() {
        let store = Arc::new(OtelStore::new(1000, 1000, 1000));
        let c = OtelConnector::new("test", store.clone());

        store.ingest_span(
            "abc123", "span1", Some("root"), "my-service", "GET /api",
            "OK", 1_000_000_000, 1_001_000_000, "{}",
        );

        let sq = SubQuery {
            table: "spans".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None,
            passthrough: None,
            offset: None,
        };
        let batches = c.execute(&sq).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_execute_respects_limit() {
        let store = Arc::new(OtelStore::new(1000, 1000, 1000));
        let c = OtelConnector::new("test", store.clone());

        for i in 0..10 {
            store.ingest_span(
                &format!("trace{i}"), &format!("span{i}"), None,
                "svc", "op", "OK", i, i + 1, "{}",
            );
        }

        let sq = SubQuery {
            table: "spans".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: Some(3),
            having: None,
            passthrough: None,
            offset: None,
        };
        let batches = c.execute(&sq).await.unwrap();
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_execute_unknown_table() {
        let sq = SubQuery {
            table: "unknown".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None,
            passthrough: None,
            offset: None,
        };
        assert!(connector().execute(&sq).await.is_err());
    }

    #[tokio::test]
    async fn test_connector_type() {
        assert_eq!(connector().connector_type(), "otel");
    }

    #[tokio::test]
    async fn test_capabilities() {
        let caps = connector().capabilities();
        assert!(caps.supports_limit);
        assert!(caps.supports_streaming);
        assert!(caps.supports_filtering);
    }

    #[tokio::test]
    async fn test_execute_with_service_filter() {
        let store = Arc::new(OtelStore::new(1000, 1000, 1000));
        let c = OtelConnector::new("test", store.clone());
        store.ingest_span("t1", "s1", None, "svc-a", "op", "OK", 100, 200, "");
        store.ingest_span("t2", "s2", None, "svc-b", "op", "OK", 100, 200, "");

        let sq = SubQuery {
            table: "spans".into(),
            projections: vec![],
            filter: Some(FilterExpr::Comparison {
                field: "service_name".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Utf8("svc-a".into()),
            }),
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None,
            passthrough: None,
            offset: None,
        };
        let batches = c.execute(&sq).await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_execute_with_time_filter() {
        let store = Arc::new(OtelStore::new(1000, 1000, 1000));
        let c = OtelConnector::new("test", store.clone());
        store.ingest_log(100, "INFO", "early", None, None, None, None);
        store.ingest_log(500, "WARN", "late", None, None, None, None);

        let sq = SubQuery {
            table: "logs".into(),
            projections: vec![],
            filter: Some(FilterExpr::Comparison {
                field: "timestamp_ns".into(),
                op: ComparisonOp::Gte,
                value: ScalarValue::Int64(300),
            }),
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None,
            passthrough: None,
            offset: None,
        };
        let batches = c.execute(&sq).await.unwrap();
        assert_eq!(batches[0].num_rows(), 1);
    }
}
