// SPDX-License-Identifier: Apache-2.0

//! CloudWatch Logs connector for Fuse.
//!
//! Queries CloudWatch Logs Insights and returns results as Arrow RecordBatches.
//! Config: `connector_type = "cloudwatch"`, requires `log_group` and `region` properties.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use aws_sdk_cloudwatchlogs as cwl;
use tokio::sync::mpsc;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

pub struct CloudWatchConnector {
    id: String,
    client: cwl::Client,
    log_group: String,
}

impl std::fmt::Debug for CloudWatchConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudWatchConnector")
            .field("id", &self.id)
            .field("log_group", &self.log_group)
            .finish()
    }
}

impl CloudWatchConnector {
    pub async fn new(id: String, region: &str, log_group: String) -> Self {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load()
            .await;
        let client = cwl::Client::new(&config);
        Self {
            id,
            client,
            log_group,
        }
    }

    fn build_insights_query(&self, query: &SubQuery) -> String {
        let mut parts = vec!["fields @timestamp, @message, @logStream".to_string()];
        if let Some(filter) = &query.filter {
            if let Some(s) = filter_to_insights(filter) {
                parts.push(format!("| filter {}", s));
            }
        }
        if !query.sort.is_empty() {
            let s: Vec<String> = query
                .sort
                .iter()
                .map(|s| format!("{} {}", s.field, if s.descending { "desc" } else { "asc" }))
                .collect();
            parts.push(format!("| sort {}", s.join(", ")));
        }
        if let Some(limit) = query.limit {
            parts.push(format!("| limit {}", limit));
        }
        parts.join(" ")
    }

    fn results_to_batches(&self, results: &[Vec<cwl::types::ResultField>]) -> Vec<RecordBatch> {
        if results.is_empty() {
            return vec![];
        }
        let mut timestamps = Vec::with_capacity(results.len());
        let mut messages = Vec::with_capacity(results.len());
        let mut streams = Vec::with_capacity(results.len());
        for row in results {
            let (mut ts, mut msg, mut stream) = (String::new(), String::new(), String::new());
            for field in row {
                match field.field().unwrap_or_default() {
                    "@timestamp" => ts = field.value().unwrap_or_default().to_string(),
                    "@message" => msg = field.value().unwrap_or_default().to_string(),
                    "@logStream" => stream = field.value().unwrap_or_default().to_string(),
                    _ => {}
                }
            }
            timestamps.push(ts);
            messages.push(msg);
            streams.push(stream);
        }
        let schema = Arc::new(self.schema());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(timestamps)),
                Arc::new(StringArray::from(messages)),
                Arc::new(StringArray::from(streams)),
            ],
        )
        .into_iter()
        .collect()
    }

    fn schema(&self) -> Schema {
        Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, false),
            Field::new("log_stream", DataType::Utf8, true),
        ])
    }
}

fn filter_to_insights(expr: &FilterExpr) -> Option<String> {
    match expr {
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=",
                ComparisonOp::Neq => "!=",
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
                ComparisonOp::Like | ComparisonOp::ILike | ComparisonOp::Contains => "like",
            };
            Some(format!(
                "{} {} {}",
                field,
                op_str,
                scalar_to_insights(value)
            ))
        }
        FilterExpr::And(l, r) => Some(format!(
            "({} and {})",
            filter_to_insights(l)?,
            filter_to_insights(r)?
        )),
        FilterExpr::Or(l, r) => Some(format!(
            "({} or {})",
            filter_to_insights(l)?,
            filter_to_insights(r)?
        )),
        FilterExpr::Not(inner) => Some(format!("not {}", filter_to_insights(inner)?)),
        FilterExpr::IsNotNull(field) => Some(format!("ispresent({})", field)),
        _ => None,
    }
}

fn scalar_to_insights(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Null => "null".into(),
        ScalarValue::Boolean(b) => b.to_string(),
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(f) => f.to_string(),
        ScalarValue::Utf8(s) => format!("\"{}\"", s.replace('"', "\\\"")),
    }
}

#[async_trait]
impl FederatedConnector for CloudWatchConnector {
    fn id(&self) -> &str {
        &self.id
    }
    fn connector_type(&self) -> &str {
        "cloudwatch"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: false,
            supports_aggregation: false,
            supports_sorting: true,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 5,
            supports_streaming: false,
            latency_class: LatencyClass::Medium,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        match self
            .client
            .describe_log_groups()
            .log_group_name_prefix(&self.log_group)
            .limit(1)
            .send()
            .await
        {
            Ok(_) => ConnectorHealth {
                status: HealthStatus::Healthy,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                message: None,
            },
            Err(e) => ConnectorHealth {
                status: HealthStatus::Unhealthy,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                message: Some(format!("{e}")),
            },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        Ok(vec![SchemaInfo {
            name: self.log_group.clone(),
            schema_type: SchemaType::Table,
            estimated_row_count: None,
        }])
    }

    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        Ok(self.schema())
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let insights_query = self.build_insights_query(query);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let start_resp = self
            .client
            .start_query()
            .log_group_name(&self.log_group)
            .start_time(now - 3600)
            .end_time(now)
            .query_string(&insights_query)
            .send()
            .await
            .map_err(|e| ConnectorError::QueryFailed(format!("start_query: {e}")))?;

        let query_id = start_resp
            .query_id()
            .ok_or_else(|| ConnectorError::QueryFailed("no query_id".into()))?
            .to_string();

        for _ in 0..60 {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            let result = self
                .client
                .get_query_results()
                .query_id(&query_id)
                .send()
                .await
                .map_err(|e| ConnectorError::QueryFailed(format!("get_query_results: {e}")))?;
            let status = result
                .status()
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            match status.as_str() {
                "Complete" => return Ok(self.results_to_batches(result.results())),
                "Failed" | "Cancelled" | "Timeout" => {
                    return Err(ConnectorError::QueryFailed(format!("query {status}")));
                }
                _ => continue,
            }
        }
        Err(ConnectorError::QueryFailed(
            "query timed out after 30s".into(),
        ))
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let batches = self.execute(query).await?;
        for batch in batches {
            let _ = tx.send(Ok(batch)).await;
        }
        Ok(())
    }
}

// ── Factory ──

pub struct CloudWatchConnectorFactory;

#[async_trait]
impl ConnectorFactory for CloudWatchConnectorFactory {
    fn connector_type(&self) -> &str {
        "cloudwatch"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let log_group = config
            .properties
            .get("log_group")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                ConnectorError::Connection("cloudwatch requires 'log_group' property".into())
            })?
            .to_string();
        let region = config
            .properties
            .get("region")
            .and_then(|v| v.as_str())
            .unwrap_or("us-east-1");
        Ok(Arc::new(
            CloudWatchConnector::new(config.id.clone(), region, log_group).await,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_connector() -> CloudWatchConnector {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(CloudWatchConnector::new(
                "cw1".into(),
                "us-east-1",
                "/aws/lambda/my-fn".into(),
            ))
    }

    fn empty_sq() -> SubQuery {
        SubQuery {
            table: "logs".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            passthrough: None,
            offset: None,
        }
    }

    #[test]
    fn test_insights_query_basic() {
        let c = make_connector();
        let q = c.build_insights_query(&SubQuery {
            limit: Some(20),
            ..empty_sq()
        });
        assert!(q.starts_with("fields @timestamp"));
        assert!(q.contains("| limit 20"));
        assert!(!q.contains("| filter"));
    }

    #[test]
    fn test_insights_query_with_filter() {
        let c = make_connector();
        let sq = SubQuery {
            filter: Some(FilterExpr::Comparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Utf8("ERROR".into()),
            }),
            ..empty_sq()
        };
        assert!(c
            .build_insights_query(&sq)
            .contains(r#"| filter level = "ERROR""#));
    }

    #[test]
    fn test_insights_query_and_filter() {
        let c = make_connector();
        let sq = SubQuery {
            filter: Some(FilterExpr::And(
                Box::new(FilterExpr::Comparison {
                    field: "level".into(),
                    op: ComparisonOp::Eq,
                    value: ScalarValue::Utf8("ERROR".into()),
                }),
                Box::new(FilterExpr::Comparison {
                    field: "status".into(),
                    op: ComparisonOp::Gte,
                    value: ScalarValue::Int64(500),
                }),
            )),
            ..empty_sq()
        };
        let q = c.build_insights_query(&sq);
        assert!(q.contains(r#"(level = "ERROR" and status >= 500)"#));
    }

    #[test]
    fn test_insights_query_sort_and_limit() {
        let c = make_connector();
        let sq = SubQuery {
            sort: vec![SortExpr {
                field: "@timestamp".into(),
                descending: true,
            }],
            limit: Some(10),
            ..empty_sq()
        };
        let q = c.build_insights_query(&sq);
        assert!(q.contains("| sort @timestamp desc"));
        assert!(q.contains("| limit 10"));
    }

    #[test]
    fn test_scalar_to_insights() {
        assert_eq!(scalar_to_insights(&ScalarValue::Int64(42)), "42");
        assert_eq!(
            scalar_to_insights(&ScalarValue::Utf8("hi".into())),
            "\"hi\""
        );
        assert_eq!(scalar_to_insights(&ScalarValue::Boolean(true)), "true");
        assert_eq!(scalar_to_insights(&ScalarValue::Null), "null");
    }

    #[test]
    fn test_results_to_batches_empty() {
        assert!(make_connector().results_to_batches(&[]).is_empty());
    }

    #[test]
    fn test_capabilities() {
        let caps = make_connector().capabilities();
        assert!(caps.supports_filtering);
        assert!(caps.supports_limit);
        assert!(caps.supports_sorting);
        assert!(!caps.supports_aggregation);
    }

    #[test]
    fn test_metadata() {
        let c = make_connector();
        assert_eq!(c.id(), "cw1");
        assert_eq!(c.connector_type(), "cloudwatch");
    }

    #[test]
    fn test_factory_type() {
        assert_eq!(CloudWatchConnectorFactory.connector_type(), "cloudwatch");
    }

    #[test]
    fn test_not_filter() {
        let c = make_connector();
        let sq = SubQuery {
            filter: Some(FilterExpr::Not(Box::new(FilterExpr::Comparison {
                field: "level".into(),
                op: ComparisonOp::Eq,
                value: ScalarValue::Utf8("DEBUG".into()),
            }))),
            ..empty_sq()
        };
        assert!(c
            .build_insights_query(&sq)
            .contains(r#"not level = "DEBUG""#));
    }

    #[test]
    fn test_is_not_null_filter() {
        let c = make_connector();
        let sq = SubQuery {
            filter: Some(FilterExpr::IsNotNull("trace_id".into())),
            ..empty_sq()
        };
        assert!(c.build_insights_query(&sq).contains("ispresent(trace_id)"));
    }
}
