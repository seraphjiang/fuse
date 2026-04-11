// SPDX-License-Identifier: Apache-2.0
//! Apache Kafka connector for the Fuse federated query engine.
//!
//! - Consumes messages from Kafka topics via rskafka (pure Rust)
//! - JSON message parsing with field extraction
//! - Filter by key, timestamp range
//! - Projection pushdown (select specific JSON fields)

use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{OffsetAt, UnknownTopicHandling};
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

pub struct KafkaConnector {
    id: String,
    brokers: Vec<String>,
    tls_config: Option<Arc<rustls::ClientConfig>>,
}

impl fmt::Debug for KafkaConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaConnector")
            .field("id", &self.id)
            .field("brokers", &self.brokers)
            .field("tls", &self.tls_config.is_some())
            .finish()
    }
}

impl KafkaConnector {
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let brokers_str = config.properties.get("brokers")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("kafka: 'brokers' required".into()))?;
        let brokers: Vec<String> = brokers_str.split(',').map(|s| s.trim().to_string()).collect();

        let tls_config = if let Some(tls) = config.tls_config() {
            tls.validate().map_err(|e| ConnectorError::Connection(e.to_string()))?;
            let rc = tls.build_rustls_config()
                .map_err(|e| ConnectorError::Connection(e.to_string()))?;
            Some(Arc::new(rc))
        } else {
            None
        };

        Ok(Self { id: config.id.clone(), brokers, tls_config })
    }

    fn client_builder(&self) -> ClientBuilder {
        let mut builder = ClientBuilder::new(self.brokers.clone());
        if let Some(ref tls) = self.tls_config {
            builder = builder.tls_config(Arc::clone(tls));
        }
        builder
    }

    fn kafka_schema() -> Schema {
        Schema::new(vec![
            Field::new("_key", DataType::Utf8, true),
            Field::new("_partition", DataType::Int64, false),
            Field::new("_offset", DataType::Int64, false),
            Field::new("_timestamp", DataType::Int64, false),
            Field::new("_payload", DataType::Utf8, true),
        ])
    }

    async fn consume_topic(
        &self,
        topic: &str,
        projections: &[String],
        filter: &Option<FilterExpr>,
        limit: Option<u64>,
    ) -> Result<Vec<RecordBatch>, ConnectorError> {
        let client = self.client_builder().build().await
            .map_err(|e| ConnectorError::Connection(format!("kafka: {e}")))?;

        let topics = client.list_topics().await
            .map_err(|e| ConnectorError::Connection(format!("kafka topics: {e}")))?;
        let topic_info = topics.iter().find(|t| t.name == topic)
            .ok_or_else(|| ConnectorError::QueryFailed(format!("topic '{}' not found", topic)))?;

        let max_msgs = limit.unwrap_or(1000) as usize;
        let mut keys: Vec<Option<String>> = Vec::new();
        let mut offsets_col: Vec<i64> = Vec::new();
        let mut partitions_col: Vec<i64> = Vec::new();
        let mut timestamps_col: Vec<i64> = Vec::new();
        let mut payloads: Vec<Option<String>> = Vec::new();

        for pid in 0..topic_info.partitions.len() as i32 {
            if keys.len() >= max_msgs { break; }

            let pc = client.partition_client(topic, pid, UnknownTopicHandling::Error).await
                .map_err(|e| ConnectorError::Connection(format!("kafka partition {pid}: {e}")))?;

            let start = pc.get_offset(OffsetAt::Earliest).await.unwrap_or(0);
            let end = pc.get_offset(OffsetAt::Latest).await.unwrap_or(0);
            if start >= end { continue; }

            let remaining = (max_msgs - keys.len()) as i32;
            let records = pc.fetch_records(start, 1..1_048_576, remaining).await
                .map_err(|e| ConnectorError::QueryFailed(format!("kafka fetch: {e}")))?;

            for rao in &records.0 {
                if keys.len() >= max_msgs { break; }

                let key = rao.record.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string());
                let ts = rao.record.timestamp.timestamp_millis();
                let payload = rao.record.value.as_ref().map(|v| String::from_utf8_lossy(v).to_string());

                if let Some(ref f) = filter {
                    if !matches_filter(f, key.as_deref(), ts) { continue; }
                }

                keys.push(key);
                offsets_col.push(rao.offset);
                partitions_col.push(pid as i64);
                timestamps_col.push(ts);
                payloads.push(payload);
            }
        }

        if keys.is_empty() { return Ok(vec![]); }

        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();
        let all = projections.is_empty();

        if all || projections.iter().any(|p| p == "_key") {
            fields.push(Field::new("_key", DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(keys.clone())));
        }
        if all || projections.iter().any(|p| p == "_partition") {
            fields.push(Field::new("_partition", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(partitions_col)));
        }
        if all || projections.iter().any(|p| p == "_offset") {
            fields.push(Field::new("_offset", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(offsets_col)));
        }
        if all || projections.iter().any(|p| p == "_timestamp") {
            fields.push(Field::new("_timestamp", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(timestamps_col)));
        }
        if all || projections.iter().any(|p| p == "_payload") {
            fields.push(Field::new("_payload", DataType::Utf8, true));
            columns.push(Arc::new(StringArray::from(payloads.clone())));
        }

        // Extract JSON fields from payload
        if !all {
            for jf in projections.iter().filter(|p| !p.starts_with('_')) {
                let vals: Vec<Option<String>> = payloads.iter().map(|p| {
                    p.as_ref().and_then(|s| {
                        serde_json::from_str::<serde_json::Value>(s).ok()
                            .and_then(|v| v.get(jf.as_str()).map(|fv| fv.to_string()))
                    })
                }).collect();
                fields.push(Field::new(jf.as_str(), DataType::Utf8, true));
                columns.push(Arc::new(StringArray::from(vals)));
            }
        }

        if fields.is_empty() { return Ok(vec![]); }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| ConnectorError::QueryFailed(format!("arrow: {e}")))?;
        Ok(vec![batch])
    }
}

fn matches_filter(filter: &FilterExpr, key: Option<&str>, timestamp: i64) -> bool {
    match filter {
        FilterExpr::Comparison { field, op, value } => match field.as_str() {
            "_key" => {
                let ScalarValue::Utf8(ref v) = value else { return true };
                let k = key.unwrap_or("");
                match op {
                    ComparisonOp::Eq => k == v,
                    ComparisonOp::Neq => k != v,
                    ComparisonOp::Like => k.contains(v.as_str()),
                    _ => true,
                }
            }
            "_timestamp" => {
                let ScalarValue::Int64(ref v) = value else { return true };
                match op {
                    ComparisonOp::Eq => timestamp == *v,
                    ComparisonOp::Gt => timestamp > *v,
                    ComparisonOp::Gte => timestamp >= *v,
                    ComparisonOp::Lt => timestamp < *v,
                    ComparisonOp::Lte => timestamp <= *v,
                    _ => true,
                }
            }
            _ => true,
        },
        FilterExpr::And(a, b) => matches_filter(a, key, timestamp) && matches_filter(b, key, timestamp),
        FilterExpr::Or(a, b) => matches_filter(a, key, timestamp) || matches_filter(b, key, timestamp),
        FilterExpr::Not(inner) => !matches_filter(inner, key, timestamp),
        _ => true,
    }
}

#[async_trait]
impl FederatedConnector for KafkaConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "kafka" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 8,
            supports_streaming: true,
            latency_class: LatencyClass::Medium,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        match self.client_builder().build().await {
            Ok(c) => match c.list_topics().await {
                Ok(_) => ConnectorHealth { status: HealthStatus::Healthy, latency_ms: None, message: None },
                Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(format!("{e}")) },
            },
            Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(format!("{e}")) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let client = self.client_builder().build().await
            .map_err(|e| ConnectorError::Connection(format!("kafka: {e}")))?;
        let topics = client.list_topics().await
            .map_err(|e| ConnectorError::Connection(format!("kafka: {e}")))?;

        Ok(topics.iter()
            .filter(|t| !t.name.starts_with("__"))
            .map(|t| SchemaInfo { name: t.name.clone(), schema_type: SchemaType::Table, estimated_row_count: None })
            .collect())
    }

    async fn get_schema(&self, _table: &str) -> Result<Schema, ConnectorError> {
        Ok(Self::kafka_schema())
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        self.consume_topic(&query.table, &query.projections, &query.filter, query.limit).await
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let batches = self.execute(query).await?;
        for batch in batches {
            if tx.send(Ok(batch)).await.is_err() { break; }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct KafkaConnectorFactory;

#[async_trait]
impl ConnectorFactory for KafkaConnectorFactory {
    fn connector_type(&self) -> &str { "kafka" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(KafkaConnector::from_config(config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(brokers: &str) -> ConnectorConfig {
        let mut props = std::collections::HashMap::new();
        props.insert("brokers".into(), toml::Value::String(brokers.into()));
        ConnectorConfig { id: "test-kafka".into(), connector_type: "kafka".into(), properties: props }
    }

    #[test]
    fn test_from_config_ok() {
        let c = KafkaConnector::from_config(&test_config("localhost:9092")).unwrap();
        assert_eq!(c.id, "test-kafka");
        assert_eq!(c.brokers, vec!["localhost:9092"]);
    }

    #[test]
    fn test_from_config_multi_broker() {
        let c = KafkaConnector::from_config(&test_config("a:9092, b:9092, c:9092")).unwrap();
        assert_eq!(c.brokers.len(), 3);
    }

    #[test]
    fn test_from_config_missing_brokers() {
        let config = ConnectorConfig {
            id: "k".into(), connector_type: "kafka".into(), properties: std::collections::HashMap::new(),
        };
        assert!(KafkaConnector::from_config(&config).is_err());
    }

    #[test]
    fn test_connector_type() {
        let c = KafkaConnector::from_config(&test_config("localhost:9092")).unwrap();
        assert_eq!(c.connector_type(), "kafka");
    }

    #[test]
    fn test_capabilities() {
        let c = KafkaConnector::from_config(&test_config("localhost:9092")).unwrap();
        let caps = c.capabilities();
        assert!(caps.supports_filtering);
        assert!(caps.supports_projection);
        assert!(caps.supports_limit);
        assert!(caps.supports_streaming);
        assert!(!caps.supports_aggregation);
    }

    #[test]
    fn test_get_schema() {
        let schema = KafkaConnector::kafka_schema();
        assert_eq!(schema.fields().len(), 5);
        assert!(schema.field_with_name("_key").is_ok());
        assert!(schema.field_with_name("_payload").is_ok());
    }

    #[test]
    fn test_matches_filter_key_eq() {
        let f = FilterExpr::Comparison {
            field: "_key".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("user-123".into()),
        };
        assert!(matches_filter(&f, Some("user-123"), 0));
        assert!(!matches_filter(&f, Some("user-456"), 0));
    }

    #[test]
    fn test_matches_filter_timestamp_gte() {
        let f = FilterExpr::Comparison {
            field: "_timestamp".into(), op: ComparisonOp::Gte, value: ScalarValue::Int64(1000),
        };
        assert!(matches_filter(&f, None, 1000));
        assert!(matches_filter(&f, None, 2000));
        assert!(!matches_filter(&f, None, 999));
    }

    #[test]
    fn test_matches_filter_and() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison {
                field: "_key".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("k".into()),
            }),
            Box::new(FilterExpr::Comparison {
                field: "_timestamp".into(), op: ComparisonOp::Gt, value: ScalarValue::Int64(100),
            }),
        );
        assert!(matches_filter(&f, Some("k"), 200));
        assert!(!matches_filter(&f, Some("k"), 50));
        assert!(!matches_filter(&f, Some("x"), 200));
    }

    #[test]
    fn test_matches_filter_not() {
        let f = FilterExpr::Not(Box::new(FilterExpr::Comparison {
            field: "_key".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("bad".into()),
        }));
        assert!(matches_filter(&f, Some("good"), 0));
        assert!(!matches_filter(&f, Some("bad"), 0));
    }

    #[test]
    fn test_factory_type() {
        assert_eq!(KafkaConnectorFactory.connector_type(), "kafka");
    }

    #[test]
    fn test_tls_config_none_when_not_set() {
        let c = KafkaConnector::from_config(&test_config("localhost:9092")).unwrap();
        assert!(c.tls_config.is_none());
    }
}
