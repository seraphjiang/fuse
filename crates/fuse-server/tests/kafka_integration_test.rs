// SPDX-License-Identifier: Apache-2.0
//! #841 Kafka connector integration tests.

use fuse_connector_kafka::KafkaConnector;
use fuse_core::config::ConnectorConfig;
use fuse_core::connector::FederatedConnector;
use std::collections::HashMap;

fn test_config(brokers: &str) -> ConnectorConfig {
    let mut props = HashMap::new();
    props.insert("brokers".into(), toml::Value::String(brokers.into()));
    ConnectorConfig {
        id: "kafka_test".into(),
        connector_type: "kafka".into(),
        properties: props,
    }
}

#[test]
fn test_kafka_config_single_broker() {
    let c = KafkaConnector::from_config(&test_config("localhost:9092")).unwrap();
    assert_eq!(c.id(), "kafka_test");
    assert_eq!(c.connector_type(), "kafka");
}

#[test]
fn test_kafka_config_multi_broker() {
    let c = KafkaConnector::from_config(&test_config("a:9092, b:9092, c:9092")).unwrap();
    assert_eq!(c.id(), "kafka_test");
}

#[test]
fn test_kafka_config_missing_brokers() {
    let config = ConnectorConfig {
        id: "bad".into(),
        connector_type: "kafka".into(),
        properties: HashMap::new(),
    };
    assert!(KafkaConnector::from_config(&config).is_err());
}

#[test]
fn test_kafka_schema_has_expected_fields() {
    let c = KafkaConnector::from_config(&test_config("localhost:9092")).unwrap();
    let caps = c.capabilities();
    assert!(caps.supports_filtering);
    assert!(caps.supports_streaming);
    assert!(
        !caps.supports_aggregation,
        "kafka should not support aggregation"
    );
}

#[test]
fn test_kafka_connector_type() {
    let c = KafkaConnector::from_config(&test_config("localhost:9092")).unwrap();
    assert_eq!(c.connector_type(), "kafka");
}

#[tokio::test]
async fn test_kafka_health_unreachable() {
    use fuse_core::connector::HealthStatus;
    let c = KafkaConnector::from_config(&test_config("localhost:19092")).unwrap();
    let health = c.health_check().await;
    assert_eq!(
        health.status,
        HealthStatus::Unhealthy,
        "unreachable broker should be unhealthy"
    );
}
