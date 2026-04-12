// SPDX-License-Identifier: Apache-2.0

//! Tests for the connector SDK testing utilities.

use fuse_connector_sdk::prelude::*;
use fuse_connector_sdk::testing::*;

#[tokio::test]
async fn test_mock_connector_smoke_test() {
    let mock = MockConnector::new("test_ds")
        .with_table("events", vec!["id", "name"])
        .with_rows("events", vec![vec!["1", "click"], vec!["2", "view"]]);
    smoke_test(&mock).await.unwrap();
}

#[tokio::test]
async fn test_mock_connector_health() {
    let mock = MockConnector::new("healthy_ds");
    assert_healthy(&mock).await;
}

#[tokio::test]
async fn test_mock_connector_unhealthy() {
    let mock = MockConnector::new("bad_ds").with_health(ConnectorHealth {
        status: HealthStatus::Unhealthy,
        latency_ms: None,
        message: Some("down".into()),
    });
    let h = mock.health_check().await;
    assert!(matches!(h.status, HealthStatus::Unhealthy));
    assert_eq!(h.message.as_deref(), Some("down"));
}

#[tokio::test]
async fn test_mock_connector_execute() {
    let mock = MockConnector::new("ds")
        .with_table("logs", vec!["service", "status"])
        .with_rows("logs", vec![vec!["api", "200"], vec!["auth", "401"]]);

    let query = SubQuery {
        table: "logs".into(),
        projections: vec![],
        filter: None,
        aggregations: vec![],
        group_by: vec![],
        sort: vec![],
        limit: None,
        offset: None,
        passthrough: None,
        having: None,
    };
    let batches = mock.execute(&query).await.unwrap();
    assert_batches_non_empty(&batches);
    assert_batch_columns(&batches[0], &["service", "status"]);
    assert_batch_row_count(&batches[0], 2);
}

#[tokio::test]
async fn test_mock_connector_execute_count() {
    let mock = MockConnector::new("ds").with_table("t", vec!["a"]);

    let query = SubQuery {
        table: "t".into(),
        projections: vec![],
        filter: None,
        aggregations: vec![],
        group_by: vec![],
        sort: vec![],
        limit: None,
        offset: None,
        passthrough: None,
        having: None,
    };
    assert_eq!(mock.execute_count(), 0);
    mock.execute(&query).await.unwrap();
    assert_eq!(mock.execute_count(), 1);
    mock.execute(&query).await.unwrap();
    assert_eq!(mock.execute_count(), 2);
}

#[test]
fn test_mock_connector_metadata() {
    let mock = MockConnector::new("my_ds").with_type("custom");
    assert_eq!(mock.id(), "my_ds");
    assert_eq!(mock.connector_type(), "custom");
}

#[tokio::test]
async fn test_mock_connector_schemas() {
    let mock = MockConnector::new("ds")
        .with_table("users", vec!["id", "name"])
        .with_table("orders", vec!["order_id", "total"]);

    let schemas = mock.discover_schemas().await.unwrap();
    assert_eq!(schemas.len(), 2);

    let schema = mock.get_schema("users").await.unwrap();
    assert_eq!(schema.fields().len(), 2);
}

#[tokio::test]
async fn test_prelude_exports_work() {
    // Verify prelude re-exports compile and are usable
    let caps = ConnectorCapabilities::full();
    assert!(caps.supports_filtering);

    let health = ConnectorHealth {
        status: HealthStatus::Healthy,
        latency_ms: Some(5),
        message: None,
    };
    assert!(matches!(health.status, HealthStatus::Healthy));
}
