// SPDX-License-Identifier: Apache-2.0

//! Test utilities for connector development: MockConnector, test harness,
//! and assertion helpers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;

use fuse_core::connector::*;
use fuse_core::error::ConnectorError;

/// A configurable mock connector for testing.
///
/// ```rust
/// use fuse_connector_sdk::testing::MockConnector;
///
/// let mock = MockConnector::new("test_ds")
///     .with_table("logs", vec!["message", "level"])
///     .with_rows("logs", vec![
///         vec!["hello world", "INFO"],
///         vec!["something broke", "ERROR"],
///     ]);
/// ```
#[derive(Debug)]
pub struct MockConnector {
    id: String,
    connector_type: String,
    capabilities: ConnectorCapabilities,
    health: ConnectorHealth,
    tables: Mutex<HashMap<String, MockTable>>,
    execute_count: Mutex<u64>,
}

#[derive(Debug, Clone)]
struct MockTable {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl MockConnector {
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            connector_type: "mock".to_string(),
            capabilities: ConnectorCapabilities::full(),
            health: ConnectorHealth {
                status: HealthStatus::Healthy,
                latency_ms: Some(1),
                message: None,
            },
            tables: Mutex::new(HashMap::new()),
            execute_count: Mutex::new(0),
        }
    }

    pub fn with_type(mut self, connector_type: &str) -> Self {
        self.connector_type = connector_type.to_string();
        self
    }

    pub fn with_capabilities(mut self, capabilities: ConnectorCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn with_health(mut self, health: ConnectorHealth) -> Self {
        self.health = health;
        self
    }

    pub fn with_table(self, name: &str, columns: Vec<&str>) -> Self {
        self.tables.lock().unwrap().insert(
            name.to_string(),
            MockTable {
                columns: columns.into_iter().map(|s| s.to_string()).collect(),
                rows: vec![],
            },
        );
        self
    }

    pub fn with_rows(self, table: &str, rows: Vec<Vec<&str>>) -> Self {
        {
            let mut tables = self.tables.lock().unwrap();
            if let Some(t) = tables.get_mut(table) {
                t.rows = rows
                    .into_iter()
                    .map(|r| r.into_iter().map(|s| s.to_string()).collect())
                    .collect();
            }
        }
        self
    }

    /// How many times execute() has been called.
    pub fn execute_count(&self) -> u64 {
        *self.execute_count.lock().unwrap()
    }
}

#[async_trait]
impl FederatedConnector for MockConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn connector_type(&self) -> &str {
        &self.connector_type
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        self.capabilities.clone()
    }

    async fn health_check(&self) -> ConnectorHealth {
        self.health.clone()
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let tables = self.tables.lock().unwrap();
        Ok(tables
            .iter()
            .map(|(name, t)| SchemaInfo {
                name: name.clone(),
                schema_type: SchemaType::Table,
                estimated_row_count: Some(t.rows.len() as u64),
            })
            .collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let tables = self.tables.lock().unwrap();
        let t = tables
            .get(table)
            .ok_or_else(|| ConnectorError::schema(format!("table '{table}' not found")))?;
        let fields: Vec<Field> = t
            .columns
            .iter()
            .map(|c| Field::new(c, DataType::Utf8, true))
            .collect();
        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        *self.execute_count.lock().unwrap() += 1;

        let tables = self.tables.lock().unwrap();
        let t = tables
            .get(&query.table)
            .ok_or_else(|| ConnectorError::query(format!("table '{}' not found", query.table)))?;

        if t.rows.is_empty() {
            let schema = Arc::new(Schema::new(
                t.columns
                    .iter()
                    .map(|c| Field::new(c, DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            ));
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        // Determine which columns to include
        let col_indices: Vec<usize> = if query.projections.is_empty() {
            (0..t.columns.len()).collect()
        } else {
            query
                .projections
                .iter()
                .filter_map(|p| t.columns.iter().position(|c| c == p))
                .collect()
        };

        let fields: Vec<Field> = col_indices
            .iter()
            .map(|&i| Field::new(&t.columns[i], DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let arrays: Vec<Arc<dyn arrow::array::Array>> = col_indices
            .iter()
            .map(|&col_idx| {
                let values: Vec<Option<&str>> = t
                    .rows
                    .iter()
                    .map(|row| row.get(col_idx).map(|s| s.as_str()))
                    .collect();
                Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>
            })
            .collect();

        let mut batch = RecordBatch::try_new(schema, arrays).map_err(ConnectorError::query)?;

        // Apply limit
        if let Some(limit) = query.limit {
            let limit = limit as usize;
            if batch.num_rows() > limit {
                batch = batch.slice(0, limit);
            }
        }

        Ok(vec![batch])
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let batches = self.execute(query).await?;
        for batch in batches {
            tx.send(Ok(batch))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

// ── Assertion helpers ──

/// Assert that a RecordBatch has the expected column names.
pub fn assert_batch_columns(batch: &RecordBatch, expected: &[&str]) {
    let schema = batch.schema();
    let actual: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        actual, expected,
        "column mismatch: got {actual:?}, expected {expected:?}"
    );
}

/// Assert that a RecordBatch has the expected number of rows.
pub fn assert_batch_row_count(batch: &RecordBatch, expected: usize) {
    assert_eq!(
        batch.num_rows(),
        expected,
        "row count mismatch: got {}, expected {expected}",
        batch.num_rows()
    );
}

/// Assert that batches are non-empty.
pub fn assert_batches_non_empty(batches: &[RecordBatch]) {
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total > 0, "expected non-empty results, got 0 rows");
}

/// Assert a connector is healthy.
pub async fn assert_healthy(connector: &dyn FederatedConnector) {
    let health = connector.health_check().await;
    assert_eq!(
        health.status,
        HealthStatus::Healthy,
        "expected Healthy, got {:?}: {:?}",
        health.status,
        health.message
    );
}

/// Run a basic smoke test against a connector: health check, discover schemas,
/// get schema for first table, execute a simple query.
pub async fn smoke_test(connector: &dyn FederatedConnector) -> Result<(), ConnectorError> {
    // Health
    let health = connector.health_check().await;
    assert_ne!(
        health.status,
        HealthStatus::Unhealthy,
        "connector is unhealthy: {:?}",
        health.message
    );

    // Discover
    let schemas = connector.discover_schemas().await?;
    assert!(!schemas.is_empty(), "no schemas discovered");

    // Get schema
    let first_table = &schemas[0].name;
    let schema = connector.get_schema(first_table).await?;
    assert!(!schema.fields().is_empty(), "schema has no fields");

    // Execute
    let query = SubQuery {
        table: first_table.clone(),
        projections: vec![],
        filter: None,
        aggregations: vec![],
        group_by: vec![],
        sort: vec![],
        limit: Some(10),
        having: None,
        offset: None,
        passthrough: None,
    };
    let _batches = connector.execute(&query).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock() -> MockConnector {
        MockConnector::new("test_ds")
            .with_table("logs", vec!["message", "level"])
            .with_rows(
                "logs",
                vec![
                    vec!["hello world", "INFO"],
                    vec!["something broke", "ERROR"],
                ],
            )
    }

    #[tokio::test]
    async fn test_mock_health_check() {
        let m = mock();
        let h = m.health_check().await;
        assert_eq!(h.status, HealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_mock_discover_schemas() {
        let m = mock();
        let schemas = m.discover_schemas().await.unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "logs");
    }

    #[tokio::test]
    async fn test_mock_get_schema() {
        let m = mock();
        let schema = m.get_schema("logs").await.unwrap();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"message"));
        assert!(names.contains(&"level"));
    }

    #[tokio::test]
    async fn test_mock_execute_returns_rows() {
        let m = mock();
        let q = SubQuery {
            table: "logs".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None,
            offset: None,
            passthrough: None,
        };
        let batches = m.execute(&q).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
        assert_eq!(m.execute_count(), 1);
    }

    #[tokio::test]
    async fn test_mock_execute_unknown_table_error() {
        let m = mock();
        let q = SubQuery {
            table: "nonexistent".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            having: None,
            offset: None,
            passthrough: None,
        };
        assert!(m.execute(&q).await.is_err());
    }

    #[tokio::test]
    async fn test_smoke_test_passes_for_mock() {
        let m = mock();
        smoke_test(&m).await.unwrap();
    }

    #[test]
    fn test_assert_batch_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(StringArray::from(vec!["y"])),
            ],
        )
        .unwrap();
        assert_batch_columns(&batch, &["a", "b"]);
        assert_batch_row_count(&batch, 1);
        assert_batches_non_empty(&[batch]);
    }

    #[tokio::test]
    async fn test_assert_healthy() {
        let m = mock();
        assert_healthy(&m).await;
    }
}
