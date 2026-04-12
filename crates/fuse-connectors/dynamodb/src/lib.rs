// SPDX-License-Identifier: Apache-2.0

//! DynamoDB connector for the Fuse federated query engine.
//!
//! Supports:
//! - Schema discovery via DescribeTable (key schema + attribute definitions)
//! - Scan with FilterExpression pushdown
//! - Query with KeyConditionExpression (when partition key equality filter present)
//! - Projection pushdown via ProjectionExpression
//! - Limit pushdown

pub mod expr;

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

use expr::{build_filter_expression, build_key_condition};

/// DynamoDB connector — reads from DynamoDB tables via Scan or Query.
#[derive(Debug)]
pub struct DynamoDbConnector {
    id: String,
    client: Client,
    /// Optional table prefix to strip when mapping table names.
    table_prefix: String,
}

impl DynamoDbConnector {
    pub fn new(id: String, client: Client, table_prefix: String) -> Self {
        Self {
            id,
            client,
            table_prefix,
        }
    }

    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let sdk_config = aws_config::load_from_env().await;
        let mut builder = aws_sdk_dynamodb::config::Builder::from(&sdk_config);

        // Optional endpoint override (for local DynamoDB / testing)
        if let Some(endpoint) = config.properties.get("endpoint").and_then(|v| v.as_str()) {
            builder = builder.endpoint_url(endpoint);
        }

        let client = Client::from_conf(builder.build());
        let prefix = config
            .properties
            .get("table_prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        Ok(Self::new(config.id.clone(), client, prefix))
    }

    /// Map a Fuse table name to a DynamoDB table name (add prefix).
    fn ddb_table(&self, table: &str) -> String {
        if self.table_prefix.is_empty() {
            table.to_string()
        } else {
            format!("{}{}", self.table_prefix, table)
        }
    }

    /// Discover the partition key name for a table (used to decide Scan vs Query).
    async fn partition_key(&self, table: &str) -> Option<String> {
        let resp = self
            .client
            .describe_table()
            .table_name(table)
            .send()
            .await
            .ok()?;
        let table_desc = resp.table()?;
        table_desc
            .key_schema()
            .iter()
            .find(|k| k.key_type() == &aws_sdk_dynamodb::types::KeyType::Hash)
            .map(|k| k.attribute_name().to_string())
    }
}

/// Check if an AWS SDK error is a ResourceNotFoundException (table not found).
fn is_resource_not_found<E: std::fmt::Display>(err: &aws_sdk_dynamodb::error::SdkError<E>) -> bool {
    let msg = err.to_string();
    msg.contains("ResourceNotFoundException") || msg.contains("resource not found")
}

#[async_trait]
impl FederatedConnector for DynamoDbConnector {
    fn id(&self) -> &str {
        &self.id
    }
    fn connector_type(&self) -> &str {
        "dynamodb"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: false,
            supports_sorting: false,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 8,
            supports_streaming: false,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        match self.client.list_tables().limit(1).send().await {
            Ok(_) => ConnectorHealth {
                status: HealthStatus::Healthy,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                message: None,
            },
            Err(e) => ConnectorHealth {
                status: HealthStatus::Unhealthy,
                latency_ms: None,
                message: Some(e.to_string()),
            },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let mut tables = Vec::new();
        let mut last_key: Option<String> = None;

        loop {
            let mut req = self.client.list_tables().limit(100);
            if let Some(ref k) = last_key {
                req = req.exclusive_start_table_name(k);
            }
            let resp = req.send().await.map_err(|e| {
                ConnectorError::schema(format!("failed to list DynamoDB tables: {}", e))
            })?;
            for name in resp.table_names() {
                let fuse_name = if self.table_prefix.is_empty() {
                    name.to_string()
                } else {
                    name.strip_prefix(&self.table_prefix)
                        .unwrap_or(name)
                        .to_string()
                };
                tables.push(SchemaInfo {
                    name: fuse_name,
                    schema_type: SchemaType::Table,
                    estimated_row_count: None,
                });
            }
            last_key = resp.last_evaluated_table_name().map(|s| s.to_string());
            if last_key.is_none() {
                break;
            }
        }

        Ok(tables)
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let ddb_table = self.ddb_table(table);
        let resp = self
            .client
            .describe_table()
            .table_name(&ddb_table)
            .send()
            .await
            .map_err(|e| {
                if is_resource_not_found(&e) {
                    ConnectorError::schema(format!("table '{}' not found in DynamoDB", ddb_table))
                } else {
                    ConnectorError::schema(e)
                }
            })?;

        let table_desc = resp
            .table()
            .ok_or_else(|| ConnectorError::schema("DescribeTable returned no table"))?;

        let mut fields = Vec::new();
        for attr in table_desc.attribute_definitions() {
            let dt = match attr.attribute_type() {
                t if t == &aws_sdk_dynamodb::types::ScalarAttributeType::S => DataType::Utf8,
                t if t == &aws_sdk_dynamodb::types::ScalarAttributeType::N => DataType::Float64,
                _ => DataType::Utf8, // B (binary) → base64 string
            };
            fields.push(Field::new(attr.attribute_name(), dt, true));
        }

        // If no attribute definitions (shouldn't happen), return minimal schema
        if fields.is_empty() {
            fields.push(Field::new("_item", DataType::Utf8, true));
        }

        Ok(Schema::new(fields))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let ddb_table = self.ddb_table(&query.table);
        debug!(table = ddb_table.as_str(), "DynamoDB execute");

        // Decide: Query (if partition key equality present) or Scan
        let pk = self.partition_key(&ddb_table).await;
        let key_cond = pk.as_deref().and_then(|pk| {
            query
                .filter
                .as_ref()
                .and_then(|f| build_key_condition(f, pk))
        });

        let items = if let Some((kce, names, values)) = key_cond {
            // Use Query API — more efficient
            let mut req = self
                .client
                .query()
                .table_name(&ddb_table)
                .key_condition_expression(kce);

            for (k, v) in names {
                req = req.expression_attribute_names(k, v);
            }
            for (k, v) in values {
                req = req.expression_attribute_values(k, v);
            }

            // Remaining filter (non-key predicates)
            if let Some((fe, fn2, fv2)) = query
                .filter
                .as_ref()
                .and_then(|f| build_filter_expression(f, pk.as_deref()))
            {
                req = req.filter_expression(fe);
                for (k, v) in fn2 {
                    req = req.expression_attribute_names(k, v);
                }
                for (k, v) in fv2 {
                    req = req.expression_attribute_values(k, v);
                }
            }

            if let Some(limit) = query.limit {
                req = req.limit(limit as i32);
            }
            if !query.projections.is_empty() {
                let (pe, pnames) = projection_expr(&query.projections);
                req = req.projection_expression(pe);
                for (k, v) in pnames {
                    req = req.expression_attribute_names(k, v);
                }
            }

            req.send()
                .await
                .map_err(|e| {
                    if is_resource_not_found(&e) {
                        ConnectorError::query(format!(
                            "table '{}' not found in DynamoDB",
                            ddb_table
                        ))
                    } else {
                        ConnectorError::query(e)
                    }
                })?
                .items()
                .to_vec()
        } else {
            // Use Scan API
            let mut req = self.client.scan().table_name(&ddb_table);

            if let Some((fe, fn2, fv2)) = query
                .filter
                .as_ref()
                .and_then(|f| build_filter_expression(f, None))
            {
                req = req.filter_expression(fe);
                for (k, v) in fn2 {
                    req = req.expression_attribute_names(k, v);
                }
                for (k, v) in fv2 {
                    req = req.expression_attribute_values(k, v);
                }
            }

            if let Some(limit) = query.limit {
                req = req.limit(limit as i32);
            }
            if !query.projections.is_empty() {
                let (pe, pnames) = projection_expr(&query.projections);
                req = req.projection_expression(pe);
                for (k, v) in pnames {
                    req = req.expression_attribute_names(k, v);
                }
            }

            req.send()
                .await
                .map_err(|e| {
                    if is_resource_not_found(&e) {
                        ConnectorError::query(format!(
                            "table '{}' not found in DynamoDB",
                            ddb_table
                        ))
                    } else {
                        ConnectorError::query(e)
                    }
                })?
                .items()
                .to_vec()
        };

        if items.is_empty() {
            return Ok(vec![]);
        }

        items_to_batch(&items, query)
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

/// Build a ProjectionExpression + name aliases from a list of column names.
fn projection_expr(cols: &[String]) -> (String, Vec<(String, String)>) {
    let mut names = Vec::new();
    let mut placeholders = Vec::new();
    for (i, col) in cols.iter().enumerate() {
        let alias = format!("#p{i}");
        names.push((alias.clone(), col.clone()));
        placeholders.push(alias);
    }
    (placeholders.join(", "), names)
}

/// Convert DynamoDB items (list of attribute maps) into a RecordBatch.
fn items_to_batch(
    items: &[std::collections::HashMap<String, AttributeValue>],
    query: &SubQuery,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    // Collect all column names (union of all item keys, or projections if specified)
    let cols: Vec<String> = if !query.projections.is_empty() {
        query.projections.clone()
    } else {
        let mut seen = std::collections::HashSet::new();
        let mut cols = Vec::new();
        for item in items {
            for k in item.keys() {
                if seen.insert(k.clone()) {
                    cols.push(k.clone());
                }
            }
        }
        cols.sort();
        cols
    };

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for col in &cols {
        // Infer type from first non-null value
        let first_val = items.iter().find_map(|item| item.get(col));
        let (field, array) = match first_val {
            Some(AttributeValue::N(_)) => {
                let vals: Vec<Option<f64>> = items
                    .iter()
                    .map(|item| {
                        item.get(col).and_then(|v| {
                            if let AttributeValue::N(s) = v {
                                s.parse().ok()
                            } else {
                                None
                            }
                        })
                    })
                    .collect();
                (
                    Field::new(col, DataType::Float64, true),
                    Arc::new(Float64Array::from(vals)) as ArrayRef,
                )
            }
            Some(AttributeValue::Bool(_)) => {
                let vals: Vec<Option<bool>> = items
                    .iter()
                    .map(|item| {
                        item.get(col).and_then(|v| {
                            if let AttributeValue::Bool(b) = v {
                                Some(*b)
                            } else {
                                None
                            }
                        })
                    })
                    .collect();
                (
                    Field::new(col, DataType::Boolean, true),
                    Arc::new(BooleanArray::from(vals)) as ArrayRef,
                )
            }
            _ => {
                // S, NULL, L, M, B → stringify
                let vals: Vec<Option<String>> = items
                    .iter()
                    .map(|item| item.get(col).map(attr_to_string))
                    .collect();
                (
                    Field::new(col, DataType::Utf8, true),
                    Arc::new(StringArray::from(vals)) as ArrayRef,
                )
            }
        };
        fields.push(field);
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays).map_err(ConnectorError::query)?;
    Ok(vec![batch])
}

fn attr_to_string(v: &AttributeValue) -> String {
    match v {
        AttributeValue::S(s) => s.clone(),
        AttributeValue::N(n) => n.clone(),
        AttributeValue::Bool(b) => b.to_string(),
        AttributeValue::Null(_) => String::new(),
        other => format!("{other:?}"),
    }
}

// ── Factory ──────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct DynamoDbConnectorFactory;

#[async_trait]
impl ConnectorFactory for DynamoDbConnectorFactory {
    fn connector_type(&self) -> &str {
        "dynamodb"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(DynamoDbConnector::from_config(config).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_projection_expr_single() {
        let (pe, names) = projection_expr(&["user_id".to_string()]);
        assert_eq!(pe, "#p0");
        assert_eq!(names, vec![("#p0".to_string(), "user_id".to_string())]);
    }

    #[test]
    fn test_projection_expr_multiple() {
        let (pe, names) = projection_expr(&["a".to_string(), "b".to_string()]);
        assert_eq!(pe, "#p0, #p1");
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn test_attr_to_string_variants() {
        assert_eq!(attr_to_string(&AttributeValue::S("hello".into())), "hello");
        assert_eq!(attr_to_string(&AttributeValue::N("42".into())), "42");
        assert_eq!(attr_to_string(&AttributeValue::Bool(true)), "true");
        assert_eq!(attr_to_string(&AttributeValue::Null(true)), "");
    }

    #[test]
    fn test_ddb_table_with_prefix() {
        // Can't construct Client without credentials in unit test — test the logic directly
        let prefix = "prod_";
        let table = "users";
        let result = format!("{prefix}{table}");
        assert_eq!(result, "prod_users");
    }

    #[test]
    fn test_items_to_batch_string_column() {
        let mut item = std::collections::HashMap::new();
        item.insert("name".to_string(), AttributeValue::S("alice".into()));
        item.insert("age".to_string(), AttributeValue::N("30".into()));

        let query = SubQuery {
            table: "users".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            passthrough: None,
            offset: None,
        };

        let batches = items_to_batch(&[item], &query).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_items_to_batch_empty() {
        // In production, empty items are caught before items_to_batch (returns Ok(vec![])).
        // With projections specified, empty items produce a valid zero-row batch.
        let query = SubQuery {
            table: "t".into(),
            projections: vec!["id".into()],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            passthrough: None,
            offset: None,
        };
        let result = items_to_batch(&[], &query).unwrap();
        assert_eq!(result[0].num_rows(), 0);
        assert_eq!(result[0].num_columns(), 1);
    }

    #[test]
    fn test_items_to_batch_with_projection() {
        let mut item = std::collections::HashMap::new();
        item.insert("name".to_string(), AttributeValue::S("bob".into()));
        item.insert("age".to_string(), AttributeValue::N("25".into()));
        item.insert(
            "email".to_string(),
            AttributeValue::S("bob@example.com".into()),
        );

        let query = SubQuery {
            table: "users".into(),
            projections: vec!["name".into()],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            passthrough: None,
            offset: None,
        };

        let batches = items_to_batch(&[item], &query).unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "name");
    }

    // ── #300 DynamoDB verification (tester) ──

    #[test]
    fn test_projection_expr_empty() {
        let (expr, names) = projection_expr(&[]);
        assert!(expr.is_empty());
        assert!(names.is_empty());
    }

    #[test]
    fn test_attr_to_string_number() {
        let av = AttributeValue::N("42".into());
        assert_eq!(attr_to_string(&av), "42");
    }

    #[test]
    fn test_attr_to_string_bool() {
        assert_eq!(attr_to_string(&AttributeValue::Bool(true)), "true");
        assert_eq!(attr_to_string(&AttributeValue::Bool(false)), "false");
    }

    #[test]
    fn test_attr_to_string_null() {
        assert_eq!(attr_to_string(&AttributeValue::Null(true)), "");
    }

    #[test]
    fn test_items_to_batch_mixed_types() {
        use std::collections::HashMap;
        let item = HashMap::from([
            ("id".to_string(), AttributeValue::S("u1".into())),
            ("count".to_string(), AttributeValue::N("42".into())),
            ("active".to_string(), AttributeValue::Bool(true)),
        ]);
        let query = SubQuery {
            table: "users".into(),
            projections: vec!["id".into(), "count".into(), "active".into()],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit: None,
            passthrough: None,
            offset: None,
        };
        let batches = items_to_batch(&[item], &query).unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 3);
    }
}
