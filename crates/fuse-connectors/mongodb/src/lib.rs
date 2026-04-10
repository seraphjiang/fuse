// SPDX-License-Identifier: Apache-2.0

//! MongoDB connector for the Fuse federated query engine.
//!
//! - Schema discovery via listCollections
//! - Filter pushdown to MongoDB query documents (BSON)
//! - Projection pushdown
//! - Limit pushdown
//! - Connection pooling via the official mongodb driver

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use mongodb::bson::{doc, Bson, Document};
use mongodb::{Client, options::ClientOptions, options::FindOptions};
use tokio::sync::mpsc;
use tracing::debug;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

#[derive(Debug)]
pub struct MongoDbConnector {
    id: String,
    client: Client,
    database: String,
}

impl MongoDbConnector {
    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let uri = config.properties.get("uri")
            .and_then(|v| v.as_str())
            .unwrap_or("mongodb://localhost:27017");
        let database = config.properties.get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("test")
            .to_string();

        let opts = ClientOptions::parse(uri).await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;
        let client = Client::with_options(opts)
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        Ok(Self { id: config.id.clone(), client, database })
    }

    fn db(&self) -> mongodb::Database {
        self.client.database(&self.database)
    }
}

#[async_trait]
impl FederatedConnector for MongoDbConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "mongodb" }

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
        match self.db().run_command(doc! {"ping": 1}).await {
            Ok(_) => ConnectorHealth { status: HealthStatus::Healthy, latency_ms: Some(start.elapsed().as_millis() as u64), message: None },
            Err(e) => ConnectorHealth { status: HealthStatus::Unhealthy, latency_ms: None, message: Some(e.to_string()) },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        let names = self.db().list_collection_names().await
            .map_err(|e| ConnectorError::schema(e))?;
        Ok(names.into_iter().map(|name| SchemaInfo {
            name,
            schema_type: SchemaType::Table,
            estimated_row_count: None,
        }).collect())
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        // Sample one document to infer schema
        let coll = self.db().collection::<Document>(table);
        let doc = coll.find_one(doc! {}).await
            .map_err(|e| ConnectorError::schema(e))?
            .ok_or_else(|| ConnectorError::schema(format!("collection '{table}' is empty or not found")))?;

        let fields: Vec<Field> = doc.iter()
            .filter(|(k, _)| *k != "_id")
            .map(|(k, v)| Field::new(k, bson_type_to_arrow(v), true))
            .collect();

        Ok(Schema::new(if fields.is_empty() {
            vec![Field::new("_id", DataType::Utf8, true)]
        } else {
            fields
        }))
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let coll = self.db().collection::<Document>(&query.table);
        debug!(table = query.table.as_str(), "MongoDB execute");

        let filter = query.filter.as_ref().map(filter_to_bson).unwrap_or_default();

        let mut opts = FindOptions::default();
        if let Some(limit) = query.limit {
            opts.limit = Some(limit as i64);
        }
        if !query.projections.is_empty() {
            let mut proj = doc! {};
            for col in &query.projections {
                proj.insert(col, 1);
            }
            proj.insert("_id", 0);
            opts.projection = Some(proj);
        }

        let mut cursor = coll.find(filter).with_options(opts).await
            .map_err(|e| ConnectorError::query(e))?;

        let mut docs = Vec::new();
        while cursor.advance().await.map_err(|e| ConnectorError::query(e))? {
            docs.push(cursor.deserialize_current().map_err(|e| ConnectorError::query(e))?);
        }

        if docs.is_empty() { return Ok(vec![]); }
        docs_to_batch(&docs, query)
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        for batch in self.execute(query).await? {
            tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}

fn filter_to_bson(f: &FilterExpr) -> Document {
    match f {
        FilterExpr::And(l, r) => doc! { "$and": [filter_to_bson(l), filter_to_bson(r)] },
        FilterExpr::Or(l, r) => doc! { "$or": [filter_to_bson(l), filter_to_bson(r)] },
        FilterExpr::Not(inner) => doc! { "$nor": [filter_to_bson(inner)] },
        FilterExpr::Comparison { field, op, value } => {
            let v = scalar_to_bson(value);
            match op {
                ComparisonOp::Eq => doc! { field: v },
                ComparisonOp::Neq => doc! { field: { "$ne": v } },
                ComparisonOp::Lt => doc! { field: { "$lt": v } },
                ComparisonOp::Lte => doc! { field: { "$lte": v } },
                ComparisonOp::Gt => doc! { field: { "$gt": v } },
                ComparisonOp::Gte => doc! { field: { "$gte": v } },
                ComparisonOp::Like | ComparisonOp::ILike => {
                    // Convert SQL LIKE pattern to regex
                    let pattern = match value {
                        ScalarValue::Utf8(s) => s.replace('%', ".*").replace('_', "."),
                        _ => String::new(),
                    };
                    let opts = if matches!(op, ComparisonOp::ILike) { "i" } else { "" };
                    doc! { field: { "$regex": pattern, "$options": opts } }
                }
            }
        }
        FilterExpr::In { field, values } => {
            let vals: Vec<Bson> = values.iter().map(scalar_to_bson).collect();
            doc! { field: { "$in": vals } }
        }
        FilterExpr::IsNull(field) => doc! { field: { "$exists": false } },
        FilterExpr::IsNotNull(field) => doc! { field: { "$exists": true } },
    }
}

fn scalar_to_bson(v: &ScalarValue) -> Bson {
    match v {
        ScalarValue::Utf8(s) => Bson::String(s.clone()),
        ScalarValue::Int64(n) => Bson::Int64(*n),
        ScalarValue::Float64(f) => Bson::Double(*f),
        ScalarValue::Boolean(b) => Bson::Boolean(*b),
        ScalarValue::Null => Bson::Null,
    }
}

fn bson_type_to_arrow(v: &Bson) -> DataType {
    match v {
        Bson::Int32(_) | Bson::Int64(_) => DataType::Int64,
        Bson::Double(_) => DataType::Float64,
        Bson::Boolean(_) => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

fn docs_to_batch(docs: &[Document], query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
    let cols: Vec<String> = if !query.projections.is_empty() {
        query.projections.clone()
    } else {
        let mut seen = std::collections::HashSet::new();
        let mut cols = Vec::new();
        for doc in docs {
            for k in doc.keys() {
                if k != "_id" && seen.insert(k.clone()) { cols.push(k.clone()); }
            }
        }
        cols.sort();
        cols
    };

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for col in &cols {
        let first = docs.iter().find_map(|d| d.get(col));
        match first {
            Some(Bson::Int32(_)) | Some(Bson::Int64(_)) => {
                let vals: Vec<Option<i64>> = docs.iter().map(|d| match d.get(col) {
                    Some(Bson::Int64(n)) => Some(*n),
                    Some(Bson::Int32(n)) => Some(*n as i64),
                    _ => None,
                }).collect();
                fields.push(Field::new(col, DataType::Int64, true));
                arrays.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
            }
            Some(Bson::Double(_)) => {
                let vals: Vec<Option<f64>> = docs.iter().map(|d| if let Some(Bson::Double(f)) = d.get(col) { Some(*f) } else { None }).collect();
                fields.push(Field::new(col, DataType::Float64, true));
                arrays.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
            }
            Some(Bson::Boolean(_)) => {
                let vals: Vec<Option<bool>> = docs.iter().map(|d| if let Some(Bson::Boolean(b)) = d.get(col) { Some(*b) } else { None }).collect();
                fields.push(Field::new(col, DataType::Boolean, true));
                arrays.push(Arc::new(BooleanArray::from(vals)) as ArrayRef);
            }
            _ => {
                let vals: Vec<Option<String>> = docs.iter().map(|d| d.get(col).map(|v| v.to_string())).collect();
                fields.push(Field::new(col, DataType::Utf8, true));
                arrays.push(Arc::new(StringArray::from(vals)) as ArrayRef);
            }
        }
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| ConnectorError::query(e))?;
    Ok(vec![batch])
}

#[derive(Debug, Default)]
pub struct MongoDbConnectorFactory;

#[async_trait]
impl ConnectorFactory for MongoDbConnectorFactory {
    fn connector_type(&self) -> &str { "mongodb" }
    async fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(MongoDbConnector::from_config(config).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_to_bson_eq() {
        let f = FilterExpr::Comparison { field: "status".into(), op: ComparisonOp::Eq, value: ScalarValue::Utf8("active".into()) };
        let doc = filter_to_bson(&f);
        assert_eq!(doc.get_str("status").unwrap(), "active");
    }

    #[test]
    fn test_filter_to_bson_gt() {
        let f = FilterExpr::Comparison { field: "age".into(), op: ComparisonOp::Gt, value: ScalarValue::Int64(18) };
        let doc = filter_to_bson(&f);
        assert!(doc.get_document("age").unwrap().contains_key("$gt"));
    }

    #[test]
    fn test_filter_to_bson_in() {
        let f = FilterExpr::In { field: "env".into(), values: vec![ScalarValue::Utf8("prod".into()), ScalarValue::Utf8("dev".into())] };
        let doc = filter_to_bson(&f);
        assert!(doc.get_document("env").unwrap().contains_key("$in"));
    }

    #[test]
    fn test_filter_to_bson_is_null() {
        let f = FilterExpr::IsNull("email".into());
        let doc = filter_to_bson(&f);
        assert!(doc.get_document("email").unwrap().contains_key("$exists"));
    }

    #[test]
    fn test_filter_to_bson_and() {
        let f = FilterExpr::And(
            Box::new(FilterExpr::Comparison { field: "a".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(1) }),
            Box::new(FilterExpr::Comparison { field: "b".into(), op: ComparisonOp::Eq, value: ScalarValue::Int64(2) }),
        );
        let doc = filter_to_bson(&f);
        assert!(doc.contains_key("$and"));
    }

    #[test]
    fn test_filter_to_bson_like() {
        let f = FilterExpr::Comparison { field: "name".into(), op: ComparisonOp::Like, value: ScalarValue::Utf8("alice%".into()) };
        let doc = filter_to_bson(&f);
        let inner = doc.get_document("name").unwrap();
        assert!(inner.contains_key("$regex"));
    }

    #[test]
    fn test_scalar_to_bson() {
        assert_eq!(scalar_to_bson(&ScalarValue::Int64(42)), Bson::Int64(42));
        assert_eq!(scalar_to_bson(&ScalarValue::Null), Bson::Null);
        assert_eq!(scalar_to_bson(&ScalarValue::Boolean(true)), Bson::Boolean(true));
    }

    #[test]
    fn test_bson_type_to_arrow() {
        assert_eq!(bson_type_to_arrow(&Bson::Int64(1)), DataType::Int64);
        assert_eq!(bson_type_to_arrow(&Bson::Double(1.0)), DataType::Float64);
        assert_eq!(bson_type_to_arrow(&Bson::Boolean(true)), DataType::Boolean);
        assert_eq!(bson_type_to_arrow(&Bson::String("x".into())), DataType::Utf8);
    }
}
