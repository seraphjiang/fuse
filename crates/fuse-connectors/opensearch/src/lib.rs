//! OpenSearch connector for the Fuse federated query engine.

pub mod client;
pub mod pushdown;
pub mod schema;

use std::sync::Arc;
use std::time::Instant;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;

use fuse_core::config::ConnectorConfig;
use fuse_core::connector::*;
use fuse_core::error::ConnectorError;
use fuse_core::registry::ConnectorFactory;

use crate::client::OpenSearchClient;

/// Queries with no limit or limit above this threshold use scroll pagination.
const SCROLL_THRESHOLD: u64 = 10_000;

/// OpenSearch connector implementing the FederatedConnector trait.
#[derive(Debug)]
pub struct OpenSearchConnector {
    id: String,
    client: OpenSearchClient,
    max_concurrent_queries: usize,
}

impl OpenSearchConnector {
    pub fn new(id: String, client: OpenSearchClient, max_concurrent_queries: usize) -> Self {
        Self {
            id,
            client,
            max_concurrent_queries,
        }
    }

    pub async fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let client = OpenSearchClient::from_config(config).await?;
        let max_concurrent = config
            .properties
            .get("max_concurrent_queries")
            .and_then(|v: &toml::Value| v.as_integer())
            .unwrap_or(16) as usize;
        Ok(Self::new(config.id.clone(), client, max_concurrent))
    }

    /// Scroll through all pages and collect into Vec<RecordBatch>.
    async fn execute_scroll(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let dsl = pushdown::translate_to_query_dsl(query);

        let resp = self
            .client
            .client()
            .search(opensearch::SearchParts::Index(&[&query.table]))
            .scroll("1m")
            .body(dsl)
            .send()
            .await
            .map_err(|e| ConnectorError::query(e))?;

        let mut body = resp
            .json::<serde_json::Value>()
            .await
            .map_err(|e| ConnectorError::query(e))?;

        let mut batches = Vec::new();
        let mut collected: usize = 0;
        let limit = query.limit;

        loop {
            let batch = parse_hits_to_batch(&body, query)?;
            if batch.num_rows() == 0 {
                break;
            }
            collected += batch.num_rows();
            batches.push(batch);
            if limit.map_or(false, |l| collected >= l as usize) {
                break;
            }

            let scroll_id = body
                .get("_scroll_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ConnectorError::query("missing _scroll_id"))?;

            let scroll_resp = self
                .client
                .client()
                .scroll(opensearch::ScrollParts::None)
                .body(serde_json::json!({"scroll": "1m", "scroll_id": scroll_id}))
                .send()
                .await
                .map_err(|e| ConnectorError::query(e))?;

            body = scroll_resp
                .json::<serde_json::Value>()
                .await
                .map_err(|e| ConnectorError::query(e))?;
        }

        Ok(batches)
    }
}

#[async_trait]
impl FederatedConnector for OpenSearchConnector {
    fn id(&self) -> &str {
        &self.id
    }

    fn connector_type(&self) -> &str {
        "opensearch"
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: true,
            supports_sorting: true,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: self.max_concurrent_queries,
            supports_streaming: true,
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        // Try _cluster/health first (managed OpenSearch), fall back to
        // a simple request (AOSS doesn't support _cluster/health)
        let resp = self
            .client
            .client()
            .cluster()
            .health(opensearch::cluster::ClusterHealthParts::None)
            .send()
            .await;

        match resp {
            Ok(r) if r.status_code().is_success() => {
                ConnectorHealth {
                    status: HealthStatus::Healthy,
                    latency_ms: Some(start.elapsed().as_millis() as u64),
                    message: None,
                }
            }
            _ => {
                // Fallback: try a simple GET / — any response means reachable
                // AOSS may return 403 but that still means the endpoint is up
                match self.client.client()
                    .send::<(), ()>(
                        opensearch::http::Method::Get,
                        "/",
                        opensearch::http::headers::HeaderMap::new(),
                        None,
                        None,
                        None,
                    )
                    .await
                {
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
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        // Try _cat/indices first (managed OpenSearch)
        let resp = self
            .client
            .client()
            .cat()
            .indices(opensearch::cat::CatIndicesParts::None)
            .format("json")
            .send()
            .await;

        if let Ok(r) = resp {
            if r.status_code().is_success() {
                if let Ok(body) = r.json::<serde_json::Value>().await {
                    if let Some(indices) = body.as_array() {
                        return Ok(indices
                            .iter()
                            .filter_map(|idx| {
                                let name = idx.get("index")?.as_str()?.to_string();
                                if name.starts_with('.') {
                                    return None;
                                }
                                let row_count = idx
                                    .get("docs.count")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<u64>().ok());
                                Some(SchemaInfo {
                                    name,
                                    schema_type: SchemaType::Index,
                                    estimated_row_count: row_count,
                                })
                            })
                            .collect());
                    }
                }
            }
        }

        // Fallback: use _aliases API, then _mapping wildcard
        // Check status code before parsing to avoid treating error responses as data
        let aliases_resp = self
            .client
            .client()
            .send::<(), ()>(
                opensearch::http::Method::Get,
                "/_aliases",
                opensearch::http::headers::HeaderMap::new(),
                None,
                None,
                None,
            )
            .await;

        if let Ok(r) = aliases_resp {
            if r.status_code().is_success() {
                if let Ok(body) = r.json::<serde_json::Value>().await {
                    if let Some(obj) = body.as_object() {
                        // Verify it's real index data, not an error response
                        if !obj.contains_key("error") && !obj.contains_key("status") {
                            return Ok(obj
                                .keys()
                                .filter(|name| !name.starts_with('.'))
                                .map(|name| SchemaInfo {
                                    name: name.clone(),
                                    schema_type: SchemaType::Index,
                                    estimated_row_count: None,
                                })
                                .collect());
                        }
                    }
                }
            }
        }

        // Last resort: try GET /_mapping (AOSS supports this)
        let mapping_resp = self
            .client
            .client()
            .send::<(), ()>(
                opensearch::http::Method::Get,
                "/_mapping",
                opensearch::http::headers::HeaderMap::new(),
                None,
                None,
                None,
            )
            .await;

        if let Ok(r) = mapping_resp {
            if r.status_code().is_success() {
                if let Ok(body) = r.json::<serde_json::Value>().await {
                    if let Some(obj) = body.as_object() {
                        if !obj.contains_key("error") {
                            return Ok(obj
                                .keys()
                                .filter(|name| !name.starts_with('.'))
                                .map(|name| SchemaInfo {
                                    name: name.clone(),
                                    schema_type: SchemaType::Index,
                                    estimated_row_count: None,
                                })
                                .collect());
                        }
                    }
                }
            }
        }

        // If all discovery methods fail, return empty rather than error
        // The connector can still execute queries if the user knows the index name
        Ok(vec![])
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        let resp = self
            .client
            .client()
            .indices()
            .get_mapping(opensearch::indices::IndicesGetMappingParts::Index(&[table]))
            .send()
            .await
            .map_err(|e| ConnectorError::schema(e))?;

        let body = resp
            .json::<serde_json::Value>()
            .await
            .map_err(|e| ConnectorError::schema(e))?;

        schema::mapping_to_arrow_schema(&body)
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError> {
        let use_scroll = query.limit.map_or(true, |l| l > SCROLL_THRESHOLD);

        if use_scroll && query.aggregations.is_empty() {
            return self.execute_scroll(query).await;
        }

        let dsl = pushdown::translate_to_query_dsl(query);

        let resp = self
            .client
            .client()
            .search(opensearch::SearchParts::Index(&[&query.table]))
            .body(dsl)
            .send()
            .await
            .map_err(|e| ConnectorError::query(e))?;

        let body = resp
            .json::<serde_json::Value>()
            .await
            .map_err(|e| ConnectorError::query(e))?;

        parse_search_response(&body, query)
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        let dsl = pushdown::translate_to_query_dsl(query);

        let resp = self
            .client
            .client()
            .search(opensearch::SearchParts::Index(&[&query.table]))
            .scroll("1m")
            .body(dsl)
            .send()
            .await
            .map_err(|e| ConnectorError::query(e))?;

        let mut body = resp
            .json::<serde_json::Value>()
            .await
            .map_err(|e| ConnectorError::query(e))?;

        loop {
            let batch = parse_hits_to_batch(&body, query)?;
            if batch.num_rows() == 0 {
                break;
            }
            tx.send(Ok(batch))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;

            let scroll_id = body
                .get("_scroll_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| ConnectorError::query("missing _scroll_id"))?;

            let scroll_resp = self
                .client
                .client()
                .scroll(opensearch::ScrollParts::None)
                .body(serde_json::json!({"scroll": "1m", "scroll_id": scroll_id}))
                .send()
                .await
                .map_err(|e| ConnectorError::query(e))?;

            body = scroll_resp
                .json::<serde_json::Value>()
                .await
                .map_err(|e| ConnectorError::query(e))?;
        }

        Ok(())
    }
}

/// Parse a full search response (hits or aggregations) into RecordBatches.
fn parse_search_response(
    body: &serde_json::Value,
    query: &SubQuery,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    // If aggregation query, parse agg buckets
    if !query.aggregations.is_empty() {
        return parse_agg_response(body, query);
    }
    let batch = parse_hits_to_batch(body, query)?;
    if batch.num_rows() == 0 {
        Ok(vec![])
    } else {
        Ok(vec![batch])
    }
}

/// Parse hits array into a RecordBatch.
/// Traverse a dot-separated field path through a JSON object.
/// `get_nested(obj, "metadata.region")` → `obj["metadata"]["region"]`
fn get_nested<'a>(mut val: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    for key in path.split('.') {
        val = val.get(key)?;
    }
    Some(val)
}

/// Simplified: extracts projected string fields from _source.
fn parse_hits_to_batch(
    body: &serde_json::Value,
    query: &SubQuery,
) -> Result<RecordBatch, ConnectorError> {
    let empty = vec![];
    let hits = body
        .pointer("/hits/hits")
        .and_then(|v| v.as_array())
        .unwrap_or(&empty);

    if hits.is_empty() {
        // Return empty batch with schema
        let fields: Vec<Field> = query
            .projections
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(if fields.is_empty() {
            vec![Field::new("_id", DataType::Utf8, true)]
        } else {
            fields
        }));
        return Ok(RecordBatch::new_empty(schema));
    }

    // Determine columns from projections or from first hit's _source keys
    let columns: Vec<String> = if query.projections.is_empty() {
        hits[0]
            .get("_source")
            .and_then(|s| s.as_object())
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_else(|| vec!["_id".to_string()])
    } else {
        query.projections.clone()
    };

    let fields: Vec<Field> = columns
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let arrays: Vec<Arc<dyn arrow::array::Array>> = columns
        .iter()
        .map(|col| {
            let values: Vec<Option<String>> = hits
                .iter()
                .map(|hit| {
                    hit.get("_source")
                        .and_then(|s| get_nested(s, col))
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                })
                .collect();
            Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>
        })
        .collect();

    RecordBatch::try_new(schema, arrays).map_err(|e| ConnectorError::query(e))
}

/// Parse aggregation response into RecordBatch.
fn parse_agg_response(
    body: &serde_json::Value,
    query: &SubQuery,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let aggs = body
        .get("aggregations")
        .ok_or_else(|| ConnectorError::query("no aggregations in response"))?;

    // Grouped aggregation
    if let Some(group_by) = aggs.get("group_by") {
        let buckets = group_by
            .get("buckets")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ConnectorError::query("no buckets in group_by aggregation"))?;

        if buckets.is_empty() {
            return Ok(vec![]);
        }

        // Key column
        let keys: Vec<Option<String>> = buckets
            .iter()
            .map(|b| b.get("key").map(|v| v.to_string()))
            .collect();

        let mut fields = vec![Field::new(
            query.group_by.first().map(|s| s.as_str()).unwrap_or("key"),
            DataType::Utf8,
            true,
        )];
        let mut arrays: Vec<Arc<dyn arrow::array::Array>> =
            vec![Arc::new(StringArray::from(keys))];

        // Metric columns
        for agg in &query.aggregations {
            let values: Vec<Option<String>> = buckets
                .iter()
                .map(|b| {
                    b.get(&agg.alias)
                        .and_then(|v| v.get("value"))
                        .map(|v| v.to_string())
                })
                .collect();
            fields.push(Field::new(&agg.alias, DataType::Utf8, true));
            arrays.push(Arc::new(StringArray::from(values)));
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).map_err(|e| ConnectorError::query(e))?;
        return Ok(vec![batch]);
    }

    // Non-grouped: single row of metric values
    let mut fields = Vec::new();
    let mut values: Vec<Option<String>> = Vec::new();
    for agg in &query.aggregations {
        fields.push(Field::new(&agg.alias, DataType::Utf8, true));
        let val = aggs
            .get(&agg.alias)
            .and_then(|v| v.get("value"))
            .map(|v| v.to_string());
        values.push(val);
    }

    // Build single-row batch
    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<Arc<dyn arrow::array::Array>> = values
        .into_iter()
        .map(|v| Arc::new(StringArray::from(vec![v])) as Arc<dyn arrow::array::Array>)
        .collect();

    let batch = RecordBatch::try_new(schema, arrays).map_err(|e| ConnectorError::query(e))?;
    Ok(vec![batch])
}

// ── Factory ──

pub struct OpenSearchConnectorFactory;

#[async_trait]
impl ConnectorFactory for OpenSearchConnectorFactory {
    fn connector_type(&self) -> &str {
        "opensearch"
    }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        Ok(Arc::new(OpenSearchConnector::from_config(config).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sq(limit: Option<u64>) -> SubQuery {
        SubQuery {
            table: "logs".into(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            having: None,
            sort: vec![],
            limit,
            passthrough: None,
        }
    }

    #[test]
    fn test_scroll_threshold_no_limit_uses_scroll() {
        let q = sq(None);
        assert!(q.limit.map_or(true, |l| l > SCROLL_THRESHOLD));
    }

    #[test]
    fn test_scroll_threshold_small_limit_no_scroll() {
        let q = sq(Some(100));
        assert!(!q.limit.map_or(true, |l| l > SCROLL_THRESHOLD));
    }

    #[test]
    fn test_scroll_threshold_exactly_at_boundary() {
        let q = sq(Some(SCROLL_THRESHOLD));
        assert!(!q.limit.map_or(true, |l| l > SCROLL_THRESHOLD));
    }

    #[test]
    fn test_scroll_threshold_above_boundary() {
        let q = sq(Some(SCROLL_THRESHOLD + 1));
        assert!(q.limit.map_or(true, |l| l > SCROLL_THRESHOLD));
    }

    #[test]
    fn test_get_nested_top_level() {
        let v = serde_json::json!({"name": "alice"});
        assert_eq!(get_nested(&v, "name").unwrap(), "alice");
    }

    #[test]
    fn test_get_nested_dot_path() {
        let v = serde_json::json!({"metadata": {"region": "us-east-1"}});
        assert_eq!(get_nested(&v, "metadata.region").unwrap(), "us-east-1");
    }

    #[test]
    fn test_get_nested_deep_path() {
        let v = serde_json::json!({"a": {"b": {"c": 42}}});
        assert_eq!(get_nested(&v, "a.b.c").unwrap(), 42);
    }

    #[test]
    fn test_get_nested_missing_key() {
        let v = serde_json::json!({"name": "alice"});
        assert!(get_nested(&v, "metadata.region").is_none());
    }

    #[test]
    fn test_parse_hits_nested_projection() {
        let body = serde_json::json!({
            "hits": {"hits": [
                {"_source": {"metadata": {"region": "us-east-1"}, "status": 200}},
                {"_source": {"metadata": {"region": "eu-west-1"}, "status": 404}}
            ]}
        });
        let q = SubQuery {
            table: "logs".into(),
            projections: vec!["metadata.region".into(), "status".into()],
            filter: None, aggregations: vec![], group_by: vec![], having: None,
            sort: vec![], limit: None, passthrough: None,
        };
        let batch = parse_hits_to_batch(&body, &q).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "metadata.region");
    }
}
