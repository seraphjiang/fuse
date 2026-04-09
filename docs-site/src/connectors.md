# Connectors

Fuse ships with 5 connectors. Each implements the `FederatedConnector` trait and is registered via a `ConnectorFactory`.

## OpenSearch

The primary connector. Supports OpenSearch and Amazon OpenSearch Serverless (AOSS).

```toml
[[connector]]
id = "cluster_a"
connector_type = "opensearch"
endpoint = "https://abc123.us-west-2.aoss.amazonaws.com"
auth = "sigv4"
region = "us-west-2"
```

**Auth options:** `none`, `basic`, `sigv4`

For basic auth:
```toml
auth = "basic"
username = "admin"
password = "admin"
```

**Capabilities:** filtering, projection, aggregation, sorting, limit, streaming.

**Pushdown:** WHERE clauses become Query DSL filters. SELECT columns become `_source` includes. LIMIT becomes `size`.

## S3 Parquet

Read Parquet files from S3.

```toml
[[connector]]
id = "data_lake"
connector_type = "s3"
bucket = "my-data-bucket"
prefix = "warehouse/"
region = "us-west-2"
```

**Auth:** IAM (uses default credential chain).

**Capabilities:** filtering, projection, limit.

## S3 O11y (NDJSON)

Read gzipped newline-delimited JSON from S3. Designed for observability log pipelines.

```toml
[[connector]]
id = "s3_o11y"
connector_type = "s3-o11y"
bucket = "s3-query-logs-544277935543-us-west-1"
prefix = "fuse/"
region = "us-west-1"
```

**Auth:** IAM.

**Format:** Gzipped NDJSON files at `{prefix}{YYYY/MM/DD/HH}/{timestamp}.json.gz`.

**Schema discovery:** Automatic from first file. Fields: any JSON keys found in records.

## Prometheus

Query Prometheus metrics via PromQL.

```toml
[[connector]]
id = "metrics"
connector_type = "prometheus"
endpoint = "https://prometheus.example.com"
auth = "bearer"
token = "your-token"
```

**Auth options:** `none`, `bearer`

**Capabilities:** filtering (time range), projection.

## Custom Connectors (SDK)

Build your own connector using `fuse-connector-sdk`. See the [Writing a Connector](./contributing.md) section.

Implement two traits:

```rust
#[async_trait]
pub trait FederatedConnector: Send + Sync {
    fn id(&self) -> &str;
    fn connector_type(&self) -> &str;
    fn capabilities(&self) -> ConnectorCapabilities;
    async fn health_check(&self) -> Result<HealthStatus>;
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>>;
    async fn get_schema(&self, table: &str) -> Result<Schema>;
    fn table_names(&self) -> Vec<String>;
    async fn get_table_schema(&self, table: &str) -> Result<Schema>;
    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>>;
    async fn execute_streaming(&self, query: &SubQuery)
        -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>>;
}

pub trait ConnectorFactory: Send + Sync {
    fn connector_type(&self) -> &str;
    fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>>;
}
```

Register your factory in `main.rs`:

```rust
registry.register_factory(Arc::new(MyConnectorFactory));
```

The connector is then available via `fuse.toml`:

```toml
[[connector]]
id = "my_source"
connector_type = "my-connector"
# ... your config fields
```
