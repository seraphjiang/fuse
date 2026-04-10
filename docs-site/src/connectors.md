# Connectors

Fuse ships with 14 connectors. Each implements the `FederatedConnector` trait and is registered via a `ConnectorFactory`. Add a connector by adding a `[[connector]]` block to `fuse.toml`.

## OpenSearch

The primary connector. Supports OpenSearch and Amazon OpenSearch Serverless (AOSS).

```toml
[[connector]]
id = "cluster_a"
type = "opensearch"
url = "https://abc123.us-west-2.aoss.amazonaws.com"
[connector.auth]
type = "sigv4"
region = "us-west-2"
service = "aoss"
```

For basic auth:
```toml
[connector.auth]
type = "basic"
username = "admin"
password = "admin"
```

**Pushdown:** Filter (Query DSL bool/must/range/term), projection (`_source` includes), aggregation, sort, limit, deep pagination via `search_after`.

## Elasticsearch

Elasticsearch 7.x and 8.x clusters. Separate from OpenSearch to handle API differences.

```toml
[[connector]]
id = "es_cluster"
type = "elasticsearch"
url = "https://es-cluster.example.com:9200"
[connector.auth]
type = "api_key"
api_key = "base64-encoded-key"
```

**Auth:** `basic`, `api_key`.

**Pushdown:** Filter (Query DSL), projection, aggregation, sort, limit.

## PostgreSQL

Full SQL pushdown via `sqlx` connection pool.

```toml
[[connector]]
id = "my_pg"
type = "postgres"
url = "postgresql://user:pass@host:5432/mydb"
```

**Pushdown:** Full SQL — the entire SubQuery is translated to a native PostgreSQL query and executed remotely.

## MySQL

Full SQL pushdown via `sqlx` connection pool.

```toml
[[connector]]
id = "my_mysql"
type = "mysql"
url = "mysql://user:pass@host:3306/mydb"
```

**Pushdown:** Full SQL — same as PostgreSQL.

## DynamoDB

Scan and Query operations with filter pushdown.

```toml
[[connector]]
id = "my_ddb"
type = "dynamodb"
region = "us-west-2"
table_names = ["users", "orders"]
```

**Auth:** IAM (default credential chain).

**Pushdown:** Filter → `FilterExpression` (Scan) or `KeyConditionExpression` (Query), projection → `ProjectionExpression`, limit.

## S3 (Parquet)

Read Parquet files from S3 with column pruning and row-group pagination.

```toml
[[connector]]
id = "data_lake"
type = "s3"
bucket = "my-data-bucket"
prefix = "warehouse/"
region = "us-west-2"
```

**Auth:** IAM (default credential chain).

**Pushdown:** Column pruning (read only needed columns), row-group pagination with early limit termination.

## S3 O11y (NDJSON)

Read gzipped newline-delimited JSON from S3. Designed for observability log pipelines.

```toml
[[connector]]
id = "s3_o11y"
type = "s3-o11y"
bucket = "my-log-bucket"
prefix = "logs/"
region = "us-west-1"
```

**Auth:** IAM.

**Format:** Gzipped NDJSON files at `{prefix}{YYYY/MM/DD/HH}/{timestamp}.json.gz`.

**Pushdown:** Projection, limit.

## Prometheus

Query Prometheus metrics. Time range and label filters pushed down as PromQL.

```toml
[[connector]]
id = "prom"
type = "prometheus"
url = "http://prometheus:9090"
[connector.auth]
type = "bearer"
token = "your-token"
```

**Auth:** `none`, `bearer`.

**Pushdown:** Time range (start/end/step via passthrough), label matchers.

**Range queries:** Pass `start`, `end`, `step` in the request body for range vector queries.

## CloudWatch

Query CloudWatch Logs via CloudWatch Logs Insights.

```toml
[[connector]]
id = "cw"
type = "cloudwatch"
region = "us-west-2"
log_group = "/aws/lambda/my-function"
```

**Auth:** IAM (default credential chain).

**Pushdown:** Log group, time range, filter pattern.

## Redis

Read key-value data from Redis. Supports hash and string types via SCAN.

```toml
[[connector]]
id = "my_redis"
type = "redis"
url = "redis://host:6379"
```

**Auth:** Password (in URL or separate field).

**Pushdown:** Key pattern (SCAN), hash field selection, string value reads.

## CSV/JSON

Read CSV or JSON files from local filesystem or S3. Auto-detects format and infers schema.

```toml
[[connector]]
id = "local_data"
type = "csv-json"
path = "/data/reports/"
```

For S3-backed files:
```toml
[[connector]]
id = "s3_csv"
type = "csv-json"
bucket = "my-bucket"
prefix = "exports/"
region = "us-west-2"
```

**Pushdown:** Schema inference on first read, in-memory filtering.

## MongoDB

Query MongoDB collections with BSON filter pushdown.

```toml
[[connector]]
id = "my_mongo"
type = "mongodb"
url = "mongodb://host:27017/mydb"
```

**Auth:** Connection string (supports username/password, SRV, replica sets).

**Pushdown:** Filter → BSON query document, projection → projection document, limit → `FindOptions.limit`.

## InfluxDB

Query InfluxDB 1.x (InfluxQL) and 2.x (Flux). Version auto-detected.

```toml
[[connector]]
id = "my_influx"
type = "influxdb"
url = "http://influxdb:8086"
database = "telegraf"
```

For InfluxDB 2.x:
```toml
[[connector]]
id = "my_influx_v2"
type = "influxdb"
url = "http://influxdb:8086"
org = "my-org"
bucket = "my-bucket"
token = "my-token"
```

**Auth:** Token (v2), Basic (v1).

**Pushdown:** InfluxQL `WHERE` clause, time range.

## ClickHouse

Full SQL pushdown via ClickHouse HTTP interface (port 8123). Response format: `JSONEachRow`.

```toml
[[connector]]
id = "my_ch"
type = "clickhouse"
url = "http://clickhouse:8123"
database = "default"
```

**Auth:** Basic (HTTP).

**Pushdown:** Full SQL — ClickHouse is SQL-native, so the entire query is pushed down.

## Custom Connectors (SDK)

Build your own connector using `fuse-connector-sdk`. See the [connector authoring guide](https://github.com/seraphjiang/fuse/blob/main/docs/guides/writing-a-connector.md).

Implement two traits:

```rust
#[async_trait]
pub trait FederatedConnector: Send + Sync + Debug {
    fn id(&self) -> &str;
    fn connector_type(&self) -> &str;
    fn capabilities(&self) -> ConnectorCapabilities;
    async fn health_check(&self) -> ConnectorHealth;
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>>;
    async fn get_schema(&self, table: &str) -> Result<Schema>;
    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>>;
    async fn execute_streaming(&self, query: &SubQuery, tx: Sender<...>);
}

pub trait ConnectorFactory: Send + Sync {
    fn connector_type(&self) -> &str;
    fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>>;
}
```

Register your factory in `main.rs` and add a `[[connector]]` block to `fuse.toml`. See `crates/fuse-connectors/example/` for a minimal working template.
