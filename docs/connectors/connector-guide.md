# Fuse Connector Guide

Reference for configuring and building Fuse connectors.

---

## Built-in Connectors

| Connector | Type string | Auth options | Push-down |
|-----------|-------------|--------------|-----------|
| OpenSearch | `opensearch` | none, basic, sigv4, bearer | filter, projection, aggregation, sort, limit |
| S3/Parquet | `s3` | AWS SDK (env/instance profile) | projection, limit |
| Prometheus | `prometheus` | none, bearer, basic | filter (PromQL), limit |
| S3 O11y | `s3-o11y` | AWS SDK | projection, limit |
| Example | `example` | none | limit |

---

## Configuration (`fuse.toml`)

Each connector is a `[[connector]]` block:

```toml
[[connector]]
id = "my_cluster"          # unique ID used in queries: SELECT * FROM my_cluster.index
type = "opensearch"        # connector type string
url = "https://..."
max_concurrent_queries = 16
scroll_size = 1000
request_timeout = "30s"

[connector.auth]
type = "sigv4"             # none | basic | sigv4 | bearer
region = "us-west-2"
service = "aoss"           # "es" for managed OpenSearch, "aoss" for Serverless
```

---

## Auth Configuration

### No auth

```toml
[connector.auth]
type = "none"
```

### Basic auth

```toml
[connector.auth]
type = "basic"
username = "admin"
password = "secret"        # or use password_env = "MY_PASSWORD_ENV_VAR"
```

### SigV4 (AWS IAM)

Used for Amazon OpenSearch Service and OpenSearch Serverless. Credentials are loaded from the standard AWS credential chain (env vars, instance profile, ECS task role, etc.).

```toml
[connector.auth]
type = "sigv4"
region = "us-west-2"
service = "es"             # "es" for managed, "aoss" for Serverless
```

For OpenSearch Serverless, the IAM principal needs:
- `aoss:APIAccessAll` on the collection
- A data access policy granting read access

### Bearer token

```toml
[connector.auth]
type = "bearer"
token = "my-token"         # or use token_env = "MY_TOKEN_ENV_VAR"
```

---

## OpenSearch Connector

Supports OpenSearch and OpenSearch Serverless (AOSS).

```toml
[[connector]]
id = "cluster_a"
type = "opensearch"
url = "https://epk7ap540halh4ufyff6.us-west-2.aoss.amazonaws.com"
scroll_size = 1000
request_timeout = "30s"

[connector.auth]
type = "sigv4"
region = "us-west-2"
service = "aoss"
```

**Push-down support:** filter, projection, aggregation, sort, limit — all translated to OpenSearch DSL.

**AOSS notes:**
- Use `service = "aoss"` (not `"es"`)
- Scroll API is not supported — uses `search_after` pagination
- `_cat/indices` returns different format — schema discovery uses `_mappings`

---

## S3/Parquet Connector

Reads Parquet files from S3. Uses AWS SDK credential chain.

```toml
[[connector]]
id = "data_lake"
type = "s3"
bucket = "my-data-bucket"
prefix = "parquet/events/"
region = "us-east-1"
```

**Push-down support:** projection (column pruning), limit. Filters are applied post-read.

---

## Prometheus Connector

Translates SQL/PPL to PromQL and queries the Prometheus HTTP API.

```toml
[[connector]]
id = "metrics"
type = "prometheus"
url = "https://prometheus.example.com"

[connector.auth]
type = "bearer"
token_env = "PROMETHEUS_TOKEN"
```

**Push-down support:** filter conditions are translated to PromQL label matchers. Time range from `@timestamp` filter maps to Prometheus range queries.

---

## S3 O11y Connector

Reads gzipped NDJSON log files from S3. Designed for the S3 O11y log analytics platform.

```toml
[[connector]]
id = "o11y_logs"
type = "s3-o11y"
bucket = "s3-query-logs-544277935543-us-west-1"
prefix = "fuse/logs/"
region = "us-west-1"
```

**Schema:** auto-discovered from the first log file. Common fields: `timestamp`, `level`, `service`, `message`.

**Push-down support:** projection, limit. Filters applied post-read.

**Log shipping:** use `scripts/ship-logs-to-o11y.py` to ship Fuse server logs to the S3 O11y bucket.

---

## Building a Custom Connector

The fastest path:

1. Copy `crates/fuse-connectors/example/` as your starting point
2. Follow the step-by-step guide: [docs/guides/writing-a-connector.md](../guides/writing-a-connector.md)
3. Use `fuse-connector-sdk` for mock testing utilities

**Minimal Cargo.toml:**

```toml
[dependencies]
fuse-core = { path = "../../fuse-core" }
arrow = { workspace = true }
async-trait = { workspace = true }
tokio = { workspace = true }
```

**Key trait methods to implement:**

| Method | Required | Notes |
|--------|----------|-------|
| `id()` | ✅ | Unique connector instance ID |
| `connector_type()` | ✅ | Type string (e.g., `"mydb"`) |
| `capabilities()` | ✅ | Declare what you push down — be accurate |
| `health_check()` | ✅ | Ping your datasource |
| `discover_schemas()` | ✅ | List tables/indices |
| `get_schema()` | ✅ | Return Arrow schema for a table |
| `execute()` | ✅ | Translate SubQuery → datasource query → RecordBatches |
| `execute_streaming()` | ✅ | Send batches via `mpsc::Sender` |

**Auth best practices:**
- Never use `_ => {}` for unhandled auth types — return `ConnectorError::Connection` with a clear message
- Use `token_env` / `password_env` for secrets — never hardcode
- Log auth type at `INFO` level on startup (not the secret itself)

**Capabilities accuracy:**
- Only set `supports_filtering: true` if `execute()` actually applies `SubQuery.filter`
- Inaccurate capabilities cause incorrect push-down decisions and wrong results
