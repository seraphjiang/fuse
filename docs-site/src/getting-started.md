# Getting Started

## Prerequisites

- Rust 1.75+ (for building from source)
- Access to at least one OpenSearch cluster or S3 bucket
- Docker (optional, for local dev environment)

## Installation

```bash
git clone https://github.com/seraphjiang/fuse.git
cd fuse
cargo build --release
```

## Configuration

Create a `fuse.toml` file:

```toml
[server]
host = "0.0.0.0"
port = 9400

[[connector]]
id = "cluster_a"
connector_type = "opensearch"
endpoint = "https://your-opensearch-endpoint"
auth = "sigv4"
region = "us-west-2"

[[connector]]
id = "cluster_b"
connector_type = "opensearch"
endpoint = "https://your-other-endpoint"
auth = "sigv4"
region = "us-west-2"
```

## Running

```bash
./target/release/fuse-server --config fuse.toml
```

The server starts on port 9400. Open `http://localhost:9400` for the playground UI.

## Your First Query

### 1. Check health

```bash
curl http://localhost:9400/api/fuse/health
```

### 2. List datasources

```bash
curl http://localhost:9400/api/fuse/datasources
```

### 3. Discover schemas

```bash
curl http://localhost:9400/api/fuse/datasources/cluster_a/schemas
```

### 4. Run a query

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT service, status, message FROM cluster_a.application_logs WHERE status >= 500 LIMIT 10"}'
```

### 5. Cross-cluster federation

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT service, status FROM cluster_a.application_logs UNION ALL SELECT service, status FROM cluster_b.application_logs LIMIT 20"}'
```

## Available Datasources (Playground)

| ID | Type | Services |
|----|------|----------|
| `cluster_a` | OpenSearch (AOSS) | api-gateway, auth-service, user-service |
| `cluster_b` | OpenSearch (AOSS) | order-service, payment-service, notification-service |
| `s3_o11y` | S3 NDJSON | Fuse server logs |

**Index:** `application_logs`

**Fields:** `timestamp`, `service`, `status`, `message`, `trace_id`, `user_id`, `response_time_ms`, `method`, `path`

## Docker Development

```bash
# Start OpenSearch + Dashboards + Fuse
docker compose up -d

# Seed sample data
./scripts/setup-dev.sh

# Run tests
./scripts/test-local.sh
```
