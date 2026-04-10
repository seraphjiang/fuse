# Getting Started

## Prerequisites

- Rust stable (1.85+) — `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- OpenSSL dev — `apt install libssl-dev pkg-config` or `yum install openssl-devel`
- Docker (optional, for local OpenSearch)

## Installation

```bash
git clone https://github.com/seraphjiang/fuse.git
cd fuse
cargo build --release
```

## Configuration

Create a `fuse.toml` file with your datasources. Here's a minimal example with two OpenSearch clusters:

```toml
[engine]
bind = "0.0.0.0:9400"
max_concurrent_queries = 64

[[connector]]
id = "cluster_a"
type = "opensearch"
url = "https://your-opensearch-endpoint"
[connector.auth]
type = "sigv4"
region = "us-west-2"
service = "aoss"

[[connector]]
id = "my_ddb"
type = "dynamodb"
region = "us-west-2"
table_names = ["users", "orders"]
```

See [Connectors](./connectors.md) for all 14 connector types and their configuration.

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

### 5. Cross-datasource JOIN

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT l.service, u.name FROM cluster_a.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id WHERE l.status >= 500",
    "format": "sql"
  }'
```

### 6. UNION ALL across sources

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT service, status FROM cluster_a.application_logs UNION ALL SELECT service, status FROM cluster_b.application_logs LIMIT 20",
    "format": "sql"
  }'
```

### 7. Cursor pagination

```bash
# First page
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", "format": "sql", "page_size": 20}'

# Next page (use next_cursor from previous response)
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", "format": "sql", "page_size": 20, "cursor": "<next_cursor>"}'
```

### 8. EXPLAIN a query plan

```bash
curl -X POST http://localhost:9400/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT l.service, count(*) FROM cluster_a.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id GROUP BY l.service"}'
```

See [EXPLAIN / ANALYZE](./explain-analyze.md) for reading query plans.

## Docker Development

```bash
# Start OpenSearch + Dashboards + Fuse
docker compose up -d

# Seed sample data
./scripts/setup-dev.sh

# Run tests
./scripts/test-local.sh
```

## Next Steps

- [Architecture](./architecture.md) — how federated query execution works
- [SQL Reference](./sql-reference.md) — JOINs, UNION, window functions, subqueries
- [PPL Reference](./ppl-reference.md) — pipe-delimited queries with `lookup`
- [Connectors](./connectors.md) — configure all 14 datasource types
