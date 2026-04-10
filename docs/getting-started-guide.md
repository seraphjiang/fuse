# Quick-Start Guide

Get Fuse running locally and execute your first federated query in under 5 minutes.

## 1. Prerequisites

```bash
# Rust stable (1.85+)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# OpenSSL dev headers
# Debian/Ubuntu:
sudo apt install libssl-dev pkg-config
# RHEL/Amazon Linux:
sudo yum install openssl-devel
```

## 2. Build and Run

```bash
git clone https://github.com/seraphjiang/fuse
cd fuse
cargo build --release
./target/release/fuse-server --config fuse.toml
```

Fuse starts on `:9400`. Open [http://localhost:9400](http://localhost:9400) for the playground UI.

## 3. Add a Datasource

Edit `fuse.toml` to add your datasources. Example with PostgreSQL:

```toml
[engine]
bind = "0.0.0.0:9400"

[[connector]]
id = "my_pg"
type = "postgres"
url = "postgresql://user:pass@localhost:5432/mydb"

[[connector]]
id = "my_os"
type = "opensearch"
url = "https://your-cluster.us-west-2.aoss.amazonaws.com"
[connector.auth]
type = "sigv4"
region = "us-west-2"
service = "aoss"
```

Restart the server after editing. See [Connectors](https://seraphjiang.github.io/fuse/connectors.html) for all 14 types.

## 4. First Query

```bash
# Check health
curl http://localhost:9400/api/fuse/health

# List datasources
curl http://localhost:9400/api/fuse/datasources

# Discover tables
curl http://localhost:9400/api/fuse/datasources/my_pg/schemas

# Query
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM my_pg.users LIMIT 10"}'
```

## 5. EXPLAIN and ANALYZE

```bash
# Plan without executing
curl -X POST http://localhost:9400/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM my_pg.users WHERE role = '\''admin'\''"}'

# Execute with timing profile
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM my_pg.users LIMIT 10", "analyze": true}'
```

The `execution_profile` in the response shows per-node timing, row counts, data bytes, and which operations were pushed down to the connector.

## 6. Cross-Datasource JOIN

Query across two different datasource types in one statement:

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT l.service, l.status, u.name FROM my_os.application_logs l JOIN my_pg.users u ON l.user_id = u.id WHERE l.status >= 500",
    "format": "sql"
  }'
```

Fuse fetches both sides in parallel, picks the smaller table as the hash join build side, and merges locally.

## 7. Cursor Pagination

Page through large result sets without OFFSET:

```bash
# First page
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM my_pg.users ORDER BY id", "page_size": 10}'

# Next page — use next_cursor from previous response
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM my_pg.users ORDER BY id", "page_size": 10, "cursor": "<next_cursor>"}'
```

When `next_cursor` is `null`, you've reached the last page.

## Next Steps

- [Architecture](https://seraphjiang.github.io/fuse/architecture.html) — how federated query execution works
- [SQL Reference](https://seraphjiang.github.io/fuse/sql-reference.html) — JOINs, UNION, window functions, CTEs
- [PPL Reference](https://seraphjiang.github.io/fuse/ppl-reference.html) — pipe-delimited queries with `lookup`
- [API Reference](https://seraphjiang.github.io/fuse/api-reference.html) — all 19 endpoints
- [Writing a Connector](docs/guides/writing-a-connector.md) — build your own
