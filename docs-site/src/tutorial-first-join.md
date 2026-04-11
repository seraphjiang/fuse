# Tutorial: From Install to Cross-Source JOIN in 5 Minutes

This walkthrough takes you from zero to running a cross-datasource JOIN query.

## 1. Install

```bash
# Clone
git clone https://github.com/seraphjiang/fuse
cd fuse

# Build (requires Rust 1.85+, OpenSSL dev headers)
cargo build --release
```

The binary is at `target/release/fuse-server`.

## 2. Configure Datasources

Create `fuse.toml` in the project root:

```toml
[engine]
bind = "0.0.0.0:9400"

# An OpenSearch cluster
[[connector]]
id = "logs"
type = "opensearch"
url = "https://your-opensearch-cluster:9200"
[connector.auth]
type = "basic"
username = "admin"
password = "admin"

# A PostgreSQL database
[[connector]]
id = "users"
type = "postgres"
url = "postgresql://user:pass@localhost:5432/mydb"
```

Any combination of the 22 supported connectors works. See the [connector table](../../README.md#connectors) for all options.

## 3. Start the Server

```bash
# With your config
./target/release/fuse-server

# Or with Docker
docker compose up -d          # starts OpenSearch
cargo run -p fuse-server      # starts Fuse on :9400
```

Verify it's running:

```bash
curl http://localhost:9400/api/fuse/health
# {"status":"healthy","connectors":{"logs":{"status":"ok"},"users":{"status":"ok"}}}
```

## 4. Explore Your Data

```bash
# List datasources
curl http://localhost:9400/api/fuse/datasources

# List tables in a datasource
curl http://localhost:9400/api/fuse/datasources/logs/schemas

# List fields in a table
curl http://localhost:9400/api/fuse/datasources/logs/schemas/application_logs/fields
```

## 5. Run Your First Query

Single-source query:

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT service, status, message FROM logs.application_logs LIMIT 5",
    "format": "sql"
  }'
```

## 6. Cross-Source JOIN

This is where Fuse shines — join data across different systems in one query:

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT l.service, l.status, u.name, u.role FROM logs.application_logs l JOIN users.accounts u ON l.user_id = u.id WHERE l.status >= 500",
    "format": "sql"
  }'
```

Fuse automatically:
1. Pushes filters down to each datasource (OpenSearch query DSL, PostgreSQL WHERE)
2. Fetches only the needed columns
3. Performs a hash join in memory
4. Returns the merged result

## 7. Check the Query Plan

See exactly how Fuse executes your query:

```bash
curl -X POST http://localhost:9400/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "EXPLAIN SELECT l.service, u.name FROM logs.application_logs l JOIN users.accounts u ON l.user_id = u.id",
    "format": "sql"
  }'
```

## 8. Try the Playground

Open `http://localhost:9400` in your browser for the interactive query editor with:
- Syntax highlighting and autocomplete
- Results table with CSV/JSON export
- EXPLAIN ANALYZE flame graph
- Query plan DAG visualization
- Dashboard builder

## What's Next

- [PPL queries](../../README.md#ppl-lookup-cross-source-enrichment) — pipe-delimited query language with `lookup` for enrichment
- [Prepared statements](api-reference-guide.md) — `PREPARE`/`EXECUTE` with parameter binding
- [Writing a connector](writing-a-connector.md) — add your own datasource
- [Federation](federation-architecture-guide.md) — chain Fuse instances across regions
- [Security hardening](security-hardening-guide.md) — TLS, RBAC, secret management
