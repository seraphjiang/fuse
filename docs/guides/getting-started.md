# Getting Started with Fuse

Fuse is a federated query engine that lets you run a single SQL or PPL query across multiple OpenSearch clusters and datasources, merging results automatically. It runs as a standalone service and is accessible from OpenSearch Dashboards or directly via REST API.

**Playground:** https://fuse.huanji.profile.aws.dev *(Amazon VPN required)*

---

## Available Datasources

| ID | Type | Cluster | Index |
|----|------|---------|-------|
| `cluster_a` | OpenSearch Serverless | fuse-cluster-a (us-west-2) | `services` |
| `cluster_b` | OpenSearch Serverless | fuse-cluster-b (us-west-2) | `services` |

The `services` index contains simulated microservice request logs with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `trace_id` | keyword | Distributed trace identifier |
| `service` | keyword | Service name (e.g., `api-gateway`, `auth`, `payments`) |
| `status` | integer | HTTP status code |
| `latency_ms` | float | Request latency in milliseconds |
| `message` | text | Log message |
| `@timestamp` | date | Event timestamp |

---

## Example Queries

### Basic search (SQL)

```sql
SELECT trace_id, service, status, latency_ms
FROM cluster_a.services
LIMIT 10
```

### Basic search (PPL)

```ppl
source = cluster_a.services
| fields trace_id, service, status, latency_ms
| head 10
```

### Filter by status code

```sql
-- SQL: find all 5xx errors
SELECT trace_id, service, status, message
FROM cluster_a.services
WHERE status >= 500
LIMIT 50
```

```ppl
-- PPL equivalent
source = cluster_a.services
| where status >= 500
| fields trace_id, service, status, message
| head 50
```

### Aggregate by service

```sql
-- Count errors per service
SELECT service, COUNT(*) AS error_count
FROM cluster_a.services
WHERE status >= 500
GROUP BY service
```

```ppl
source = cluster_a.services
| where status >= 500
| stats count() as error_count by service
| sort - error_count
```

### Cross-cluster federated query

This is the core Fuse feature — one query, two clusters, merged results:

```sql
-- Search both clusters simultaneously
SELECT trace_id, service, status
FROM cluster_a.services
WHERE status >= 500
LIMIT 100
```

> **Note:** To query both clusters in a single statement, list them comma-separated (Phase 1 multi-source syntax):
> ```sql
> SELECT * FROM cluster_a.services, cluster_b.services WHERE status = 500
> ```

### Cross-cluster trace correlation

Find a trace that spans both clusters:

```sql
-- Find all events for a specific trace across both clusters
SELECT trace_id, service, status, message
FROM cluster_a.services
WHERE trace_id = 'abc-1234'
```

Then run the same query against `cluster_b.services` to correlate.

---

## API Usage

All endpoints are at `https://fuse.huanji.profile.aws.dev/api/fuse/`.

### Health check

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/health
```

```json
{
  "status": "ok",
  "version": "0.1.0",
  "connectors": {
    "cluster_a": { "status": "healthy", "latency_ms": 12 },
    "cluster_b": { "status": "healthy", "latency_ms": 15 }
  }
}
```

### List datasources

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources
```

### List schemas for a datasource

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources/cluster_a/schemas
```

### Get fields for a table

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources/cluster_a/schemas/services/fields
```

### Execute a query (SQL)

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT service, status FROM cluster_a.services WHERE status >= 500 LIMIT 10",
    "format": "sql"
  }'
```

### Execute a query (PPL)

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "source = cluster_a.services | where status >= 500 | stats count() by service",
    "format": "ppl"
  }'
```

### Validate a query

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query/validate \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.services", "format": "sql"}'
```

```json
{ "valid": true }
```

### Explain a query plan

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.services WHERE status = 500", "format": "sql"}'
```

```json
{
  "plan": "FederatedPlan {\n  datasource: \"cluster_a\",\n  table: \"services\",\n  format: \"sql\",\n  connector_found: true,\n  strategy: FanOut\n}"
}
```

---

## Understanding Results

Query responses have this structure:

```json
{
  "columns": ["trace_id", "service", "status"],
  "rows": [
    ["abc-001", "api-gateway", "500"],
    ["abc-002", "auth", "200"]
  ],
  "metadata": {
    "total_rows": 2,
    "format": "sql"
  }
}
```

| Field | Description |
|-------|-------------|
| `columns` | Ordered list of column names |
| `rows` | Array of arrays — each inner array is one row, values in column order |
| `metadata.total_rows` | Number of rows returned |
| `metadata.format` | Query format used (`sql` or `ppl`) |

All values are returned as strings. Parse numeric fields client-side as needed.

---

## PPL vs SQL

Both languages are supported. Choose based on your use case:

| | SQL | PPL |
|--|-----|-----|
| Best for | Joins, aggregations, subqueries | Log analysis, pipe-style filtering |
| FROM syntax | `FROM datasource.table` | `source = datasource.table` |
| Filter | `WHERE status = 500` | `\| where status = 500` |
| Aggregate | `GROUP BY service` | `\| stats count() by service` |
| Sort | `ORDER BY latency_ms DESC` | `\| sort - latency_ms` |
| Limit | `LIMIT 100` | `\| head 100` |
| Select fields | `SELECT a, b` | `\| fields a, b` |

**Quick syntax comparison:**

```sql
-- SQL
SELECT service, COUNT(*) AS cnt
FROM cluster_a.services
WHERE status >= 500
GROUP BY service
ORDER BY cnt DESC
LIMIT 10
```

```ppl
-- PPL equivalent
source = cluster_a.services
| where status >= 500
| stats count() as cnt by service
| sort - cnt
| head 10
```

---

## Troubleshooting

### `"datasource 'X' not found"`

The datasource ID in your query doesn't match a registered connector.

```bash
# Check available datasource IDs
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources | python3 -m json.tool
```

Use the exact `id` value from the response (e.g., `cluster_a`, not `Cluster_A`).

### `"SQL query must contain FROM clause"`

Your SQL query is missing a `FROM datasource.table` clause. Fuse requires qualified table references:

```sql
-- ❌ Wrong
SELECT * FROM services

-- ✅ Correct
SELECT * FROM cluster_a.services
```

### `"PPL query must start with 'source = '"`

PPL queries must begin with `source = datasource.table`:

```ppl
-- ❌ Wrong
search source=services | where status=500

-- ✅ Correct
source = cluster_a.services | where status = 500
```

### `"expected 'datasource.table', got 'X'"`

The table reference must be qualified with a datasource prefix:

```sql
-- ❌ Wrong: unqualified
SELECT * FROM services

-- ✅ Correct: qualified
SELECT * FROM cluster_a.services
```

### Health shows `"degraded"` connectors

The engine is running but the OpenSearch Serverless clusters may be warming up or the AOSS compatibility fix hasn't deployed yet. Wait a few minutes and retry:

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/health
```

If status is `"healthy"`, queries will work. If `"degraded"`, basic queries may still succeed — schema discovery may be limited.

### Empty results

If a query returns `"rows": []`, the filter may be too restrictive or the index may not have matching data. Try removing the `WHERE` clause first:

```sql
SELECT * FROM cluster_a.services LIMIT 5
```

---

## Next Steps

- **Build a connector:** See [Writing a Connector](./writing-a-connector.md) and the `fuse-connector-sdk` crate
- **OSD Plugin:** Install the `fuseQuery` plugin to query from within OpenSearch Dashboards
- **API Reference:** Full OpenAPI spec at [docs/api/openapi.yaml](../api/openapi.yaml)
- **GitHub:** https://github.com/seraphjiang/fuse
