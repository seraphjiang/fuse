# API Reference Guide

Complete reference for every Fuse REST endpoint with curl examples and request/response schemas.

Base URL: `http://localhost:9400` (local) or `https://fuse.huanji.profile.aws.dev` (playground).

---

## POST /api/fuse/query

Execute a SQL or PPL query.

**Request:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `query` | string | required | SQL or PPL query |
| `format` | string | `"sql"` | `"sql"` or `"ppl"` |
| `analyze` | bool | `false` | Include execution profile with timing |
| `timeout_ms` | int | `30000` | Per-query timeout in milliseconds |
| `result_format` | string | `"json"` | `"json"` or `"csv"` |
| `params` | object | `{}` | Named parameters for `$name` placeholders |
| `page_size` | int | none | Rows per page (enables cursor pagination) |
| `cursor` | string | none | Cursor from previous `next_cursor` |
| `start` | string | none | Prometheus range query start time |
| `end` | string | none | Prometheus range query end time |
| `step` | string | none | Prometheus range query step (e.g., `"15s"`) |

**SQL example:**

```bash
curl -s -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT service, count(*) as errors FROM cluster_a.application_logs WHERE status >= 500 GROUP BY service ORDER BY errors DESC LIMIT 5",
    "format": "sql"
  }'
```

```json
{
  "columns": ["service", "errors"],
  "rows": [
    ["api-gateway", 42],
    ["auth-service", 18],
    ["user-service", 7]
  ],
  "metadata": {
    "total_rows": 3,
    "format": "sql",
    "trace_id": "q-00a1b2c3d4e5f6"
  }
}
```

**PPL example:**

```bash
curl -s -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "source = cluster_a.application_logs | where status >= 500 | stats count() as errors by service | sort - errors | head 5",
    "format": "ppl"
  }'
```

**Cross-datasource (UNION ALL) — response with datasource stats:**

```bash
curl -s -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT service, status FROM cluster_a.application_logs UNION ALL SELECT service, status FROM cluster_b.application_logs LIMIT 20",
    "format": "sql"
  }'
```

```json
{
  "columns": ["service", "status", "_datasource"],
  "rows": [
    ["api-gateway", 500, "cluster_a"],
    ["order-service", 503, "cluster_b"]
  ],
  "metadata": {
    "total_rows": 20,
    "format": "sql",
    "trace_id": "q-00f1e2d3c4b5a6",
    "datasources_queried": ["cluster_a", "cluster_b"],
    "datasource_stats": {
      "cluster_a": {"rows": 12, "latency_ms": 45},
      "cluster_b": {"rows": 8, "latency_ms": 67}
    }
  },
  "partial_errors": []
}
```

**With analyze (execution profile):**

```bash
curl -s -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs LIMIT 10", "analyze": true}'
```

```json
{
  "columns": ["..."],
  "rows": ["..."],
  "metadata": {"..."},
  "execution_profile": {
    "total_ms": 52,
    "nodes": [{
      "operator": "Scan",
      "datasource": "cluster_a",
      "rows": 10,
      "time_ms": 52,
      "data_bytes": 4096,
      "pushdown": ["filter", "projection", "limit"]
    }]
  }
}
```

**Cursor pagination:**

```bash
# First page
curl -s -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", "page_size": 10}'
```

```json
{
  "columns": ["timestamp", "service", "status"],
  "rows": [["2026-04-10T05:30:00Z", "api-gateway", 200], "..."],
  "metadata": {"total_rows": 10},
  "next_cursor": "eyJvZmZzZXQiOjEwfQ=="
}
```

```bash
# Next page
curl -s -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs ORDER BY timestamp DESC", "page_size": 10, "cursor": "eyJvZmZzZXQiOjEwfQ=="}'
```

When `next_cursor` is absent, you've reached the last page.

**Parameterized query:**

```bash
curl -s -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT * FROM cluster_a.application_logs WHERE service = $svc AND status >= $code LIMIT $n",
    "params": {"svc": "api-gateway", "code": 500, "n": 10}
  }'
```

**CSV output:**

```bash
curl -s -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT service, status FROM cluster_a.application_logs LIMIT 5", "result_format": "csv"}'
```

Returns `text/csv` with headers.

---

## POST /api/fuse/query/stream

Server-Sent Events (SSE) streaming. Same request body as `/api/fuse/query`.

```bash
curl -N -X POST http://localhost:9400/api/fuse/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs LIMIT 100"}'
```

**Event stream:**

```
data: {"type":"metadata","columns":["timestamp","service","status"]}

data: {"type":"batch","rows":[["2026-04-10T05:30:00Z","api-gateway",200],["..."]]}

data: {"type":"progress","batches_sent":1}

data: {"type":"batch","rows":[["..."]]}

data: {"type":"done","total_rows":100}
```

On error:
```
data: {"type":"error","message":"datasource 'missing' not found"}
```

---

## POST /api/fuse/query/validate

Check query syntax without executing.

```bash
curl -s -X POST http://localhost:9400/api/fuse/query/validate \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs"}'
```

```json
{"valid": true, "error": null}
```

Invalid query:
```bash
curl -s -X POST http://localhost:9400/api/fuse/query/validate \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELEC * FORM bad_syntax"}'
```

```json
{"valid": false, "error": "no datasource.table references found"}
```

---

## POST /api/fuse/query/explain

Show execution plan without running the query.

```bash
curl -s -X POST http://localhost:9400/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs UNION ALL SELECT * FROM cluster_b.application_logs LIMIT 20"}'
```

```json
{
  "plan": "Merge\n  RemoteScan [cluster_a] application_logs (est. 1000 rows, cost: 1.0)\n  RemoteScan [cluster_b] application_logs (est. 1000 rows, cost: 1.0)",
  "plan_tree": {
    "op": "Merge",
    "children": [
      {"op": "RemoteScan", "detail": "cluster_a.application_logs", "estimated_rows": 1000, "estimated_cost": 1.0},
      {"op": "RemoteScan", "detail": "cluster_b.application_logs", "estimated_rows": 1000, "estimated_cost": 1.0}
    ]
  }
}
```

---

## GET /api/fuse/datasources

List all registered datasource connectors.

```bash
curl -s http://localhost:9400/api/fuse/datasources
```

```json
[
  {
    "id": "cluster_a",
    "connector_type": "opensearch",
    "capabilities": {
      "supports_filtering": true,
      "supports_projection": true,
      "supports_aggregation": true,
      "supports_sorting": true,
      "supports_limit": true,
      "supports_join": false,
      "max_concurrent_queries": 16,
      "supports_streaming": true,
      "latency_class": "low"
    }
  },
  {
    "id": "dynamodb",
    "connector_type": "dynamodb",
    "capabilities": {
      "supports_filtering": true,
      "supports_projection": true,
      "supports_aggregation": false,
      "supports_sorting": false,
      "supports_limit": true,
      "supports_join": false,
      "max_concurrent_queries": 8,
      "supports_streaming": true,
      "latency_class": "medium"
    }
  }
]
```

---

## GET /api/fuse/datasources/{id}/schemas

List tables/indices for a datasource.

```bash
curl -s http://localhost:9400/api/fuse/datasources/cluster_a/schemas
```

```json
[
  {"name": "application_logs", "schema_type": "index", "estimated_row_count": 5000},
  {"name": "access_logs", "schema_type": "index", "estimated_row_count": 12000}
]
```

---

## GET /api/fuse/datasources/{id}/schemas/{table}/fields

Get field names and Arrow types for a table.

```bash
curl -s http://localhost:9400/api/fuse/datasources/cluster_a/schemas/application_logs/fields
```

```json
[
  {"name": "timestamp", "data_type": "Utf8", "nullable": true},
  {"name": "service", "data_type": "Utf8", "nullable": true},
  {"name": "status", "data_type": "Int64", "nullable": true},
  {"name": "message", "data_type": "Utf8", "nullable": true},
  {"name": "trace_id", "data_type": "Utf8", "nullable": true},
  {"name": "user_id", "data_type": "Utf8", "nullable": true},
  {"name": "response_time_ms", "data_type": "Int64", "nullable": true}
]
```

---

## GET /api/fuse/trace/{trace_id}

Reconstruct a distributed trace by searching all datasources.

```bash
curl -s http://localhost:9400/api/fuse/trace/trace-001
```

```json
{
  "trace_id": "trace-001",
  "spans": [
    {
      "datasource": "cluster_a",
      "timestamp": "2026-04-10T05:30:00Z",
      "fields": {"service": "api-gateway", "status": 200, "message": "request received", "user_id": "user-042"}
    },
    {
      "datasource": "dynamodb",
      "timestamp": "2026-04-10T05:30:01Z",
      "fields": {"user_id": "user-042", "action": "profile_lookup"}
    },
    {
      "datasource": "s3_o11y",
      "timestamp": "2026-04-10T05:30:02Z",
      "fields": {"service": "api-gateway", "level": "INFO", "message": "response sent"}
    }
  ],
  "datasources_searched": ["cluster_a", "cluster_b", "dynamodb", "s3_o11y", "cloudwatch", "s3_demo"],
  "datasources_matched": ["cluster_a", "dynamodb", "s3_o11y"],
  "total_spans": 3,
  "search_ms": 187
}
```

Spans are sorted by timestamp. Returns empty `spans` array if no matches found.

---

## GET /api/fuse/health

Engine and per-connector health status.

```bash
curl -s http://localhost:9400/api/fuse/health
```

```json
{
  "status": "ok",
  "connectors": {
    "cluster_a": {"status": "healthy", "latency_ms": 23, "message": null},
    "cluster_b": {"status": "healthy", "latency_ms": 31, "message": null},
    "dynamodb": {"status": "healthy", "latency_ms": 45, "message": null},
    "s3_o11y": {"status": "degraded", "latency_ms": 520, "message": "high latency"}
  }
}
```

Status values: `ok` (all healthy), `degraded` (some unhealthy), `error` (all unhealthy).
Per-connector: `healthy`, `degraded`, `unhealthy`.

---

## GET /api/fuse/history

Recent query executions.

```bash
curl -s http://localhost:9400/api/fuse/history
```

```json
[
  {
    "query": "SELECT * FROM cluster_a.application_logs LIMIT 5",
    "format": "sql",
    "timestamp": 1712728800,
    "latency_ms": 45,
    "row_count": 5,
    "error": null
  }
]
```

---

## GET /api/fuse/stats

Aggregated query statistics.

```bash
curl -s http://localhost:9400/api/fuse/stats
```

```json
{
  "total_queries": 142,
  "successful": 138,
  "failed": 4,
  "avg_latency_ms": 87
}
```

---

## Saved Queries

```bash
# List
curl -s http://localhost:9400/api/fuse/saved

# Save
curl -s -X POST http://localhost:9400/api/fuse/saved \
  -H 'Content-Type: application/json' \
  -d '{"name": "error_search", "query": "SELECT * FROM cluster_a.application_logs WHERE status >= $code", "format": "sql"}'

# Get
curl -s http://localhost:9400/api/fuse/saved/error_search

# Delete
curl -s -X DELETE http://localhost:9400/api/fuse/saved/error_search
```

---

## Running Queries

```bash
# List running queries
curl -s http://localhost:9400/api/fuse/queries/running

# Cancel a query
curl -s -X DELETE http://localhost:9400/api/fuse/query/q-00a1b2c3/cancel
```

---

## Error Responses

All errors return:

```json
{"error": "description of what went wrong"}
```

| Status | Meaning |
|--------|---------|
| 400 | Parse error, missing FROM, bad syntax |
| 404 | Datasource, saved query, or view not found |
| 408 | Query timeout (exceeded `timeout_ms`) |
| 429 | Rate limited |
| 500 | Connector error, internal failure |
