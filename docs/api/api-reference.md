# Fuse API Reference

Base URL: `https://fuse.huanji.profile.aws.dev`

All endpoints return JSON. Errors use `{"error": "message"}` with appropriate HTTP status codes.

---

## 1. Health Check

`GET /api/fuse/health`

Returns overall service health and per-connector status.

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `healthy`, `degraded`, or `unhealthy` |
| `version` | string | Server version |
| `connectors` | object | Map of connector ID → health info |
| `connectors.*.status` | string | `healthy`, `degraded`, or `unhealthy` |
| `connectors.*.latency_ms` | number? | Round-trip latency in milliseconds |
| `connectors.*.message` | string? | Error message if unhealthy |

**Example:**

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/health
```

```json
{
  "status": "healthy",
  "version": "0.1.0",
  "connectors": {
    "cluster_a": { "status": "healthy", "latency_ms": 12 },
    "cluster_b": { "status": "healthy", "latency_ms": 8 }
  }
}
```

---

## 2. List Datasources

`GET /api/fuse/datasources`

Returns all registered datasources with their capabilities.

**Response:** Array of datasource objects.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Datasource identifier |
| `connector_type` | string | `opensearch`, `s3`, `prometheus` |
| `capabilities.supports_filtering` | bool | Can push down WHERE clauses |
| `capabilities.supports_projection` | bool | Can push down column selection |
| `capabilities.supports_aggregation` | bool | Can push down GROUP BY / aggregations |
| `capabilities.supports_sorting` | bool | Can push down ORDER BY |
| `capabilities.supports_limit` | bool | Can push down LIMIT |
| `capabilities.supports_join` | bool | Can execute JOINs remotely |
| `capabilities.supports_streaming` | bool | Supports streaming results |
| `capabilities.max_concurrent_queries` | number | Concurrency limit |
| `capabilities.latency_class` | string | `Low`, `Medium`, or `High` |

**Example:**

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources
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
      "supports_streaming": true,
      "max_concurrent_queries": 16,
      "latency_class": "Low"
    }
  },
  {
    "id": "cluster_b",
    "connector_type": "opensearch",
    "capabilities": {
      "supports_filtering": true,
      "supports_projection": true,
      "supports_aggregation": true,
      "supports_sorting": true,
      "supports_limit": true,
      "supports_join": false,
      "supports_streaming": true,
      "max_concurrent_queries": 16,
      "latency_class": "Low"
    }
  }
]
```

---

## 3. List Schemas

`GET /api/fuse/datasources/{id}/schemas`

Discovers available tables/indices from a datasource.

**Path parameters:**

| Parameter | Description |
|-----------|-------------|
| `id` | Datasource identifier |

**Response:** Array of schema name strings.

**Example:**

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources/cluster_a/schemas
```

```json
["logs", "metrics", "traces"]
```

**Errors:**

| Status | Condition |
|--------|-----------|
| 404 | Datasource not found |
| 500 | Schema discovery failed |

---

## 4. Get Fields

`GET /api/fuse/datasources/{id}/schemas/{table}/fields`

Returns field names, data types, and nullability for a table.

**Path parameters:**

| Parameter | Description |
|-----------|-------------|
| `id` | Datasource identifier |
| `table` | Table/index name |

**Response:** Array of field objects.

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Field name |
| `data_type` | string | Arrow data type (e.g. `Utf8`, `Int64`, `Timestamp`) |
| `nullable` | bool | Whether the field allows nulls |

**Example:**

```bash
curl https://fuse.huanji.profile.aws.dev/api/fuse/datasources/cluster_a/schemas/logs/fields
```

```json
[
  { "name": "@timestamp", "data_type": "Utf8", "nullable": false },
  { "name": "service", "data_type": "Utf8", "nullable": true },
  { "name": "status", "data_type": "Int64", "nullable": true },
  { "name": "message", "data_type": "Utf8", "nullable": true }
]
```

**Errors:**

| Status | Condition |
|--------|-----------|
| 404 | Datasource not found |
| 500 | Schema retrieval failed |

---

## 5. Execute Query

`POST /api/fuse/query`

Execute a SQL or PPL query against registered datasources. Tables are referenced as `datasource.table`.

**Request body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | yes | SQL or PPL query string |
| `format` | string | no | `sql` (default) or `ppl` |

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `columns` | string[] | Column names |
| `rows` | array[] | Array of row arrays (values as strings or null) |
| `metadata.total_rows` | number | Total rows returned |
| `metadata.format` | string | Query format used |

**Example — SQL:**

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT * FROM cluster_a.logs WHERE status = 500",
    "format": "sql"
  }'
```

```json
{
  "columns": ["@timestamp", "service", "status", "message"],
  "rows": [
    ["2026-04-09T08:00:00Z", "auth-svc", "500", "internal error"],
    ["2026-04-09T08:01:12Z", "api-gw", "500", "upstream timeout"]
  ],
  "metadata": {
    "total_rows": 2,
    "format": "sql"
  }
}
```

**Example — PPL (multi-source federation):**

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "source = cluster_a.logs, cluster_b.logs | where status >= 500 | stats count() by service",
    "format": "ppl"
  }'
```

```json
{
  "columns": ["service", "count"],
  "rows": [
    ["auth-svc", "14"],
    ["api-gw", "7"],
    ["payment-svc", "3"]
  ],
  "metadata": {
    "total_rows": 3,
    "format": "ppl"
  }
}
```

**Errors:**

| Status | Condition |
|--------|-----------|
| 400 | Invalid query syntax or missing FROM clause |
| 404 | Datasource not found |
| 500 | Connector execution failure |

---

## 6. Validate Query

`POST /api/fuse/query/validate`

Checks query syntax and datasource availability without executing.

**Request body:** Same as [Execute Query](#5-execute-query).

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `valid` | bool | Whether the query is valid |
| `error` | string? | Error message if invalid |

**Example — valid query:**

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query/validate \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.logs", "format": "sql"}'
```

```json
{ "valid": true }
```

**Example — invalid query:**

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query/validate \
  -H 'Content-Type: application/json' \
  -d '{"query": "source = nonexistent.logs | head 10", "format": "ppl"}'
```

```json
{
  "valid": false,
  "error": "datasource 'nonexistent' not found in registry"
}
```

---

## 7. Explain Query

`POST /api/fuse/query/explain`

Returns the federated execution plan without running the query.

**Request body:** Same as [Execute Query](#5-execute-query).

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `plan` | string | Human-readable execution plan |

**Example:**

```bash
curl -X POST https://fuse.huanji.profile.aws.dev/api/fuse/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM cluster_a.logs", "format": "sql"}'
```

```json
{
  "plan": "FederatedPlan {\n  datasource: \"cluster_a\",\n  table: \"logs\",\n  format: \"sql\",\n  connector_found: true,\n  strategy: FanOut\n}"
}
```

**Errors:**

| Status | Condition |
|--------|-----------|
| 400 | Invalid query syntax |

---

## Common Error Response

All error responses use the same format:

```json
{
  "error": "descriptive error message"
}
```

| Status | Meaning |
|--------|---------|
| 400 | Bad request — invalid query syntax |
| 404 | Not found — datasource or table doesn't exist |
| 500 | Internal error — connector failure |
