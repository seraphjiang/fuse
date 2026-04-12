# Multi-Tenant SaaS Mode

Isolate tenants with usage metering, rate limiting, and query governance.

## Overview

Fuse supports multi-tenant operation where each tenant (identified by API key) has isolated query limits, usage tracking, and rate controls.

## Configuration

```toml
[tenants.acme]
api_key = "key-acme-prod"
max_queries_per_minute = 100
max_rows = 100000
max_execution_time_ms = 30000
max_result_bytes = 52428800  # 50 MB

[tenants.startup]
api_key = "key-startup-dev"
max_queries_per_minute = 20
max_rows = 10000
max_execution_time_ms = 10000
max_result_bytes = 10485760  # 10 MB
```

## Authentication

Pass the API key in the `X-API-Key` header:

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: key-acme-prod' \
  -d '{"query": "SELECT * FROM cluster_a.application_logs LIMIT 10", "format": "sql"}'
```

## Query Governor

The governor enforces per-tenant limits:

- **max_rows** — rejects queries returning more rows than allowed
- **max_execution_time_ms** — kills queries exceeding the time limit
- **max_result_bytes** — rejects oversized result sets

## Usage Metering

Per-tenant usage is tracked automatically:

- Query count
- Total rows returned
- Total bytes transferred

Access via `/api/fuse/stats` (includes tenant-level breakdown when authenticated).

## Error Responses

| Error | Cause |
|-------|-------|
| `rate limit exceeded for API key` | Exceeded `max_queries_per_minute` |
| `query governor: max rows exceeded` | Result exceeds `max_rows` |
| `query governor: execution time exceeded` | Query exceeds `max_execution_time_ms` |
| `query governor: result size exceeded` | Response exceeds `max_result_bytes` |
