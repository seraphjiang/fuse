# Administration Guide

Manage multi-tenancy, API keys, rate limits, query governors, audit logs, and monitoring for production Fuse deployments.

## 1. Multi-Tenancy

Multi-tenancy isolates datasource access per API key identity. When enabled, each tenant can only query datasources in their allowlist.

### Configuration

In `fuse.toml`:

```toml
# Admin tenant — access to all datasources
[[tenant]]
id = "ops-team"
datasources = []  # empty = all access

# Restricted tenant — specific datasources only
[[tenant]]
id = "team-alpha"
datasources = ["cluster_a", "s3_o11y"]
max_rows = 10000
max_time_ms = 30000
max_result_bytes = 50000000  # 50MB

[[tenant]]
id = "team-beta"
datasources = ["cluster_b", "dynamodb"]
max_rows = 5000
max_time_ms = 15000
max_result_bytes = 10000000  # 10MB
```

### How It Works

- Tenant ID maps to the API key identity (from `[[api_key]]` config)
- When a query references a datasource not in the tenant's allowlist, it returns 403
- Unknown identities (no matching tenant config) get zero access when tenancy is enabled
- When no `[[tenant]]` blocks exist, isolation is disabled — all keys access everything

### Verify Tenant Access

```bash
# As team-alpha (can access cluster_a)
curl -H "x-api-key: alpha-key" \
  http://localhost:9400/api/fuse/datasources
# Returns: only cluster_a and s3_o11y

# As team-alpha (cannot access cluster_b)
curl -H "x-api-key: alpha-key" -X POST \
  http://localhost:9400/api/fuse/query \
  -d '{"query": "SELECT * FROM cluster_b.logs LIMIT 1"}'
# Returns: 403 — datasource 'cluster_b' not accessible
```

## 2. API Key Management

### Create Keys

In `fuse.toml`:

```toml
[[api_key]]
key = "ak-ops-2026-04"
identity = "ops-team"
role = "admin"

[[api_key]]
key = "ak-alpha-readonly"
identity = "team-alpha"
role = "viewer"

[[api_key]]
key = "ak-beta-editor"
identity = "team-beta"
role = "editor"
```

### Roles

| Role | Query | Saved Queries | Dashboards | Admin |
|------|-------|---------------|------------|-------|
| `viewer` | ✅ read-only | ✅ read | ✅ read | ❌ |
| `editor` | ✅ | ✅ CRUD | ✅ CRUD | ❌ |
| `admin` | ✅ | ✅ CRUD | ✅ CRUD | ✅ config, keys, tenants |

### Usage

```bash
# x-api-key header
curl -H "x-api-key: ak-ops-2026-04" http://localhost:9400/api/fuse/datasources

# Bearer token
curl -H "Authorization: Bearer ak-ops-2026-04" http://localhost:9400/api/fuse/datasources
```

### Revoke a Key

Remove the `[[api_key]]` block from `fuse.toml` and restart the server. The key is immediately invalid.

### Public Endpoints

These endpoints never require authentication:
- `GET /api/fuse/health`
- `GET /metrics`

## 3. Rate Limiting

### Global Configuration

```toml
[engine]
rate_limit_global = 1000    # total requests/minute across all clients
rate_limit_per_ip = 100     # requests/minute per client IP
```

### Behavior

- Uses token bucket algorithm (via `governor` crate)
- Exceeding limits returns HTTP 429 with `Retry-After` header
- Limits reset automatically (per-minute window)
- IP detection: `X-Forwarded-For` → `X-Real-IP` → socket address

### Tuning

| Deployment | Global | Per-IP | Notes |
|-----------|--------|--------|-------|
| Development | 0 (disabled) | 0 | Set to 0 to disable |
| Internal team | 5000 | 500 | Trusted clients |
| Public API | 1000 | 50 | Protect backends |
| Dashboard auto-refresh | 2000 | 200 | Frequent polling |

## 4. Query Governor

Per-tenant resource limits prevent any single tenant from overloading the system.

### Limits

| Limit | Config Key | Description |
|-------|-----------|-------------|
| Max rows | `max_rows` | Truncates results beyond this count |
| Max time | `max_time_ms` | Cancels query if execution exceeds this |
| Max result size | `max_result_bytes` | Rejects results larger than this |

### How Limits Apply

1. **Timeout**: `effective_timeout = min(request.timeout_ms, tenant.max_time_ms)`
2. **Row limit**: results are truncated to `min(query LIMIT, tenant.max_rows)`
3. **Result size**: checked after execution — returns error if exceeded

### Example

```toml
[[tenant]]
id = "free-tier"
datasources = ["cluster_a"]
max_rows = 1000          # max 1000 rows per query
max_time_ms = 10000      # max 10 seconds
max_result_bytes = 5000000  # max 5MB
```

A query requesting `LIMIT 5000` from this tenant gets truncated to 1000 rows. A query running longer than 10s is cancelled.

## 5. Audit Log

Every API action is recorded with identity, query, timing, and result.

### Audit Entry Fields

| Field | Description |
|-------|-------------|
| `timestamp` | Unix timestamp (seconds) |
| `identity` | API key identity (tenant ID) |
| `action` | Query, Explain, Validate, ListDatasources, GetSchema, TraceReconstruction, SavedQueryCreate, SavedQueryDelete |
| `query` | SQL/PPL query string (if applicable) |
| `datasources` | Datasources accessed |
| `duration_ms` | Execution time |
| `row_count` | Rows returned |
| `status` | Success, Error, Denied |
| `error` | Error message (if failed) |
| `client_ip` | Client IP address |

### Viewing Audit Logs

Audit entries are emitted as structured JSON via `tracing`:

```json
{"timestamp":"2026-04-10T06:00:00Z","level":"INFO","audit":true,"identity":"team-alpha","action":"Query","status":"Success","duration_ms":45,"row_count":20}
```

Filter with standard log tools:

```bash
# All audit entries
grep '"audit":true' /var/log/fuse/fuse.log

# Failed queries
grep '"audit":true' /var/log/fuse/fuse.log | grep '"status":"Error"'

# Specific tenant
grep '"audit":true' /var/log/fuse/fuse.log | grep '"identity":"team-alpha"'

# Denied access attempts
grep '"audit":true' /var/log/fuse/fuse.log | grep '"status":"Denied"'
```

### In-Memory Audit API

Recent entries are also available via API (admin role required):

```bash
curl -H "x-api-key: admin-key" http://localhost:9400/api/fuse/history
```

## 6. Monitoring and Alerting

### Health Check

```bash
# All connectors
curl http://localhost:9400/api/fuse/health
```

Monitor for `degraded` or `error` status. A degraded connector slows all queries that touch it.

### Prometheus Metrics

```bash
curl http://localhost:9400/metrics
```

Exposed metrics:
- `fuse_queries_total{format, success}` — query count by format and outcome
- `fuse_query_duration_ms` — latency histogram
- `fuse_active_queries` — current in-flight queries
- `fuse_connector_healthy{connector_id}` — per-connector health (1=healthy, 0=unhealthy)

### Grafana Integration

Import Fuse metrics into Grafana:

1. Add Prometheus as a Grafana datasource (pointing to your Prometheus scraping `/metrics`)
2. Create dashboard panels:
   - Query rate: `rate(fuse_queries_total[5m])`
   - Error rate: `rate(fuse_queries_total{success="false"}[5m])`
   - P99 latency: `histogram_quantile(0.99, fuse_query_duration_ms)`
   - Active queries: `fuse_active_queries`
   - Connector health: `fuse_connector_healthy`

### Alert Rules

Configure Fuse's built-in alerting for federated query monitoring:

```bash
# List alert rules
curl http://localhost:9400/api/fuse/alerts

# Evaluate all rules now
curl -X POST http://localhost:9400/api/fuse/alerts/evaluate
```

### Query History

```bash
# Recent queries with timing
curl http://localhost:9400/api/fuse/history

# Aggregated stats
curl http://localhost:9400/api/fuse/stats
```

Look for queries with high `latency_ms` or non-null `error` fields.

## Quick Reference

| Task | Config / Command |
|------|-----------------|
| Enable auth | Add `[[api_key]]` blocks to `fuse.toml` |
| Add tenant | Add `[[tenant]]` block with `datasources` allowlist |
| Set rate limits | `[engine]` → `rate_limit_global`, `rate_limit_per_ip` |
| Set query limits | `[[tenant]]` → `max_rows`, `max_time_ms`, `max_result_bytes` |
| View audit log | `grep '"audit":true' /var/log/fuse/fuse.log` |
| Check health | `GET /api/fuse/health` |
| Scrape metrics | `GET /metrics` (Prometheus format) |
| Revoke key | Remove `[[api_key]]` block, restart server |
