# Federation Architecture Guide

Design multi-cluster Fuse deployments with routing, topology patterns, and cross-region federation.

## Topologies

### Single Instance

```
Client → Fuse → [OpenSearch, DynamoDB, S3, Prometheus]
```

Simplest setup. One Fuse server connects to all datasources directly. Good for development and small teams.

### Horizontally Scaled (Shared-Nothing)

```
Client → Load Balancer → Fuse 1 ──┐
                         Fuse 2 ──┤→ Redis (cache/tenants)
                         Fuse 3 ──┘
                           │
                    [All Datasources]
```

Multiple stateless Fuse instances behind a load balancer. All instances connect to all datasources. Redis provides shared cache and tenant registry. See [Horizontal Scaling Guide](./horizontal-scaling-guide.md).

### Hub-and-Spoke Federation

```
                    ┌──────────────┐
                    │   Hub Fuse   │  ← client-facing, routes queries
                    └──────┬───────┘
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Spoke A  │ │ Spoke B  │ │ Spoke C  │
        │ (US-East)│ │ (EU-West)│ │ (AP-NE)  │
        └────┬─────┘ └────┬─────┘ └────┬─────┘
             │             │             │
        [US sources]  [EU sources]  [AP sources]
```

A hub Fuse instance routes queries to regional spoke instances. Each spoke manages its local datasources. The hub merges results.

**Hub config (`fuse.toml`):**

```toml
# Spoke A appears as a datasource of type "fuse" (upstream federation)
[[datasource]]
id = "us_east"
type = "fuse"
url = "https://spoke-a.internal:9400"
api_key = "spoke-a-key"

[[datasource]]
id = "eu_west"
type = "fuse"
url = "https://spoke-b.internal:9400"
api_key = "spoke-b-key"

[[datasource]]
id = "ap_northeast"
type = "fuse"
url = "https://spoke-c.internal:9400"
api_key = "spoke-c-key"
```

**Query through the hub:**

```sql
-- Federated across 3 regions
SELECT region, service, count(*) as errors
FROM us_east.logs
UNION ALL
SELECT region, service, count(*) as errors
FROM eu_west.logs
UNION ALL
SELECT region, service, count(*) as errors
FROM ap_northeast.logs
ORDER BY errors DESC
```

The hub fans out to all three spokes in parallel, each spoke queries its local datasources, and the hub merges the results.

### Tiered Federation

```
        ┌──────────────┐
        │  Global Hub  │
        └──────┬───────┘
        ┌──────┴───────┐
        ▼              ▼
   ┌─────────┐   ┌─────────┐
   │ US Hub  │   │ EU Hub  │
   └────┬────┘   └────┬────┘
   ┌────┴────┐   ┌────┴────┐
   ▼         ▼   ▼         ▼
 Spoke     Spoke Spoke   Spoke
 US-E      US-W  EU-W    EU-C
```

For large organizations: regional hubs aggregate local spokes, a global hub aggregates regional hubs. Each tier adds latency but enables organizational isolation.

## Routing

### Datasource-Based Routing

Queries are routed based on the datasource prefix:

```sql
-- Routes to spoke-a (us_east datasource)
SELECT * FROM us_east.application_logs WHERE status >= 500

-- Routes to spoke-b (eu_west datasource)
SELECT * FROM eu_west.application_logs WHERE status >= 500

-- Routes to both (hub merges)
SELECT * FROM us_east.application_logs
UNION ALL
SELECT * FROM eu_west.application_logs
```

### Cross-Region JOINs

```sql
-- Join US logs with EU user profiles
SELECT l.service, u.name, l.error_message
FROM us_east.application_logs l
JOIN eu_west.user_profiles u ON l.user_id = u.user_id
WHERE l.status >= 500
```

The hub fetches from both spokes in parallel, then performs the hash join locally. Data crosses regions — consider latency and data residency requirements.

## Configuration Patterns

### Environment-Based Datasource Naming

Use consistent naming across environments:

```toml
# Production
[[datasource]]
id = "logs"
type = "opensearch"
url = "${OPENSEARCH_PROD_URL}"

# Staging (same id, different URL)
[[datasource]]
id = "logs"
type = "opensearch"
url = "${OPENSEARCH_STAGING_URL}"
```

Queries use `logs.application_logs` in both environments — the URL changes, the query doesn't.

### Datasource Groups

Organize datasources by function:

```toml
# Observability cluster
[[datasource]]
id = "obs"
type = "opensearch"
url = "https://obs-cluster:9200"

# Business data
[[datasource]]
id = "biz"
type = "postgres"
url = "postgresql://biz-db:5432/main"

# Metrics
[[datasource]]
id = "metrics"
type = "prometheus"
url = "http://prometheus:9090"
```

```sql
-- Cross-functional query: correlate business events with system metrics
SELECT b.order_id, b.status, m.cpu_usage
FROM biz.orders b
JOIN metrics.node_cpu m ON b.timestamp BETWEEN m.timestamp - INTERVAL '1 minute' AND m.timestamp
WHERE b.status = 'failed'
```

## Health and Failover

### Spoke Health Monitoring

The hub checks spoke health via their `/api/fuse/health` endpoints:

```bash
# Hub health includes spoke status
curl http://hub:9400/api/fuse/health
```

```json
{
  "status": "degraded",
  "connectors": {
    "us_east": { "status": "healthy", "latency_ms": 12 },
    "eu_west": { "status": "healthy", "latency_ms": 45 },
    "ap_northeast": { "status": "unhealthy", "error": "connection refused" }
  }
}
```

### Graceful Degradation

When a spoke is down, queries that only touch healthy spokes succeed. Queries that need the unhealthy spoke return a partial failure with results from healthy spokes and an error for the failed spoke.

```sql
-- If ap_northeast is down, this returns US + EU results with an error annotation
SELECT * FROM us_east.logs
UNION ALL SELECT * FROM eu_west.logs
UNION ALL SELECT * FROM ap_northeast.logs
```

## Capacity Planning

| Topology | Instances | Use Case |
|----------|-----------|----------|
| Single | 1 | Dev, small team (< 10 users) |
| Horizontal | 2–5 | Medium team (10–100 users) |
| Hub-spoke | 1 hub + N spokes | Multi-region, data residency |
| Tiered | Hubs + spokes | Enterprise (100+ users, 5+ regions) |

### Sizing Per Instance

| Metric | Guideline |
|--------|-----------|
| CPU | 1 core per 50 concurrent queries |
| Memory | 256MB base + 100MB per concurrent query |
| Network | Dominated by connector data transfer |
| Redis | 1GB per 10,000 cached query results |
