# Horizontal Scaling Guide

Run multiple Fuse instances behind a load balancer with shared state in Redis.

## Architecture

```
                    ┌──────────────┐
                    │  nginx / ALB │
                    └──────┬───────┘
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │  Fuse 1  │ │  Fuse 2  │ │  Fuse 3  │
        └────┬─────┘ └────┬─────┘ └────┬─────┘
             │             │             │
             └─────────────┼─────────────┘
                           ▼
                    ┌──────────────┐
                    │    Redis     │  ← cache, tenants, sessions
                    └──────────────┘
                           │
         ┌─────────┬───────┼───────┬──────────┐
         ▼         ▼       ▼       ▼          ▼
     OpenSearch  DynamoDB  S3   PostgreSQL  Prometheus
```

Each Fuse instance is stateless — query cache, tenant registry, and session data live in Redis. Any instance can handle any request.

## Configuration

### Enable Stateless Mode

```toml
[engine]
mode = "stateless"

[redis]
url = "redis://redis:6379"
cache_ttl_secs = 300       # query result cache TTL
tenant_reload_secs = 30    # how often to reload tenant config from Redis
```

When `mode = "stateless"`:
- Query result cache uses Redis instead of in-memory LRU
- Tenant registry loads from Redis (hot-reloadable)
- Materialized view results stored in Redis
- Health checks include Redis connectivity

### Tenant Registry in Redis

Store tenant configs as JSON in Redis:

```bash
# Add a tenant
redis-cli SET "fuse:tenant:team-alpha" '{"datasources":["cluster_a","s3_o11y"],"max_rows":10000,"max_time_ms":30000}'

# Add admin tenant (empty datasources = all access)
redis-cli SET "fuse:tenant:ops-team" '{"datasources":[]}'

# Remove a tenant (takes effect within tenant_reload_secs)
redis-cli DEL "fuse:tenant:team-alpha"
```

Changes propagate to all instances within `tenant_reload_secs` — no restart needed.

## Docker Compose (Multi-Instance)

```yaml
version: "3.8"

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  fuse-1:
    build: .
    environment:
      - FUSE_CONFIG=/etc/fuse/fuse.toml
      - RUST_LOG=info
    volumes:
      - ./fuse.stateless.toml:/etc/fuse/fuse.toml:ro
    depends_on:
      redis: { condition: service_healthy }

  fuse-2:
    build: .
    environment:
      - FUSE_CONFIG=/etc/fuse/fuse.toml
      - RUST_LOG=info
    volumes:
      - ./fuse.stateless.toml:/etc/fuse/fuse.toml:ro
    depends_on:
      redis: { condition: service_healthy }

  nginx:
    image: nginx:alpine
    ports:
      - "9400:9400"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - fuse-1
      - fuse-2

volumes:
  os-data:
```

### nginx.conf

```nginx
events { worker_connections 1024; }

http {
    upstream fuse {
        least_conn;
        server fuse-1:9400;
        server fuse-2:9400;
    }

    server {
        listen 9400;

        location / {
            proxy_pass http://fuse;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        }

        location /api/fuse/health {
            proxy_pass http://fuse;
            proxy_connect_timeout 2s;
        }
    }
}
```

### fuse.stateless.toml

```toml
[engine]
mode = "stateless"
rate_limit_global = 5000
rate_limit_per_ip = 500

[redis]
url = "redis://redis:6379"
cache_ttl_secs = 300
tenant_reload_secs = 30

[[datasource]]
id = "cluster_a"
type = "opensearch"
url = "https://opensearch:9200"

# ... other datasources
```

## Scaling

### Add Instances

```bash
docker compose up -d --scale fuse=4
```

Update nginx upstream to include new instances, or use Docker's built-in DNS with `server fuse:9400 resolve`.

### Health Checks

Each instance exposes `/api/fuse/health`. The load balancer should health-check each instance independently:

```bash
# Check individual instance
curl http://fuse-1:9400/api/fuse/health

# Check through load balancer
curl http://localhost:9400/api/fuse/health
```

### Monitoring

All instances emit the same Prometheus metrics. Use instance labels to distinguish:

```
fuse_queries_total{instance="fuse-1:9400"}
fuse_queries_total{instance="fuse-2:9400"}
```

Aggregate across instances:

```promql
sum(rate(fuse_queries_total[5m]))                    # total QPS
max(fuse_active_queries) by (instance)               # per-instance load
histogram_quantile(0.99, sum(rate(fuse_query_duration_ms[5m])) by (le))  # global p99
```

## Materialized Views in Stateless Mode

Materialized view results are stored in Redis. Only one instance refreshes each view (leader election via Redis `SET NX`). All instances serve the cached result.

```toml
[[view]]
name = "error_summary"
query = "SELECT service, count(*) as errors FROM cluster_a.logs WHERE status >= 500 GROUP BY service"
refresh_secs = 300
max_age_secs = 600
```

Check view status across all instances:

```bash
curl http://localhost:9400/api/fuse/views
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Cache misses on every request | Redis not connected | Check `[redis]` config, verify `redis-cli ping` |
| Tenant changes not propagating | Reload interval too high | Lower `tenant_reload_secs` |
| Uneven load distribution | nginx round-robin with slow queries | Switch to `least_conn` |
| Stale materialized views | Refresh leader crashed | Views auto-recover on next `refresh_secs` tick |
| High Redis memory | Large query results cached | Lower `cache_ttl_secs`, add `maxmemory` to Redis |
