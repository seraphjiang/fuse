# Adaptive Caching

Fuse automatically caches repeat query patterns with per-datasource TTLs.

## How It Works

1. Fuse tracks query frequency (normalized query fingerprints)
2. Queries exceeding the promotion threshold are auto-cached
3. Each datasource can have a different cache TTL
4. Cache stats are visible on the Status page (`/status`)

## Configuration

```toml
[cache]
enabled = true
default_ttl_secs = 300
promotion_threshold = 3    # cache after 3 executions
max_tracked = 10000        # max query fingerprints to track

[cache.datasource_ttls]
cluster_a = 60             # volatile data: 1 min
dynamodb = 600             # stable data: 10 min
s3_o11y = 3600             # archival: 1 hour
```

## Monitoring

The `/api/fuse/stats` endpoint includes adaptive cache metrics:

```json
{
  "cache_hit_rate": 0.42,
  "adaptive_cache": {
    "tracked_queries": 156,
    "hot_queries": 12,
    "promotion_threshold": 3,
    "datasource_ttls": {
      "cluster_a": 60,
      "dynamodb": 600
    }
  }
}
```

These are also displayed on the Status page in the playground.
