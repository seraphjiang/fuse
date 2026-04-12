# OpenTelemetry Collector Mode

Fuse can act as an OpenTelemetry backend — ingest OTLP traces, metrics, and logs, then query them with SQL.

## Overview

When enabled, Fuse exposes OTLP-compatible ingest endpoints. Point your OTel SDK or Collector at Fuse and query the telemetry data using standard SQL.

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/traces` | Ingest OTLP trace spans |
| POST | `/v1/metrics` | Ingest OTLP metrics |
| POST | `/v1/logs` | Ingest OTLP log records |
| GET | `/v1/health` | Collector health + ingest counts |

## Configuration

```toml
[otel]
enabled = true
max_spans = 100000
max_metrics = 100000
max_logs = 100000
```

## Querying Ingested Data

Once ingested, telemetry is queryable as virtual tables:

```sql
-- Find slow spans
SELECT trace_id, span_name, duration_ms, service_name
FROM otel.traces
WHERE duration_ms > 1000
ORDER BY duration_ms DESC LIMIT 20

-- Error rate by service
SELECT service_name, count(*) as errors
FROM otel.traces
WHERE status_code = 'ERROR'
GROUP BY service_name

-- Log analysis
SELECT severity, body, resource_attributes
FROM otel.logs
WHERE severity >= 'ERROR'
ORDER BY timestamp DESC LIMIT 50
```

## Monitoring

The Status page (`/status`) shows an OTel Collector widget with ingest counts for traces, metrics, and logs, auto-refreshing every 5 seconds.

## SDK Configuration

Point any OTLP exporter at Fuse:

```yaml
# OpenTelemetry Collector config
exporters:
  otlphttp:
    endpoint: http://localhost:9400/v1
```
