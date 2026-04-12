# Webhook Subscriptions

Event-driven notifications when query results match conditions.

## Overview

Fuse webhooks deliver HTTP POST callbacks when events occur — query completion, schedule alerts, quality failures, or anomaly detection. Includes exponential backoff retry and a dead letter queue for failed deliveries.

## Creating a Webhook

```bash
curl -X POST http://localhost:9400/api/fuse/webhooks \
  -H 'Content-Type: application/json' \
  -d '{
    "url": "https://example.com/hook",
    "event": "quality_fail",
    "filter": "datasource = '\''cluster_a'\''"
  }'
```

### Via Playground

Navigate to `/webhooks` to create, test, and manage subscriptions visually.

## Event Types

| Event | Trigger |
|-------|---------|
| `query_complete` | Any query finishes execution |
| `schedule_alert` | Scheduled query result changed |
| `quality_fail` | Data quality rule failed |
| `anomaly` | Anomaly detection triggered |

## Retry Policy

Failed deliveries retry with exponential backoff (1s, 2s, 4s, 8s, 16s). After 5 attempts, the event moves to the dead letter queue.

## Dead Letter Queue

```bash
# List failed deliveries
curl http://localhost:9400/api/fuse/webhooks/dlq

# Clear DLQ
curl -X DELETE http://localhost:9400/api/fuse/webhooks/dlq
```

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/fuse/webhooks` | List subscriptions |
| POST | `/api/fuse/webhooks` | Create subscription |
| GET | `/api/fuse/webhooks/{id}` | Get subscription |
| DELETE | `/api/fuse/webhooks/{id}` | Delete subscription |
| POST | `/api/fuse/webhooks/{id}/test` | Send test event |
| GET | `/api/fuse/webhooks/dlq` | List dead letters |
| DELETE | `/api/fuse/webhooks/dlq` | Clear dead letters |
