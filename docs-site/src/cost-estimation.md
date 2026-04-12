# Cost Estimation

Pre-execution dollar cost estimates per connector.

## Overview

Fuse estimates the real dollar cost of a query before execution — Athena at $5/TB scanned, DynamoDB at $/RCU, S3 at $/request. Use EXPLAIN or the Cost Explorer page to see estimates.

## Usage

### Via EXPLAIN

```bash
curl -X POST http://localhost:9400/api/fuse/query \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "EXPLAIN SELECT * FROM athena.events WHERE date > '\''2024-01-01'\''",
    "format": "sql"
  }'
```

The response includes a `cost_estimate` field with per-connector breakdown.

### Via Playground

Navigate to `/cost` for a visual cost explorer — enter a query and see estimated cost per connector with bar chart breakdown.

## Pricing Models

| Connector | Model | Unit |
|-----------|-------|------|
| Athena | $5 per TB scanned | $/TB |
| DynamoDB | Read capacity units | $/RCU |
| S3 | GET/SELECT requests | $/request |
| BigQuery | $5 per TB processed | $/TB |
| Snowflake | Credit-based | $/credit |
| Others | Row-based estimate | $/1M rows |

## Response Format

```json
{
  "cost_estimate": {
    "total_cost": 0.0234,
    "estimated_rows": 15000,
    "connectors": [
      {"id": "athena", "type": "athena", "cost": 0.02, "estimated_rows": 10000},
      {"id": "my_ddb", "type": "dynamodb", "cost": 0.0034, "estimated_rows": 5000}
    ]
  }
}
```
