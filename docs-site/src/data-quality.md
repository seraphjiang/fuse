# Data Quality Rules

Define expectations per datasource and validate data quality automatically.

## Overview

Fuse's data quality engine lets you define rules (null rate, freshness, row count, cardinality) per table and evaluate them on demand or on a schedule.

## Rule Types

| Type | Description | Threshold |
|------|-------------|-----------|
| `null_rate` | Max % of null values in a column | Percentage (e.g., 5) |
| `freshness` | Max minutes since last data update | Minutes (e.g., 60) |
| `row_count_min` | Minimum expected row count | Count (e.g., 1000) |
| `cardinality_min` | Minimum distinct values in a column | Count (e.g., 10) |
| `custom_sql` | Custom SQL that must return 0 for pass | 0 = pass |

## Creating Rules

### Via API

```bash
curl -X POST http://localhost:9400/api/fuse/quality/rules \
  -H 'Content-Type: application/json' \
  -d '{
    "datasource": "cluster_a",
    "table": "application_logs",
    "rule_type": "null_rate",
    "column": "trace_id",
    "threshold": 5
  }'
```

### Via Playground

Navigate to `/quality` to create rules, view pass/warn/fail summary, and run all checks.

## Evaluating Rules

```bash
# Run all checks
curl -X POST http://localhost:9400/api/fuse/quality/evaluate
```

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/fuse/quality/rules` | List all rules |
| POST | `/api/fuse/quality/rules` | Create a rule |
| DELETE | `/api/fuse/quality/rules/{id}` | Delete a rule |
| POST | `/api/fuse/quality/evaluate` | Run all checks |
