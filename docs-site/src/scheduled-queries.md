# Scheduled Queries

Run queries on a cron schedule with automatic result storage and alerting.

## Overview

Fuse supports cron-based query scheduling. Define a query, set a schedule, and Fuse executes it automatically — storing results and optionally alerting when results change.

## Creating a Schedule

### Via API

```bash
curl -X POST http://localhost:9400/api/fuse/schedules \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "daily-error-report",
    "query": "SELECT service, count(*) as errors FROM cluster_a.application_logs WHERE status >= 500 GROUP BY service",
    "format": "sql",
    "cron": "0 */6 * * *",
    "alert_on": "row_count"
  }'
```

### Via Playground

Navigate to `/schedules` in the playground UI to create, pause, resume, and delete schedules visually.

## Cron Expressions

Standard 5-field cron: `minute hour day-of-month month day-of-week`

| Expression | Meaning |
|-----------|---------|
| `*/5 * * * *` | Every 5 minutes |
| `0 */6 * * *` | Every 6 hours |
| `0 9 * * 1-5` | 9 AM weekdays |
| `0 0 * * *` | Daily at midnight |

## Alert Modes

- `none` — no alerting (default)
- `row_count` — alert when row count changes from previous run
- `any` — alert when any result value changes

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/fuse/schedules` | List all schedules |
| POST | `/api/fuse/schedules` | Create a schedule |
| POST | `/api/fuse/schedules/{id}/toggle` | Pause/resume |
| DELETE | `/api/fuse/schedules/{id}` | Delete a schedule |
