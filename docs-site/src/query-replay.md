# Query Replay

Record production queries and replay against staging to detect regressions.

## Overview

Query replay records queries with their results, then re-executes them later to compare. Useful for validating schema changes, connector upgrades, or configuration changes.

## Recording Queries

```bash
curl -X POST http://localhost:9400/api/fuse/replay/record \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "q1",
    "query": "SELECT service, count(*) FROM cluster_a.application_logs GROUP BY service",
    "format": "sql",
    "datasources": ["cluster_a"],
    "recorded_at": 1712900000,
    "duration_ms": 45,
    "row_count": 8,
    "column_names": ["service", "count"],
    "result_hash": "abc123"
  }'
```

## Listing Recordings

```bash
curl http://localhost:9400/api/fuse/replay/recordings
```

## Replaying

The `/replay` playground page provides a "Replay All" button that re-executes each recorded query and compares results:

- **Match** — same row count and result hash
- **Diff** — results changed, with diff details

## Clearing Recordings

```bash
curl -X DELETE http://localhost:9400/api/fuse/replay/recordings
```

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/fuse/replay/recordings` | List recordings |
| POST | `/api/fuse/replay/record` | Record a query |
| DELETE | `/api/fuse/replay/recordings` | Clear all |
