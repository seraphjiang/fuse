# Query Lineage

Trace data flow across connectors for any query.

## Overview

Fuse extracts lineage from SQL and PPL queries — identifying source tables, transforms (JOIN, GROUP BY, filter), and the result sink. Useful for data governance, impact analysis, and understanding cross-source dependencies.

## Tracing Lineage

```bash
curl -X POST http://localhost:9400/api/fuse/lineage \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "SELECT l.service, u.name FROM cluster_a.application_logs l JOIN dynamodb.users u ON l.user_id = u.user_id WHERE l.status >= 500",
    "format": "sql"
  }'
```

### Response

```json
{
  "query": "SELECT ...",
  "nodes": [
    {"id": "n0", "label": "cluster_a.application_logs", "node_type": "source", "metadata": {"datasource": "cluster_a", "table": "application_logs"}},
    {"id": "n1", "label": "dynamodb.users", "node_type": "source", "metadata": {"datasource": "dynamodb", "table": "users"}},
    {"id": "n2", "label": "JOIN", "node_type": "transform", "metadata": {}},
    {"id": "n3", "label": "FILTER", "node_type": "transform", "metadata": {}},
    {"id": "n4", "label": "Result", "node_type": "sink", "metadata": {}}
  ],
  "edges": [
    {"from": "n0", "to": "n2"}, {"from": "n1", "to": "n2"},
    {"from": "n2", "to": "n3"}, {"from": "n3", "to": "n4"}
  ]
}
```

## Playground

Navigate to `/lineage` for a visual graph of data flow with source → transform → sink nodes.

## Node Types

- **source** — datasource table (green border)
- **transform** — JOIN, GROUP BY, FILTER, AGGREGATE (blue border)
- **sink** — query result (purple border)
