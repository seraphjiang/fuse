# GraphQL API

Query Fuse with GraphQL — schema introspection, query execution, and saved queries.

## Overview

Fuse exposes a GraphQL endpoint at `/api/fuse/graphql` supporting queries, introspection, and subscriptions. Use the built-in GraphQL Playground at `/graphql` or any GraphQL client.

## Endpoint

```
POST /api/fuse/graphql
```

## Examples

### Execute a SQL query

```graphql
{
  query(sql: "SELECT service, count(*) FROM cluster_a.application_logs GROUP BY service") {
    columns
    rows
    row_count
    duration_ms
  }
}
```

### List datasources

```graphql
{
  datasources {
    id
    type
    tables {
      name
      row_count
    }
  }
}
```

### Health check

```graphql
{
  health {
    status
    uptime_secs
    connectors {
      id
      status
      latency_ms
    }
  }
}
```

### Schema introspection

```graphql
{
  __schema {
    types {
      name
      kind
      fields {
        name
        type { name kind }
      }
    }
  }
}
```

## Playground

Navigate to `/graphql` for a dedicated GraphQL IDE with query editor, variables pane, snippet templates, and prettify.
