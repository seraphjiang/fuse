# Go SDK

Fuse provides a Go client library using only the standard library (`net/http`, `encoding/json`).

## Install

Copy `sdks/go/fuse.go` into your project.

## Usage

```go
client := fuse.NewClient("http://localhost:9400", "")

// Query
result, err := client.Query("SELECT * FROM cluster_a.logs LIMIT 10", "sql")

// With cursor pagination
result, err := client.QueryWithCursor("SELECT * FROM logs ORDER BY ts", "sql", 20, "")

// EXPLAIN
plan, err := client.Explain("SELECT * FROM logs WHERE status >= 500", "sql")

// Health
health, err := client.Health()

// Datasources
ds, err := client.Datasources()
```

## Authentication

Pass an API key as the second argument to `NewClient`:

```go
client := fuse.NewClient("http://localhost:9400", "my-api-key")
```

## API Reference

| Method | Description |
|--------|-------------|
| `Query(query, format)` | Execute SQL or PPL query |
| `QueryWithCursor(query, format, pageSize, cursor)` | Paginated query |
| `Explain(query, format)` | Get query plan |
| `Health()` | Server health + connector status |
| `Datasources()` | List configured datasources |
| `ToDicts(result)` | Convert result to `[]map[string]interface{}` |
