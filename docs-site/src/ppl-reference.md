# PPL Reference

Piped Processing Language (PPL) uses a pipe syntax: `source = datasource.table | command | command`.

## Basic Syntax

```
source = cluster_a.application_logs | where status >= 500 | head 10
```

## source

Specify one or more datasources:

```
source = cluster_a.application_logs
```

Multi-source (cross-cluster):

```
source = cluster_a.application_logs, cluster_b.application_logs
```

## where

Filter rows:

```
source = cluster_a.application_logs
| where status >= 500
| where service = 'api-gateway'
| head 20
```

## stats

Aggregate with optional grouping:

```
source = cluster_a.application_logs
| stats count() as total by service
| sort - total
```

Functions: `count()`, `sum()`, `avg()`, `min()`, `max()`.

## sort

Sort results. Use `-` for descending:

```
source = cluster_a.application_logs
| sort - response_time_ms
| head 10
```

## head

Limit output rows:

```
source = cluster_a.application_logs | head 5
```

## fields

Select specific fields:

```
source = cluster_a.application_logs
| fields service, status, message
| head 10
```

## dedup

Remove duplicate rows by field:

```
source = cluster_a.application_logs
| dedup trace_id
| head 10
```

## eval

Create computed fields:

```
source = cluster_a.application_logs
| eval is_error = status >= 500
| where is_error = true
| head 10
```

## Multi-Source Queries

Query across clusters in a single PPL statement:

```
source = cluster_a.application_logs, cluster_b.application_logs
| where status >= 500
| stats count() as errors by service
| sort - errors
| head 10
```

This fans out to both clusters, merges results, then applies the pipeline.

## Tips

- PPL commands are applied in order (left to right through pipes)
- `where` pushes down to connectors when possible
- Use `head` to limit results early
- Multi-source queries add a `_datasource` column — see [Data Provenance](./data-provenance.md)
