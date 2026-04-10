# Fuse Grafana Datasource Plugin

Use Fuse as a datasource in Grafana to query across OpenSearch, S3, DynamoDB, PostgreSQL, and 10+ connectors from your Grafana dashboards.

## Installation

1. Copy `fuse-datasource/` to your Grafana plugins directory
2. Restart Grafana
3. Add "Fuse" as a datasource in Configuration → Data Sources

## Configuration

- **Fuse URL**: Base URL of your Fuse server (e.g., `http://localhost:3000`)
- **API Key**: Optional authentication key
- **Timeout**: Default query timeout in milliseconds

## Usage

In any Grafana panel, select the Fuse datasource and write SQL or PPL queries:

```sql
SELECT service, count(*) as errors
FROM cluster_a.application_logs
WHERE status >= 500
GROUP BY service
ORDER BY errors DESC
```

Press `Ctrl+Enter` to execute. Results are automatically converted to Grafana DataFrames with proper field type detection (time, number, string).

## Features

- SQL and PPL query support
- Auto field type detection (timestamps, numbers, strings)
- Multi-query panels (multiple targets)
- Health check via "Save & Test"
- API key authentication
- Alerting support
