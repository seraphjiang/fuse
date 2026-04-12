# Fuse Grafana Datasource Plugin

Query across OpenSearch, S3, DynamoDB, PostgreSQL, and 20+ connectors from Grafana dashboards.

## Quick Start (Docker)

```bash
# 1. Build the plugin
cd grafana-plugin/fuse-datasource
npm install && npm run build

# 2. Start Grafana with the plugin pre-loaded
cd ..
docker compose -f docker-compose.grafana.yml up -d

# 3. Open Grafana (admin/admin) — Fuse datasource is auto-provisioned
open http://localhost:3000
```

Requires a running Fuse server (default `http://localhost:9400`).

## Manual Installation

1. Build: `npm install && npm run build`
2. Copy `dist/` to `$GRAFANA_HOME/plugins/fuse-datasource/`
3. Allow unsigned: set `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=fuse-datasource`
4. Restart Grafana
5. Add "Fuse" in Configuration → Data Sources

## Configuration

- **Fuse URL**: Base URL of your Fuse server (e.g., `http://localhost:9400`)
- **API Key**: Optional authentication key
- **Timeout**: Query timeout in milliseconds (default 30000)

## Usage

Select the Fuse datasource in any panel and write SQL or PPL:

```sql
SELECT service, count(*) as errors
FROM cluster_a.application_logs
WHERE status >= 500
GROUP BY service
```

`Ctrl+Enter` to execute. Results auto-convert to Grafana DataFrames with field type detection.

## Template Variables

| Query | Returns |
|-------|---------|
| `datasources()` | All registered Fuse datasources |
| `tables($datasource)` | Tables for a datasource |

## Features

- SQL and PPL query support
- Auto field type detection (time, number, string)
- Multi-query panels, template variables, cost estimate notices
- Health check via "Save & Test", API key auth, alerting support
- Sample dashboard and provisioning configs included

## Files

```
fuse-datasource/
├── src/                        # Plugin source
├── dashboards/                 # Sample dashboard JSON
├── provisioning/               # Grafana auto-provisioning configs
├── plugin.json, package.json, tsconfig.json
docker-compose.grafana.yml      # Dev environment
```
