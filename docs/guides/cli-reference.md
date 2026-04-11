# CLI Reference

The `fuse` CLI provides server management, query execution, and administration from the terminal.

## Usage

```
fuse [COMMAND] [OPTIONS]
```

## Commands

### `fuse serve`

Start the Fuse server.

```bash
fuse serve [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config`, `-c` | `fuse.toml` | Config file path |
| `--bind`, `-b` | `0.0.0.0:9400` | Listen address (overrides config) |
| `--log-format` | `text` | Log format: `text` or `json` |
| `--log-level` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |

```bash
# Start with defaults
fuse serve

# Custom config and JSON logging
fuse serve -c /etc/fuse/fuse.toml --log-format json

# Override bind address
fuse serve -b 127.0.0.1:9400
```

Environment variables:
- `FUSE_CONFIG` — config file path (same as `--config`)
- `FUSE_LOG_FORMAT` — log format (same as `--log-format`)
- `RUST_LOG` — fine-grained log filtering (e.g., `info,fuse_server=debug`)

### `fuse query`

Execute a query against a running Fuse server.

```bash
fuse query [OPTIONS] <QUERY>
```

| Flag | Default | Description |
|------|---------|-------------|
| `--server`, `-s` | `http://localhost:9400` | Fuse server URL |
| `--api-key`, `-k` | (none) | API key |
| `--format`, `-f` | `sql` | Query format: `sql` or `ppl` |
| `--output`, `-o` | `table` | Output format: `table`, `json`, `csv` |
| `--limit`, `-l` | (none) | Row limit |
| `--timeout`, `-t` | `30s` | Query timeout |
| `--all` | false | Fetch all pages (cursor pagination) |

```bash
# Simple query
fuse query "SELECT service, count(*) FROM cluster_a.logs GROUP BY service"

# PPL query with JSON output
fuse query -f ppl -o json "source = cluster_a.logs | stats count() by service"

# CSV export to file
fuse query -o csv "SELECT * FROM cluster_a.logs LIMIT 1000" > logs.csv

# Fetch all pages
fuse query --all "SELECT * FROM cluster_a.logs ORDER BY timestamp DESC"

# Against remote server with auth
fuse query -s https://fuse.internal:9400 -k my-api-key "SELECT 1"
```

### `fuse explain`

Show the query execution plan.

```bash
fuse explain [OPTIONS] <QUERY>
```

| Flag | Default | Description |
|------|---------|-------------|
| `--server`, `-s` | `http://localhost:9400` | Fuse server URL |
| `--api-key`, `-k` | (none) | API key |
| `--analyze` | false | Execute and show actual timing |

```bash
# Plan only
fuse explain "SELECT * FROM cluster_a.logs WHERE status >= 500"

# With actual execution stats
fuse explain --analyze "SELECT * FROM cluster_a.logs WHERE status >= 500"
```

### `fuse health`

Check server and connector health.

```bash
fuse health [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--server`, `-s` | `http://localhost:9400` | Fuse server URL |
| `--json` | false | Output as JSON |

```bash
fuse health
# Output:
# ✅ cluster_a (opensearch) — 12ms
# ✅ ddb (dynamodb) — 8ms
# ❌ pg (postgres) — connection refused
# Status: degraded (2/3 healthy)

fuse health --json
```

### `fuse datasources`

List configured datasources.

```bash
fuse datasources [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--server`, `-s` | `http://localhost:9400` | Fuse server URL |
| `--api-key`, `-k` | (none) | API key |

```bash
fuse datasources
# Output:
# ID            TYPE          TABLES
# cluster_a     opensearch    application_logs, access_logs
# ddb           dynamodb      users, orders
# metrics       prometheus    node_cpu, node_memory
```

### `fuse validate`

Check if a query is syntactically valid without executing it.

```bash
fuse validate [OPTIONS] <QUERY>
```

```bash
fuse validate "SELECT * FROM cluster_a.logs"
# ✅ Valid

fuse validate "SELEC * FORM logs"
# ❌ Invalid: Expected SELECT, found SELEC at position 0
```

### `fuse history`

Show recent query history.

```bash
fuse history [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--server`, `-s` | `http://localhost:9400` | Fuse server URL |
| `--api-key`, `-k` | (none) | API key |
| `--limit`, `-l` | `20` | Number of entries |

```bash
fuse history
# ID    TIME                 DURATION  ROWS   QUERY
# q-42  2026-04-11 01:30:00  45ms      1250   SELECT service, count(*)...
# q-41  2026-04-11 01:29:55  12ms      20     SELECT * FROM ddb.users...
```

### `fuse views`

Manage materialized views.

```bash
fuse views [SUBCOMMAND]
```

| Subcommand | Description |
|-----------|-------------|
| `list` | List all materialized views with status |
| `refresh <NAME>` | Trigger immediate refresh |
| `status <NAME>` | Show view details (last refresh, row count, staleness) |

```bash
fuse views list
# NAME            STATUS   LAST REFRESH         ROWS   REFRESH INTERVAL
# error_summary   Fresh    2026-04-11 01:25:00  150    5m
# daily_stats     Stale    2026-04-11 00:00:00  30     1h

fuse views refresh error_summary
# ✅ Refreshed error_summary (150 rows, 45ms)
```

### `fuse config`

Validate and inspect configuration.

```bash
fuse config [SUBCOMMAND]
```

| Subcommand | Description |
|-----------|-------------|
| `check` | Validate config file syntax |
| `show` | Print resolved config (with env vars expanded) |

```bash
fuse config check -c fuse.toml
# ✅ Config valid: 5 datasources, 3 tenants, 2 API keys

fuse config show -c fuse.toml
# [engine]
# bind = "0.0.0.0:9400"
# ...
```

## Global Options

These flags work with all commands:

| Flag | Description |
|------|-------------|
| `--help`, `-h` | Show help |
| `--version`, `-V` | Show version |
| `--quiet`, `-q` | Suppress non-essential output |
| `--verbose`, `-v` | Increase verbosity (repeat for more: `-vv`) |

## Shell Completion

```bash
# Bash
fuse completions bash > /etc/bash_completion.d/fuse

# Zsh
fuse completions zsh > ~/.zfunc/_fuse

# Fish
fuse completions fish > ~/.config/fish/completions/fuse.fish
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid arguments |
| 3 | Connection error (server unreachable) |
| 4 | Authentication error (invalid API key) |
| 5 | Query error (syntax or execution failure) |
