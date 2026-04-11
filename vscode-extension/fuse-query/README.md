# Fuse Query — VS Code Extension

Query editor for the Fuse federated query engine with syntax highlighting, execution, results panel, and IntelliSense.

## Features

- **Syntax Highlighting** — SQL and PPL grammars with keywords, functions, strings, comments, variables
- **Query Execution** — Run queries with `Ctrl+Enter`, results in side panel with table view
- **Explain & Validate** — `Ctrl+Shift+E` for explain plan, `Ctrl+Shift+V` for validation
- **IntelliSense** — SQL keywords, PPL commands, datasource names auto-complete
- **Datasource Explorer** — Browse datasources → tables → fields in the sidebar
- **Query History** — Recent queries with row counts, click to re-insert
- **Results Panel** — Tabular results with row count, elapsed time, hover highlighting

## File Types

| Extension | Language |
|-----------|----------|
| `.fsql`, `.fuse` | Fuse SQL |
| `.fppl` | Fuse PPL |

## Keyboard Shortcuts

| Shortcut | Command |
|----------|---------|
| `Ctrl+Enter` | Run Query |
| `Ctrl+Shift+E` | Explain Query |
| `Ctrl+Shift+V` | Validate Query |

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `fuse.serverUrl` | `http://localhost:3000` | Fuse server URL |
| `fuse.defaultFormat` | `sql` | Default query format (`sql` or `ppl`) |

## Development

```bash
cd vscode-extension/fuse-query
npm install
npm run compile
# Press F5 in VS Code to launch Extension Development Host
```

## Packaging

```bash
npx vsce package
# Produces fuse-query-0.1.0.vsix
```
