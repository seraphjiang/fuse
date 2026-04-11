# Plugin Development Guide

Build custom connectors as WASM plugins. Plugins run in a sandboxed WebAssembly runtime (wasmtime) and implement the same `FederatedConnector` interface as built-in connectors.

## Overview

```
plugins/
├── my-connector/
│   ├── manifest.toml       ← plugin metadata
│   └── my_connector.wasm   ← compiled WASM module
```

Fuse scans the `plugins/` directory on startup, loads each `.wasm` module, and registers it as a connector type. Queries can then reference datasources backed by your plugin.

## Prerequisites

- Rust toolchain with `wasm32-wasi` target: `rustup target add wasm32-wasi`
- Fuse plugin SDK: `fuse-plugin-sdk` crate (provides the trait and types)

## Quick Start

### 1. Create the Plugin

```bash
cargo new --lib my-connector
cd my-connector
```

`Cargo.toml`:

```toml
[package]
name = "my-connector"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
fuse-plugin-sdk = { path = "../path/to/fuse/sdk/plugin" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

### 2. Implement the Connector

`src/lib.rs`:

```rust
use fuse_plugin_sdk::*;

struct MyConnector {
    base_url: String,
}

impl FederatedConnectorPlugin for MyConnector {
    fn init(config: &ConnectorConfig) -> Result<Self, PluginError> {
        let base_url = config.get("url")
            .ok_or(PluginError::Config("missing 'url'".into()))?;
        Ok(Self { base_url: base_url.to_string() })
    }

    fn connector_type(&self) -> &str {
        "my-connector"
    }

    fn schema(&self, table: &str) -> Result<Schema, PluginError> {
        // Return Arrow schema for the given table
        Ok(Schema {
            fields: vec![
                Field { name: "id".into(), data_type: DataType::Utf8, nullable: false },
                Field { name: "value".into(), data_type: DataType::Float64, nullable: true },
            ],
        })
    }

    fn execute(&self, query: &SubQuery) -> Result<RecordBatches, PluginError> {
        // Translate SubQuery filters/projections into your datasource's native query
        let url = format!("{}/query?table={}", self.base_url, query.table);

        // Apply filter pushdown
        let native_filter = translate_filters(&query.filters);

        // Execute and return Arrow record batches
        let response = http_get(&format!("{}&filter={}", url, native_filter))?;
        parse_to_batches(response)
    }

    fn pushdown_capabilities(&self) -> PushdownCapabilities {
        PushdownCapabilities {
            filter: true,
            projection: true,
            limit: true,
            sort: false,
            aggregation: false,
        }
    }
}

// Register the plugin (required macro)
export_plugin!(MyConnector);
```

### 3. Build

```bash
cargo build --target wasm32-wasi --release
```

Output: `target/wasm32-wasi/release/my_connector.wasm`

### 4. Write the Manifest

`manifest.toml`:

```toml
[plugin]
name = "my-connector"
version = "0.1.0"
description = "Custom connector for MyDataSource"
connector_type = "my-connector"
wasm = "my_connector.wasm"

[plugin.config_schema]
required = ["url"]
optional = ["timeout_ms", "max_retries"]
```

### 5. Install

```bash
mkdir -p plugins/my-connector
cp target/wasm32-wasi/release/my_connector.wasm plugins/my-connector/
cp manifest.toml plugins/my-connector/
```

### 6. Configure a Datasource

In `fuse.toml`:

```toml
[[datasource]]
id = "my_data"
type = "my-connector"    # matches connector_type in manifest
url = "https://my-datasource.example.com"
timeout_ms = 5000
```

### 7. Query

```sql
SELECT id, value FROM my_data.measurements WHERE value > 100 LIMIT 10
```

## Plugin SDK Types

### SubQuery

The query plan fragment sent to your connector:

```rust
pub struct SubQuery {
    pub table: String,                    // table name within datasource
    pub projections: Vec<String>,         // columns to return
    pub filters: Vec<Filter>,            // WHERE conditions (pushdown)
    pub limit: Option<u64>,              // LIMIT value
    pub sort: Option<Vec<SortExpr>>,     // ORDER BY
}
```

### Filter

```rust
pub enum Filter {
    Eq(String, Value),          // column = value
    Gt(String, Value),          // column > value
    Lt(String, Value),          // column < value
    Gte(String, Value),         // column >= value
    Lte(String, Value),         // column <= value
    Like(String, String),       // column LIKE pattern
    In(String, Vec<Value>),     // column IN (values)
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}
```

### PushdownCapabilities

Declare what your connector can handle natively:

```rust
pub struct PushdownCapabilities {
    pub filter: bool,       // WHERE pushdown
    pub projection: bool,   // SELECT column pushdown
    pub limit: bool,        // LIMIT pushdown
    pub sort: bool,         // ORDER BY pushdown
    pub aggregation: bool,  // GROUP BY pushdown
}
```

Capabilities you don't support are handled by Fuse's in-memory engine after fetching raw data.

## Testing

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init() {
        let config = ConnectorConfig::from([("url", "http://localhost:8080")]);
        let connector = MyConnector::init(&config).unwrap();
        assert_eq!(connector.connector_type(), "my-connector");
    }

    #[test]
    fn test_schema() {
        let config = ConnectorConfig::from([("url", "http://localhost:8080")]);
        let connector = MyConnector::init(&config).unwrap();
        let schema = connector.schema("measurements").unwrap();
        assert_eq!(schema.fields.len(), 2);
    }
}
```

### Integration Test with Fuse

```bash
# Start Fuse with your plugin
./target/release/fuse-server --config fuse.toml

# Verify plugin loaded
curl http://localhost:9400/api/fuse/datasources | jq '.[] | select(.type == "my-connector")'

# Run a query
curl -X POST http://localhost:9400/api/fuse/query \
  -d '{"query": "SELECT * FROM my_data.measurements LIMIT 5"}'
```

## Plugin Management API

```bash
# List loaded plugins
curl http://localhost:9400/api/fuse/plugins

# Plugin details
curl http://localhost:9400/api/fuse/plugins/my-connector
```

## Limitations

- WASM plugins run in a sandbox — no filesystem or network access except through the SDK's `http_get`/`http_post` helpers
- Memory limit: 256MB per plugin instance (configurable in `manifest.toml`)
- No async — plugin `execute()` is synchronous from the plugin's perspective (Fuse runs it on a blocking thread)
- Arrow schema must be declared upfront in `schema()` — dynamic schemas not yet supported
