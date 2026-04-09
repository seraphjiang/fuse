# Writing a Fuse Connector

This guide walks through implementing a new `FederatedConnector` for the Fuse
query engine. By the end you'll have a connector crate that plugs into the
registry, appears in the REST API, and participates in federated queries.

The OpenSearch connector (`crates/fuse-connectors/opensearch/`) is the reference
implementation — this guide follows the same patterns.

## 1. Create the Crate

```bash
mkdir -p crates/fuse-connectors/mydb
cd crates/fuse-connectors/mydb
cargo init --lib
```

Add it to the workspace in the root `Cargo.toml`:

```toml
members = [
    # ...
    "crates/fuse-connectors/mydb",
]
```

Minimum dependencies in your crate's `Cargo.toml`:

```toml
[dependencies]
fuse-core = { path = "../../fuse-core" }
arrow = { workspace = true }
async-trait = { workspace = true }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
```

## 2. Implement `FederatedConnector`

The core trait lives in `fuse_core::connector`. Every method must be
implemented except `table_names()` and `get_table_schema()`, which have
default implementations that delegate to `discover_schemas()` and
`get_schema()` respectively.

```rust
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::sync::mpsc;

use fuse_core::connector::*;
use fuse_core::error::ConnectorError;

#[derive(Debug)]  // Required — the trait has a Debug bound
pub struct MyDbConnector {
    id: String,
    // your client, connection pool, etc.
}

#[async_trait]
impl FederatedConnector for MyDbConnector {
    fn id(&self) -> &str { &self.id }

    fn connector_type(&self) -> &str { "mydb" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_aggregation: false,
            supports_sorting: true,
            supports_limit: true,
            supports_join: false,
            max_concurrent_queries: 8,
            supports_streaming: false,
            latency_class: LatencyClass::Medium,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        // Ping your datasource
        ConnectorHealth {
            status: HealthStatus::Healthy,
            latency_ms: Some(start.elapsed().as_millis() as u64),
            message: None,
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        // List tables/indices/collections
        Ok(vec![SchemaInfo {
            name: "my_table".into(),
            schema_type: SchemaType::Table,
            estimated_row_count: Some(1000),
        }])
    }

    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError> {
        // Return Arrow schema for the table
        Ok(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    async fn execute(
        &self,
        query: &SubQuery,
    ) -> Result<Vec<RecordBatch>, ConnectorError> {
        // Translate SubQuery → native query, execute, return Arrow batches
        todo!("implement query execution")
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        // Stream results in batches via the channel.
        // If streaming isn't supported, fall back to execute():
        let batches = self.execute(query).await?;
        for batch in batches {
            tx.send(Ok(batch))
                .await
                .map_err(|_| ConnectorError::ChannelClosed)?;
        }
        Ok(())
    }
}
```

### Key types in `SubQuery`

The engine translates SQL/PPL into a `SubQuery` that your connector receives:

| Field | Type | Description |
|-------|------|-------------|
| `table` | `String` | Table/index name |
| `projections` | `Vec<String>` | Column names to return (empty = all) |
| `filter` | `Option<FilterExpr>` | Push-down filter tree |
| `aggregations` | `Vec<AggregationExpr>` | COUNT, SUM, AVG, MIN, MAX |
| `group_by` | `Vec<String>` | Group-by columns |
| `sort` | `Vec<SortExpr>` | Sort expressions |
| `limit` | `Option<u64>` | Row limit |
| `passthrough` | `Option<String>` | Raw native query (bypass translation) |

Your connector should translate whichever fields it supports into native
queries. Unsupported operations are handled by DataFusion post-execution —
just set the corresponding `supports_*` capability to `false`.

### Error handling

Use `ConnectorError` variants:

```rust
ConnectorError::QueryFailed("timeout".into())     // query execution errors
ConnectorError::SchemaDiscovery("...".into())      // schema/metadata errors
ConnectorError::Connection("refused".into())       // connectivity errors
ConnectorError::Auth("invalid token".into())       // auth errors
ConnectorError::ChannelClosed                      // streaming channel dropped
ConnectorError::Unsupported("joins".into())        // unsupported operations

// Convenience constructors (accept any Display type):
ConnectorError::query(some_error)
ConnectorError::schema(some_error)
```

## 3. Implement `ConnectorFactory`

The factory creates connector instances from `fuse.toml` config entries:

```rust
use fuse_core::config::ConnectorConfig;
use fuse_core::registry::ConnectorFactory;

pub struct MyDbConnectorFactory;

impl ConnectorFactory for MyDbConnectorFactory {
    fn connector_type(&self) -> &str {
        "mydb"
    }

    fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        // config.id — the connector ID from fuse.toml
        // config.connector_type — "mydb"
        // config.properties — HashMap<String, toml::Value> with all other fields
        let url = config.properties.get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("missing 'url'".into()))?;

        let connector = MyDbConnector {
            id: config.id.clone(),
            // build client from url, auth, etc.
        };
        Ok(Arc::new(connector))
    }
}
```

## 4. Register the Factory

In `crates/fuse-server/src/main.rs`, add your factory to the list:

```rust
use fuse_connector_mydb::MyDbConnectorFactory;

let factories: Vec<Box<dyn ConnectorFactory>> = vec![
    Box::new(OpenSearchConnectorFactory),
    Box::new(MyDbConnectorFactory),  // ← add here
];
```

That's it. The server loop matches `connector_type` from each `[[connector]]`
entry in `fuse.toml` to the factory and calls `create()`.

## 5. Add Config

Add a `[[connector]]` block to `fuse.toml`:

```toml
[[connector]]
id = "my_source"
type = "mydb"
url = "http://localhost:5432"
database = "analytics"

[connector.auth]
type = "basic"
username = "admin"
password_env = "MYDB_PASSWORD"
```

All fields beyond `id` and `type` land in `config.properties` as
`HashMap<String, toml::Value>`. Parse them in your factory's `create()`.

## 6. Test

Follow the pattern in `crates/fuse-engine/tests/federation_test.rs`:

```rust
#[derive(Debug)]
struct MockMyDbConnector { /* ... */ }

#[async_trait]
impl FederatedConnector for MockMyDbConnector {
    // Return hardcoded data
}

#[tokio::test]
async fn test_mydb_connector() {
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(MockMyDbConnector::new("test_mydb"))).unwrap();

    assert!(registry.get("test_mydb").is_some());

    let connector = registry.get("test_mydb").unwrap();
    let schemas = connector.discover_schemas().await.unwrap();
    assert!(!schemas.is_empty());

    let batches = connector.execute(&SubQuery {
        table: "my_table".into(),
        projections: vec![],
        filter: None,
        aggregations: vec![],
        group_by: vec![],
        sort: vec![],
        limit: Some(10),
        passthrough: None,
    }).await.unwrap();
    assert!(!batches.is_empty());
}
```

## 7. Checklist

Before submitting your connector:

- [ ] Crate added to workspace `Cargo.toml`
- [ ] `#[derive(Debug)]` on your connector struct
- [ ] All 8 trait methods implemented (or using defaults for `table_names`/`get_table_schema`)
- [ ] `ConnectorCapabilities` accurately reflects what you push down
- [ ] `ConnectorFactory` registered in `main.rs`
- [ ] `health_check()` actually pings the datasource
- [ ] `discover_schemas()` skips internal/system tables
- [ ] `get_schema()` returns accurate Arrow types (not all Utf8)
- [ ] `execute()` respects `projections`, `filter`, `limit` when supported
- [ ] `execute_streaming()` sends batches via `tx` and handles `ChannelClosed`
- [ ] Error types are specific (`Connection` vs `Auth` vs `QueryFailed`)
- [ ] Integration test with mock connector
- [ ] Sample `[[connector]]` block documented
- [ ] `cargo check` and `cargo test` pass

## Reference

| File | Purpose |
|------|---------|
| `crates/fuse-core/src/connector.rs` | `FederatedConnector` trait, `SubQuery`, `FilterExpr`, capabilities |
| `crates/fuse-core/src/error.rs` | `ConnectorError` variants |
| `crates/fuse-core/src/config.rs` | `ConnectorConfig` (id, type, properties) |
| `crates/fuse-core/src/registry.rs` | `ConnectorRegistry`, `ConnectorFactory` trait |
| `crates/fuse-connectors/opensearch/` | Reference implementation |
| `crates/fuse-engine/tests/federation_test.rs` | Test patterns |
| `docs/api/openapi.yaml` | REST API spec (your connector appears automatically) |
