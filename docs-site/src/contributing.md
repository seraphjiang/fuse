# Contributing

We welcome contributions! Fuse is Apache-2.0 licensed.

## Quick Links

- [CONTRIBUTING.md](https://github.com/seraphjiang/fuse/blob/main/CONTRIBUTING.md) — Full contributing guide
- [Issue Templates](https://github.com/seraphjiang/fuse/issues/new/choose) — Bug reports, feature requests, connector requests
- [RFC-001](https://github.com/seraphjiang/fuse/blob/main/docs/rfcs/RFC-001-fuse-integration.md) — Architecture RFC

## Development Setup

```bash
git clone https://github.com/seraphjiang/fuse.git
cd fuse
cargo build
cargo test --all-targets
```

## Code Style

- `cargo fmt` before committing
- `cargo clippy` must pass with no warnings
- Every public function needs at least one test
- Conventional commit messages (`feat:`, `fix:`, `docs:`, `test:`)

## DCO Sign-Off

All commits must include a DCO sign-off:

```bash
git commit -s -m "feat: add new feature"
```

## Writing a Custom Connector

The fastest way to contribute is adding a new connector. Here's the template:

### 1. Create the crate

```
crates/fuse-connectors/my-connector/
├── Cargo.toml
└── src/
    └── lib.rs
```

### 2. Implement `FederatedConnector`

```rust
use async_trait::async_trait;
use fuse_core::connector::*;
use fuse_core::error::Result;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

pub struct MyConnector {
    id: String,
    // your config fields
}

#[async_trait]
impl FederatedConnector for MyConnector {
    fn id(&self) -> &str { &self.id }
    fn connector_type(&self) -> &str { "my-connector" }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_filtering: true,
            supports_projection: true,
            supports_limit: true,
            ..Default::default()
        }
    }

    async fn health_check(&self) -> Result<HealthStatus> {
        Ok(HealthStatus::Healthy)
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>> {
        // Return available tables/indices
        todo!()
    }

    async fn get_schema(&self, table: &str) -> Result<Schema> {
        // Return Arrow schema for a table
        todo!()
    }

    fn table_names(&self) -> Vec<String> {
        // Return list of table names
        todo!()
    }

    async fn get_table_schema(&self, table: &str) -> Result<Schema> {
        self.get_schema(table).await
    }

    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>> {
        // Execute query and return Arrow RecordBatches
        todo!()
    }

    async fn execute_streaming(&self, query: &SubQuery)
        -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<RecordBatch>> + Send>>>
    {
        // Streaming variant (can wrap execute() if not natively supported)
        let batches = self.execute(query).await?;
        Ok(Box::pin(futures::stream::iter(batches.into_iter().map(Ok))))
    }
}
```

### 3. Implement `ConnectorFactory`

```rust
pub struct MyConnectorFactory;

impl ConnectorFactory for MyConnectorFactory {
    fn connector_type(&self) -> &str { "my-connector" }

    fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>> {
        Ok(Arc::new(MyConnector {
            id: config.id.clone(),
            // parse config.settings for your fields
        }))
    }
}
```

### 4. Register in main.rs

```rust
registry.register_factory(Arc::new(MyConnectorFactory));
```

### 5. Add to fuse.toml

```toml
[[connector]]
id = "my_source"
connector_type = "my-connector"
# your config fields in [connector.settings]
```

### 6. Write tests

At minimum: health check, schema discovery, basic query execution, error handling.

### Checklist

- [ ] Implements all `FederatedConnector` methods
- [ ] Has a `ConnectorFactory`
- [ ] `capabilities()` accurately reflects what's supported
- [ ] Health check verifies connectivity
- [ ] Schema discovery returns correct Arrow types
- [ ] At least 4 tests (health, schema, query, error)
- [ ] `cargo fmt` + `cargo clippy` clean
- [ ] DCO sign-off on all commits
