# fuse-connector-sdk

SDK for building connectors for the [Fuse](https://github.com/seraphjiang/fuse) federated query engine.

## Quick Start

```toml
[dependencies]
fuse-connector-sdk = "0.1"
```

```rust
use fuse_connector_sdk::prelude::*;

#[derive(Debug)]
struct MyConnector { /* ... */ }

#[async_trait]
impl FederatedConnector for MyConnector {
    fn id(&self) -> &str { "my_datasource" }
    fn connector_type(&self) -> &str { "custom" }
    fn capabilities(&self) -> ConnectorCapabilities { ConnectorCapabilities::full() }
    // ... implement remaining methods
}
```

## Testing

```rust
use fuse_connector_sdk::testing::{MockConnector, smoke_test};

#[tokio::test]
async fn test_connector() {
    let mock = MockConnector::new("test")
        .with_table("events", vec!["id", "name"])
        .with_rows("events", vec![vec!["1", "click"]]);
    smoke_test(&mock).await.unwrap();
}
```

## License

Apache-2.0
