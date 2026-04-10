# Connector Development Guide

End-to-end tutorial for adding a new connector to Fuse. Takes you from `cargo init` to a tested, registered connector in ~30 minutes.

For trait reference and field descriptions, see [Writing a Connector](writing-a-connector.md).

## Overview

A Fuse connector is a Rust crate that implements two traits:
- `FederatedConnector` — query execution, schema discovery, health checks
- `ConnectorFactory` — creates connector instances from `fuse.toml` config

The engine sends a `SubQuery` struct to your connector. You translate it to your datasource's native query language, execute it, and return Arrow `RecordBatch`es.

## Step 1: Scaffold the Crate

```bash
# Copy the example connector as a starting point
cp -r crates/fuse-connectors/example crates/fuse-connectors/mydb
```

Edit `crates/fuse-connectors/mydb/Cargo.toml`:

```toml
[package]
name = "fuse-connector-mydb"
version = "0.1.0"
edition = "2021"

[dependencies]
fuse-core = { path = "../../fuse-core" }
arrow = { workspace = true }
async-trait = { workspace = true }
tokio = { workspace = true }
tracing = { workspace = true }
serde_json = { workspace = true }
# Add your datasource client crate here, e.g.:
# sqlx = { version = "0.8", features = ["runtime-tokio", "postgres"] }
```

Add to root `Cargo.toml`:

```toml
members = [
    # ...existing...
    "crates/fuse-connectors/mydb",
]
```

Add the dependency to `crates/fuse-server/Cargo.toml`:

```toml
fuse-connector-mydb = { path = "../fuse-connectors/mydb" }
```

## Step 2: Implement the Connector

Replace the example code in `src/lib.rs`. The key method is `execute()` — it receives a `SubQuery` and returns Arrow batches.

### SubQuery → Native Query Translation

This is the core of your connector. The `SubQuery` struct contains:

```rust
pub struct SubQuery {
    pub table: String,           // "users"
    pub projections: Vec<String>, // ["id", "name"] or [] for all
    pub filter: Option<FilterExpr>, // WHERE clause tree
    pub aggregations: Vec<AggregationExpr>,
    pub group_by: Vec<String>,
    pub sort: Vec<SortExpr>,
    pub limit: Option<u64>,
    pub passthrough: Option<Value>, // connector-specific raw data
}
```

Translate each field you support into your native query. Example for a SQL-based datasource:

```rust
fn subquery_to_sql(sq: &SubQuery) -> String {
    let cols = if sq.projections.is_empty() {
        "*".to_string()
    } else {
        sq.projections.join(", ")
    };

    let mut sql = format!("SELECT {} FROM {}", cols, sq.table);

    if let Some(filter) = &sq.filter {
        sql.push_str(&format!(" WHERE {}", filter_to_sql(filter)));
    }
    if !sq.group_by.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", sq.group_by.join(", ")));
    }
    if !sq.sort.is_empty() {
        let clauses: Vec<String> = sq.sort.iter()
            .map(|s| format!("{} {}", s.field, if s.descending { "DESC" } else { "ASC" }))
            .collect();
        sql.push_str(&format!(" ORDER BY {}", clauses.join(", ")));
    }
    if let Some(limit) = sq.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    }
    sql
}
```

### FilterExpr Translation

`FilterExpr` is a recursive enum. Translate it to your native filter format:

```rust
fn filter_to_sql(expr: &FilterExpr) -> String {
    match expr {
        FilterExpr::Comparison { field, op, value } => {
            let op_str = match op {
                ComparisonOp::Eq => "=",
                ComparisonOp::Ne => "!=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Like => "LIKE",
                ComparisonOp::ILike => "ILIKE",
            };
            format!("{} {} {}", field, op_str, scalar_to_sql(value))
        }
        FilterExpr::And(left, right) =>
            format!("({} AND {})", filter_to_sql(left), filter_to_sql(right)),
        FilterExpr::Or(left, right) =>
            format!("({} OR {})", filter_to_sql(left), filter_to_sql(right)),
        FilterExpr::Not(inner) =>
            format!("NOT ({})", filter_to_sql(inner)),
        FilterExpr::In { field, values } => {
            let vals: Vec<String> = values.iter().map(scalar_to_sql).collect();
            format!("{} IN ({})", field, vals.join(", "))
        }
        FilterExpr::IsNull(field) => format!("{} IS NULL", field),
        FilterExpr::IsNotNull(field) => format!("{} IS NOT NULL", field),
    }
}

fn scalar_to_sql(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Int64(n) => n.to_string(),
        ScalarValue::Float64(f) => f.to_string(),
        ScalarValue::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
        ScalarValue::Boolean(b) => b.to_string(),
    }
}
```

For non-SQL datasources, translate to your native format instead. See these real examples:
- **OpenSearch** → Query DSL JSON: `crates/fuse-connectors/opensearch/src/pushdown.rs`
- **DynamoDB** → `FilterExpression` + `KeyConditionExpression`: `crates/fuse-connectors/dynamodb/src/lib.rs`
- **MongoDB** → BSON query document: `crates/fuse-connectors/mongodb/src/lib.rs`

### Building Arrow RecordBatches

Your `execute()` must return `Vec<RecordBatch>`. Convert your datasource's response format to Arrow:

```rust
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

fn rows_to_batch(rows: Vec<MyRow>, schema: Arc<Schema>) -> Result<RecordBatch, ConnectorError> {
    let ids: Int64Array = rows.iter().map(|r| Some(r.id)).collect();
    let names: StringArray = rows.iter().map(|r| Some(r.name.as_str())).collect();

    RecordBatch::try_new(schema, vec![
        Arc::new(ids),
        Arc::new(names),
    ]).map_err(ConnectorError::query)
}
```

### Capabilities

Set `ConnectorCapabilities` to accurately reflect what your `execute()` handles. If you declare `supports_filtering: true` but ignore `SubQuery.filter`, queries will return wrong results.

```rust
fn capabilities(&self) -> ConnectorCapabilities {
    ConnectorCapabilities {
        supports_filtering: true,   // only if execute() translates SubQuery.filter
        supports_projection: true,  // only if execute() translates SubQuery.projections
        supports_aggregation: false, // set true when you handle aggregations + group_by
        supports_sorting: true,
        supports_limit: true,
        supports_join: false,       // almost always false (Fuse handles joins)
        max_concurrent_queries: 8,
        supports_streaming: false,
        latency_class: LatencyClass::Medium, // Low (<10ms), Medium (<100ms), High (>100ms)
    }
}
```

## Step 3: Implement the Factory

```rust
pub struct MyDbConnectorFactory;

#[async_trait::async_trait]
impl ConnectorFactory for MyDbConnectorFactory {
    fn connector_type(&self) -> &str { "mydb" }

    async fn create(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let url = config.properties.get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ConnectorError::Connection("missing 'url' in config".into()))?;

        // Build your client/pool here
        Ok(Arc::new(MyDbConnector::new(&config.id, url)))
    }
}
```

## Step 4: Register

In `crates/fuse-server/src/main.rs`, add your factory:

```rust
use fuse_connector_mydb::MyDbConnectorFactory;

// In the factory registration block:
factory_registry.register("mydb", Arc::new(MyDbConnectorFactory));
```

## Step 5: Configure

Add to `fuse.toml`:

```toml
[[connector]]
id = "my_source"
type = "mydb"
url = "http://localhost:5432/analytics"
```

## Step 6: Test

### Unit tests (in your crate)

Test each method independently:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn connector() -> MyDbConnector {
        MyDbConnector::new("test", "http://localhost:5432")
    }

    #[test]
    fn test_capabilities() {
        let caps = connector().capabilities();
        assert!(caps.supports_filtering);
        assert!(caps.supports_limit);
    }

    #[test]
    fn test_filter_translation() {
        let filter = FilterExpr::Comparison {
            field: "status".into(),
            op: ComparisonOp::Gte,
            value: ScalarValue::Int64(500),
        };
        assert_eq!(filter_to_sql(&filter), "status >= 500");
    }

    #[test]
    fn test_subquery_to_sql() {
        let sq = SubQuery {
            table: "users".into(),
            projections: vec!["id".into(), "name".into()],
            filter: Some(FilterExpr::Comparison {
                field: "age".into(),
                op: ComparisonOp::Gt,
                value: ScalarValue::Int64(18),
            }),
            aggregations: vec![], group_by: vec![],
            sort: vec![], limit: Some(10),
            having: None, passthrough: None,
        };
        assert_eq!(
            subquery_to_sql(&sq),
            "SELECT id, name FROM users WHERE age > 18 LIMIT 10"
        );
    }
}
```

### SDK smoke test

Use `fuse-connector-sdk` for standardized testing:

```rust
#[cfg(test)]
mod sdk_tests {
    use fuse_connector_sdk::testing::*;

    #[tokio::test]
    async fn test_smoke() {
        let connector = super::MyDbConnector::new("test", "http://localhost:5432");
        // Runs: health_check, discover_schemas, get_schema, execute with limit
        smoke_test(&connector).await.unwrap();
    }

    #[tokio::test]
    async fn test_health() {
        let connector = super::MyDbConnector::new("test", "http://localhost:5432");
        assert_healthy(&connector).await;
    }
}
```

### MockConnector for integration tests

Test your connector in a federated context without a live datasource:

```rust
use fuse_connector_sdk::testing::MockConnector;

#[tokio::test]
async fn test_federated_query() {
    let mock = MockConnector::new("mydb")
        .with_type("mydb")
        .with_table("users", vec!["id", "name"])
        .with_rows("users", vec![
            vec!["1", "Alice"],
            vec!["2", "Bob"],
        ]);

    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(mock)).unwrap();

    let connector = registry.get("mydb").unwrap();
    let batches = connector.execute(&SubQuery {
        table: "users".into(),
        projections: vec![],
        filter: None, aggregations: vec![], group_by: vec![],
        sort: vec![], limit: Some(10),
        having: None, passthrough: None,
    }).await.unwrap();

    assert_batches_non_empty(&batches);
    assert_batch_columns(&batches[0], &["id", "name"]);
    assert_batch_row_count(&batches[0], 2);
}
```

## Checklist

Before submitting:

- [ ] Crate in workspace `Cargo.toml` and `fuse-server/Cargo.toml`
- [ ] `#[derive(Debug)]` on connector struct
- [ ] `ConnectorCapabilities` matches what `execute()` actually handles
- [ ] `health_check()` pings the real datasource
- [ ] `discover_schemas()` returns real tables (skips internal/system tables)
- [ ] `get_schema()` returns accurate Arrow types
- [ ] `execute()` translates filter, projections, limit when declared as supported
- [ ] `execute_streaming()` implemented (can delegate to `execute()`)
- [ ] Error types are specific (`Connection` vs `Auth` vs `QueryFailed`)
- [ ] Factory registered in `main.rs`
- [ ] Unit tests for filter translation and SubQuery→native conversion
- [ ] SDK `smoke_test` passes
- [ ] Sample `[[connector]]` block in fuse.toml
- [ ] `cargo check` and `cargo test` pass

## Reference Files

| File | What to look at |
|------|-----------------|
| `crates/fuse-connectors/example/src/lib.rs` | Minimal template — start here |
| `crates/fuse-connectors/opensearch/src/pushdown.rs` | FilterExpr → Query DSL translation |
| `crates/fuse-connectors/dynamodb/src/lib.rs` | FilterExpr → DynamoDB expressions |
| `crates/fuse-connectors/postgres/src/lib.rs` | Full SQL pushdown pattern |
| `crates/fuse-connectors/mongodb/src/lib.rs` | FilterExpr → BSON documents |
| `crates/fuse-connector-sdk/src/testing.rs` | MockConnector, smoke_test, assertions |
| `crates/fuse-core/src/connector.rs` | FederatedConnector trait, SubQuery, FilterExpr |
| `crates/fuse-core/src/error.rs` | ConnectorError variants |
