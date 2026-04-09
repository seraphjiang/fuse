# Fuse Design: Connector Interface & Query Parser

**Author:** general (hive agent)
**Date:** 2026-04-08
**Status:** Draft
**Scope:** Connector trait, query parser for multi-source FROM, OpenSearch connector, connector registration

---

## 1. Connector Interface (`FederatedConnector` Trait)

The connector interface is the contract every datasource must implement. It covers five concerns: capabilities declaration, schema discovery, query execution, streaming results, and health checks.

### 1.1 Core Trait

```rust
use async_trait::async_trait;
use tokio::sync::mpsc;

/// Every datasource connector implements this trait.
#[async_trait]
pub trait FederatedConnector: Send + Sync {
    /// Unique identifier for this connector instance (e.g., "prod_cluster_a").
    fn id(&self) -> &str;

    /// Connector type (e.g., "opensearch", "s3", "prometheus").
    fn connector_type(&self) -> ConnectorType;

    /// Declare what this connector can do — the planner uses this
    /// to decide what to push down vs. execute locally.
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Health check. Called periodically and before query execution.
    async fn health_check(&self) -> ConnectorHealth;

    /// List available schemas (indices, tables, buckets, etc.).
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError>;

    /// Get fields for a specific schema.
    async fn get_fields(&self, schema: &str) -> Result<Vec<FieldInfo>, ConnectorError>;

    /// Execute a sub-query. Returns a complete result set.
    /// Use for small/bounded queries.
    async fn execute(&self, query: &SubQuery) -> Result<ResultSet, ConnectorError>;

    /// Execute a sub-query with streaming results.
    /// The connector pushes RecordBatches into the channel as they arrive.
    /// Use for large/unbounded queries.
    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError>;
}
```

### 1.2 Capabilities

The planner inspects capabilities to decide push-down strategy. If a connector can't aggregate, the engine does it locally after fetching raw rows.

```rust
pub struct ConnectorCapabilities {
    /// Which operations can be pushed down to this datasource.
    pub filter: PushDownSupport,
    pub projection: PushDownSupport,
    pub aggregation: PushDownSupport,
    pub sorting: PushDownSupport,
    pub limit: PushDownSupport,
    pub join: PushDownSupport,

    /// Max concurrent queries this connector can handle.
    pub max_concurrent_queries: usize,

    /// Whether the connector supports streaming (scroll/cursor).
    pub supports_streaming: bool,

    /// Estimated latency class — helps the planner order execution.
    pub latency_class: LatencyClass,
}

pub enum PushDownSupport {
    Full,       // Connector handles it natively
    Partial,    // Some expressions supported (e.g., S3 Select supports = but not regex)
    None,       // Engine must handle it
}

pub enum LatencyClass {
    Low,        // < 100ms typical (OpenSearch, in-memory)
    Medium,     // 100ms - 2s (Prometheus, JDBC)
    High,       // > 2s (S3, Athena)
}
```

### 1.3 Health Check

```rust
pub struct ConnectorHealth {
    pub status: HealthStatus,
    pub latency_ms: Option<u64>,
    pub message: Option<String>,
}

pub enum HealthStatus {
    Healthy,
    Degraded,   // Slow but functional
    Unhealthy,  // Cannot serve queries
}
```

### 1.4 Schema Discovery Types

```rust
pub struct SchemaInfo {
    pub name: String,                    // e.g., "logs-2026.04"
    pub schema_type: SchemaType,         // Index, Table, Bucket, MetricName
    pub estimated_row_count: Option<u64>,
}

pub enum SchemaType {
    Index,       // OpenSearch
    Table,       // JDBC, Glue
    Bucket,      // S3
    MetricName,  // Prometheus
}

pub struct FieldInfo {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: HashMap<String, String>, // connector-specific (e.g., "analyzer": "standard")
}

/// Unified type system across all connectors.
pub enum DataType {
    Boolean,
    Integer,
    Long,
    Float,
    Double,
    Text,
    Keyword,
    Date,
    Timestamp,
    Binary,
    Geo,
    Nested(Vec<FieldInfo>),
    Array(Box<DataType>),
}
```

### 1.5 SubQuery and ResultSet

The planner decomposes the user's federated query into `SubQuery` objects — one per connector. Each SubQuery contains only what that connector should execute.

```rust
pub struct SubQuery {
    pub schema: String,                          // target index/table
    pub projections: Vec<Projection>,            // fields to return
    pub filter: Option<FilterExpr>,              // WHERE clause pushed down
    pub aggregations: Vec<AggregationExpr>,      // pushed-down aggregations
    pub sort: Vec<SortExpr>,                     // pushed-down ORDER BY
    pub limit: Option<u64>,
    pub passthrough: Option<String>,             // raw DSL for connector-specific features
}

pub struct ResultSet {
    pub schema: Vec<FieldInfo>,
    pub batches: Vec<RecordBatch>,
    pub total_rows: u64,
    pub truncated: bool,
}

/// A batch of rows in columnar format (Arrow-compatible).
pub struct RecordBatch {
    pub columns: Vec<ColumnData>,
    pub row_count: usize,
}
```

---

## 2. Query Parser for Multi-Source FROM Syntax

### 2.1 Design Approach

We extend PPL and SQL with `datasource.table` qualified references. The parser produces a unified AST that the planner consumes. We use a hand-written recursive-descent parser (not ANTLR) for two reasons:
1. Rust doesn't have great ANTLR support — `nom`, `pest`, or hand-written parsers are idiomatic.
2. The grammar is small and well-defined; a hand-written parser gives us better error messages and control.

We can also consider `pest` (PEG parser) for the grammar definition if we want a formal grammar file.

### 2.2 Grammar Extensions

```
// PPL multi-source FROM
source = <qualified_name> [',' <qualified_name>]*

// SQL multi-source FROM
FROM <qualified_name> [AS <alias>]
     [JOIN <qualified_name> [AS <alias>] ON <join_condition>]*

// Qualified name: datasource.schema
qualified_name = <identifier> '.' <identifier>
               | <identifier>   // unqualified = default datasource
```

### 2.3 AST Node Definitions

```rust
/// Root of a parsed query.
pub enum Statement {
    PplQuery(PplQuery),
    SqlQuery(SqlQuery),
}

// ── PPL AST ──

pub struct PplQuery {
    pub source: SourceClause,
    pub commands: Vec<PplCommand>,
}

pub enum PplCommand {
    Where(FilterExpr),
    Stats(StatsCommand),
    Sort(Vec<SortExpr>),
    Head(u64),
    Fields(Vec<Projection>),
    Join(JoinClause),
    Eval(Vec<EvalExpr>),
    Dedup(Vec<String>),
}

pub struct StatsCommand {
    pub aggregations: Vec<AggregationExpr>,
    pub group_by: Vec<Expr>,
}

// ── SQL AST ──

pub struct SqlQuery {
    pub projections: Vec<Projection>,
    pub from: SourceClause,
    pub joins: Vec<JoinClause>,
    pub filter: Option<FilterExpr>,
    pub group_by: Vec<Expr>,
    pub having: Option<FilterExpr>,
    pub order_by: Vec<SortExpr>,
    pub limit: Option<u64>,
}

// ── Shared AST nodes ──

/// The FROM clause — supports single, multi-source, and subquery sources.
pub struct SourceClause {
    pub sources: Vec<TableRef>,
}

/// A reference to a datasource + schema, with optional alias.
pub struct TableRef {
    pub datasource: Option<String>,  // None = default datasource
    pub schema: String,              // index/table name
    pub alias: Option<String>,
}

pub struct JoinClause {
    pub join_type: JoinType,
    pub right: TableRef,
    pub condition: FilterExpr,
}

pub enum JoinType {
    Inner,
    Left,
    Right,
    Cross,
}

// ── Expressions ──

pub enum Expr {
    Column(ColumnRef),
    Literal(Literal),
    Function(FunctionCall),
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr> },
    UnaryOp { op: UnaryOp, operand: Box<Expr> },
    Wildcard,
}

pub struct ColumnRef {
    pub table_alias: Option<String>,  // e.g., "l" in l.trace_id
    pub name: String,
}

pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Expr>,
}

pub enum BinaryOp {
    Eq, Neq, Lt, Gt, Lte, Gte,
    And, Or,
    Add, Sub, Mul, Div,
    Like, In,
}

pub enum UnaryOp { Not, Neg }

pub type FilterExpr = Expr;

pub struct Projection {
    pub expr: Expr,
    pub alias: Option<String>,
}

pub struct AggregationExpr {
    pub function: String,   // "count", "avg", "sum", "min", "max"
    pub args: Vec<Expr>,
    pub alias: Option<String>,
}

pub struct SortExpr {
    pub expr: Expr,
    pub descending: bool,
}

pub struct EvalExpr {
    pub name: String,
    pub expr: Expr,
}
```

### 2.4 Parser Sketch

```rust
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn parse(input: &str) -> Result<Statement, ParseError> {
        let tokens = Lexer::tokenize(input)?;
        let mut parser = Parser { tokens, pos: 0 };

        // Detect PPL vs SQL by first token
        if parser.peek_keyword("source") || parser.peek_keyword("search") {
            Ok(Statement::PplQuery(parser.parse_ppl()?))
        } else {
            Ok(Statement::SqlQuery(parser.parse_sql()?))
        }
    }

    fn parse_table_ref(&mut self) -> Result<TableRef, ParseError> {
        let first = self.expect_identifier()?;

        if self.try_consume(Token::Dot) {
            // qualified: datasource.schema
            let schema = self.expect_identifier()?;
            let alias = self.parse_optional_alias()?;
            Ok(TableRef { datasource: Some(first), schema, alias })
        } else {
            let alias = self.parse_optional_alias()?;
            Ok(TableRef { datasource: None, schema: first, alias })
        }
    }

    fn parse_source_clause(&mut self) -> Result<SourceClause, ParseError> {
        let mut sources = vec![self.parse_table_ref()?];
        while self.try_consume(Token::Comma) {
            sources.push(self.parse_table_ref()?);
        }
        Ok(SourceClause { sources })
    }
}
```

### 2.5 Query Decomposition

After parsing, the planner walks the AST and splits it into per-connector SubQueries:

```
User query:
  source = cluster_a.logs, cluster_b.logs
  | where status >= 500
  | stats count() by service

Parsed AST:
  PplQuery {
    source: [
      TableRef { datasource: "cluster_a", schema: "logs" },
      TableRef { datasource: "cluster_b", schema: "logs" },
    ],
    commands: [Where(...), Stats(...)]
  }

Decomposed into:
  SubQuery → cluster_a connector:
    schema: "logs", filter: status >= 500, aggregations: [count() by service]

  SubQuery → cluster_b connector:
    schema: "logs", filter: status >= 500, aggregations: [count() by service]

  Engine merge step:
    Merge aggregation results (sum the counts per service across both)
```

For cross-type JOINs, decomposition is more complex — the planner creates a DAG:

```
User query:
  SELECT l.trace_id, a.user_id
  FROM os_prod.logs AS l
  JOIN s3_archive.audit AS a ON l.trace_id = a.trace_id
  WHERE l.@timestamp > now() - 24h

Decomposed DAG:
  Step 1 (parallel-safe): SubQuery → os_prod: SELECT trace_id FROM logs WHERE @timestamp > now()-24h
  Step 2 (depends on 1):  SubQuery → s3_archive: SELECT trace_id, user_id FROM audit WHERE trace_id IN (...)
  Step 3 (engine):        Hash-join step1 ⋈ step2 on trace_id
```

---

## 3. OpenSearch Connector (Phase 1)

### 3.1 Implementation

```rust
use opensearch::OpenSearch;

pub struct OpenSearchConnector {
    id: String,
    client: OpenSearch,
    config: OpenSearchConfig,
}

pub struct OpenSearchConfig {
    pub url: String,
    pub auth: AuthConfig,
    pub max_concurrent_queries: usize,
    pub scroll_size: usize,          // batch size for streaming (default: 1000)
    pub request_timeout: Duration,
}

pub enum AuthConfig {
    None,
    Basic { username: String, password: String },
    SigV4 { region: String },
}

#[async_trait]
impl FederatedConnector for OpenSearchConnector {
    fn id(&self) -> &str { &self.id }

    fn connector_type(&self) -> ConnectorType { ConnectorType::OpenSearch }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            filter: PushDownSupport::Full,
            projection: PushDownSupport::Full,
            aggregation: PushDownSupport::Full,
            sorting: PushDownSupport::Full,
            limit: PushDownSupport::Full,
            join: PushDownSupport::None,  // OS doesn't do cross-index joins
            max_concurrent_queries: self.config.max_concurrent_queries,
            supports_streaming: true,     // via scroll API
            latency_class: LatencyClass::Low,
        }
    }

    async fn health_check(&self) -> ConnectorHealth {
        let start = Instant::now();
        match self.client.cluster().health().send().await {
            Ok(resp) => ConnectorHealth {
                status: match resp.status() {
                    200 => HealthStatus::Healthy,
                    _ => HealthStatus::Degraded,
                },
                latency_ms: Some(start.elapsed().as_millis() as u64),
                message: None,
            },
            Err(e) => ConnectorHealth {
                status: HealthStatus::Unhealthy,
                latency_ms: None,
                message: Some(e.to_string()),
            },
        }
    }

    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError> {
        // GET _cat/indices?format=json
        let indices = self.client.cat().indices().send().await?;
        // Map each index to SchemaInfo
        Ok(indices.into_iter().map(|idx| SchemaInfo {
            name: idx.index,
            schema_type: SchemaType::Index,
            estimated_row_count: idx.docs_count.parse().ok(),
        }).collect())
    }

    async fn get_fields(&self, schema: &str) -> Result<Vec<FieldInfo>, ConnectorError> {
        // GET /{schema}/_mapping
        let mapping = self.client.indices().get_mapping().index(schema).send().await?;
        Ok(parse_mapping_to_fields(mapping))
    }

    async fn execute(&self, query: &SubQuery) -> Result<ResultSet, ConnectorError> {
        let body = translate_to_os_dsl(query)?;
        let resp = self.client.search().index(&query.schema).body(body).send().await?;
        Ok(parse_search_response(resp, query)?)
    }

    async fn execute_streaming(
        &self,
        query: &SubQuery,
        tx: mpsc::Sender<Result<RecordBatch, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        // Use scroll API for large result sets
        let body = translate_to_os_dsl(query)?;
        let mut resp = self.client.search()
            .index(&query.schema)
            .scroll("1m")
            .body(body)
            .send().await?;

        loop {
            let batch = parse_hits_to_batch(&resp)?;
            if batch.row_count == 0 { break; }
            tx.send(Ok(batch)).await.map_err(|_| ConnectorError::ChannelClosed)?;

            let scroll_id = resp.scroll_id().ok_or(ConnectorError::NoScrollId)?;
            resp = self.client.scroll().scroll_id(scroll_id).scroll("1m").send().await?;
        }
        Ok(())
    }
}
```

### 3.2 SubQuery → OpenSearch DSL Translation

```rust
/// Translates a SubQuery AST into an OpenSearch JSON query body.
fn translate_to_os_dsl(query: &SubQuery) -> Result<serde_json::Value, ConnectorError> {
    let mut body = serde_json::Map::new();

    // Projections → _source
    if !query.projections.is_empty() {
        let fields: Vec<&str> = query.projections.iter()
            .filter_map(|p| match &p.expr {
                Expr::Column(c) => Some(c.name.as_str()),
                _ => None,
            })
            .collect();
        body.insert("_source".into(), json!(fields));
    }

    // Filter → query.bool
    if let Some(filter) = &query.filter {
        body.insert("query".into(), translate_filter(filter)?);
    }

    // Aggregations → aggs
    if !query.aggregations.is_empty() {
        body.insert("aggs".into(), translate_aggregations(&query.aggregations, &query)?);
        body.insert("size".into(), json!(0)); // agg-only, no hits
    }

    // Sort
    if !query.sort.is_empty() {
        let sorts: Vec<serde_json::Value> = query.sort.iter().map(|s| {
            let order = if s.descending { "desc" } else { "asc" };
            json!({ expr_to_field_name(&s.expr): { "order": order } })
        }).collect();
        body.insert("sort".into(), json!(sorts));
    }

    // Limit
    if let Some(limit) = query.limit {
        body.insert("size".into(), json!(limit));
    }

    Ok(serde_json::Value::Object(body))
}
```

---

## 4. Connector Registration & Configuration

### 4.1 Registry

The `ConnectorRegistry` is the central place where connectors are registered, looked up, and lifecycle-managed.

```rust
pub struct ConnectorRegistry {
    connectors: RwLock<HashMap<String, Arc<dyn FederatedConnector>>>,
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self { connectors: RwLock::new(HashMap::new()) }
    }

    /// Register a connector instance. Fails if ID already exists.
    pub fn register(&self, connector: Arc<dyn FederatedConnector>) -> Result<(), RegistryError> {
        let mut map = self.connectors.write().unwrap();
        let id = connector.id().to_string();
        if map.contains_key(&id) {
            return Err(RegistryError::DuplicateId(id));
        }
        map.insert(id, connector);
        Ok(())
    }

    /// Look up a connector by its datasource ID (used during query planning).
    pub fn get(&self, id: &str) -> Option<Arc<dyn FederatedConnector>> {
        self.connectors.read().unwrap().get(id).cloned()
    }

    /// Remove a connector (for dynamic reconfiguration).
    pub fn deregister(&self, id: &str) -> Option<Arc<dyn FederatedConnector>> {
        self.connectors.write().unwrap().remove(id)
    }

    /// List all registered connectors.
    pub fn list(&self) -> Vec<Arc<dyn FederatedConnector>> {
        self.connectors.read().unwrap().values().cloned().collect()
    }

    /// Health check all connectors in parallel.
    pub async fn health_check_all(&self) -> HashMap<String, ConnectorHealth> {
        let connectors: Vec<_> = self.list();
        let futures = connectors.iter().map(|c| {
            let c = c.clone();
            async move { (c.id().to_string(), c.health_check().await) }
        });
        futures::future::join_all(futures).await.into_iter().collect()
    }
}
```

### 4.2 Configuration File

Connectors are declared in a TOML config file. The engine reads this at startup and instantiates connectors via a factory pattern.

```toml
# fuse.toml

[engine]
bind = "0.0.0.0:9400"
max_concurrent_queries = 64
default_timeout = "30s"

[[connector]]
id = "prod_cluster"
type = "opensearch"
url = "https://opensearch-prod.example.com:9200"
auth = { type = "basic", username = "admin", password_env = "FUSE_PROD_PASSWORD" }
max_concurrent_queries = 16
scroll_size = 1000

[[connector]]
id = "staging_cluster"
type = "opensearch"
url = "https://opensearch-staging.example.com:9200"
auth = { type = "sigv4", region = "us-west-2" }

[[connector]]
id = "s3_archive"
type = "s3"
bucket = "my-data-lake"
prefix = "logs/"
format = "parquet"
glue_database = "my_catalog"
auth = { type = "sigv4", region = "us-east-1" }

[[connector]]
id = "prometheus_prod"
type = "prometheus"
url = "http://prometheus.internal:9090"
auth = { type = "bearer", token_env = "FUSE_PROM_TOKEN" }
```

### 4.3 Connector Factory

```rust
pub trait ConnectorFactory: Send + Sync {
    fn connector_type(&self) -> &str;
    fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError>;
}

pub struct ConnectorFactoryRegistry {
    factories: HashMap<String, Box<dyn ConnectorFactory>>,
}

impl ConnectorFactoryRegistry {
    pub fn new() -> Self {
        let mut reg = Self { factories: HashMap::new() };
        // Register built-in factories
        reg.register(Box::new(OpenSearchConnectorFactory));
        reg
    }

    pub fn register(&mut self, factory: Box<dyn ConnectorFactory>) {
        self.factories.insert(factory.connector_type().to_string(), factory);
    }

    /// Build all connectors from config file.
    pub fn build_from_config(&self, config: &FuseConfig) -> Result<Vec<Arc<dyn FederatedConnector>>, ConnectorError> {
        config.connectors.iter().map(|cc| {
            let factory = self.factories.get(&cc.connector_type)
                .ok_or_else(|| ConnectorError::UnknownType(cc.connector_type.clone()))?;
            factory.create(cc)
        }).collect()
    }
}

struct OpenSearchConnectorFactory;

impl ConnectorFactory for OpenSearchConnectorFactory {
    fn connector_type(&self) -> &str { "opensearch" }

    fn create(&self, config: &ConnectorConfig) -> Result<Arc<dyn FederatedConnector>, ConnectorError> {
        let os_config = OpenSearchConfig::from_connector_config(config)?;
        Ok(Arc::new(OpenSearchConnector::new(config.id.clone(), os_config)?))
    }
}
```

### 4.4 Dynamic Registration via API

Beyond static config, connectors can be registered at runtime via the Fuse HTTP API:

```
POST /api/connectors
{
  "id": "new_cluster",
  "type": "opensearch",
  "url": "https://new-cluster:9200",
  "auth": { "type": "basic", "username": "admin", "password": "..." }
}

GET  /api/connectors              → list all
GET  /api/connectors/{id}         → details + health
GET  /api/connectors/{id}/schemas → schema discovery
DELETE /api/connectors/{id}       → deregister
```

---

## 5. Design Decisions & Rationale

| Decision | Rationale |
|----------|-----------|
| Rust for code sketches | Proposal emphasizes performance, parallelism, zero-copy, streaming. Rust is the natural fit for a standalone query engine. Planner is deciding language separately — we'll reconcile. |
| `async_trait` + `mpsc` for streaming | Async is essential for fan-out to multiple connectors. Channel-based streaming lets the merger start processing before all results arrive. |
| `PushDownSupport` enum (Full/Partial/None) | More nuanced than a boolean. S3 Select supports `=` filters but not regex — `Partial` captures this. The planner can make smarter decisions. |
| `LatencyClass` on capabilities | The planner should execute low-latency connectors first in dependent joins (e.g., OS before S3 in semi-join). |
| Hand-written parser over ANTLR | Rust ecosystem favors hand-written or PEG parsers. Grammar is small. Better error messages. |
| `passthrough` field on SubQuery | Allows connector-specific features (e.g., `match_phrase` for OS, `rate()` for Prometheus) without polluting the shared AST. |
| TOML config + runtime API | Static config for production deployments, dynamic API for development and testing. |
| Factory pattern for connectors | Clean separation between config parsing and connector instantiation. Makes it easy to add new connector types. |
| Credentials via env vars (`password_env`) | Never store secrets in config files. Reference env vars that are injected at deploy time. |

---

## 6. Open Questions for Team

1. **Arrow as the internal data format?** Using Apache Arrow `RecordBatch` as the internal columnar format would give us zero-copy interop with many tools. Worth the dependency?
2. **Parser library choice:** Hand-written vs `pest` vs `nom`? Hand-written gives best errors, `pest` gives a formal grammar file we can share.
3. **Connector versioning:** Should connectors declare a protocol version for forward/backward compatibility?
4. **Schema caching:** How aggressively should we cache `discover_schemas` / `get_fields` results? TTL per connector type?
