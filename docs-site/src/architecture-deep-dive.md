# Architecture Deep Dive

Internals of the Fuse federated query engine for contributors and advanced users.

## 1. Request Lifecycle

```
HTTP Request
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│ Middleware Pipeline                                      │
│  auth → rate_limit → tenant_check → timeout → audit     │
└────────────────────────┬────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────┐
│ Parse                                                    │
│  SQL → DataFusion LogicalPlan                           │
│  PPL → PPL AST → DataFusion LogicalPlan                 │
└────────────────────────┬────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────┐
│ Plan                                                     │
│  1. Identify datasource references (datasource.table)   │
│  2. Check tenant access (can_access per datasource)     │
│  3. Build SubQuery per connector                        │
│  4. Apply pushdown (filter, projection, agg, sort, limit)│
│  5. Cost estimation → join order, build/probe selection  │
└────────────────────────┬────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────┐
│ Fan-Out                                                  │
│  tokio::spawn per SubQuery → connectors execute parallel │
│  Each connector translates SubQuery → native query       │
│  (Query DSL, SQL, PromQL, FilterExpression, BSON, etc.) │
└────────────────────────┬────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────┐
│ Merge                                                    │
│  Hash join / UNION ALL / re-aggregate / sort / limit    │
│  All operations on Arrow RecordBatch in memory          │
└────────────────────────┬────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────┐
│ Response                                                 │
│  Serialize → JSON (rows + columns + metadata)           │
│  Cache result (Redis or in-memory LRU)                  │
│  Record audit entry                                      │
└─────────────────────────────────────────────────────────┘
```

### Timing Budget (typical cross-datasource JOIN)

| Phase | Time | Notes |
|-------|------|-------|
| Middleware | < 1ms | Auth lookup, rate limit check |
| Parse | 1–5ms | DataFusion SQL parser |
| Plan | 1–3ms | SubQuery construction, pushdown |
| Fan-out | 10–100ms | Dominated by slowest connector |
| Merge | 1–20ms | Hash join, sort |
| Serialize | 1–5ms | JSON encoding |
| **Total** | **15–130ms** | |

## 2. Connector Trait Design

Every datasource implements `FederatedConnector`:

```rust
#[async_trait]
pub trait FederatedConnector: Send + Sync + Debug {
    fn id(&self) -> &str;
    fn connector_type(&self) -> &str;
    fn capabilities(&self) -> ConnectorCapabilities;
    async fn health_check(&self) -> ConnectorHealth;
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>, ConnectorError>;
    async fn get_schema(&self, table: &str) -> Result<Schema, ConnectorError>;
    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>, ConnectorError>;
    async fn execute_streaming(&self, query: &SubQuery, tx: Sender<Result<RecordBatch, ConnectorError>>) -> Result<(), ConnectorError>;
}
```

### SubQuery

The planner's output — a connector-agnostic query fragment:

```rust
pub struct SubQuery {
    pub table: String,
    pub projections: Vec<String>,       // SELECT columns
    pub filter: Option<FilterExpr>,     // WHERE (tree of And/Or/Not/Comparison/In/IsNull)
    pub aggregations: Vec<AggregationExpr>,  // GROUP BY aggregates
    pub group_by: Vec<String>,
    pub having: Option<FilterExpr>,
    pub sort: Vec<SortExpr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}
```

Each connector translates `SubQuery` into its native language:

| Connector | SubQuery.filter → | SubQuery.aggregations → |
|-----------|-------------------|------------------------|
| OpenSearch | Query DSL `bool.filter` | `aggs` with `terms`/`avg`/`sum` |
| PostgreSQL | SQL `WHERE` clause | SQL `GROUP BY` + aggregate functions |
| DynamoDB | `FilterExpression` + `ExpressionAttributeValues` | Client-side (no pushdown) |
| Prometheus | PromQL label matchers + time range | PromQL `rate()`, `sum()` |
| MongoDB | BSON `$match` pipeline stage | `$group` pipeline stage |
| S3 Parquet | Arrow predicate pushdown on row groups | Client-side |

### ConnectorCapabilities

Declares what the planner can push down:

```rust
pub struct ConnectorCapabilities {
    pub supports_filtering: bool,
    pub supports_projection: bool,
    pub supports_aggregation: bool,
    pub supports_sorting: bool,
    pub supports_limit: bool,
    pub supports_join: bool,          // false for all current connectors
    pub max_concurrent_queries: usize,
    pub supports_streaming: bool,
    pub latency_class: LatencyClass,  // Low, Medium, High
}
```

The planner checks capabilities before adding operations to the SubQuery. Unsupported operations stay in the merge phase.

### ConnectorFactory

Connectors are registered via factories:

```rust
pub trait ConnectorFactory: Send + Sync {
    fn connector_type(&self) -> &str;
    fn create(&self, id: &str, config: &serde_json::Value) -> Result<Arc<dyn FederatedConnector>>;
}
```

`main.rs` registers all factories at startup. The `ConnectorRegistry` maps datasource IDs to connector instances.

## 3. Query Planner Internals

### Parse → LogicalPlan

SQL and PPL both produce a DataFusion `LogicalPlan`:

```
SQL: "SELECT service, count(*) FROM cluster_a.logs WHERE status >= 500 GROUP BY service"
                    ↓ DataFusion parser
PPL: "source = cluster_a.logs | where status >= 500 | stats count() by service"
                    ↓ PPL parser → DataFusion plan
                    
LogicalPlan::Aggregate {
    input: LogicalPlan::Filter {
        predicate: col("status") >= lit(500),
        input: LogicalPlan::TableScan { table: "cluster_a.logs" }
    },
    group_expr: [col("service")],
    aggr_expr: [count(*)]
}
```

### Rewrite Rules

The optimizer applies rewrite rules before SubQuery construction:

1. **Predicate pushdown** — move filters as close to the scan as possible
2. **Projection pruning** — remove unused columns from scans
3. **Aggregation pushdown** — push GROUP BY to connectors that support it
4. **Limit pushdown** — push LIMIT to connectors (respecting sort order)
5. **Sort elimination** — remove sorts that are redundant after a later sort
6. **Top-N pushdown** — combine ORDER BY + LIMIT into a single pushdown

Each rule checks `ConnectorCapabilities` before applying. If a connector doesn't support aggregation, the GROUP BY stays in the merge phase.

### Cost Estimation

```rust
pub struct CostEstimate {
    pub cpu_cost: f64,      // local compute cost
    pub network_cost: f64,  // data transfer cost
}

pub fn estimate_remote_cost(
    caps: &ConnectorCapabilities,
    stats: &TableStats,
    workload: &QueryWorkload,
) -> CostEstimate
```

Cost factors:
- **Selectivity** — estimated fraction of rows matching filters (default: 0.1 for equality, 0.33 for range)
- **Network** — `estimated_rows × avg_row_bytes` (reduced by projection and filter pushdown)
- **Latency class** — Low (1x), Medium (3x), High (10x) multiplier on network cost
- **Concurrency** — `max_concurrent_queries` limits parallel fan-out

The planner uses costs to decide:
- **Join order** — smaller (cheaper) side becomes the build side
- **Build vs probe** — build the hash table from the side with fewer estimated rows
- **Pushdown depth** — push more operations to high-latency connectors to reduce network cost

## 4. Join Engine

Located in `crates/fuse-engine/src/join.rs`.

### Join Types

| Type | Behavior | SQL |
|------|----------|-----|
| `Inner` | Rows matching in both sides | `JOIN` |
| `Left` | All left rows + matching right (NULL if no match) | `LEFT JOIN` |
| `Right` | Swaps sides, executes as Left | `RIGHT JOIN` |
| `Full` | All rows from both sides, NULLs where no match | `FULL OUTER JOIN` |
| `Semi` | Left rows that have a match in right | `WHERE EXISTS (...)` |
| `Anti` | Left rows that have NO match in right | `WHERE NOT EXISTS (...)` |

### Join Strategies

```rust
pub enum JoinStrategy {
    HashJoin,       // Build hash table from build side, probe with probe side
    SemiJoinPush,   // Extract keys → push IN-list filter → hash join filtered results
}
```

**HashJoin flow:**
1. Collect all RecordBatches from the build side
2. Build a `HashMap<key_value, Vec<row_index>>` from the join key column
3. For each probe-side batch, look up each key in the hash table
4. Emit matched rows (with NULL padding for outer joins)

**SemiJoinPush flow** (the key Fuse optimization):
1. Execute the build side query
2. Extract distinct join key values from the result
3. Construct an `IN (key1, key2, ...)` filter
4. Push the IN-list filter to the probe side's SubQuery
5. Execute the probe side (now filtered — much less data)
6. Hash join the two filtered result sets

SemiJoinPush is chosen when:
- The build side has < 10,000 distinct keys
- The probe-side connector supports `IN` filter pushdown
- Estimated probe-side reduction > 10x

### Build Side Selection

The planner picks the smaller side as the build side:

```
estimated_rows(left) < estimated_rows(right)
    → left = build, right = probe
    
estimated_rows(right) < estimated_rows(left)
    → right = build, left = probe
```

For `RIGHT JOIN`, the engine swaps sides and executes as `LEFT JOIN`.

## 5. Caching Layer

Two-tier cache: plan cache (parsed query plans) and result cache (query results).

### Plan Cache

In-memory LRU. Key: `"{format}:{query_text}"`. Stores the parsed LogicalPlan and SubQuery list. Avoids re-parsing identical queries.

### Result Cache

**Single-instance mode:** In-memory LRU with configurable max entries.

**Stateless mode:** Redis with TTL.

```rust
pub enum RedisResultCache {
    Redis { pool: Pool<RedisConnectionManager>, ttl: u64 },
    InMemory { cache: Mutex<LruCache<String, Value>>, ttl: u64 },
}
```

Cache key: `"{format}:{query_text}"` — same query with different format (sql vs ppl) gets separate cache entries.

Cache invalidation:
- **TTL-based** — entries expire after `cache_ttl_secs`
- **Materialized view refresh** — replaces the cached result
- **No explicit invalidation** — underlying data changes are not tracked (eventual consistency)

### Materialized View Cache

Materialized views use a separate `MaterializedViewRegistry`:

```rust
pub struct MaterializedViewRegistry {
    views: RwLock<HashMap<String, ViewEntry>>,  // name → (def, batches, last_refreshed, status)
}
```

- Views store Arrow `RecordBatch` directly (no serialization overhead)
- Refresh is background: `views_needing_refresh()` returns stale views, executor re-runs the query
- `get_stale_ok()` serves stale data during refresh (degraded mode)
- In stateless mode, results are serialized to Redis; leader election via `SET NX` prevents duplicate refreshes

## 6. Enterprise Middleware Pipeline

Requests pass through an ordered middleware chain:

```
Request → Auth → RateLimit → TenantCheck → Timeout → [Handler] → Audit → Response
```

### Auth (`auth.rs`)

1. Extract key from `x-api-key` header or `Authorization: Bearer` header
2. Look up `AuthIdentity { identity, role }` from API key registry
3. Attach identity to request extensions for downstream use
4. Public endpoints (`/health`, `/metrics`) skip auth

### Rate Limiter (`rate_limit.rs`)

Uses the `governor` crate (token bucket algorithm):

```rust
// Two limiters:
RateLimiter<NotKeyed, InMemoryState, DefaultClock>           // global
RateLimiter<String, DefaultKeyedStateStore<String>, DefaultClock>  // per-IP
```

IP detection: `X-Forwarded-For` → `X-Real-IP` → socket address. Returns 429 with `Retry-After` header.

### Tenant Check (`tenant.rs`)

```rust
pub fn can_access(&self, datasource_id: &str) -> bool {
    self.allowed_datasources.is_empty() || self.allowed_datasources.contains(datasource_id)
}
```

Empty allowlist = admin (access to everything). The check runs after parsing, before fan-out — rejected datasources return 403 without touching the connector.

### Query Governor

Per-tenant limits applied during execution:
- `max_time_ms` → sets a `tokio::time::timeout` wrapper around the query
- `max_rows` → `min(query.limit, tenant.max_rows)` applied to SubQuery
- `max_result_bytes` → checked after execution, returns error if exceeded

### Audit (`audit.rs`)

Records every API action as a structured entry:

```rust
pub struct AuditEntry {
    pub timestamp: u64,
    pub identity: String,
    pub action: AuditAction,    // Query, Explain, Validate, ListDatasources, ...
    pub status: AuditStatus,    // Success, Error, Denied
    pub duration_ms: u64,
    pub row_count: Option<u64>,
    pub query: Option<String>,
    pub error: Option<String>,
    pub client_ip: Option<String>,
}
```

Emitted via `tracing` as structured JSON. Also stored in an in-memory ring buffer for the `/api/fuse/history` endpoint.

## 7. WASM Plugin Sandbox

Plugins run in a wasmtime WebAssembly runtime with strict isolation:

```
┌─────────────────────────────────────┐
│ Fuse Server Process                 │
│                                     │
│  ┌───────────────────────────────┐  │
│  │ wasmtime Runtime              │  │
│  │  ┌─────────────────────────┐  │  │
│  │  │ Plugin Instance         │  │  │
│  │  │  - 256MB memory limit   │  │  │
│  │  │  - No filesystem access │  │  │
│  │  │  - No raw network       │  │  │
│  │  │  - SDK host functions   │  │  │
│  │  └─────────────────────────┘  │  │
│  └───────────────────────────────┘  │
│                                     │
│  Host functions exposed to WASM:    │
│  - http_get(url) → bytes           │
│  - http_post(url, body) → bytes    │
│  - log(level, message)             │
└─────────────────────────────────────┘
```

### Plugin Loading

1. Scan `plugins/` directory for `manifest.toml` files
2. Parse manifest: name, version, connector_type, wasm path, config schema
3. Compile `.wasm` module with wasmtime (cached after first compile)
4. Instantiate with memory limits and host function imports
5. Call `init(config)` with datasource config from `fuse.toml`
6. Register as a `FederatedConnector` in the `ConnectorRegistry`

### Execution Flow

```
SubQuery → serialize to JSON → call plugin.execute(json) via WASM
         → plugin translates to native query
         → plugin calls http_get/http_post (host function)
         → plugin returns Arrow IPC bytes
         → deserialize to RecordBatch
```

Plugins execute on a blocking thread (`tokio::task::spawn_blocking`) since WASM execution is synchronous.

## 8. Extension Points for Contributors

### Add a New Built-In Connector

1. Create `crates/fuse-connectors/my-source/`
2. Implement `FederatedConnector` trait
3. Implement `ConnectorFactory` trait
4. Register factory in `main.rs`
5. Add integration tests in `crates/fuse-connectors/my-source/tests/`

Key files to modify:
- `Cargo.toml` (workspace members)
- `crates/fuse-server/src/main.rs` (factory registration)
- `docs-site/src/connectors.md` (documentation)

See [Connector Development Guide](./connector-development-guide.md).

### Add a New SQL Function

1. Register the function in `crates/fuse-engine/src/` via DataFusion's UDF API
2. Add pushdown translation in relevant connectors (if the function maps to a native operation)
3. Add tests

### Add a New Rewrite Rule

1. Add rule in `crates/fuse-engine/src/optimizer.rs`
2. Rules implement DataFusion's `OptimizerRule` trait
3. Check `ConnectorCapabilities` before applying pushdown
4. Add cost model adjustments in `crates/fuse-engine/src/cost.rs`

### Add a New API Endpoint

1. Add handler in `crates/fuse-server/src/api.rs`
2. Add route in `crates/fuse-server/src/lib.rs`
3. Add audit action variant in `crates/fuse-server/src/audit.rs`
4. Add role check if the endpoint requires specific permissions
5. Update OpenAPI spec at `docs/api/openapi.yaml`

### Crate Map

```
crates/
├── fuse-core/           ← traits, types, config, error (no business logic)
│   ├── connector.rs     ← FederatedConnector, SubQuery, FilterExpr
│   ├── registry.rs      ← ConnectorRegistry, ConnectorFactory
│   ├── config.rs        ← FuseConfig, EngineConfig
│   └── materialized_view.rs ← MaterializedViewRegistry
├── fuse-engine/         ← query planning and execution
│   ├── planner.rs       ← LogicalPlan → SubQuery conversion
│   ├── optimizer.rs     ← pushdown rules
│   ├── cost.rs          ← cost estimation
│   ├── join.rs          ← HashJoin, SemiJoinPush
│   ├── merger.rs        ← UNION ALL, re-aggregation
│   ├── rewrite.rs       ← SQL rewrite rules
│   ├── anomaly.rs       ← anomaly detection primitives
│   └── plan.rs          ← PlanNode (EXPLAIN output)
├── fuse-server/         ← HTTP server, middleware, API
│   ├── api.rs           ← all endpoint handlers
│   ├── auth.rs          ← API key auth, RBAC
│   ├── tenant.rs        ← multi-tenancy isolation
│   ├── audit.rs         ← audit logging
│   ├── rate_limit.rs    ← governor-based rate limiting
│   ├── redis_cache.rs   ← Redis/in-memory result cache
│   └── main.rs          ← startup, factory registration
└── fuse-connectors/     ← one sub-crate per connector type
    ├── opensearch/
    ├── postgres/        ← also MySQL, Redshift, SQLite
    ├── dynamodb/
    ├── s3/
    ├── prometheus/
    └── ...
```
