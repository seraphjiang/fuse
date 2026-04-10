# Fuse Architecture: Federated Query Execution Model

This document describes how Fuse executes a federated query — from SQL/PPL input to merged response — across 14 heterogeneous connectors.

## High-Level Flow

```
                          ┌──────────────┐
                          │  SQL or PPL  │
                          └──────┬───────┘
                                 │
                          ┌──────▼───────┐
                     ┌────│    Parse     │
                     │    └──────┬───────┘
                     │           │
                PPL? │    ┌──────▼───────┐
                yes──┘    │  PPL→SQL     │
                          │  Translation │
                          └──────┬───────┘
                                 │
                          ┌──────▼───────┐
                          │  Classify    │
                          │  Query Type  │
                          └──┬───┬───┬───┘
                             │   │   │
                   ┌─────────┘   │   └─────────┐
                   ▼             ▼             ▼
              Single-Source   UNION ALL     JOIN
                   │             │             │
                   ▼             ▼             ▼
              ┌─────────┐  ┌─────────┐  ┌─────────┐
              │SubQuery │  │SubQuery │  │SubQuery │
              │ build   │  │per src  │  │per side │
              └────┬────┘  └────┬────┘  └────┬────┘
                   │            │            │
              ┌────▼────┐  ┌───▼────┐  ┌────▼────┐
              │Connector│  │Fan-out │  │Parallel │
              │execute()│  │(tokio  │  │fetch    │
              └────┬────┘  │spawn)  │  │(join!)  │
                   │       └───┬────┘  └────┬────┘
                   │           │            │
                   │      ┌────▼────┐  ┌────▼────┐
                   │      │ Merge   │  │HashJoin │
                   │      │ + align │  │+ merge  │
                   │      └────┬────┘  └────┬────┘
                   │           │            │
                   └─────┬─────┘────────────┘
                         │
                  ┌──────▼───────┐
                  │Post-Process  │
                  │ re-aggregate │
                  │ ORDER BY     │
                  │ DISTINCT     │
                  │ OFFSET/LIMIT │
                  │ cursor page  │
                  └──────┬───────┘
                         │
                  ┌──────▼───────┐
                  │   Response   │
                  │  (JSON/CSV)  │
                  └──────────────┘
```

## Phase 1: Parse

The query enters via `POST /api/fuse/query` with a `format` field (`sql` or `ppl`).

**SQL path:** The raw SQL string is parsed to extract datasource.table references (e.g., `cluster_a.application_logs`). The parser identifies the query type — single-source, UNION/UNION ALL, or JOIN — by inspecting the FROM clause and set operators.

**PPL path:** If the format is `ppl` (or the query starts with `source`/`search`), the PPL parser (`fuse-engine/src/ppl.rs`) tokenizes the pipe-delimited query into a `PplQuery` struct containing source tables and a chain of commands (`where`, `stats`, `sort`, `head`, `fields`, `lookup`, `dedup`, `eval`, `rename`, `top`, `rare`). This is then translated to equivalent SQL via `ppl_to_sql()`.

**CTE resolution:** If the query contains `WITH` clauses, `resolve_ctes()` executes each CTE subquery first and registers the results as temporary in-memory datasources, making them available to the main query.

**Correlated subqueries:** `IN (SELECT ...)` subqueries are detected by `extract_in_subqueries()`. Each inner query is executed against its connector first, and the results are inlined as a literal `IN (val1, val2, ...)` list before the main query executes.

## Phase 2: Plan — SubQuery Construction

The SQL string is converted into a `SubQuery` struct via `sql_to_subquery()`:

```rust
pub struct SubQuery {
    pub table: String,
    pub projections: Vec<String>,       // SELECT columns
    pub filter: Option<FilterExpr>,     // WHERE clause
    pub aggregations: Vec<AggregationExpr>, // COUNT, SUM, AVG, etc.
    pub group_by: Vec<String>,          // GROUP BY columns
    pub having: Option<FilterExpr>,     // HAVING clause
    pub sort: Vec<SortExpr>,            // ORDER BY
    pub limit: Option<u64>,             // LIMIT
    pub passthrough: Option<Value>,     // Connector-specific (e.g., Prometheus range params)
}
```

This is the universal query representation that every connector understands. The `SubQuery` is connector-agnostic — each connector translates it into its native query language (Query DSL for OpenSearch, SQL for PostgreSQL/ClickHouse, BSON for MongoDB, InfluxQL for InfluxDB, etc.).

### Pushdown Rewrite

For multi-source queries (UNION ALL), `push_down_to_sources()` copies applicable clauses from the parsed base query into each per-source `SubQuery`:

- **Filter** → pushed to all sources (reduces data transferred)
- **Projections** → pushed to all sources (column pruning)
- **Sort** → pushed to all sources (pre-sorted for merge)
- **Limit** → pushed to all sources (each source gets the full limit; global limit applied after merge)
- **Aggregations + GROUP BY + HAVING** → pushed to all sources (partial aggregation)

The cost estimator (`fuse-engine/src/cost.rs`) decides whether pushdown is beneficial by comparing estimated remote vs. local execution cost, factoring in connector capabilities and latency class.

## Phase 3: Fan-Out to Connectors

Execution diverges based on query type:

### Single-Source

Direct call to `connector.execute(&sub_query)`. The connector translates the `SubQuery` to its native format and returns Arrow `RecordBatch`es.

### UNION ALL

Each source gets its own `SubQuery` (with pushdown applied). Sources execute in parallel via `tokio::spawn`:

```
┌──────────┐  ┌──────────┐  ┌──────────┐
│ OS query │  │ CW query │  │ S3 query │
│ (spawn)  │  │ (spawn)  │  │ (spawn)  │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │              │              │
     └──────────────┼──────────────┘
                    ▼
              union_batches()
```

Each result set is tagged with a `_datasource` column identifying its origin. Schema alignment (`union_schema` + `align_batch`) handles type mismatches across sources by widening to a common type (e.g., Int32 + Int64 → Int64, any + Utf8 → Utf8).

Partial failures are tolerated — if one source errors, the others still return results with the error reported in `partial_errors`.

### JOIN

Both sides execute in parallel via `tokio::join!`. The planner selects the smaller table as the build side for memory efficiency:

```
┌─────────────────┐    ┌─────────────────┐
│  Left (probe)   │    │ Right (build)   │
│  e.g., OS logs  │    │ e.g., DDB users │
└────────┬────────┘    └────────┬────────┘
         │                      │
         └──────────┬───────────┘
                    ▼
            hash_join(left, key, right, key, type)
```

**Join types:** Inner, Left, Semi (EXISTS), Anti (NOT EXISTS).

**Join strategies** (selected by `plan_join()` based on cost estimation):
- **Hash join** — build a hash table from the smaller side, probe with the larger side
- **Semi-join optimization** — when the build side is small enough (< threshold), extract join keys and push them as an `IN` filter to the probe side's connector, reducing data transfer

## Phase 4: Merge and Re-Aggregate

After fan-out results return:

1. **Schema alignment** — `union_schema()` computes the widest common schema; `align_batch()` casts each batch to match
2. **UNION dedup** — for plain `UNION` (not `UNION ALL`), `dedup_batches()` removes duplicate rows across all non-`_datasource` columns
3. **Cross-datasource GROUP BY** — `reaggregate_batches()` performs a second-pass aggregation when GROUP BY spans multiple sources. Each source returns partial aggregates; the merger sums numeric columns per group key to produce correct global totals
4. **Global ORDER BY** — `sort_batches()` applies multi-column sorting with mixed ASC/DESC
5. **DISTINCT** — deduplication on all projected columns
6. **OFFSET + LIMIT** — applied after all other post-processing
7. **Cursor pagination** — keyset-based: `cursor` offset is added to SQL OFFSET; `next_cursor` is returned when more rows exist beyond the page

## The Connector Trait

Every datasource implements `FederatedConnector`:

```rust
pub trait FederatedConnector: Send + Sync + Debug {
    fn id(&self) -> &str;                    // Instance identifier
    fn connector_type(&self) -> &str;        // "opensearch", "dynamodb", etc.
    fn capabilities(&self) -> ConnectorCapabilities;  // What can be pushed down
    async fn health_check(&self) -> ConnectorHealth;
    async fn discover_schemas(&self) -> Result<Vec<SchemaInfo>>;  // List tables
    async fn get_schema(&self, table: &str) -> Result<Schema>;    // Arrow schema
    async fn execute(&self, query: &SubQuery) -> Result<Vec<RecordBatch>>;
    async fn execute_streaming(&self, query: &SubQuery, tx: Sender<...>);
}
```

### Capabilities Declaration

Each connector declares what it supports. The planner uses this to decide what to push down vs. compute locally:

```rust
pub struct ConnectorCapabilities {
    pub supports_filtering: bool,
    pub supports_projection: bool,
    pub supports_aggregation: bool,
    pub supports_sorting: bool,
    pub supports_limit: bool,
    pub supports_join: bool,
    pub max_concurrent_queries: usize,
    pub supports_streaming: bool,
    pub latency_class: LatencyClass,  // Low, Medium, High
}
```

### How Pushdown Works Per Connector

The `SubQuery` is the contract. Each connector translates it to its native language:

| Connector | Native Translation |
|-----------|-------------------|
| OpenSearch | `SubQuery.filter` → Query DSL (`bool/must/range/term`), `projections` → `_source`, `sort` → `sort`, `limit` → `size`, deep pagination → `search_after` |
| Elasticsearch | Same as OpenSearch (Query DSL), plus API key auth |
| PostgreSQL / MySQL | `SubQuery` → full SQL string (`SELECT ... WHERE ... GROUP BY ... ORDER BY ... LIMIT`) |
| ClickHouse | Full SQL passthrough via HTTP interface (`JSONEachRow` response format) |
| DynamoDB | `filter` → `FilterExpression` (Scan) or `KeyConditionExpression` (Query), `projections` → `ProjectionExpression` |
| MongoDB | `filter` → BSON query document, `projections` → projection document, `limit` → `FindOptions.limit` |
| S3 (Parquet) | `projections` → column pruning (read only needed columns), `limit` → early row-group termination |
| Prometheus | `filter` → PromQL label matchers, time range via `passthrough` (start/end/step) |
| InfluxDB | `filter` → InfluxQL `WHERE` clause, time range pushdown |
| CloudWatch | `filter` → CloudWatch Logs Insights filter pattern, time range + log group |
| Redis | Key pattern via `SCAN`, type-specific reads (hash fields, string values) |
| CSV/JSON | Schema inference on first read, in-memory filtering |

### Connector Registration

At startup, `FuseEngine::new()` iterates over all connectors in the registry, discovers their tables, and registers each as a DataFusion `FederatedTableProviderAdaptor`. This enables DataFusion's optimizer to route queries to the correct connector via the federation layer.

```rust
// For each connector:
for (ds_name, connector) in registry.connectors() {
    let table_names = connector.table_names().await?;
    let executor = Arc::new(FuseExecutor::new(ds_name, connector));
    let provider = Arc::new(SQLFederationProvider::new(executor));

    for table_name in &table_names {
        let schema = connector.get_table_schema(table_name).await?;
        // Register as "datasource.table" for qualified access
        ctx.register_table(&format!("{}.{}", ds_name, table_name), adaptor);
    }
}
```

## Cost Estimator

The cost estimator (`fuse-engine/src/cost.rs`) compares remote execution (pushdown) vs. local execution for each operation:

**Remote cost** factors:
- Latency class multiplier (Low=1.0, Medium=2.0, High=5.0)
- Estimated result rows after filter selectivity and aggregation reduction
- Network transfer = result_rows × row_bytes × latency factor

**Local cost** factors:
- Full table scan (all rows transferred)
- CPU cost for local filtering/aggregation

**Decision:** `should_push_down()` returns true when `remote_cost.total <= local_cost.total`. The join planner (`plan_join()`) uses the same cost model to select the build side (smaller/cheaper table) and join strategy (hash vs. semi-join with key extraction).

## Execution Profile

When `analyze: true` is set on the request, the response includes an `execution_profile` tree showing:

```json
{
  "execution_profile": {
    "total_ms": 142,
    "nodes": [{
      "operator": "HashJoin",
      "rows": 47,
      "time_ms": 12,
      "children": [
        {"operator": "Scan", "datasource": "cluster_a", "rows": 200, "time_ms": 89, "data_bytes": 48000, "pushdown": ["probe side"]},
        {"operator": "Scan", "datasource": "dynamodb", "rows": 50, "time_ms": 41, "data_bytes": 3200, "pushdown": ["build side (smaller)"]}
      ]
    }]
  }
}
```

## Crate Structure

```
fuse-core/          Connector trait, SubQuery, FilterExpr, capabilities, registry, config
fuse-engine/
  ├── planner.rs    FuseEngine — DataFusion session setup, table registration
  ├── ppl.rs        PPL parser and PPL→SQL translator
  ├── sql_to_subquery.rs  SQL→SubQuery conversion
  ├── rewrite.rs    Pushdown rewrite (filter, projection, sort, limit, agg)
  ├── cost.rs       Cost estimator, pushdown decisions, join planning
  ├── join.rs       Hash join, semi-join, anti-join, build-side selection
  ├── merger.rs     Schema alignment, UNION, sort, dedup, merge
  ├── cache.rs      Query result cache (per-connector TTL)
  └── materialized.rs  Materialized views (scheduled refresh)
fuse-server/
  ├── api.rs        REST handlers: query_handler (orchestrates the full flow)
  ├── streaming.rs  SSE streaming endpoint
  └── health.rs     Connector health aggregation
fuse-connectors/    14 connector implementations
fuse-connector-sdk/ Mock utilities for connector development
```
