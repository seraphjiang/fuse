// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use fuse_core::registry::ConnectorRegistry;
use fuse_core::alerting::{AlertEvaluator, AlertRule};
use fuse_engine::materialized::MaterializedViewRegistry;
use crate::history::QueryHistory;
use crate::saved_queries::SavedQueryRegistry;

use crate::health;

/// Tracks running queries for cancellation.
pub struct RunningQueries {
    inner: std::sync::Mutex<std::collections::HashMap<String, CancellationToken>>,
}

impl RunningQueries {
    pub fn new() -> Self {
        Self { inner: std::sync::Mutex::new(std::collections::HashMap::new()) }
    }
    fn insert(&self, id: String, token: CancellationToken) {
        self.inner.lock().unwrap().insert(id, token);
    }
    fn remove(&self, id: &str) {
        self.inner.lock().unwrap().remove(id);
    }
    /// Cancel a running query. Returns true if found.
    pub fn cancel(&self, id: &str) -> bool {
        if let Some(token) = self.inner.lock().unwrap().remove(id) {
            token.cancel();
            true
        } else {
            false
        }
    }
    pub fn list(&self) -> Vec<String> {
        self.inner.lock().unwrap().keys().cloned().collect()
    }
}

/// Shared application state passed to all handlers.
pub struct AppState {
    pub registry: Arc<ConnectorRegistry>,
    #[allow(dead_code)]
    pub alert_rules: Vec<AlertRule>,
    pub view_registry: Arc<MaterializedViewRegistry>,
    pub history: Arc<QueryHistory>,
    pub running_queries: Arc<RunningQueries>,
    pub saved_queries: Arc<SavedQueryRegistry>,
}

/// Result from multi-datasource execution, carrying batches + per-source stats.
struct FederatedResult {
    batches: Vec<arrow::record_batch::RecordBatch>,
    stats: Option<std::collections::HashMap<String, DatasourceStat>>,
    datasources: Vec<String>,
    profile_nodes: Vec<ProfileNode>,
    /// Per-datasource errors for partial failure reporting.
    partial_errors: Vec<PartialError>,
}

#[derive(Serialize, Clone, Debug)]
pub struct PartialError {
    pub datasource: String,
    pub error: String,
}

// ── Request / Response types ──

#[derive(Deserialize)]
pub struct QueryRequest {
    pub query: String,
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default)]
    pub analyze: bool,
    /// Per-query timeout in milliseconds. If not set, uses server default (30s).
    pub timeout_ms: Option<u64>,
    /// Output format: "json" (default) or "csv".
    #[serde(default = "default_result_format")]
    pub result_format: String,
    /// Named query parameters. Keys without $ prefix are matched against $key in query.
    #[serde(default)]
    pub params: std::collections::HashMap<String, serde_json::Value>,
}

fn default_format() -> String {
    "sql".to_string()
}

fn default_result_format() -> String {
    "json".to_string()
}

const DEFAULT_TIMEOUT_MS: u64 = 30_000;

#[derive(Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub metadata: QueryMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_profile: Option<ExecutionProfile>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub partial_errors: Vec<PartialError>,
}

#[derive(Serialize)]
pub struct QueryMetadata {
    pub total_rows: u64,
    pub format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasources_queried: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasource_stats: Option<std::collections::HashMap<String, DatasourceStat>>,
}

#[derive(Serialize, Clone, Debug)]
pub struct DatasourceStat {
    pub rows: u64,
    pub latency_ms: u64,
}

#[derive(Serialize, Clone, Debug)]
pub struct ExecutionProfile {
    pub total_ms: u64,
    pub nodes: Vec<ProfileNode>,
}

#[derive(Serialize, Clone, Debug)]
pub struct ProfileNode {
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasource: Option<String>,
    pub actual_rows: u64,
    pub actual_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub pushdown: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<ProfileNode>,
}

#[derive(Serialize)]
pub struct DatasourceInfo {
    pub id: String,
    pub connector_type: String,
    pub capabilities: fuse_core::connector::ConnectorCapabilities,
}

#[derive(Serialize)]
pub struct ExplainResponse {
    pub plan: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_tree: Option<fuse_engine::plan::PlanNode>,
}

#[derive(Serialize)]
pub struct ValidateResponse {
    pub valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct FieldInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

fn error_json(status: StatusCode, msg: impl ToString) -> impl IntoResponse {
    (
        status,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

/// Substitute `$key` placeholders in query with parameter values.
/// String values are single-quoted and escaped. Numbers/bools are inlined.
fn bind_params(query: &str, params: &std::collections::HashMap<String, serde_json::Value>) -> String {
    // Sort by key length descending to avoid $host matching inside $hostname
    let mut sorted: Vec<_> = params.iter().collect();
    sorted.sort_by(|a, b| b.0.len().cmp(&a.0.len()));

    let mut result = query.to_string();
    for (key, val) in sorted {
        let placeholder = format!("${}", key);
        let replacement = match val {
            serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => "NULL".to_string(),
            other => format!("'{}'", other.to_string().replace('\'', "''")),
        };
        result = result.replace(&placeholder, &replacement);
    }
    result
}

// ── Handlers ──

/// POST /api/fuse/query
pub async fn query_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let t0 = std::time::Instant::now();
    let format = req.format.to_lowercase();

    // Bind parameters if provided
    let query = if req.params.is_empty() {
        req.query.clone()
    } else {
        bind_params(&req.query, &req.params)
    };

    // Parse all datasource.table references from the query
    let refs = match format.as_str() {
        "ppl" => parse_ppl_sources(&query),
        _ => parse_sql_sources(&query),
    };

    let refs = match refs {
        Ok(r) if r.is_empty() => {
            return error_json(StatusCode::BAD_REQUEST, "no datasource.table references found")
                .into_response()
        }
        Ok(r) => r,
        Err(e) => return error_json(StatusCode::BAD_REQUEST, e).into_response(),
    };

    // Validate all datasources exist
    for (ds_id, _) in &refs {
        if state.registry.get(ds_id).is_none() {
            return error_json(
                StatusCode::NOT_FOUND,
                format!("datasource '{}' not found", ds_id),
            )
            .into_response();
        }
    }

    let timeout = std::time::Duration::from_millis(req.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));

    // Register cancellable query
    let query_id = format!("q-{:016x}", t0.elapsed().as_nanos() ^ (std::process::id() as u128));
    let cancel_token = CancellationToken::new();
    state.running_queries.insert(query_id.clone(), cancel_token.clone());

    let exec_future = async {
        if refs.len() == 1 {
            execute_single(&state, &query, &format, &refs[0]).await
        } else if format == "ppl" || is_union_query(&query) {
            execute_union(&state, &query, &format, &refs).await
        } else {
            execute_join(&state, &refs).await
        }
    };

    let result = tokio::select! {
        r = tokio::time::timeout(timeout, exec_future) => match r {
            Ok(r) => r,
            Err(_) => Err(format!("query timed out after {}ms", timeout.as_millis())),
        },
        _ = cancel_token.cancelled() => Err("query cancelled".into()),
    };

    state.running_queries.remove(&query_id);

    match result {
        Ok(fed) => {
            let order_by = parse_order_by(&query);
            let limit = parse_limit(&query);
            let is_distinct = strip_string_literals(&query).to_lowercase().contains("select distinct");

            // Apply global ORDER BY if present
            let batches = if let Some((col, desc)) = &order_by {
                if let Ok(schema) = fed.batches.first().map(|b| b.schema()).ok_or(()) {
                    if let Ok(idx) = schema.index_of(col) {
                        fuse_engine::sort_batches(fed.batches, &[idx], &[*desc], None)
                            .unwrap_or_default()
                    } else {
                        fed.batches
                    }
                } else {
                    fed.batches
                }
            } else {
                fed.batches
            };

            // Apply DISTINCT — dedup on all non-_datasource columns
            let batches = if is_distinct && !batches.is_empty() {
                let dedup_cols: Vec<String> = batches[0].schema().fields().iter()
                    .map(|f| f.name().clone())
                    .filter(|n| n != "_datasource")
                    .collect();
                let col_refs: Vec<&str> = dedup_cols.iter().map(|s| s.as_str()).collect();
                fuse_engine::dedup_batches(batches, &col_refs).unwrap_or_default()
            } else {
                batches
            };

            // Apply global OFFSET + LIMIT
            let offset = parse_offset(&query).unwrap_or(0);
            let batches = if offset > 0 || limit.is_some() {
                // Flatten, skip offset, take limit
                let all_rows: Vec<_> = batches.iter()
                    .flat_map(|b| (0..b.num_rows()).map(move |i| (b, i)))
                    .skip(offset)
                    .collect();
                let take_n = limit.unwrap_or(all_rows.len());
                let rows_to_take: Vec<_> = all_rows.into_iter().take(take_n).collect();
                if rows_to_take.is_empty() || batches.is_empty() {
                    vec![]
                } else {
                    // Rebuild batches from selected rows
                    let schema = batches[0].schema();
                    let mut result_cols: Vec<Vec<arrow::array::ArrayRef>> = (0..schema.fields().len()).map(|_| Vec::new()).collect();
                    for (batch, row_idx) in &rows_to_take {
                        for col_idx in 0..schema.fields().len() {
                            result_cols[col_idx].push(batch.column(col_idx).slice(*row_idx, 1));
                        }
                    }
                    let arrays: Result<Vec<arrow::array::ArrayRef>, _> = result_cols.into_iter()
                        .map(|slices| {
                            let refs: Vec<&dyn arrow::array::Array> = slices.iter().map(|a| a.as_ref()).collect();
                            arrow::compute::concat(&refs)
                        })
                        .collect();
                    match arrays.and_then(|a| arrow::record_batch::RecordBatch::try_new(schema, a)) {
                        Ok(batch) => vec![batch],
                        Err(_) => vec![],
                    }
                }
            } else {
                batches
            };
            let (columns, rows) = batches_to_json(&batches);
            let total_rows = rows.len() as u64;
            state.history.push(crate::history::HistoryEntry {
                query: req.query.clone(),
                format: req.format.clone(),
                timestamp: crate::history::now_secs(),
                latency_ms: t0.elapsed().as_millis() as u64,
                row_count: total_rows,
                error: None,
            });
            if req.result_format == "csv" {
                let csv = batches_to_csv(&batches);
                (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "text/csv")],
                    csv,
                ).into_response()
            } else {
                Json(QueryResponse {
                    columns,
                    rows,
                    metadata: QueryMetadata {
                        total_rows,
                        format: req.format,
                        datasources_queried: if fed.datasources.len() > 1 {
                            Some(fed.datasources)
                        } else {
                            None
                        },
                        datasource_stats: fed.stats,
                    },
                    execution_profile: if req.analyze {
                        Some(ExecutionProfile {
                            total_ms: t0.elapsed().as_millis() as u64,
                            nodes: fed.profile_nodes,
                        })
                    } else {
                        None
                    },
                    partial_errors: fed.partial_errors,
                })
                .into_response()
            }
        }
        Err(e) => {
            state.history.push(crate::history::HistoryEntry {
                query: req.query.clone(),
                format: req.format.clone(),
                timestamp: crate::history::now_secs(),
                latency_ms: t0.elapsed().as_millis() as u64,
                row_count: 0,
                error: Some(e.clone()),
            });
            error_json(StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
    }
}

/// Execute a single-datasource query.
async fn execute_single(
    state: &AppState,
    query: &str,
    format: &str,
    (ds_id, table): &(String, String),
) -> Result<FederatedResult, String> {
    let connector = state.registry.get(ds_id)
        .ok_or_else(|| format!("datasource '{}' not found", ds_id))?;
    let sub_query = build_sub_query(query, format, table)?;
    let start = std::time::Instant::now();
    let batches = connector.execute(&sub_query).await.map_err(|e| e.to_string())?;
    let elapsed = start.elapsed().as_millis() as u64;
    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let data_bytes: u64 = batches.iter().map(|b| b.get_array_memory_size() as u64).sum();
    Ok(FederatedResult {
        batches,
        stats: None,
        datasources: vec![ds_id.clone()],
        profile_nodes: vec![ProfileNode {
            op: "RemoteScan".into(),
            datasource: Some(ds_id.clone()),
            actual_rows: row_count,
            actual_ms: elapsed,
            data_bytes: Some(data_bytes),
            pushdown: vec![],
            children: vec![],
        }],
        partial_errors: vec![],
    })
}

/// Execute a UNION ALL query — fan out to each connector in parallel, merge.
async fn execute_union(
    state: &AppState,
    query: &str,
    format: &str,
    refs: &[(String, String)],
) -> Result<FederatedResult, String> {
    let base_sq = build_sub_query(query, format, &refs[0].1)
        .unwrap_or_else(|_| fuse_core::connector::SubQuery {
            table: String::new(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            passthrough: None,
        });

    let mut per_source: Vec<fuse_core::connector::SubQuery> = refs
        .iter()
        .map(|(_, table)| fuse_core::connector::SubQuery {
            table: table.clone(),
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            passthrough: None,
        })
        .collect();
    fuse_engine::rewrite::push_down_to_sources(&base_sq, &mut per_source);

    let mut handles = Vec::new();
    for (i, (ds_id, _)) in refs.iter().enumerate() {
        let connector = match state.registry.get(ds_id) {
            Some(c) => c,
            None => continue, // skip missing datasource
        };
        let sub_query = per_source[i].clone();
        let conn = connector.clone();
        let ds = ds_id.clone();
        handles.push(tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = conn.execute(&sub_query).await;
            let latency_ms = start.elapsed().as_millis() as u64;
            (ds, result, latency_ms)
        }));
    }

    let mut batch_sets = Vec::new();
    let mut ds_stats = std::collections::HashMap::new();
    let mut datasources = Vec::new();
    let mut scan_nodes = Vec::new();
    let mut partial_errors = Vec::new();

    for handle in handles {
        let (ds_id, result, latency_ms) = handle.await.map_err(|e| format!("task join error: {e}"))?;
        datasources.push(ds_id.clone());

        match result {
            Ok(batches) => {
                let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                let data_bytes: u64 = batches.iter().map(|b| b.get_array_memory_size() as u64).sum();
                let tagged = add_datasource_column(&batches, &ds_id);

                scan_nodes.push(ProfileNode {
                    op: "RemoteScan".into(),
                    datasource: Some(ds_id.clone()),
                    actual_rows: row_count,
                    actual_ms: latency_ms,
                    data_bytes: Some(data_bytes),
                    pushdown: vec![],
                    children: vec![],
                });

                ds_stats.insert(ds_id, DatasourceStat { rows: row_count, latency_ms });
                batch_sets.push(tagged);
            }
            Err(e) => {
                partial_errors.push(PartialError { datasource: ds_id.clone(), error: e.to_string() });
                ds_stats.insert(ds_id, DatasourceStat { rows: 0, latency_ms });
            }
        }
    }

    // If ALL sources failed, return error
    if batch_sets.is_empty() && !partial_errors.is_empty() {
        return Err(partial_errors.iter().map(|e| format!("{}: {}", e.datasource, e.error)).collect::<Vec<_>>().join("; "));
    }

    let merged = fuse_engine::union_batches(batch_sets).map_err(|e| e.to_string())?;
    let total_rows: u64 = merged.iter().map(|b| b.num_rows() as u64).sum();

    Ok(FederatedResult {
        batches: merged,
        stats: Some(ds_stats),
        datasources,
        profile_nodes: vec![ProfileNode {
            op: "UnionAll".into(),
            datasource: None,
            actual_rows: total_rows,
            actual_ms: 0,
            data_bytes: None,
            pushdown: vec![],
            children: scan_nodes,
        }],
        partial_errors,
    })
}

/// Execute a cross-datasource JOIN using the join executor.
async fn execute_join(
    state: &AppState,
    refs: &[(String, String)],
) -> Result<FederatedResult, String> {
    if refs.len() != 2 {
        return Err(format!("JOIN requires exactly 2 datasources, got {}", refs.len()));
    }

    let (ds_a, table_a) = &refs[0];
    let (ds_b, table_b) = &refs[1];

    let conn_a = state.registry.get(ds_a)
        .ok_or_else(|| format!("datasource '{}' not found", ds_a))?;
    let conn_b = state.registry.get(ds_b)
        .ok_or_else(|| format!("datasource '{}' not found", ds_b))?;

    let sq_a = fuse_core::connector::SubQuery {
        table: table_a.clone(),
        projections: vec![],
        filter: None,
        aggregations: vec![],
        group_by: vec![],
        sort: vec![],
        limit: None,
        passthrough: None,
    };
    let mut sq_b = sq_a.clone();
    sq_b.table = table_b.clone();

    let start_a = std::time::Instant::now();
    let start_b = start_a;
    let (res_a, res_b) = tokio::join!(conn_a.execute(&sq_a), conn_b.execute(&sq_b));
    let latency_a = start_a.elapsed().as_millis() as u64;
    let latency_b = start_b.elapsed().as_millis() as u64;

    let batches_a = res_a.map_err(|e| e.to_string())?;
    let batches_b = res_b.map_err(|e| e.to_string())?;

    let rows_a: u64 = batches_a.iter().map(|b| b.num_rows() as u64).sum();
    let rows_b: u64 = batches_b.iter().map(|b| b.num_rows() as u64).sum();

    let mut ds_stats = std::collections::HashMap::new();
    ds_stats.insert(ds_a.clone(), DatasourceStat { rows: rows_a, latency_ms: latency_a });
    ds_stats.insert(ds_b.clone(), DatasourceStat { rows: rows_b, latency_ms: latency_b });

    if batches_a.is_empty() || batches_b.is_empty() {
        return Ok(FederatedResult {
            batches: vec![],
            stats: Some(ds_stats),
            datasources: vec![ds_a.clone(), ds_b.clone()],
            profile_nodes: vec![],
            partial_errors: vec![],
        });
    }

    let schema_a = batches_a[0].schema();
    let schema_b = batches_b[0].schema();
    let join_key = find_join_key(&schema_a, &schema_b)
        .ok_or_else(|| "no common column found for JOIN key".to_string())?;

    let tagged_a = add_datasource_column(&batches_a, ds_a);
    let tagged_b = add_datasource_column(&batches_b, ds_b);

    let join_start = std::time::Instant::now();
    let joined = fuse_engine::hash_join(
        &tagged_a,
        &join_key,
        &tagged_b,
        &join_key,
        fuse_engine::JoinType::Inner,
    )
    .map_err(|e| e.to_string())?;
    let join_ms = join_start.elapsed().as_millis() as u64;

    let join_rows: u64 = joined.iter().map(|b| b.num_rows() as u64).sum();
    let bytes_a: u64 = batches_a.iter().map(|b| b.get_array_memory_size() as u64).sum();
    let bytes_b: u64 = batches_b.iter().map(|b| b.get_array_memory_size() as u64).sum();

    let scan_a = ProfileNode {
        op: "RemoteScan".into(),
        datasource: Some(ds_a.clone()),
        actual_rows: rows_a,
        actual_ms: latency_a,
        data_bytes: Some(bytes_a),
        pushdown: vec![],
        children: vec![],
    };
    let scan_b = ProfileNode {
        op: "RemoteScan".into(),
        datasource: Some(ds_b.clone()),
        actual_rows: rows_b,
        actual_ms: latency_b,
        data_bytes: Some(bytes_b),
        pushdown: vec![],
        children: vec![],
    };

    Ok(FederatedResult {
        batches: joined,
        stats: Some(ds_stats),
        datasources: vec![ds_a.clone(), ds_b.clone()],
        profile_nodes: vec![ProfileNode {
            op: "HashJoin".into(),
            datasource: None,
            actual_rows: join_rows,
            actual_ms: join_ms,
            data_bytes: None,
            pushdown: vec![],
            children: vec![scan_a, scan_b],
        }],
        partial_errors: vec![],
    })
}

/// Find the first column name that exists in both schemas.
fn find_join_key(
    a: &arrow::datatypes::SchemaRef,
    b: &arrow::datatypes::SchemaRef,
) -> Option<String> {
    for field in a.fields() {
        if b.field_with_name(field.name()).is_ok() {
            return Some(field.name().clone());
        }
    }
    None
}

/// GET /api/fuse/datasources
pub async fn list_datasources(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let connectors = state.registry.list();
    let infos: Vec<DatasourceInfo> = connectors
        .iter()
        .map(|c| DatasourceInfo {
            id: c.id().to_string(),
            connector_type: c.connector_type().to_string(),
            capabilities: c.capabilities(),
        })
        .collect();
    Json(infos)
}

/// GET /api/fuse/datasources/:id/schemas
pub async fn get_schemas(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let connector = match state.registry.get(&id) {
        Some(c) => c,
        None => {
            return error_json(StatusCode::NOT_FOUND, format!("datasource '{}' not found", id))
                .into_response()
        }
    };

    match connector.discover_schemas().await {
        Ok(schemas) => Json(schemas).into_response(),
        Err(e) => error_json(StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

/// GET /api/fuse/datasources/:id/schemas/:table/fields
pub async fn get_fields(
    State(state): State<Arc<AppState>>,
    Path((id, table)): Path<(String, String)>,
) -> impl IntoResponse {
    let connector = match state.registry.get(&id) {
        Some(c) => c,
        None => {
            return error_json(StatusCode::NOT_FOUND, format!("datasource '{}' not found", id))
                .into_response()
        }
    };

    match connector.get_schema(&table).await {
        Ok(schema) => {
            let fields: Vec<FieldInfo> = schema
                .fields()
                .iter()
                .map(|f| FieldInfo {
                    name: f.name().clone(),
                    data_type: format!("{:?}", f.data_type()),
                    nullable: f.is_nullable(),
                })
                .collect();
            Json(fields).into_response()
        }
        Err(e) => error_json(StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

/// POST /api/fuse/query/explain
pub async fn explain_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let format = req.format.to_lowercase();
    let parse_result = match format.as_str() {
        "ppl" => parse_ppl_sources(&req.query),
        _ => parse_sql_sources(&req.query),
    };

    match parse_result {
        Ok(refs) => {
            let workload = build_workload(&req.query);
            let limit = parse_limit(&req.query);

            // Collect capabilities for each datasource
            let caps: Vec<_> = refs
                .iter()
                .filter_map(|(ds, _)| state.registry.get(ds).map(|c| c.capabilities()))
                .collect();

            let plan_tree = if refs.len() == 1 {
                let c = caps.first().cloned().unwrap_or_else(fuse_core::connector::ConnectorCapabilities::full);
                fuse_engine::plan::plan_single(&refs[0].0, &refs[0].1, &c, &workload)
            } else if is_union_query(&req.query) {
                fuse_engine::plan::plan_union(&refs, &caps, &workload, limit)
            } else if refs.len() == 2 {
                let c0 = caps.first().cloned().unwrap_or_else(fuse_core::connector::ConnectorCapabilities::full);
                let c1 = caps.get(1).cloned().unwrap_or_else(fuse_core::connector::ConnectorCapabilities::full);
                fuse_engine::plan::plan_join(
                    (&refs[0].0, &refs[0].1),
                    (&refs[1].0, &refs[1].1),
                    &c0, &c1, "auto",
                )
            } else {
                fuse_engine::plan::PlanNode::leaf("Unknown", format!("{} datasources", refs.len()))
            };

            let plan_text = plan_tree.to_text(0);
            Json(ExplainResponse {
                plan: plan_text,
                plan_tree: Some(plan_tree),
            })
            .into_response()
        }
        Err(e) => error_json(StatusCode::BAD_REQUEST, e).into_response(),
    }
}

/// POST /api/fuse/query/validate
pub async fn validate_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let format = req.format.to_lowercase();
    let parse_result = match format.as_str() {
        "ppl" => parse_ppl_sources(&req.query),
        _ => parse_sql_sources(&req.query),
    };

    match parse_result {
        Ok(refs) => {
            for (ds_id, table) in &refs {
                let connector = match state.registry.get(ds_id) {
                    Some(c) => c,
                    None => return Json(ValidateResponse {
                        valid: false,
                        error: Some(format!("datasource '{}' not found in registry", ds_id)),
                    }),
                };
                // Check table exists in datasource
                match connector.discover_schemas().await {
                    Ok(schemas) => {
                        if !schemas.iter().any(|s| s.name == *table) {
                            return Json(ValidateResponse {
                                valid: false,
                                error: Some(format!("table '{}' not found in datasource '{}'", table, ds_id)),
                            });
                        }
                    }
                    Err(e) => return Json(ValidateResponse {
                        valid: false,
                        error: Some(format!("failed to discover schemas for '{}': {}", ds_id, e)),
                    }),
                }
            }
            Json(ValidateResponse {
                valid: true,
                error: None,
            })
        }
        Err(e) => Json(ValidateResponse {
            valid: false,
            error: Some(e),
        }),
    }
}

/// GET /api/fuse/health
pub async fn health_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let resp = health::check_health(&state.registry).await;
    Json(resp)
}

// ── Helpers ──

/// Parse all datasource.table references from a PPL query.
/// PPL: `source = ds1.table1, ds2.table2 | ...`
fn parse_ppl_sources(query: &str) -> Result<Vec<(String, String)>, String> {
    let rest = query
        .trim()
        .strip_prefix("source")
        .or_else(|| query.trim().strip_prefix("search"))
        .and_then(|s| s.trim_start().strip_prefix('='))
        .map(|s| s.trim_start())
        .ok_or_else(|| "PPL query must start with 'source = '".to_string())?;

    let source_part = rest.split('|').next().unwrap_or(rest).trim();
    source_part
        .split(',')
        .map(|s| parse_qualified_name(s.trim()))
        .collect()
}

/// Parse all datasource.table references from a SQL query.
/// Finds all `datasource.table` patterns after FROM, JOIN, and in UNION ALL subqueries.
fn parse_sql_sources(query: &str) -> Result<Vec<(String, String)>, String> {
    let mut refs = Vec::new();
    let lower = query.to_lowercase();

    // Find references after FROM and JOIN keywords (word-boundary aware)
    for keyword in &["from ", "join "] {
        let mut search_from = 0;
        while let Some(pos) = lower[search_from..].find(keyword) {
            let abs_pos = search_from + pos;
            // Ensure word boundary: keyword must be preceded by whitespace, '(' or start of string
            let is_word_start = abs_pos == 0
                || lower.as_bytes()[abs_pos - 1].is_ascii_whitespace()
                || lower.as_bytes()[abs_pos - 1] == b'('
                || lower.as_bytes()[abs_pos - 1] == b',';
            let token_start = abs_pos + keyword.len();
            if is_word_start {
                let after = query[token_start..].trim_start();
                // Skip if inside a string literal (count unescaped quotes before this position)
                let quotes_before = query[..abs_pos].chars().filter(|&c| c == '\'').count();
                if quotes_before % 2 == 0 {
                    // Take the first token
                    let token = after
                        .split(|c: char| c.is_whitespace() || c == ',' || c == ')')
                        .next()
                        .unwrap_or("");
                    // Strip alias: "ds.table AS a" or "ds.table a" → "ds.table"
                    if let Ok(r) = parse_qualified_name(token) {
                        if !refs.contains(&r) {
                            refs.push(r);
                        }
                    }
                }
            }
            search_from = token_start;
        }
    }

    if refs.is_empty() {
        Err("SQL query must contain a FROM clause with a qualified datasource.table reference".into())
    } else {
        Ok(refs)
    }
}

/// Add a `_datasource` column to each RecordBatch.
fn add_datasource_column(
    batches: &[arrow::record_batch::RecordBatch],
    datasource: &str,
) -> Vec<arrow::record_batch::RecordBatch> {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};

    batches
        .iter()
        .filter_map(|batch| {
            let n = batch.num_rows();
            let ds_array = Arc::new(StringArray::from(vec![datasource; n]));
            let mut fields: Vec<Arc<Field>> = batch.schema().fields().iter().cloned().collect();
            fields.push(Arc::new(Field::new("_datasource", DataType::Utf8, false)));
            let schema = Arc::new(arrow::datatypes::Schema::new(fields));
            let mut columns: Vec<Arc<dyn arrow::array::Array>> =
                batch.columns().to_vec();
            columns.push(ds_array);
            arrow::record_batch::RecordBatch::try_new(schema, columns).ok()
        })
        .collect()
}

/// Check if a SQL query contains UNION ALL.
/// Strip single-quoted string literals from SQL to avoid false keyword matches.
/// Replaces 'content' with '' (empty string literal placeholder).
fn strip_string_literals(query: &str) -> String {
    let mut result = String::with_capacity(query.len());
    let mut in_quote = false;
    let mut chars = query.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            if in_quote {
                // Check for escaped quote ''
                if chars.peek() == Some(&'\'') {
                    chars.next();
                    continue;
                }
                in_quote = false;
                result.push('\'');
            } else {
                in_quote = true;
                result.push('\'');
            }
        } else if in_quote {
            // Skip content inside quotes
        } else {
            result.push(c);
        }
    }
    result
}

fn is_union_query(query: &str) -> bool {
    strip_string_literals(query).to_lowercase().contains("union all")
}

/// Extract LIMIT value from end of query.
fn parse_limit(query: &str) -> Option<usize> {
    let stripped = strip_string_literals(query).to_lowercase();
    let pos = stripped.rfind("limit ")?;
    let after = stripped[pos + 6..].trim();
    let num_str = after
        .split(|c: char| !c.is_ascii_digit())
        .next()?;
    num_str.parse().ok()
}

/// Extract OFFSET value from query.
fn parse_offset(query: &str) -> Option<usize> {
    let stripped = strip_string_literals(query).to_lowercase();
    let pos = stripped.rfind("offset ")?;
    let after = stripped[pos + 7..].trim();
    let num_str = after
        .split(|c: char| !c.is_ascii_digit())
        .next()?;
    num_str.parse().ok()
}

/// Extract ORDER BY column name and direction from query.
fn parse_order_by(query: &str) -> Option<(String, bool)> {
    let stripped = strip_string_literals(query);
    let lower = stripped.to_lowercase();
    let pos = lower.rfind("order by ")?;
    let after = stripped[pos + 9..].trim();
    let clause = after.split_once("limit").map(|(c, _)| c.trim()).unwrap_or(after.trim());
    let parts: Vec<&str> = clause.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }
    let col = parts[0].trim_matches(|c: char| !c.is_alphanumeric() && c != '_').to_string();
    let desc = parts.get(1).map(|d| d.to_lowercase() == "desc").unwrap_or(false);
    Some((col, desc))
}

/// Build a QueryWorkload from query text for cost estimation.
fn build_workload(query: &str) -> fuse_engine::QueryWorkload {
    let lower = query.to_lowercase();
    fuse_engine::QueryWorkload {
        has_filter: lower.contains("where "),
        has_aggregation: lower.contains("group by")
            || lower.contains("count(")
            || lower.contains("sum(")
            || lower.contains("avg("),
        has_sort: lower.contains("order by") || lower.contains("sort "),
        has_limit: lower.contains("limit ") || lower.contains("head "),
        limit_value: parse_limit(query).map(|n| n as u64),
        projection_count: 0,
        total_columns: 0,
    }
}

fn parse_qualified_name(name: &str) -> Result<(String, String), String> {
    // Strip alias: "ds.table AS a" or "ds.table a" → "ds.table"
    let clean = name.split_whitespace().next().unwrap_or(name);
    clean
        .split_once('.')
        .map(|(ds, tbl)| (ds.to_string(), tbl.to_string()))
        .ok_or_else(|| format!("expected 'datasource.table', got '{}'", clean))
}

/// Single-source wrappers used by evaluate_alerts handler.
fn parse_ppl_source(query: &str) -> Result<(String, String), String> {
    parse_ppl_sources(query).and_then(|v| v.into_iter().next().ok_or_else(|| "no source found".to_string()))
}

fn parse_sql_source(query: &str) -> Result<(String, String), String> {
    parse_sql_sources(query).and_then(|v| v.into_iter().next().ok_or_else(|| "no source found".to_string()))
}

/// Build a SubQuery from a user query string using the full translation pipeline.
///
/// For PPL: parse PPL → translate to SQL → parse SQL into SubQuery.
/// For SQL: parse SQL directly into SubQuery.
/// Falls back to a minimal SubQuery if translation fails.
pub fn build_sub_query(
    query: &str,
    format: &str,
    table: &str,
) -> Result<fuse_core::connector::SubQuery, String> {
    let sql = if format == "ppl" {
        let parsed = fuse_engine::ppl::parse_ppl(query)
            .map_err(|e| format!("PPL parse error: {e}"))?;
        fuse_engine::ppl::ppl_to_sql(&parsed)
            .map_err(|e| format!("PPL translation error: {e}"))?
    } else {
        query.to_string()
    };

    match fuse_engine::sql_to_subquery::sql_to_subquery(&sql) {
        Ok(mut sq) => {
            sq.table = table.to_string();
            Ok(sq)
        }
        Err(_) => {
            // Fallback: minimal SubQuery with no hardcoded limit
            Ok(fuse_core::connector::SubQuery {
                table: table.to_string(),
                projections: vec![],
                filter: None,
                aggregations: vec![],
                group_by: vec![],
                sort: vec![],
                limit: None,
                passthrough: None,
            })
        }
    }
}

/// Convert Arrow RecordBatches to JSON columns + rows.
fn batches_to_json(
    batches: &[arrow::record_batch::RecordBatch],
) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    if batches.is_empty() {
        return (vec![], vec![]);
    }

    let schema = batches[0].schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<serde_json::Value> = (0..batch.num_columns())
                .map(|col_idx| {
                    let col = batch.column(col_idx);
                    // Use Arrow JSON writer for the value
                    if col.is_null(row_idx) {
                        serde_json::Value::Null
                    } else {
                        // Stringify via Arrow's display
                        let val = arrow::util::display::array_value_to_string(col, row_idx)
                            .unwrap_or_default();
                        serde_json::Value::String(val)
                    }
                })
                .collect();
            rows.push(row);
        }
    }

    (columns, rows)
}

/// Convert Arrow RecordBatches to CSV string.
fn batches_to_csv(batches: &[arrow::record_batch::RecordBatch]) -> String {
    if batches.is_empty() {
        return String::new();
    }
    let mut buf = Vec::new();
    {
        let mut writer = arrow::csv::WriterBuilder::new()
            .with_header(true)
            .build(&mut buf);
        for batch in batches {
            writer.write(batch).ok();
        }
    }
    String::from_utf8(buf).unwrap_or_default()
}

/// GET /api/fuse/history — last 50 queries with stats.
pub async fn history_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.history.list())
}

/// GET /api/fuse/stats — aggregated query statistics.
pub async fn stats_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.history.stats())
}

// ── Saved query handlers ──

/// GET /api/fuse/saved — list saved queries.
pub async fn list_saved_queries(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.saved_queries.list())
}

/// POST /api/fuse/saved — save a named query.
pub async fn save_query(
    State(state): State<Arc<AppState>>,
    Json(sq): Json<crate::saved_queries::SavedQuery>,
) -> impl IntoResponse {
    state.saved_queries.save(sq);
    StatusCode::CREATED
}

/// GET /api/fuse/saved/:name — get a saved query by name.
pub async fn get_saved_query(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.saved_queries.get(&name) {
        Some(sq) => Json(serde_json::json!(sq)).into_response(),
        None => error_json(StatusCode::NOT_FOUND, format!("saved query '{}' not found", name)).into_response(),
    }
}

/// DELETE /api/fuse/saved/:name — delete a saved query.
pub async fn delete_saved_query(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    if state.saved_queries.delete(&name) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        error_json(StatusCode::NOT_FOUND, format!("saved query '{}' not found", name)).into_response()
    }
}

// ── Query cancellation handlers ──

/// DELETE /api/fuse/query/:id — cancel a running query.
pub async fn cancel_query(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if state.running_queries.cancel(&id) {
        (StatusCode::OK, Json(serde_json::json!({"cancelled": true, "query_id": id}))).into_response()
    } else {
        error_json(StatusCode::NOT_FOUND, format!("query '{}' not found or already completed", id)).into_response()
    }
}

/// GET /api/fuse/queries/running — list currently running query IDs.
pub async fn list_running_queries(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    Json(serde_json::json!({"running": state.running_queries.list()}))
}

// ── Alert handlers ──

/// GET /api/fuse/alerts — list configured alert rules.
pub async fn list_alerts(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[derive(Serialize)]
    struct AlertInfo {
        name: String,
        query: String,
        format: String,
        interval_secs: u64,
    }
    let infos: Vec<AlertInfo> = state
        .alert_rules
        .iter()
        .map(|r| AlertInfo {
            name: r.name.clone(),
            query: r.query.clone(),
            format: r.format.clone(),
            interval_secs: r.interval_secs,
        })
        .collect();
    Json(infos)
}

/// POST /api/fuse/alerts/evaluate — run all alert rules now and return firing alerts.
pub async fn evaluate_alerts(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let mut firing = Vec::new();
    for rule in &state.alert_rules {
        // Parse the rule's query to find the datasource
        let parse_result = match rule.format.as_str() {
            "ppl" => parse_ppl_source(&rule.query),
            _ => parse_sql_source(&rule.query),
        };
        let (ds_id, table) = match parse_result {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(rule = rule.name.as_str(), "alert rule parse error: {e}");
                continue;
            }
        };
        let connector = match state.registry.get(&ds_id) {
            Some(c) => c,
            None => {
                tracing::warn!(rule = rule.name.as_str(), "datasource '{}' not found", ds_id);
                continue;
            }
        };
        let sub_query = match build_sub_query(&rule.query, &rule.format, &table) {
            Ok(sq) => sq,
            Err(e) => {
                tracing::warn!(rule = rule.name.as_str(), "sub_query build error: {e}");
                continue;
            }
        };
        match connector.execute(&sub_query).await {
            Ok(batches) => {
                let result = AlertEvaluator::evaluate(rule, &batches);
                if result.state == fuse_core::alerting::AlertState::Firing {
                    firing.push(result);
                }
            }
            Err(e) => tracing::warn!(rule = rule.name.as_str(), "alert query error: {e}"),
        }
    }
    Json(firing)
}

// ── Materialized view handlers ──

/// GET /api/fuse/views — list registered materialized views.
pub async fn list_views(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[derive(Serialize)]
    struct ViewInfo { name: String, stale: bool }
    let names = state.view_registry.list();
    let infos: Vec<ViewInfo> = names.into_iter().map(|name| {
        let stale = state.view_registry.get(&name)
            .map(|v| v.read().unwrap().needs_refresh())
            .unwrap_or(true);
        ViewInfo { name, stale }
    }).collect();
    Json(infos)
}

/// GET /api/fuse/views/:name — query a materialized view (returns cached results).
pub async fn get_view(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.view_registry.get_results(&name) {
        None => {
            if state.view_registry.get(&name).is_none() {
                error_json(StatusCode::NOT_FOUND, format!("view '{}' not found", name)).into_response()
            } else {
                error_json(StatusCode::SERVICE_UNAVAILABLE, format!("view '{}' not yet refreshed", name)).into_response()
            }
        }
        Some(batches) => {
            let (columns, rows) = batches_to_json(&batches);
            Json(QueryResponse {
                columns,
                rows: rows.clone(),
                metadata: QueryMetadata { total_rows: rows.len() as u64, format: "view".into(), datasources_queried: None, datasource_stats: None },
                execution_profile: None,
                partial_errors: vec![],
            }).into_response()
        }
    }
}

/// POST /api/fuse/views/:name/refresh — trigger a synchronous refresh of a view.
pub async fn refresh_view(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let view_arc = match state.view_registry.get(&name) {
        Some(v) => v,
        None => return error_json(StatusCode::NOT_FOUND, format!("view '{}' not found", name)).into_response(),
    };

    let query = view_arc.read().unwrap().def.query.clone();
    let format = "sql";

    let refs = match parse_sql_sources(&query) {
        Ok(r) if !r.is_empty() => r,
        _ => {
            view_arc.write().unwrap().set_error("failed to parse view query".into());
            return error_json(StatusCode::BAD_REQUEST, "failed to parse view query").into_response();
        }
    };

    let (ds_id, table) = &refs[0];
    let connector = match state.registry.get(ds_id) {
        Some(c) => c,
        None => {
            let msg = format!("datasource '{}' not found", ds_id);
            view_arc.write().unwrap().set_error(msg.clone());
            return error_json(StatusCode::NOT_FOUND, msg).into_response();
        }
    };

    let sub_query = match build_sub_query(&query, format, table) {
        Ok(sq) => sq,
        Err(e) => {
            view_arc.write().unwrap().set_error(e.clone());
            return error_json(StatusCode::BAD_REQUEST, e).into_response();
        }
    };

    match connector.execute(&sub_query).await {
        Ok(batches) => {
            view_arc.write().unwrap().set_results(batches);
            Json(serde_json::json!({ "refreshed": true, "view": name })).into_response()
        }
        Err(e) => {
            view_arc.write().unwrap().set_error(e.to_string());
            error_json(StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_running_queries_cancel_returns_true_when_found() {
        let rq = RunningQueries::new();
        let token = CancellationToken::new();
        rq.insert("q-001".into(), token.clone());
        assert!(rq.cancel("q-001"));
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_running_queries_cancel_returns_false_when_not_found() {
        let rq = RunningQueries::new();
        assert!(!rq.cancel("q-nonexistent"));
    }

    #[test]
    fn test_running_queries_list() {
        let rq = RunningQueries::new();
        assert!(rq.list().is_empty());
        rq.insert("q-001".into(), CancellationToken::new());
        rq.insert("q-002".into(), CancellationToken::new());
        let mut ids = rq.list();
        ids.sort();
        assert_eq!(ids, vec!["q-001", "q-002"]);
    }

    #[test]
    fn test_running_queries_remove_cleans_up() {
        let rq = RunningQueries::new();
        rq.insert("q-001".into(), CancellationToken::new());
        rq.remove("q-001");
        assert!(rq.list().is_empty());
    }

    #[test]
    fn test_running_queries_cancel_removes_entry() {
        let rq = RunningQueries::new();
        rq.insert("q-001".into(), CancellationToken::new());
        rq.cancel("q-001");
        // After cancel, entry is removed — second cancel returns false
        assert!(!rq.cancel("q-001"));
    }
}
