// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use fuse_core::registry::ConnectorRegistry;

use crate::health;

/// Shared application state passed to all handlers.
pub struct AppState {
    pub registry: Arc<ConnectorRegistry>,
}

// ── Request / Response types ──

#[derive(Deserialize)]
pub struct QueryRequest {
    pub query: String,
    #[serde(default = "default_format")]
    pub format: String,
}

fn default_format() -> String {
    "sql".to_string()
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub metadata: QueryMetadata,
}

#[derive(Serialize)]
pub struct QueryMetadata {
    pub total_rows: u64,
    pub format: String,
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

// ── Handlers ──

/// POST /api/fuse/query
pub async fn query_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> impl IntoResponse {
    let format = req.format.to_lowercase();

    // Parse all datasource.table references from the query
    let refs = match format.as_str() {
        "ppl" => parse_ppl_sources(&req.query),
        _ => parse_sql_sources(&req.query),
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

    let result = if refs.len() == 1 {
        // Single datasource — direct execution
        execute_single(&state, &req.query, &format, &refs[0]).await
    } else if is_union_query(&req.query) {
        // UNION ALL — fan out to each connector, merge results
        execute_union(&state, &req.query, &format, &refs).await
    } else {
        // JOIN — use join executor
        execute_join(&state, &refs).await
    };

    match result {
        Ok(batches) => {
            // Apply global limit if present in query
            let limit = parse_limit(&req.query);
            let batches = if let Some(n) = limit {
                fuse_engine::merge_batches(batches, Some(n)).unwrap_or_default()
            } else {
                batches
            };
            let (columns, rows) = batches_to_json(&batches);
            let total_rows = rows.len() as u64;
            Json(QueryResponse {
                columns,
                rows,
                metadata: QueryMetadata {
                    total_rows,
                    format: req.format,
                },
            })
            .into_response()
        }
        Err(e) => error_json(StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

/// Execute a single-datasource query.
async fn execute_single(
    state: &AppState,
    query: &str,
    format: &str,
    (ds_id, table): &(String, String),
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let connector = state.registry.get(ds_id).unwrap();
    let sub_query = build_sub_query(query, format, table)?;
    connector.execute(&sub_query).await.map_err(|e| e.to_string())
}

/// Execute a UNION ALL query — fan out to each connector in parallel, merge.
async fn execute_union(
    state: &AppState,
    query: &str,
    format: &str,
    refs: &[(String, String)],
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    let mut handles = Vec::new();

    for (ds_id, table) in refs {
        let connector = state.registry.get(ds_id).unwrap();
        let sub_query = build_sub_query(query, format, table)?;
        let conn = connector.clone();
        handles.push(tokio::spawn(async move { conn.execute(&sub_query).await }));
    }

    let mut batch_sets = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Ok(batches)) => batch_sets.push(batches),
            Ok(Err(e)) => return Err(e.to_string()),
            Err(e) => return Err(format!("task join error: {e}")),
        }
    }

    fuse_engine::union_batches(batch_sets).map_err(|e| e.to_string())
}

/// Execute a cross-datasource JOIN using the join executor.
async fn execute_join(
    state: &AppState,
    refs: &[(String, String)],
) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
    if refs.len() != 2 {
        return Err(format!("JOIN requires exactly 2 datasources, got {}", refs.len()));
    }

    let (ds_a, table_a) = &refs[0];
    let (ds_b, table_b) = &refs[1];

    let conn_a = state.registry.get(ds_a).unwrap();
    let conn_b = state.registry.get(ds_b).unwrap();

    // Fetch both sides in parallel with no filters (join executor handles the rest)
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

    let (res_a, res_b) = tokio::join!(conn_a.execute(&sq_a), conn_b.execute(&sq_b));
    let batches_a = res_a.map_err(|e| e.to_string())?;
    let batches_b = res_b.map_err(|e| e.to_string())?;

    if batches_a.is_empty() || batches_b.is_empty() {
        return Ok(vec![]);
    }

    // Find common columns to use as join key
    let schema_a = batches_a[0].schema();
    let schema_b = batches_b[0].schema();
    let join_key = find_join_key(&schema_a, &schema_b)
        .ok_or_else(|| "no common column found for JOIN key".to_string())?;

    fuse_engine::hash_join(
        &batches_a,
        &join_key,
        &batches_b,
        &join_key,
        fuse_engine::JoinType::Inner,
    )
    .map_err(|e| e.to_string())
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
            let strategy = if refs.len() == 1 {
                "SingleSource"
            } else if is_union_query(&req.query) {
                "UnionAll"
            } else {
                "CrossSourceJoin"
            };
            let ds_list: Vec<String> = refs.iter().map(|(ds, t)| format!("{ds}.{t}")).collect();
            let all_found = refs.iter().all(|(ds, _)| state.registry.get(ds).is_some());
            let plan = format!(
                "FederatedPlan {{\n  datasources: {:?},\n  format: \"{}\",\n  all_connectors_found: {},\n  strategy: {}\n}}",
                ds_list, format, all_found, strategy
            );
            Json(ExplainResponse { plan }).into_response()
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
            for (ds_id, _) in &refs {
                if state.registry.get(ds_id).is_none() {
                    return Json(ValidateResponse {
                        valid: false,
                        error: Some(format!("datasource '{}' not found in registry", ds_id)),
                    });
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

    // Find references after FROM and JOIN keywords
    for keyword in &["from ", "join "] {
        let mut search_from = 0;
        while let Some(pos) = lower[search_from..].find(keyword) {
            let abs_pos = search_from + pos + keyword.len();
            let after = query[abs_pos..].trim_start();
            // Take the first token (might be `ds.table` or `ds.table` with alias)
            let token = after
                .split(|c: char| c.is_whitespace() || c == ',' || c == ')')
                .next()
                .unwrap_or("");
            if let Ok(r) = parse_qualified_name(token) {
                if !refs.contains(&r) {
                    refs.push(r);
                }
            }
            search_from = abs_pos;
        }
    }

    if refs.is_empty() {
        Err("SQL query must contain a FROM clause with a qualified datasource.table reference".into())
    } else {
        Ok(refs)
    }
}

/// Check if a SQL query contains UNION ALL.
fn is_union_query(query: &str) -> bool {
    query.to_lowercase().contains("union all")
}

/// Extract LIMIT value from end of query.
fn parse_limit(query: &str) -> Option<usize> {
    let lower = query.to_lowercase();
    let pos = lower.rfind("limit ")?;
    let after = query[pos + 6..].trim();
    let num_str = after
        .split(|c: char| !c.is_ascii_digit())
        .next()?;
    num_str.parse().ok()
}

fn parse_qualified_name(name: &str) -> Result<(String, String), String> {
    // Strip alias: "ds.table AS a" or "ds.table a" → "ds.table"
    let clean = name.split_whitespace().next().unwrap_or(name);
    clean
        .split_once('.')
        .map(|(ds, tbl)| (ds.to_string(), tbl.to_string()))
        .ok_or_else(|| format!("expected 'datasource.table', got '{}'", clean))
}

/// Build a SubQuery from a user query string using the full translation pipeline.
///
/// For PPL: parse PPL → translate to SQL → parse SQL into SubQuery.
/// For SQL: parse SQL directly into SubQuery.
/// Falls back to a minimal SubQuery if translation fails.
fn build_sub_query(
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
