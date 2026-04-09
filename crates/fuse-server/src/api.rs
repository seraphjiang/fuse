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
    // Phase 1: parse datasource.table from query, fan out to connector
    let format = req.format.to_lowercase();
    let parse_result = match format.as_str() {
        "ppl" => parse_ppl_source(&req.query),
        _ => parse_sql_source(&req.query),
    };

    let (ds_id, table) = match parse_result {
        Ok(v) => v,
        Err(e) => return error_json(StatusCode::BAD_REQUEST, e).into_response(),
    };

    let connector = match state.registry.get(&ds_id) {
        Some(c) => c,
        None => {
            return error_json(
                StatusCode::NOT_FOUND,
                format!("datasource '{}' not found", ds_id),
            )
            .into_response()
        }
    };

    let sub_query = match format.as_str() {
        "ppl" => fuse_core::connector::SubQuery {
            table,
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: Some(100),
            passthrough: None,
        },
        _ => {
            // Use full SQL→SubQuery translation for filter/limit/sort pushdown
            match fuse_engine::sql_to_subquery::sql_to_subquery(&req.query) {
                Ok(mut sq) => {
                    sq.table = table; // override with resolved table name
                    sq
                }
                Err(_) => fuse_core::connector::SubQuery {
                    table,
                    projections: vec![],
                    filter: None,
                    aggregations: vec![],
                    group_by: vec![],
                    sort: vec![],
                    limit: Some(100),
                    passthrough: None,
                },
            }
        }
    };

    match connector.execute(&sub_query).await {
        Ok(batches) => {
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
        "ppl" => parse_ppl_source(&req.query),
        _ => parse_sql_source(&req.query),
    };

    match parse_result {
        Ok((ds_id, table)) => {
            let connector_exists = state.registry.get(&ds_id).is_some();
            let plan = format!(
                "FederatedPlan {{\n  datasource: \"{}\",\n  table: \"{}\",\n  format: \"{}\",\n  connector_found: {},\n  strategy: FanOut\n}}",
                ds_id, table, format, connector_exists
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
        "ppl" => parse_ppl_source(&req.query),
        _ => parse_sql_source(&req.query),
    };

    match parse_result {
        Ok((ds_id, _table)) => {
            if state.registry.get(&ds_id).is_none() {
                Json(ValidateResponse {
                    valid: false,
                    error: Some(format!("datasource '{}' not found in registry", ds_id)),
                })
            } else {
                Json(ValidateResponse {
                    valid: true,
                    error: None,
                })
            }
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

/// Minimal PPL source parser: `source = datasource.table | ...`
fn parse_ppl_source(query: &str) -> Result<(String, String), String> {
    let rest = query
        .trim()
        .strip_prefix("source")
        .and_then(|s| s.trim_start().strip_prefix('='))
        .map(|s| s.trim_start())
        .ok_or_else(|| "PPL query must start with 'source = '".to_string())?;

    let source_part = rest.split('|').next().unwrap_or(rest).trim();
    let first = source_part.split(',').next().unwrap_or(source_part).trim();
    parse_qualified_name(first)
}

/// Minimal SQL source parser: `... FROM datasource.table ...`
fn parse_sql_source(query: &str) -> Result<(String, String), String> {
    let lower = query.to_lowercase();
    let pos = lower
        .find("from ")
        .ok_or_else(|| "SQL query must contain FROM clause".to_string())?;
    let after = query[pos + 5..].trim_start();
    let token = after
        .split_whitespace()
        .next()
        .ok_or_else(|| "expected table reference after FROM".to_string())?;
    parse_qualified_name(token)
}

fn parse_qualified_name(name: &str) -> Result<(String, String), String> {
    name.split_once('.')
        .map(|(ds, tbl)| (ds.to_string(), tbl.to_string()))
        .ok_or_else(|| format!("expected 'datasource.table', got '{}'", name))
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
