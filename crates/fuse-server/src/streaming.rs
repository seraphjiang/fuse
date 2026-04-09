// SPDX-License-Identifier: Apache-2.0

//! Server-Sent Events (SSE) streaming endpoint for query results.
//!
//! `POST /api/fuse/query/stream` accepts the same `QueryRequest` as the
//! regular query endpoint but returns `text/event-stream` with chunked results:
//!
//! - `{type: "metadata", columns: [...]}` — column names
//! - `{type: "batch", rows: [[...], ...]}` — a chunk of rows
//! - `{type: "progress", batches_sent: N}` — progress update
//! - `{type: "done", total_rows: N}` — stream complete
//! - `{type: "error", message: "..."}` — error during streaming

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Json, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::stream::Stream;
use serde::Serialize;

use crate::api::{AppState, QueryRequest};

#[derive(Serialize)]
struct MetadataEvent {
    r#type: &'static str,
    columns: Vec<String>,
}

#[derive(Serialize)]
struct BatchEvent {
    r#type: &'static str,
    rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Serialize)]
struct ProgressEvent {
    r#type: &'static str,
    batches_sent: u64,
}

#[derive(Serialize)]
struct DoneEvent {
    r#type: &'static str,
    total_rows: u64,
}

#[derive(Serialize)]
struct ErrorEvent {
    r#type: &'static str,
    message: String,
}

/// POST /api/fuse/query/stream
pub async fn stream_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        // Parse datasource.table from query
        let format = req.format.to_lowercase();
        let parse_result = match format.as_str() {
            "ppl" => parse_ppl_source(&req.query),
            _ => parse_sql_source(&req.query),
        };

        let (ds_id, table) = match parse_result {
            Ok(v) => v,
            Err(e) => {
                yield Ok(make_error_event(&e));
                return;
            }
        };

        let connector = match state.registry.get(&ds_id) {
            Some(c) => c,
            None => {
                yield Ok(make_error_event(&format!("datasource '{}' not found", ds_id)));
                return;
            }
        };

        // Get schema for metadata event
        let schema = match connector.get_table_schema(&table).await {
            Ok(s) => s,
            Err(e) => {
                yield Ok(make_error_event(&e.to_string()));
                return;
            }
        };

        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        yield Ok(make_event(&MetadataEvent {
            r#type: "metadata",
            columns,
        }));

        // Execute via streaming
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);
        let sub_query = fuse_core::connector::SubQuery {
            table,
            projections: vec![],
            filter: None,
            aggregations: vec![],
            group_by: vec![],
            sort: vec![],
            limit: None,
            passthrough: None,
        };

        let conn = connector.clone();
        tokio::spawn(async move {
            let _ = conn.execute_streaming(&sub_query, tx).await;
        });

        let mut total_rows: u64 = 0;
        let mut batches_sent: u64 = 0;

        while let Some(result) = rx.recv().await {
            match result {
                Ok(batch) => {
                    let rows = batch_to_rows(&batch);
                    total_rows += rows.len() as u64;
                    batches_sent += 1;

                    yield Ok(make_event(&BatchEvent {
                        r#type: "batch",
                        rows,
                    }));

                    // Progress every 5 batches
                    if batches_sent % 5 == 0 {
                        yield Ok(make_event(&ProgressEvent {
                            r#type: "progress",
                            batches_sent,
                        }));
                    }
                }
                Err(e) => {
                    yield Ok(make_error_event(&e.to_string()));
                    return;
                }
            }
        }

        yield Ok(make_event(&DoneEvent {
            r#type: "done",
            total_rows,
        }));
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn make_event<T: Serialize>(data: &T) -> Event {
    Event::default().data(serde_json::to_string(data).unwrap_or_default())
}

fn make_error_event(msg: &str) -> Event {
    make_event(&ErrorEvent {
        r#type: "error",
        message: msg.to_string(),
    })
}

fn batch_to_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<Vec<serde_json::Value>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let row: Vec<serde_json::Value> = (0..batch.num_columns())
            .map(|col_idx| {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    let val = arrow::util::display::array_value_to_string(col, row_idx)
                        .unwrap_or_default();
                    serde_json::Value::String(val)
                }
            })
            .collect();
        rows.push(row);
    }
    rows
}

// Re-use the parsing helpers from api.rs (they're private, so duplicate minimally)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_event_serializes() {
        let evt = make_event(&DoneEvent {
            r#type: "done",
            total_rows: 42,
        });
        // Event was created without panic
        let _ = evt;
    }

    #[test]
    fn test_make_error_event() {
        let evt = make_error_event("something broke");
        let _ = evt;
    }

    #[test]
    fn test_batch_to_rows() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(vec!["a", "b"])),
                std::sync::Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let rows = batch_to_rows(&batch);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], serde_json::Value::String("a".into()));
        assert_eq!(rows[0][1], serde_json::Value::String("1".into()));
    }

    #[test]
    fn test_batch_to_rows_with_nulls() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("x", DataType::Utf8, true),
            Field::new("y", DataType::Int64, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None])),
                std::sync::Arc::new(Int64Array::from(vec![None, Some(2)])),
            ],
        )
        .unwrap();

        let rows = batch_to_rows(&batch);
        assert_eq!(rows[0][1], serde_json::Value::Null);
        assert_eq!(rows[1][0], serde_json::Value::Null);
    }

    #[test]
    fn test_batch_to_rows_empty() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();

        let rows = batch_to_rows(&batch);
        assert!(rows.is_empty());
    }

    #[test]
    fn test_parse_ppl_source_valid() {
        let (ds, tbl) = parse_ppl_source("source = cluster_a.logs | head 10").unwrap();
        assert_eq!(ds, "cluster_a");
        assert_eq!(tbl, "logs");
    }

    #[test]
    fn test_parse_sql_source_valid() {
        let (ds, tbl) = parse_sql_source("SELECT * FROM cluster_b.metrics WHERE x > 1").unwrap();
        assert_eq!(ds, "cluster_b");
        assert_eq!(tbl, "metrics");
    }

    #[test]
    fn test_parse_sql_source_no_from() {
        assert!(parse_sql_source("SELECT 1").is_err());
    }

    #[test]
    fn test_parse_ppl_source_invalid() {
        assert!(parse_ppl_source("not a ppl query").is_err());
    }
}
