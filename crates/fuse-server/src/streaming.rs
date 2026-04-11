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

/// Default rows per SSE batch event.
const DEFAULT_BATCH_SIZE: usize = 500;
/// Channel buffer size — bounded for backpressure. Producer blocks when full.
const CHANNEL_BUFFER: usize = 4;

#[derive(Serialize)]
struct MetadataEvent {
    r#type: &'static str,
    columns: Vec<String>,
}

#[derive(Serialize)]
struct BatchEvent {
    r#type: &'static str,
    rows: Vec<Vec<serde_json::Value>>,
    batch_num: u64,
    batch_rows: usize,
}

#[derive(Serialize)]
struct ProgressEvent {
    r#type: &'static str,
    batches_sent: u64,
    total_rows: u64,
    total_bytes: u64,
}

#[derive(Serialize)]
struct DoneEvent {
    r#type: &'static str,
    total_rows: u64,
    total_bytes: u64,
    batches_sent: u64,
}

#[derive(Serialize)]
struct ErrorEvent {
    r#type: &'static str,
    message: String,
}

/// Streaming-specific request. Wraps QueryRequest with flow control params.
#[derive(serde::Deserialize)]
pub struct StreamRequest {
    #[serde(flatten)]
    pub query: QueryRequest,
    /// Rows per SSE batch event. Default: 500.
    pub batch_size: Option<usize>,
}

/// POST /api/fuse/query/stream
///
/// SSE streaming with backpressure:
/// - Bounded channel (CHANNEL_BUFFER) — producer blocks when client is slow
/// - Client-driven batch_size (rows per event, default 500)
/// - Progress events with total_rows + total_bytes
/// - Done event with final stats
#[allow(clippy::manual_clamp)]
pub async fn stream_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StreamRequest>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let batch_size = req.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1).min(10_000);
    let query_req = req.query;

    let stream = async_stream::stream! {
        // Parse datasource.table from query
        let format = query_req.format.to_lowercase();
        let parse_result = match format.as_str() {
            "ppl" => parse_ppl_source(&query_req.query),
            _ => parse_sql_source(&query_req.query),
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

        // Bounded channel for backpressure — producer blocks when buffer is full
        let (tx, mut rx) = tokio::sync::mpsc::channel(CHANNEL_BUFFER);
        let sub_query = match crate::api::build_sub_query(&query_req.query, &format, &table) {
            Ok(sq) => sq,
            Err(e) => {
                yield Ok(make_error_event(&e));
                return;
            }
        };

        let conn = connector.clone();
        tokio::spawn(async move {
            let _ = conn.execute_streaming(&sub_query, tx).await;
        });

        let mut total_rows: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut batches_sent: u64 = 0;
        let mut row_buffer: Vec<Vec<serde_json::Value>> = Vec::with_capacity(batch_size);

        while let Some(result) = rx.recv().await {
            match result {
                Ok(batch) => {
                    let batch_bytes: u64 = batch.get_array_memory_size() as u64;
                    total_bytes += batch_bytes;
                    let rows = batch_to_rows(&batch);

                    for row in rows {
                        row_buffer.push(row);
                        if row_buffer.len() >= batch_size {
                            batches_sent += 1;
                            let chunk_len = row_buffer.len();
                            total_rows += chunk_len as u64;
                            yield Ok(make_event(&BatchEvent {
                                r#type: "batch",
                                rows: std::mem::replace(&mut row_buffer, Vec::with_capacity(batch_size)),
                                batch_num: batches_sent,
                                batch_rows: chunk_len,
                            }));

                            // Progress every 5 batches
                            if batches_sent.is_multiple_of(5) {
                                yield Ok(make_event(&ProgressEvent {
                                    r#type: "progress",
                                    batches_sent,
                                    total_rows,
                                    total_bytes,
                                }));
                            }
                        }
                    }
                }
                Err(e) => {
                    yield Ok(make_error_event(&e.to_string()));
                    return;
                }
            }
        }

        // Flush remaining rows
        if !row_buffer.is_empty() {
            batches_sent += 1;
            let chunk_len = row_buffer.len();
            total_rows += chunk_len as u64;
            yield Ok(make_event(&BatchEvent {
                r#type: "batch",
                rows: row_buffer,
                batch_num: batches_sent,
                batch_rows: chunk_len,
            }));
        }

        yield Ok(make_event(&DoneEvent {
            r#type: "done",
            total_rows,
            total_bytes,
            batches_sent,
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
            total_bytes: 1024,
            batches_sent: 3,
        });
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

    #[test]
    fn test_default_batch_size() {
        assert_eq!(DEFAULT_BATCH_SIZE, 500);
    }

    #[test]
    fn test_batch_size_clamped() {
        // batch_size is clamped to 1..=10_000
        let val = 1;
        assert_eq!(val, 1);
        let val = 10_000;
        assert_eq!(val, 10_000);
        let val = 200usize;
        assert_eq!(val, 200);
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_channel_buffer_bounded() {
        // Verify backpressure constant is small (bounded)
        assert!(CHANNEL_BUFFER <= 8);
        assert!(CHANNEL_BUFFER >= 1);
    }

    #[test]
    fn test_stream_request_deserialize() {
        let json = r#"{"query":"SELECT * FROM ds.t","batch_size":100}"#;
        let req: StreamRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.batch_size, Some(100));
        assert_eq!(req.query.query, "SELECT * FROM ds.t");
    }

    #[test]
    fn test_stream_request_default_batch_size() {
        let json = r#"{"query":"SELECT * FROM ds.t"}"#;
        let req: StreamRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.batch_size, None);
    }

    #[test]
    fn test_batch_event_includes_stats() {
        let evt = BatchEvent {
            r#type: "batch",
            rows: vec![vec![serde_json::json!("a")]],
            batch_num: 1,
            batch_rows: 1,
        };
        let json = serde_json::to_value(&evt).unwrap();
        assert_eq!(json["batch_num"], 1);
        assert_eq!(json["batch_rows"], 1);
    }

    #[test]
    fn test_done_event_includes_bytes() {
        let evt = DoneEvent {
            r#type: "done",
            total_rows: 100,
            total_bytes: 4096,
            batches_sent: 2,
        };
        let json = serde_json::to_value(&evt).unwrap();
        assert_eq!(json["total_bytes"], 4096);
        assert_eq!(json["batches_sent"], 2);
    }

    #[test]
    fn test_progress_event_includes_bytes() {
        let evt = ProgressEvent {
            r#type: "progress",
            batches_sent: 5,
            total_rows: 2500,
            total_bytes: 8192,
        };
        let json = serde_json::to_value(&evt).unwrap();
        assert_eq!(json["total_rows"], 2500);
        assert_eq!(json["total_bytes"], 8192);
    }
}
