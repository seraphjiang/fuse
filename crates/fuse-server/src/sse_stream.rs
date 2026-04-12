// SPDX-License-Identifier: Apache-2.0

//! Server-Sent Events (SSE) streaming for query results.
//!
//! Streams query results incrementally to clients as they arrive from
//! connectors, reducing time-to-first-byte for large result sets.
//!
//! `GET /api/fuse/query/stream?query=...&format=sql`

use serde::Serialize;

/// SSE event types for query streaming.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum StreamEvent {
    /// Query accepted, execution starting.
    Started { query_id: String },
    /// Schema/columns available.
    Schema { columns: Vec<ColumnDef> },
    /// A batch of rows.
    Rows {
        rows: Vec<Vec<serde_json::Value>>,
        batch_index: u32,
    },
    /// Query complete with summary.
    Complete { total_rows: u64, elapsed_ms: u64 },
    /// Error during execution.
    Error { message: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}

/// Format a StreamEvent as an SSE message (data: ... \n\n).
pub fn format_sse(event: &StreamEvent) -> String {
    let event_type = match event {
        StreamEvent::Started { .. } => "started",
        StreamEvent::Schema { .. } => "schema",
        StreamEvent::Rows { .. } => "rows",
        StreamEvent::Complete { .. } => "complete",
        StreamEvent::Error { .. } => "error",
    };
    let data = serde_json::to_string(event).unwrap_or_default();
    format!("event: {}\ndata: {}\n\n", event_type, data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_sse_started() {
        let e = StreamEvent::Started {
            query_id: "q1".into(),
        };
        let sse = format_sse(&e);
        assert!(sse.starts_with("event: started\n"));
        assert!(sse.contains("\"query_id\":\"q1\""));
        assert!(sse.ends_with("\n\n"));
    }

    #[test]
    fn test_format_sse_schema() {
        let e = StreamEvent::Schema {
            columns: vec![ColumnDef {
                name: "id".into(),
                data_type: "Int64".into(),
            }],
        };
        let sse = format_sse(&e);
        assert!(sse.contains("event: schema"));
        assert!(sse.contains("\"name\":\"id\""));
    }

    #[test]
    fn test_format_sse_rows() {
        let e = StreamEvent::Rows {
            rows: vec![vec![serde_json::json!(1), serde_json::json!("a")]],
            batch_index: 0,
        };
        let sse = format_sse(&e);
        assert!(sse.contains("event: rows"));
    }

    #[test]
    fn test_format_sse_complete() {
        let e = StreamEvent::Complete {
            total_rows: 100,
            elapsed_ms: 42,
        };
        let sse = format_sse(&e);
        assert!(sse.contains("event: complete"));
        assert!(sse.contains("\"total_rows\":100"));
    }

    #[test]
    fn test_format_sse_error() {
        let e = StreamEvent::Error {
            message: "timeout".into(),
        };
        let sse = format_sse(&e);
        assert!(sse.contains("event: error"));
        assert!(sse.contains("timeout"));
    }
}
