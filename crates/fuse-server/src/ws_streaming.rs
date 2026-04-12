// SPDX-License-Identifier: Apache-2.0
//! WebSocket streaming for real-time query results.
//!
//! Clients connect via `ws://host:9400/api/fuse/query/ws` and send
//! query requests as JSON. Results stream back as individual row batches.

use serde::{Deserialize, Serialize};

/// WebSocket query request.
#[derive(Debug, Deserialize)]
pub struct WsQueryRequest {
    pub query: String,
    pub format: String,
    #[serde(default)]
    pub batch_size: Option<usize>,
}

/// WebSocket message sent to client.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum WsMessage {
    /// Query accepted, streaming will begin.
    Ack { query_id: String },
    /// A batch of rows.
    Batch {
        rows: Vec<Vec<serde_json::Value>>,
        batch_index: usize,
    },
    /// Column metadata (sent before first batch).
    Schema { columns: Vec<String> },
    /// Query complete.
    Done { total_rows: u64, duration_ms: u64 },
    /// Error occurred.
    Error { message: String },
}

impl WsMessage {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|_| r#"{"type":"error","message":"serialization failed"}"#.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_request_parse() {
        let json = r#"{"query":"SELECT 1","format":"sql"}"#;
        let req: WsQueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.query, "SELECT 1");
        assert_eq!(req.format, "sql");
        assert!(req.batch_size.is_none());
    }

    #[test]
    fn test_ws_request_with_batch_size() {
        let json = r#"{"query":"SELECT 1","format":"sql","batch_size":100}"#;
        let req: WsQueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.batch_size, Some(100));
    }

    #[test]
    fn test_ws_message_ack() {
        let msg = WsMessage::Ack {
            query_id: "q-1".into(),
        };
        let json = msg.to_json();
        assert!(json.contains("\"type\":\"ack\""));
        assert!(json.contains("q-1"));
    }

    #[test]
    fn test_ws_message_schema() {
        let msg = WsMessage::Schema {
            columns: vec!["id".into(), "name".into()],
        };
        let json = msg.to_json();
        assert!(json.contains("\"type\":\"schema\""));
    }

    #[test]
    fn test_ws_message_done() {
        let msg = WsMessage::Done {
            total_rows: 42,
            duration_ms: 100,
        };
        let json = msg.to_json();
        assert!(json.contains("\"total_rows\":42"));
    }

    #[test]
    fn test_ws_message_error() {
        let msg = WsMessage::Error {
            message: "timeout".into(),
        };
        let json = msg.to_json();
        assert!(json.contains("\"type\":\"error\""));
    }
}
