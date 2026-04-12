// SPDX-License-Identifier: Apache-2.0

//! Arrow IPC result format (#1802).
//!
//! Serialize query results as Arrow IPC (Feather v2) for zero-copy
//! consumption by Python (pyarrow), Rust, and other Arrow-native clients.
//! Used via `POST /api/fuse/query` with `Accept: application/vnd.apache.arrow.stream`.

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Serialize RecordBatches to Arrow IPC stream format (bytes).
pub fn batches_to_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .map_err(|e| format!("IPC writer init failed: {e}"))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| format!("IPC write failed: {e}"))?;
        }
        writer
            .finish()
            .map_err(|e| format!("IPC finish failed: {e}"))?;
    }
    Ok(buf)
}

/// Content type for Arrow IPC stream responses.
pub const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

/// Check if the client accepts Arrow IPC format.
pub fn accepts_arrow(accept_header: Option<&str>) -> bool {
    accept_header.is_some_and(|h| h.contains("apache.arrow") || h.contains("arrow.stream"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_batches_to_ipc() {
        let batch = sample_batch();
        let bytes = batches_to_ipc(&[batch]).unwrap();
        assert!(!bytes.is_empty());
        // Arrow IPC stream format starts with schema message (0xFFFFFFFF continuation marker)
        assert_eq!(&bytes[..4], &[255, 255, 255, 255]);
    }

    #[test]
    fn test_batches_to_ipc_multiple() {
        let b1 = sample_batch();
        let b2 = sample_batch();
        let bytes = batches_to_ipc(&[b1, b2]).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_batches_to_ipc_empty() {
        let bytes = batches_to_ipc(&[]).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_accepts_arrow_true() {
        assert!(accepts_arrow(Some("application/vnd.apache.arrow.stream")));
        assert!(accepts_arrow(Some(
            "text/html, application/vnd.apache.arrow.stream"
        )));
    }

    #[test]
    fn test_accepts_arrow_false() {
        assert!(!accepts_arrow(Some("application/json")));
        assert!(!accepts_arrow(None));
    }

    #[test]
    fn test_roundtrip() {
        let batch = sample_batch();
        let bytes = batches_to_ipc(std::slice::from_ref(&batch)).unwrap();
        // Verify we can read it back
        let cursor = std::io::Cursor::new(bytes);
        let mut reader = arrow::ipc::reader::StreamReader::try_new(cursor, None).unwrap();
        let read_batch = reader.next().unwrap().unwrap();
        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 2);
    }
}
