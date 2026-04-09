// SPDX-License-Identifier: Apache-2.0

//! NDJSON parsing: gzip decompression, schema discovery, and Arrow conversion.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use flate2::read::GzDecoder;

use fuse_core::error::ConnectorError;

/// Decompress gzipped bytes. Returns raw bytes if not gzip.
pub fn decompress(data: &[u8]) -> Result<Vec<u8>, ConnectorError> {
    // Check gzip magic bytes
    if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
        let mut decoder = GzDecoder::new(data);
        let mut out = Vec::new();
        decoder
            .read_to_end(&mut out)
            .map_err(|e| ConnectorError::QueryFailed(format!("gzip decompress failed: {e}")))?;
        Ok(out)
    } else {
        Ok(data.to_vec())
    }
}

/// Parse NDJSON lines from raw bytes.
pub fn parse_lines(data: &[u8]) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let text = String::from_utf8_lossy(data);
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| match v {
            serde_json::Value::Object(m) => Some(m),
            _ => None,
        })
        .collect()
}

/// Discover schema from NDJSON records by sampling field names.
/// All fields are typed as Utf8 (string) since NDJSON is untyped.
pub fn discover_schema(
    records: &[serde_json::Map<String, serde_json::Value>],
) -> Schema {
    let mut fields: BTreeMap<String, ()> = BTreeMap::new();
    for record in records.iter().take(100) {
        for key in record.keys() {
            fields.entry(key.clone()).or_default();
        }
    }
    Schema::new(
        fields
            .keys()
            .map(|k| Field::new(k, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    )
}

/// Convert NDJSON records to Arrow RecordBatch with the given schema.
pub fn records_to_batch(
    records: &[serde_json::Map<String, serde_json::Value>],
    schema: &Schema,
    projections: &[String],
) -> Result<RecordBatch, ConnectorError> {
    let fields: Vec<&Field> = if projections.is_empty() {
        schema.fields().iter().map(|f| f.as_ref()).collect()
    } else {
        projections
            .iter()
            .filter_map(|p| schema.field_with_name(p).ok())
            .collect()
    };

    if fields.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
    }

    let proj_schema = Schema::new(
        fields.iter().map(|f| (*f).clone()).collect::<Vec<_>>(),
    );

    let arrays: Vec<Arc<dyn arrow::array::Array>> = fields
        .iter()
        .map(|field| {
            let values: Vec<Option<String>> = records
                .iter()
                .map(|r| {
                    r.get(field.name()).and_then(|v| match v {
                        serde_json::Value::Null => None,
                        serde_json::Value::String(s) => Some(s.clone()),
                        other => Some(other.to_string()),
                    })
                })
                .collect();
            Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>
        })
        .collect();

    RecordBatch::try_new(Arc::new(proj_schema), arrays)
        .map_err(|e| ConnectorError::query(e))
}

/// Apply a simple client-side limit to records.
pub fn apply_limit(
    records: Vec<serde_json::Map<String, serde_json::Value>>,
    limit: Option<u64>,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    match limit {
        Some(n) => records.into_iter().take(n as usize).collect(),
        None => records,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn sample_ndjson() -> &'static str {
        r#"{"timestamp":"2025-01-01T00:00:00Z","level":"ERROR","service":"api-gw","message":"timeout","trace_id":"t-001"}
{"timestamp":"2025-01-01T00:00:01Z","level":"INFO","service":"auth","message":"ok","trace_id":"t-002"}
{"timestamp":"2025-01-01T00:00:02Z","level":"WARN","service":"api-gw","message":"slow","trace_id":"t-003"}"#
    }

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn test_decompress_gzip() {
        let raw = b"hello world";
        let compressed = gzip(raw);
        let result = decompress(&compressed).unwrap();
        assert_eq!(result, raw);
    }

    #[test]
    fn test_decompress_plain() {
        let raw = b"not gzipped";
        let result = decompress(raw).unwrap();
        assert_eq!(result, raw);
    }

    #[test]
    fn test_parse_lines() {
        let records = parse_lines(sample_ndjson().as_bytes());
        assert_eq!(records.len(), 3);
        assert_eq!(records[0]["service"], "api-gw");
        assert_eq!(records[1]["level"], "INFO");
        assert_eq!(records[2]["trace_id"], "t-003");
    }

    #[test]
    fn test_parse_lines_skips_invalid() {
        let data = b"{\"a\":1}\nnot json\n{\"b\":2}\n";
        let records = parse_lines(data);
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_parse_lines_empty() {
        let records = parse_lines(b"");
        assert!(records.is_empty());
    }

    #[test]
    fn test_discover_schema() {
        let records = parse_lines(sample_ndjson().as_bytes());
        let schema = discover_schema(&records);
        assert_eq!(schema.fields().len(), 5);
        // BTreeMap sorts alphabetically
        assert_eq!(schema.field(0).name(), "level");
        assert_eq!(schema.field(1).name(), "message");
        assert_eq!(schema.field(2).name(), "service");
        assert_eq!(schema.field(3).name(), "timestamp");
        assert_eq!(schema.field(4).name(), "trace_id");
        // All Utf8
        for f in schema.fields() {
            assert_eq!(*f.data_type(), DataType::Utf8);
            assert!(f.is_nullable());
        }
    }

    #[test]
    fn test_records_to_batch_all_columns() {
        let records = parse_lines(sample_ndjson().as_bytes());
        let schema = discover_schema(&records);
        let batch = records_to_batch(&records, &schema, &[]).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_records_to_batch_projection() {
        let records = parse_lines(sample_ndjson().as_bytes());
        let schema = discover_schema(&records);
        let batch = records_to_batch(
            &records,
            &schema,
            &["service".into(), "trace_id".into()],
        )
        .unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "service");
        assert_eq!(batch.schema().field(1).name(), "trace_id");

        let svc = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(svc.value(0), "api-gw");
        assert_eq!(svc.value(1), "auth");
    }

    #[test]
    fn test_records_to_batch_with_null() {
        use arrow::array::Array;
        let data = b"{\"a\":\"x\",\"b\":null}\n{\"a\":\"y\"}\n";
        let records = parse_lines(data);
        let schema = discover_schema(&records);
        let batch = records_to_batch(&records, &schema, &[]).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let col_b = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(col_b.is_null(0));
        assert!(col_b.is_null(1));
    }

    #[test]
    fn test_apply_limit() {
        let records = parse_lines(sample_ndjson().as_bytes());
        let limited = apply_limit(records, Some(2));
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn test_apply_limit_none() {
        let records = parse_lines(sample_ndjson().as_bytes());
        let all = apply_limit(records, None);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_end_to_end_gzip_ndjson() {
        let compressed = gzip(sample_ndjson().as_bytes());
        let raw = decompress(&compressed).unwrap();
        let records = parse_lines(&raw);
        let schema = discover_schema(&records);
        let batch = records_to_batch(&records, &schema, &[]).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_numeric_values_stringified() {
        let data = b"{\"status\":500,\"count\":42.5,\"active\":true}\n";
        let records = parse_lines(data);
        let schema = discover_schema(&records);
        let batch = records_to_batch(&records, &schema, &[]).unwrap();
        let col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col.value(0), "500"); // numeric → string
    }
}
