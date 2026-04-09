// SPDX-License-Identifier: Apache-2.0

//! Parquet file reader — reads Parquet bytes into Arrow RecordBatches.

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use fuse_core::error::ConnectorError;

/// Read the Arrow schema from Parquet file bytes (footer only).
pub fn read_schema(data: &Bytes) -> Result<Schema, ConnectorError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())
        .map_err(|e| ConnectorError::schema(e))?;
    Ok(builder.schema().as_ref().clone())
}

/// Read Parquet bytes into RecordBatches, optionally projecting columns.
pub fn read_batches(
    data: &Bytes,
    projections: &[String],
    batch_size: usize,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())
        .map_err(|e| ConnectorError::query(e))?;

    // Apply projection if specified
    if !projections.is_empty() {
        let parquet_schema = builder.parquet_schema().clone();
        let indices: Vec<usize> = projections
            .iter()
            .filter_map(|name| {
                parquet_schema
                    .columns()
                    .iter()
                    .position(|c| c.name() == name)
            })
            .collect();

        if !indices.is_empty() {
            let mask = ProjectionMask::leaves(&parquet_schema, indices);
            builder = builder.with_projection(mask);
        }
    }

    let reader = builder
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| ConnectorError::query(e))?;

    reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ConnectorError::query(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;

    /// Write a RecordBatch to in-memory Parquet bytes.
    fn to_parquet_bytes(batch: RecordBatch) -> Bytes {
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_read_schema() {
        let data = to_parquet_bytes(sample_batch());
        let schema = read_schema(&data).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn test_read_batches_all_columns() {
        let data = to_parquet_bytes(sample_batch());
        let batches = read_batches(&data, &[], 1024).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_read_batches_with_projection() {
        let data = to_parquet_bytes(sample_batch());
        let batches = read_batches(&data, &["name".to_string()], 1024).unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "name");
    }

    #[test]
    fn test_read_batches_unknown_projection_ignored() {
        let data = to_parquet_bytes(sample_batch());
        // Unknown column — falls back to all columns
        let batches = read_batches(&data, &["nonexistent".to_string()], 1024).unwrap();
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_read_batches_respects_batch_size() {
        let data = to_parquet_bytes(sample_batch());
        let batches = read_batches(&data, &[], 1).unwrap();
        // batch_size=1 → up to 3 batches of 1 row each
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        assert!(batches.iter().all(|b| b.num_rows() <= 1));
    }

    #[test]
    fn test_read_schema_invalid_bytes_returns_error() {
        let bad = Bytes::from(b"not parquet".to_vec());
        assert!(read_schema(&bad).is_err());
    }
}
