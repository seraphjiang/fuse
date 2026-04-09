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
