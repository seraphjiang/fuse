// SPDX-License-Identifier: Apache-2.0

//! Result merging utilities for federated query results.
//!
//! When sub-queries fan out to multiple connectors (e.g. same query to 3
//! OpenSearch clusters), the results need to be merged. This module provides
//! utilities for union, global sort, and global limit on `RecordBatch` vectors.

use arrow::array::RecordBatch;
use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn};
use datafusion::error::{DataFusionError, Result};

/// Union multiple sets of RecordBatches into a single Vec, aligning schemas.
///
/// All batches must share the same schema (or be empty). Returns an empty vec
/// if all inputs are empty.
pub fn union_batches(batch_sets: Vec<Vec<RecordBatch>>) -> Result<Vec<RecordBatch>> {
    Ok(batch_sets.into_iter().flatten().collect())
}

/// Concatenate all batches into one, then apply a global limit.
pub fn merge_batches(batches: Vec<RecordBatch>, limit: Option<usize>) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let schema = batches[0].schema();
    let merged = concat_batches(&schema, &batches)?;

    match limit {
        Some(n) if n < merged.num_rows() => Ok(vec![merged.slice(0, n)]),
        _ => Ok(vec![merged]),
    }
}

/// Sort batches globally by the given column indices (ascending).
///
/// Concatenates all batches, sorts, and returns a single sorted batch.
pub fn sort_batches(
    batches: Vec<RecordBatch>,
    sort_columns: &[usize],
    descending: &[bool],
    limit: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() || sort_columns.is_empty() {
        return merge_batches(batches, limit);
    }

    let schema = batches[0].schema();
    let merged = concat_batches(&schema, &batches)?;

    let columns: Vec<SortColumn> = sort_columns
        .iter()
        .zip(descending.iter())
        .map(|(&col_idx, &desc)| SortColumn {
            values: merged.column(col_idx).clone(),
            options: Some(arrow::compute::SortOptions {
                descending: desc,
                nulls_first: !desc,
            }),
        })
        .collect();

    let indices = lexsort_to_indices(&columns, limit)?;

    let sorted_columns: Vec<_> = merged
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).map_err(DataFusionError::from))
        .collect::<Result<_>>()?;

    let sorted = RecordBatch::try_new(schema, sorted_columns)?;
    Ok(vec![sorted])
}
