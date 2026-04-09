// SPDX-License-Identifier: Apache-2.0

//! Result merging utilities for federated query results.
//!
//! When sub-queries fan out to multiple connectors (e.g. same query to 3
//! OpenSearch clusters), the results need to be merged. This module provides
//! utilities for union, global sort, global limit, schema alignment, and
//! deduplication on `RecordBatch` vectors.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{new_null_array, RecordBatch};
use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn};
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};

/// Compute the union schema of multiple schemas.
///
/// Fields are collected in order of first appearance. If a field appears in
/// multiple schemas, it is included once (using the first occurrence's type).
/// Fields missing from some schemas will be filled with nulls during alignment.
pub fn union_schema(schemas: &[SchemaRef]) -> SchemaRef {
    let mut seen = HashSet::new();
    let mut fields = Vec::new();
    for schema in schemas {
        for field in schema.fields() {
            if seen.insert(field.name().clone()) {
                // Mark nullable since not all sources may have this field
                let f = if schemas.len() > 1 {
                    Field::new(field.name(), field.data_type().clone(), true)
                } else {
                    field.as_ref().clone()
                };
                fields.push(f);
            }
        }
    }
    Arc::new(Schema::new(fields))
}

/// Align a RecordBatch to a target schema.
///
/// Columns present in the target but missing from the batch are filled with
/// null arrays. Columns in the batch but not in the target are dropped.
pub fn align_batch(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let columns: Vec<_> = target
        .fields()
        .iter()
        .map(|field| {
            match batch.schema().index_of(field.name()) {
                Ok(idx) => Ok(batch.column(idx).clone()),
                Err(_) => {
                    // Missing column → null array
                    Ok(new_null_array(field.data_type(), num_rows))
                }
            }
        })
        .collect::<Result<_>>()?;

    RecordBatch::try_new(target.clone(), columns).map_err(DataFusionError::from)
}

/// Union multiple sets of RecordBatches, aligning schemas.
///
/// Computes a union schema across all batches, then aligns each batch to it.
/// Returns an empty vec if all inputs are empty.
pub fn union_batches(batch_sets: Vec<Vec<RecordBatch>>) -> Result<Vec<RecordBatch>> {
    let all_batches: Vec<RecordBatch> = batch_sets.into_iter().flatten().collect();
    if all_batches.is_empty() {
        return Ok(vec![]);
    }

    let schemas: Vec<SchemaRef> = all_batches.iter().map(|b| b.schema()).collect();
    // Fast path: all schemas identical
    if schemas.windows(2).all(|w| w[0] == w[1]) {
        return Ok(all_batches);
    }

    let target = union_schema(&schemas);
    all_batches
        .iter()
        .map(|b| align_batch(b, &target))
        .collect()
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

/// Sort batches globally by the given column indices.
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

/// Deduplicate rows by the specified column names.
///
/// Keeps the first occurrence of each unique combination of values in the
/// dedup columns. Concatenates all batches first, then deduplicates.
pub fn dedup_batches(
    batches: Vec<RecordBatch>,
    dedup_columns: &[&str],
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() || dedup_columns.is_empty() {
        return Ok(batches);
    }

    let schema = batches[0].schema();
    let merged = concat_batches(&schema, &batches)?;
    let num_rows = merged.num_rows();

    // Resolve column indices
    let col_indices: Vec<usize> = dedup_columns
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|_| DataFusionError::Plan(format!("Dedup column '{}' not found", name)))
        })
        .collect::<Result<_>>()?;

    // Sort by dedup columns to group duplicates, then pick first of each group.
    let sort_cols: Vec<SortColumn> = col_indices
        .iter()
        .map(|&idx| SortColumn {
            values: merged.column(idx).clone(),
            options: Some(arrow::compute::SortOptions {
                descending: false,
                nulls_first: true,
            }),
        })
        .collect();

    let sorted_indices = lexsort_to_indices(&sort_cols, None)?;

    // Reorder all columns by sorted indices
    let sorted_columns: Vec<_> = merged
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &sorted_indices, None).map_err(DataFusionError::from))
        .collect::<Result<_>>()?;
    let sorted = RecordBatch::try_new(schema.clone(), sorted_columns)?;

    // Walk sorted rows, keep row if dedup-key columns differ from previous
    let mut keep = vec![false; num_rows];
    if num_rows > 0 {
        keep[0] = true;
    }
    for row in 1..num_rows {
        let mut differs = false;
        for &col_idx in &col_indices {
            let col = sorted.column(col_idx);
            // Compare string representations — works for all Arrow types
            let prev = format!("{:?}", col.slice(row - 1, 1));
            let curr = format!("{:?}", col.slice(row, 1));
            if prev != curr {
                differs = true;
                break;
            }
        }
        keep[row] = differs;
    }

    // Build indices of rows to keep
    let keep_indices: Vec<u32> = keep
        .iter()
        .enumerate()
        .filter_map(|(i, &k)| if k { Some(i as u32) } else { None })
        .collect();

    let indices_array = arrow::array::UInt32Array::from(keep_indices);
    let deduped_columns: Vec<_> = sorted
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices_array, None).map_err(DataFusionError::from))
        .collect::<Result<_>>()?;

    let result = RecordBatch::try_new(schema, deduped_columns)?;
    Ok(vec![result])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(schema: &SchemaRef, names: &[&str], vals: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Int64Array::from(vals.to_vec())),
            ],
        )
        .unwrap()
    }

    fn schema_ab() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    #[test]
    fn test_union_batches_same_schema() {
        let s = schema_ab();
        let b1 = make_batch(&s, &["a", "b"], &[1, 2]);
        let b2 = make_batch(&s, &["c"], &[3]);
        let result = union_batches(vec![vec![b1], vec![b2]]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].num_rows() + result[1].num_rows(), 3);
    }

    #[test]
    fn test_schema_alignment() {
        let s1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
        ]));
        let s2 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let target = union_schema(&[s1.clone(), s2.clone()]);
        assert_eq!(target.fields().len(), 2);

        let batch = RecordBatch::try_new(
            s1,
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let aligned = align_batch(&batch, &target).unwrap();
        assert_eq!(aligned.num_columns(), 2);
        assert_eq!(aligned.num_rows(), 2);
        assert!(aligned.column(1).is_null(0)); // "b" column is null
    }

    #[test]
    fn test_dedup() {
        let s = schema_ab();
        let b = make_batch(&s, &["a", "a", "b", "b", "c"], &[1, 2, 3, 4, 5]);
        let result = dedup_batches(vec![b], &["name"]).unwrap();
        assert_eq!(result[0].num_rows(), 3); // a, b, c
    }

    #[test]
    fn test_merge_with_limit() {
        let s = schema_ab();
        let b = make_batch(&s, &["a", "b", "c", "d"], &[1, 2, 3, 4]);
        let result = merge_batches(vec![b], Some(2)).unwrap();
        assert_eq!(result[0].num_rows(), 2);
    }

    #[test]
    fn test_union_schema_merges_fields() {
        let s1 = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
        ]));
        let s2 = Arc::new(Schema::new(vec![
            Field::new("y", DataType::Utf8, false),
        ]));
        let merged = union_schema(&[s1, s2]);
        assert_eq!(merged.fields().len(), 2);
        assert_eq!(merged.field(0).name(), "x");
        assert_eq!(merged.field(1).name(), "y");
    }

    #[test]
    fn test_union_schema_deduplicates() {
        let s1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let s2 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false),
        ]));
        let merged = union_schema(&[s1, s2]);
        assert_eq!(merged.fields().len(), 3); // a, b, c
    }

    #[test]
    fn test_sort_batches() {
        let s = schema_ab();
        let b = make_batch(&s, &["c", "a", "b"], &[3, 1, 2]);
        let sorted = sort_batches(vec![b], &[0], &[false], None).unwrap();
        let names = sorted[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(1), "b");
        assert_eq!(names.value(2), "c");
    }

    #[test]
    fn test_sort_batches_desc() {
        let s = schema_ab();
        let b = make_batch(&s, &["a", "c", "b"], &[1, 3, 2]);
        let sorted = sort_batches(vec![b], &[0], &[true], None).unwrap();
        let names = sorted[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "c");
        assert_eq!(names.value(1), "b");
        assert_eq!(names.value(2), "a");
    }

    #[test]
    fn test_merge_no_limit() {
        let s = schema_ab();
        let b = make_batch(&s, &["a", "b", "c"], &[1, 2, 3]);
        let result = merge_batches(vec![b], None).unwrap();
        assert_eq!(result[0].num_rows(), 3);
    }
}
