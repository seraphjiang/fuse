// SPDX-License-Identifier: Apache-2.0

//! Integration tests for result merger: schema alignment, dedup, edge cases.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use fuse_engine::{
    align_batch, dedup_batches, merge_batches, sort_batches, union_batches, union_schema,
};

fn schema_2col(name1: &str, name2: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(name1, DataType::Utf8, false),
        Field::new(name2, DataType::Int64, false),
    ]))
}

fn batch_str_int(schema: &SchemaRef, names: &[&str], vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Int64Array::from(vals.to_vec())),
        ],
    )
    .unwrap()
}

// ── Schema alignment: different column sets from 2 connectors ──

#[test]
fn test_schema_alignment_disjoint_columns() {
    let s1 = Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, false)]));
    let s2 = Arc::new(Schema::new(vec![Field::new(
        "region",
        DataType::Utf8,
        false,
    )]));
    let target = union_schema(&[s1.clone(), s2.clone()]);
    assert_eq!(target.fields().len(), 2);
    assert_eq!(target.field(0).name(), "host");
    assert_eq!(target.field(1).name(), "region");

    // Align s1 batch to target — region should be null
    let b1 = RecordBatch::try_new(s1, vec![Arc::new(StringArray::from(vec!["h1"]))]).unwrap();
    let aligned = align_batch(&b1, &target).unwrap();
    assert_eq!(aligned.num_columns(), 2);
    assert!(aligned.column(1).is_null(0));
}

#[test]
fn test_schema_alignment_overlapping_columns() {
    let s1 = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let s2 = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Utf8, false),
        Field::new("c", DataType::Int64, false),
    ]));
    let target = union_schema(&[s1, s2]);
    assert_eq!(target.fields().len(), 3); // a, b, c
}

#[test]
fn test_union_batches_different_schemas() {
    let s1 = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let s2 = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, false),
    ]));

    let b1 = RecordBatch::try_new(
        s1,
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let b2 = RecordBatch::try_new(
        s2,
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["t"])),
        ],
    )
    .unwrap();

    let result = union_batches(vec![vec![b1], vec![b2]]).unwrap();
    assert_eq!(result.len(), 2);
    // Both should have 3 columns: name, val, tag
    assert_eq!(result[0].num_columns(), 3);
    assert_eq!(result[1].num_columns(), 3);
}

// ── dedup_batches ──

#[test]
fn test_dedup_batches_no_duplicates() {
    let s = schema_2col("name", "val");
    let b = batch_str_int(&s, &["a", "b", "c"], &[1, 2, 3]);
    let result = dedup_batches(vec![b], &["name"]).unwrap();
    assert_eq!(result[0].num_rows(), 3);
}

#[test]
fn test_dedup_batches_all_duplicates() {
    let s = schema_2col("name", "val");
    let b = batch_str_int(&s, &["x", "x", "x"], &[1, 2, 3]);
    let result = dedup_batches(vec![b], &["name"]).unwrap();
    assert_eq!(result[0].num_rows(), 1);
}

#[test]
fn test_dedup_batches_multi_column_key() {
    let s = schema_2col("name", "val");
    // (a,1), (a,2), (a,1) — dedup on both columns → 2 unique
    let b = batch_str_int(&s, &["a", "a", "a"], &[1, 2, 1]);
    let result = dedup_batches(vec![b], &["name", "val"]).unwrap();
    assert_eq!(result[0].num_rows(), 2);
}

#[test]
fn test_dedup_batches_across_batches() {
    let s = schema_2col("name", "val");
    let b1 = batch_str_int(&s, &["a", "b"], &[1, 2]);
    let b2 = batch_str_int(&s, &["b", "c"], &[2, 3]);
    let result = dedup_batches(vec![b1, b2], &["name"]).unwrap();
    assert_eq!(result[0].num_rows(), 3); // a, b, c
}

#[test]
fn test_dedup_empty_columns_returns_input() {
    let s = schema_2col("name", "val");
    let b = batch_str_int(&s, &["a"], &[1]);
    let result = dedup_batches(vec![b.clone()], &[]).unwrap();
    assert_eq!(result[0].num_rows(), 1);
}

// ── Edge cases ──

#[test]
fn test_union_batches_empty_input() {
    let result = union_batches(vec![]).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_union_batches_one_empty_set() {
    let s = schema_2col("name", "val");
    let b = batch_str_int(&s, &["a"], &[1]);
    let result = union_batches(vec![vec![b], vec![]]).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 1);
}

#[test]
fn test_merge_batches_empty() {
    let result = merge_batches(vec![], Some(10)).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_merge_batches_limit_exceeds_rows() {
    let s = schema_2col("name", "val");
    let b = batch_str_int(&s, &["a", "b"], &[1, 2]);
    let result = merge_batches(vec![b], Some(100)).unwrap();
    assert_eq!(result[0].num_rows(), 2);
}

#[test]
fn test_merge_batches_no_limit() {
    let s = schema_2col("name", "val");
    let b = batch_str_int(&s, &["a", "b", "c"], &[1, 2, 3]);
    let result = merge_batches(vec![b], None).unwrap();
    assert_eq!(result[0].num_rows(), 3);
}

#[test]
fn test_sort_batches_empty() {
    let result = sort_batches(vec![], &[0], &[false], None).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_sort_batches_single_row() {
    let s = schema_2col("name", "val");
    let b = batch_str_int(&s, &["a"], &[1]);
    let result = sort_batches(vec![b], &[1], &[false], None).unwrap();
    assert_eq!(result[0].num_rows(), 1);
}

#[test]
fn test_sort_batches_descending_with_limit() {
    let s = schema_2col("name", "val");
    let b = batch_str_int(&s, &["a", "b", "c"], &[3, 1, 2]);
    let result = sort_batches(vec![b], &[1], &[true], Some(2)).unwrap();
    assert_eq!(result[0].num_rows(), 2);
    let vals = result[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(vals.value(0), 3);
    assert_eq!(vals.value(1), 2);
}
