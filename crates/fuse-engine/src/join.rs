// SPDX-License-Identifier: Apache-2.0

//! Cross-source JOIN execution for federated queries.
//!
//! When a JOIN spans two different connectors (e.g. OpenSearch × S3), DataFusion
//! cannot push the entire plan to a single remote engine. This module provides:
//!
//! - **HashJoin**: Build a hash table from the smaller side, probe with the larger.
//! - **SemiJoin**: Extract join keys from one side, push as an IN-list filter to
//!   the other side, then hash-join the filtered results. This is the key
//!   optimization from the Fuse proposal — it avoids full table scans on the
//!   probe side.
//! - **JoinPlanner**: Uses the cost model to decide execution order and strategy.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    UInt32Array,
};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};

use fuse_core::connector::{
    ConnectorCapabilities, FederatedConnector, FilterExpr, ScalarValue, SubQuery,
};

use crate::cost::{estimate_remote_cost, CostEstimate, QueryWorkload, TableStats};

/// Join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    /// Right outer join: swap sides then left join
    Right,
    /// Full outer join: all rows from both sides, NULLs where no match
    Full,
    /// Semi-join: return left rows that have a match in right (EXISTS)
    Semi,
    /// Anti-join: return left rows that have NO match in right (NOT EXISTS)
    Anti,
}

/// Strategy chosen by the planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    /// Build hash table from build side, probe with probe side.
    Hash,
    /// Extract keys from build side, push IN-filter to probe side, then hash-join.
    Semi,
}

/// Plan for executing a cross-source join.
#[derive(Debug, Clone)]
pub struct JoinPlan {
    /// Which side to scan first (build the hash table from).
    pub build_side: JoinSide,
    /// Strategy to use.
    pub strategy: JoinStrategy,
    /// Estimated cost.
    pub estimated_cost: CostEstimate,
}

/// Identifies one side of a join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    Left,
    Right,
}

/// Threshold: if the build side has fewer estimated rows than this, use
/// semi-join (extract keys → IN-filter). Above this, plain hash join is
/// cheaper because the IN-list would be too large.
const SEMI_JOIN_KEY_THRESHOLD: u64 = 10_000;

/// Plan a cross-source join given stats from both sides.
pub fn plan_join(
    left_caps: &ConnectorCapabilities,
    left_stats: &TableStats,
    right_caps: &ConnectorCapabilities,
    right_stats: &TableStats,
    workload: &QueryWorkload,
) -> JoinPlan {
    let left_cost = estimate_remote_cost(left_caps, left_stats, workload);
    let right_cost = estimate_remote_cost(right_caps, right_stats, workload);

    // Build from the smaller/cheaper side
    let (build_side, build_rows) = if left_stats.estimated_rows <= right_stats.estimated_rows {
        (JoinSide::Left, left_stats.estimated_rows)
    } else {
        (JoinSide::Right, right_stats.estimated_rows)
    };

    // Use semi-join when build side is small enough to extract keys as IN-filter
    let strategy = if build_rows <= SEMI_JOIN_KEY_THRESHOLD {
        JoinStrategy::Semi
    } else {
        JoinStrategy::Hash
    };

    let estimated_cost = CostEstimate::new(
        left_cost.cpu + right_cost.cpu,
        left_cost.network + right_cost.network,
    );

    JoinPlan {
        build_side,
        strategy,
        estimated_cost,
    }
}

/// Extract distinct join key values from record batches.
///
/// Returns scalar values suitable for constructing an IN-list filter to push
/// to the probe-side connector.
pub fn extract_join_keys(batches: &[RecordBatch], key_column: &str) -> Result<Vec<ScalarValue>> {
    let mut seen = std::collections::HashSet::new();
    let mut keys = Vec::new();

    for batch in batches {
        let col_idx = batch
            .schema()
            .index_of(key_column)
            .map_err(|_| DataFusionError::Plan(format!("join key '{}' not found", key_column)))?;
        let col = batch.column(col_idx);

        for row in 0..col.len() {
            if col.is_null(row) {
                continue;
            }
            let val = array_value_to_scalar(col, row)?;
            let key = format!("{:?}", val);
            if seen.insert(key) {
                keys.push(val);
            }
        }
    }

    Ok(keys)
}

/// Build an IN-list filter from extracted keys.
pub fn keys_to_in_filter(field: &str, keys: Vec<ScalarValue>) -> Option<FilterExpr> {
    if keys.is_empty() {
        return None;
    }
    Some(FilterExpr::In {
        field: field.to_string(),
        values: keys,
    })
}

/// Execute a hash join on two sets of RecordBatches.
///
/// Builds a hash table from `build_batches` on `build_key`, then probes with
/// `probe_batches` on `probe_key`. Returns joined batches with columns from
/// both sides (build columns first, then probe columns).
pub fn hash_join(
    build_batches: &[RecordBatch],
    build_key: &str,
    probe_batches: &[RecordBatch],
    probe_key: &str,
    join_type: JoinType,
) -> Result<Vec<RecordBatch>> {
    // RIGHT JOIN = swap sides + LEFT JOIN
    if join_type == JoinType::Right {
        return hash_join(probe_batches, probe_key, build_batches, build_key, JoinType::Left);
    }

    if build_batches.is_empty() || probe_batches.is_empty() {
        return Ok(vec![]);
    }

    let build_schema = build_batches[0].schema();
    let probe_schema = probe_batches[0].schema();
    let build_merged = concat_batches(&build_schema, build_batches)?;
    let probe_merged = concat_batches(&probe_schema, probe_batches)?;

    let build_key_idx = build_schema.index_of(build_key).map_err(|_| {
        DataFusionError::Plan(format!("build key '{}' not found", build_key))
    })?;
    let probe_key_idx = probe_schema.index_of(probe_key).map_err(|_| {
        DataFusionError::Plan(format!("probe key '{}' not found", probe_key))
    })?;

    // Build hash table: key_string -> Vec<row_index>
    let build_col = build_merged.column(build_key_idx);
    let mut hash_table: HashMap<String, Vec<u32>> = HashMap::new();
    for row in 0..build_merged.num_rows() {
        if build_col.is_null(row) {
            continue;
        }
        let key = arrow::util::display::array_value_to_string(build_col, row)
            .unwrap_or_default();
        hash_table.entry(key).or_default().push(row as u32);
    }

    // Probe
    let probe_col = probe_merged.column(probe_key_idx);
    let mut build_indices = Vec::new();
    let mut probe_indices = Vec::new();
    let mut build_matched = vec![false; build_merged.num_rows()];

    for probe_row in 0..probe_merged.num_rows() {
        if probe_col.is_null(probe_row) {
            if join_type == JoinType::Left || join_type == JoinType::Anti || join_type == JoinType::Full {
                build_indices.push(None);
                probe_indices.push(Some(probe_row as u32));
            }
            continue;
        }
        let key = arrow::util::display::array_value_to_string(probe_col, probe_row)
            .unwrap_or_default();
        match hash_table.get(&key) {
            Some(build_rows) => {
                match join_type {
                    JoinType::Semi => {
                        // Emit probe row once (no build columns)
                        probe_indices.push(Some(probe_row as u32));
                    }
                    JoinType::Anti => {
                        // Has match — skip (anti-join excludes matches)
                    }
                    _ => {
                        for &br in build_rows {
                            build_indices.push(Some(br));
                            probe_indices.push(Some(probe_row as u32));
                            build_matched[br as usize] = true;
                        }
                    }
                }
            }
            None => {
                if join_type == JoinType::Left || join_type == JoinType::Anti || join_type == JoinType::Full {
                    build_indices.push(None);
                    probe_indices.push(Some(probe_row as u32));
                }
            }
        }
    }

    // Full outer join: emit unmatched build rows with NULL probe columns
    if join_type == JoinType::Full {
        for (row, matched) in build_matched.iter().enumerate() {
            if !matched {
                build_indices.push(Some(row as u32));
                probe_indices.push(None);
            }
        }
    }

    // Semi/Anti joins return only probe-side columns
    if join_type == JoinType::Semi || join_type == JoinType::Anti {
        let pi: Vec<u32> = probe_indices.iter().filter_map(|p| *p).collect();
        let probe_idx_array = UInt32Array::from(pi);
        let mut output_columns: Vec<ArrayRef> = Vec::new();
        for col_idx in 0..probe_merged.num_columns() {
            let col = probe_merged.column(col_idx);
            let gathered = take(col.as_ref(), &probe_idx_array, None)?;
            output_columns.push(gathered);
        }
        let result = RecordBatch::try_new(probe_schema.clone(), output_columns)?;
        return Ok(vec![result]);
    }

    // Assemble output: build columns (with nulls for unmatched) + probe columns (with nulls for unmatched)
    let output_schema = merge_schemas(&build_schema, &probe_schema, build_key, probe_key, join_type);

    let mut output_columns: Vec<ArrayRef> = Vec::new();

    // Build-side columns
    for col_idx in 0..build_merged.num_columns() {
        let col = build_merged.column(col_idx);
        let gathered = gather_with_nulls(col, &build_indices)?;
        output_columns.push(gathered);
    }

    // Probe-side columns (skip the join key to avoid duplication)
    for col_idx in 0..probe_merged.num_columns() {
        if col_idx == probe_key_idx {
            continue;
        }
        let col = probe_merged.column(col_idx);
        let gathered = gather_with_nulls(col, &probe_indices)?;
        output_columns.push(gathered);
    }

    let result = RecordBatch::try_new(Arc::new(output_schema), output_columns)?;
    Ok(vec![result])
}

/// Execute the full semi-join pipeline:
/// 1. Query build-side connector
/// 2. Extract join keys
/// 3. Create IN-filter for probe-side
/// 4. Query probe-side connector with filter
/// 5. Hash-join the results
pub async fn execute_semi_join(
    build_connector: &Arc<dyn FederatedConnector>,
    build_query: &SubQuery,
    build_key: &str,
    probe_connector: &Arc<dyn FederatedConnector>,
    probe_query: &SubQuery,
    probe_key: &str,
    join_type: JoinType,
) -> Result<Vec<RecordBatch>> {
    // Step 1: fetch build side
    let build_batches = build_connector
        .execute(build_query)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    if build_batches.is_empty() {
        return Ok(vec![]);
    }

    // Step 2: extract keys
    let keys = extract_join_keys(&build_batches, build_key)?;
    if keys.is_empty() {
        return Ok(vec![]);
    }

    // Step 3: build IN-filter and augment probe query
    let in_filter = keys_to_in_filter(probe_key, keys);
    let augmented_probe = SubQuery {
        filter: match (&probe_query.filter, in_filter) {
            (Some(existing), Some(in_f)) => {
                Some(FilterExpr::And(Box::new(existing.clone()), Box::new(in_f)))
            }
            (None, Some(in_f)) => Some(in_f),
            (Some(existing), None) => Some(existing.clone()),
            (None, None) => None,
        },
        ..probe_query.clone()
    };

    // Step 4: fetch probe side with IN-filter pushed down
    let probe_batches = probe_connector
        .execute(&augmented_probe)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Step 5: local hash join
    hash_join(&build_batches, build_key, &probe_batches, probe_key, join_type)
}

// ── Internal helpers ──

fn array_value_to_scalar(col: &ArrayRef, row: usize) -> Result<ScalarValue> {
    match col.data_type() {
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(ScalarValue::Utf8(arr.value(row).to_string()))
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(ScalarValue::Int64(arr.value(row)))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(ScalarValue::Float64(arr.value(row)))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(ScalarValue::Boolean(arr.value(row)))
        }
        _dt => Ok(ScalarValue::Utf8(
            arrow::util::display::array_value_to_string(col, row)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        )),
    }
}

fn gather_with_nulls(col: &ArrayRef, indices: &[Option<u32>]) -> Result<ArrayRef> {
    // For rows with Some(idx), take from col. For None, produce null.
    let valid_indices: Vec<u32> = indices.iter().map(|o| o.unwrap_or(0)).collect();
    let idx_array = UInt32Array::from(valid_indices);
    let taken = take(col.as_ref(), &idx_array, None)?;

    // Apply null mask for None entries
    let nulls: Vec<bool> = indices.iter().map(|o| o.is_some()).collect();
    let null_buffer = BooleanArray::from(nulls);

    // Use arrow compute filter to null out unmatched rows
    let result = arrow::compute::nullif(&taken, &arrow::compute::not(&null_buffer)?)?;
    Ok(result)
}

fn merge_schemas(
    build: &SchemaRef,
    probe: &SchemaRef,
    _build_key: &str,
    probe_key: &str,
    join_type: JoinType,
) -> Schema {
    let mut fields: Vec<Field> = build
        .fields()
        .iter()
        .map(|f| {
            // Left/Full join: build-side columns become nullable (unmatched probe rows)
            if join_type == JoinType::Left || join_type == JoinType::Full {
                Field::new(f.name(), f.data_type().clone(), true)
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    for field in probe.fields() {
        if field.name() == probe_key {
            continue;
        }
        let name = if build.field_with_name(field.name()).is_ok() {
            format!("probe_{}", field.name())
        } else {
            field.name().clone()
        };
        fields.push(Field::new(name, field.data_type().clone(), true));
    }
    Schema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_batch(keys: &[&str], vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(vals.to_vec())),
            ],
        )
        .unwrap()
    }

    fn probe_batch(keys: &[&str], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_hash_join_inner() {
        let build = build_batch(&["a", "b", "c"], &[1, 2, 3]);
        let probe = probe_batch(&["b", "c", "d"], &["bob", "carol", "dave"]);

        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Inner).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2); // b, c match
        assert_eq!(result[0].num_columns(), 3); // id, value, name (probe id skipped)
    }

    #[test]
    fn test_hash_join_left() {
        let build = build_batch(&["a", "b"], &[1, 2]);
        let probe = probe_batch(&["b", "c", "d"], &["bob", "carol", "dave"]);

        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Left).unwrap();
        assert_eq!(result.len(), 1);
        // Left join: b matches, c and d have no build match → included with nulls
        assert_eq!(result[0].num_rows(), 3);
    }

    #[test]
    fn test_extract_join_keys() {
        let batch = build_batch(&["a", "b", "a", "c"], &[1, 2, 3, 4]);
        let keys = extract_join_keys(&[batch], "id").unwrap();
        assert_eq!(keys.len(), 3); // a, b, c (deduplicated)
    }

    #[test]
    fn test_keys_to_in_filter() {
        let keys = vec![ScalarValue::Utf8("a".into()), ScalarValue::Utf8("b".into())];
        let filter = keys_to_in_filter("id", keys).unwrap();
        match filter {
            FilterExpr::In { field, values } => {
                assert_eq!(field, "id");
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected In filter"),
        }
    }

    #[test]
    fn test_plan_join_picks_smaller_build_side() {
        let small = TableStats {
            estimated_rows: 100,
            avg_row_bytes: 256,
        };
        let large = TableStats {
            estimated_rows: 1_000_000,
            avg_row_bytes: 256,
        };
        let caps = ConnectorCapabilities::full();
        let workload = QueryWorkload::default();

        let plan = plan_join(&caps, &small, &caps, &large, &workload);
        assert_eq!(plan.build_side, JoinSide::Left); // small is left
        assert_eq!(plan.strategy, JoinStrategy::Semi); // 100 rows < threshold
    }

    #[test]
    fn test_plan_join_uses_hash_for_large_build() {
        let large_a = TableStats {
            estimated_rows: 500_000,
            avg_row_bytes: 256,
        };
        let large_b = TableStats {
            estimated_rows: 1_000_000,
            avg_row_bytes: 256,
        };
        let caps = ConnectorCapabilities::full();
        let workload = QueryWorkload::default();

        let plan = plan_join(&caps, &large_a, &caps, &large_b, &workload);
        assert_eq!(plan.build_side, JoinSide::Left);
        assert_eq!(plan.strategy, JoinStrategy::Hash); // 500k > threshold
    }

    #[test]
    fn test_hash_join_empty_inputs() {
        let build = build_batch(&["a"], &[1]);
        let result = hash_join(&[build], "id", &[], "id", JoinType::Inner).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_hash_join_many_to_many_duplicate_keys() {
        // Build: two rows with key "a", Probe: two rows with key "a"
        // Should produce 2×2 = 4 result rows
        let build = build_batch(&["a", "a"], &[1, 2]);
        let probe = probe_batch(&["a", "a"], &["x", "y"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Inner).unwrap();
        assert_eq!(result[0].num_rows(), 4);
    }

    #[test]
    fn test_extract_join_keys_skips_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None, Some("b"), None])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();
        let keys = extract_join_keys(&[batch], "id").unwrap();
        assert_eq!(keys.len(), 2); // only "a" and "b", nulls skipped
    }

    #[test]
    fn test_keys_to_in_filter_empty_returns_none() {
        let result = keys_to_in_filter("id", vec![]);
        assert!(result.is_none());
    }

    #[test]
    fn test_left_join_empty_build_side() {
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let result = hash_join(&[], "id", &[probe], "id", JoinType::Left).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_semi_join() {
        let build = build_batch(&["a", "c"], &[100, 300]);
        let probe = probe_batch(&["a", "b", "d"], &["alice", "bob", "dave"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Semi).unwrap();
        assert_eq!(result.len(), 1);
        // Only probe row "a" matches — should return 1 row with probe columns only
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[0].num_columns(), 2); // id + name (probe schema)
    }

    #[test]
    fn test_anti_join() {
        let build = build_batch(&["a", "c"], &[100, 300]);
        let probe = probe_batch(&["a", "b", "d"], &["alice", "bob", "dave"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Anti).unwrap();
        assert_eq!(result.len(), 1);
        // Probe rows "b" and "d" have no match — should return 2 rows
        assert_eq!(result[0].num_rows(), 2);
        assert_eq!(result[0].num_columns(), 2); // probe schema only
    }

    // ── #341 Semi/anti-join verification (tester) ──

    #[test]
    fn test_semi_join_no_overlap() {
        // No common keys → semi returns 0 rows
        let build = build_batch(&["x", "y"], &[1, 2]);
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Semi).unwrap();
        let rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0, "semi with no overlap should return 0 rows");
    }

    #[test]
    fn test_anti_join_no_overlap() {
        // No common keys → anti returns ALL probe rows
        let build = build_batch(&["x", "y"], &[1, 2]);
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Anti).unwrap();
        let rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "anti with no overlap should return all probe rows");
    }

    #[test]
    fn test_semi_join_full_overlap() {
        // All keys match → semi returns ALL probe rows
        let build = build_batch(&["a", "b"], &[1, 2]);
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Semi).unwrap();
        let rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "semi with full overlap should return all probe rows");
    }

    #[test]
    fn test_anti_join_full_overlap() {
        // All keys match → anti returns 0 rows
        let build = build_batch(&["a", "b"], &[1, 2]);
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Anti).unwrap();
        let rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0, "anti with full overlap should return 0 rows");
    }

    #[test]
    fn test_semi_anti_complement() {
        // Semi + Anti should equal total probe rows
        let build = build_batch(&["a", "c"], &[1, 3]);
        let probe = probe_batch(&["a", "b", "c", "d"], &["a1", "b1", "c1", "d1"]);
        let semi: usize = hash_join(std::slice::from_ref(&build), "id", std::slice::from_ref(&probe), "id", JoinType::Semi)
            .unwrap().iter().map(|b| b.num_rows()).sum();
        let anti: usize = hash_join(std::slice::from_ref(&build), "id", std::slice::from_ref(&probe), "id", JoinType::Anti)
            .unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(semi + anti, 4, "semi ({}) + anti ({}) should equal probe rows (4)", semi, anti);
    }

    #[test]
    fn test_semi_join_returns_probe_columns_only() {
        let build = build_batch(&["a"], &[100]);
        let probe = probe_batch(&["a"], &["alice"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Semi).unwrap();
        let schema = result[0].schema();
        let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // Should have probe columns (id, name), NOT build columns (id, value)
        assert!(cols.contains(&"name"), "should have probe 'name' column: {:?}", cols);
        assert!(!cols.contains(&"value"), "should NOT have build 'value' column: {:?}", cols);
    }

    #[test]
    fn test_full_outer_join() {
        let build = build_batch(&["a", "c"], &[100, 300]);
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Full).unwrap();
        assert_eq!(result.len(), 1);
        // a matches → 1 row, b unmatched probe → 1 row, c unmatched build → 1 row = 3
        assert_eq!(result[0].num_rows(), 3);
    }

    #[test]
    fn test_full_outer_join_no_overlap() {
        let build = build_batch(&["x", "y"], &[100, 200]);
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Full).unwrap();
        assert_eq!(result.len(), 1);
        // No matches: 2 unmatched probe + 2 unmatched build = 4
        assert_eq!(result[0].num_rows(), 4);
    }

    // ── #405 FULL OUTER JOIN verification (tester) ──

    #[test]
    fn test_full_outer_join_all_match() {
        let build = build_batch(&["a", "b"], &[100, 200]);
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Full).unwrap();
        assert_eq!(result[0].num_rows(), 2, "all match → no extra NULL rows");
    }

    #[test]
    fn test_full_outer_join_nulls_present() {
        let build = build_batch(&["a", "c"], &[100, 300]);
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Full).unwrap();
        let batch = &result[0];
        // Unmatched rows should have nulls
        let mut has_null = false;
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            if col.null_count() > 0 { has_null = true; break; }
        }
        assert!(has_null, "FULL OUTER JOIN should produce NULL values for unmatched rows");
    }

    #[test]
    fn test_full_outer_join_superset_of_inner() {
        let build = build_batch(&["a", "c"], &[100, 300]);
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let inner = hash_join(std::slice::from_ref(&build), "id", std::slice::from_ref(&probe), "id", JoinType::Inner).unwrap();
        let full = hash_join(std::slice::from_ref(&build), "id", std::slice::from_ref(&probe), "id", JoinType::Full).unwrap();
        assert!(full[0].num_rows() >= inner[0].num_rows(),
            "FULL should have >= INNER rows: {} vs {}", full[0].num_rows(), inner[0].num_rows());
    }

    #[test]
    fn test_right_join() {
        // build={a,c}, probe={a,b} → RIGHT JOIN keeps all build rows
        // Equivalent to LEFT JOIN with swapped sides
        let build = build_batch(&["a", "c"], &[100, 300]);
        let probe = probe_batch(&["a", "b"], &["alice", "bob"]);
        let result = hash_join(&[build], "id", &[probe], "id", JoinType::Right).unwrap();
        assert_eq!(result.len(), 1);
        // "a" matches, "c" has no probe match → 2 rows (like LEFT JOIN on swapped)
        assert_eq!(result[0].num_rows(), 2);
    }
}
