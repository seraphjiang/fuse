// SPDX-License-Identifier: Apache-2.0

//! Complex JOIN tests: 3-way joins, self-joins, cross-source anti-joins,
//! correlated subquery patterns, and multi-batch edge cases.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use fuse_engine::{extract_join_keys, hash_join, keys_to_in_filter, JoinType};

fn batch(fields: &[(&str, DataType)], cols: Vec<Arc<dyn arrow::array::Array>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        fields.iter().map(|(n, dt)| Field::new(*n, dt.clone(), true)).collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, cols).unwrap()
}

fn str_col(vals: &[&str]) -> Arc<dyn arrow::array::Array> {
    Arc::new(StringArray::from(vals.to_vec()))
}

fn i64_col(vals: &[i64]) -> Arc<dyn arrow::array::Array> {
    Arc::new(Int64Array::from(vals.to_vec()))
}

fn nullable_str_col(vals: &[Option<&str>]) -> Arc<dyn arrow::array::Array> {
    Arc::new(StringArray::from(vals.to_vec()))
}

// ── 3-way JOIN ──

#[test]
fn test_three_way_inner_join() {
    let logs = batch(
        &[("user_id", DataType::Utf8), ("message", DataType::Utf8)],
        vec![str_col(&["u1", "u2", "u3"]), str_col(&["err", "ok", "err"])],
    );
    let users = batch(
        &[("user_id", DataType::Utf8), ("role_id", DataType::Utf8)],
        vec![str_col(&["u1", "u2", "u4"]), str_col(&["r1", "r2", "r1"])],
    );
    let roles = batch(
        &[("role_id", DataType::Utf8), ("role_name", DataType::Utf8)],
        vec![str_col(&["r1", "r2", "r3"]), str_col(&["admin", "viewer", "editor"])],
    );

    let step1 = hash_join(&[logs], "user_id", &[users], "user_id", JoinType::Inner).unwrap();
    assert_eq!(step1[0].num_rows(), 2);

    let step2 = hash_join(&step1, "role_id", &[roles], "role_id", JoinType::Inner).unwrap();
    assert_eq!(step2[0].num_rows(), 2);
    assert_eq!(step2[0].num_columns(), 4);
}

#[test]
fn test_three_way_left_join_preserves_all() {
    let a = batch(
        &[("id", DataType::Utf8), ("val", DataType::Int64)],
        vec![str_col(&["a", "b", "c"]), i64_col(&[1, 2, 3])],
    );
    let b = batch(
        &[("id", DataType::Utf8), ("info", DataType::Utf8)],
        vec![str_col(&["a", "c"]), str_col(&["a-info", "c-info"])],
    );
    let c = batch(
        &[("id", DataType::Utf8), ("extra", DataType::Utf8)],
        vec![str_col(&["a"]), str_col(&["a-extra"])],
    );

    let step1 = hash_join(&[b], "id", &[a], "id", JoinType::Left).unwrap();
    assert_eq!(step1[0].num_rows(), 3);

    let step2 = hash_join(&[c], "id", &step1, "id", JoinType::Left).unwrap();
    assert_eq!(step2[0].num_rows(), 3);
}

// ── Self-join ──

#[test]
fn test_self_join_circular_refs() {
    let data = batch(
        &[("id", DataType::Utf8), ("parent_id", DataType::Utf8)],
        vec![str_col(&["n1", "n2", "n3"]), str_col(&["n2", "n3", "n1"])],
    );
    let result = hash_join(&[data.clone()], "id", &[data], "parent_id", JoinType::Inner).unwrap();
    assert_eq!(result[0].num_rows(), 3);
}

#[test]
fn test_self_join_employee_hierarchy() {
    let emp = batch(
        &[("emp_id", DataType::Utf8), ("name", DataType::Utf8), ("mgr_id", DataType::Utf8)],
        vec![
            str_col(&["e1", "e2", "e3", "e4"]),
            str_col(&["alice", "bob", "carol", "dave"]),
            str_col(&["e3", "e3", "e4", "e4"]),
        ],
    );
    let result = hash_join(&[emp.clone()], "emp_id", &[emp], "mgr_id", JoinType::Inner).unwrap();
    assert_eq!(result[0].num_rows(), 4);
}

// ── Cross-source anti-join ──

#[test]
fn test_anti_join_multi_batch_build() {
    let b1 = batch(&[("id", DataType::Utf8), ("v", DataType::Int64)], vec![str_col(&["a", "b"]), i64_col(&[1, 2])]);
    let b2 = batch(&[("id", DataType::Utf8), ("v", DataType::Int64)], vec![str_col(&["c", "d"]), i64_col(&[3, 4])]);
    let probe = batch(
        &[("id", DataType::Utf8), ("name", DataType::Utf8)],
        vec![str_col(&["a", "b", "c", "d", "e", "f"]), str_col(&["A", "B", "C", "D", "E", "F"])],
    );

    let result = hash_join(&[b1, b2], "id", &[probe], "id", JoinType::Anti).unwrap();
    assert_eq!(result[0].num_rows(), 2); // e, f
}

#[test]
fn test_anti_join_multi_batch_probe() {
    let build = batch(&[("id", DataType::Utf8), ("v", DataType::Int64)], vec![str_col(&["a", "c"]), i64_col(&[1, 3])]);
    let p1 = batch(&[("id", DataType::Utf8), ("name", DataType::Utf8)], vec![str_col(&["a", "b"]), str_col(&["A", "B"])]);
    let p2 = batch(&[("id", DataType::Utf8), ("name", DataType::Utf8)], vec![str_col(&["c", "d"]), str_col(&["C", "D"])]);

    let result = hash_join(&[build], "id", &[p1, p2], "id", JoinType::Anti).unwrap();
    assert_eq!(result[0].num_rows(), 2); // b, d
}

#[test]
fn test_anti_join_blocklist_pattern() {
    let blocklist = batch(&[("id", DataType::Utf8)], vec![str_col(&["u2", "u4"])]);
    let users = batch(
        &[("id", DataType::Utf8), ("name", DataType::Utf8)],
        vec![str_col(&["u1", "u2", "u3", "u4", "u5"]), str_col(&["alice", "bob", "carol", "dave", "eve"])],
    );

    let keys = extract_join_keys(&[blocklist.clone()], "id").unwrap();
    assert_eq!(keys.len(), 2);

    let result = hash_join(&[blocklist], "id", &[users], "id", JoinType::Anti).unwrap();
    assert_eq!(result[0].num_rows(), 3); // u1, u3, u5
}

// ── Correlated subquery patterns ──

#[test]
fn test_exists_via_semi_join() {
    let orders = batch(
        &[("user_id", DataType::Utf8), ("amount", DataType::Int64)],
        vec![str_col(&["u1", "u1", "u3"]), i64_col(&[100, 200, 50])],
    );
    let users = batch(
        &[("id", DataType::Utf8), ("name", DataType::Utf8)],
        vec![str_col(&["u1", "u2", "u3", "u4"]), str_col(&["alice", "bob", "carol", "dave"])],
    );

    let result = hash_join(&[orders], "user_id", &[users], "id", JoinType::Semi).unwrap();
    assert_eq!(result[0].num_rows(), 2); // u1, u3
    assert_eq!(result[0].num_columns(), 2); // probe cols only
}

#[test]
fn test_not_exists_via_anti_join() {
    let orders = batch(
        &[("user_id", DataType::Utf8), ("amount", DataType::Int64)],
        vec![str_col(&["u1", "u3"]), i64_col(&[100, 50])],
    );
    let users = batch(
        &[("id", DataType::Utf8), ("name", DataType::Utf8)],
        vec![str_col(&["u1", "u2", "u3", "u4"]), str_col(&["alice", "bob", "carol", "dave"])],
    );

    let result = hash_join(&[orders], "user_id", &[users], "id", JoinType::Anti).unwrap();
    assert_eq!(result[0].num_rows(), 2); // u2, u4
}

#[test]
fn test_in_subquery_via_key_extraction() {
    let premium = batch(&[("user_id", DataType::Utf8)], vec![str_col(&["u1", "u3", "u5"])]);
    let logs = batch(
        &[("user_id", DataType::Utf8), ("event", DataType::Utf8)],
        vec![str_col(&["u1", "u2", "u3", "u4"]), str_col(&["login", "login", "purchase", "logout"])],
    );

    let keys = extract_join_keys(&[premium.clone()], "user_id").unwrap();
    let filter = keys_to_in_filter("user_id", keys).unwrap();
    match &filter {
        fuse_core::connector::FilterExpr::In { values, .. } => assert_eq!(values.len(), 3),
        _ => panic!("expected In filter"),
    }

    let result = hash_join(&[premium], "user_id", &[logs], "user_id", JoinType::Semi).unwrap();
    assert_eq!(result[0].num_rows(), 2); // u1, u3
}

// ── Edge cases ──

#[test]
fn test_join_nulls_dont_match() {
    let build = batch(
        &[("id", DataType::Utf8), ("v", DataType::Int64)],
        vec![nullable_str_col(&[Some("a"), None, Some("c")]), i64_col(&[1, 2, 3])],
    );
    let probe = batch(
        &[("id", DataType::Utf8), ("name", DataType::Utf8)],
        vec![nullable_str_col(&[Some("a"), None, Some("b")]), str_col(&["alice", "null-user", "bob"])],
    );

    let inner = hash_join(&[build.clone()], "id", &[probe.clone()], "id", JoinType::Inner).unwrap();
    assert_eq!(inner[0].num_rows(), 1); // only "a"

    let anti = hash_join(&[build], "id", &[probe], "id", JoinType::Anti).unwrap();
    assert_eq!(anti[0].num_rows(), 2); // "b" + NULL
}

#[test]
fn test_many_to_many_cross_product() {
    let build = batch(
        &[("id", DataType::Utf8), ("v", DataType::Int64)],
        vec![str_col(&["x", "x", "x"]), i64_col(&[1, 2, 3])],
    );
    let probe = batch(
        &[("id", DataType::Utf8), ("name", DataType::Utf8)],
        vec![str_col(&["x", "x"]), str_col(&["p1", "p2"])],
    );

    let result = hash_join(&[build], "id", &[probe], "id", JoinType::Inner).unwrap();
    assert_eq!(result[0].num_rows(), 6); // 3×2
}

#[test]
fn test_single_row_join() {
    let build = batch(&[("id", DataType::Utf8), ("v", DataType::Int64)], vec![str_col(&["a"]), i64_col(&[1])]);
    let probe = batch(&[("id", DataType::Utf8), ("name", DataType::Utf8)], vec![str_col(&["a"]), str_col(&["alice"])]);

    let result = hash_join(&[build], "id", &[probe], "id", JoinType::Inner).unwrap();
    assert_eq!(result[0].num_rows(), 1);
    assert_eq!(result[0].num_columns(), 3);
}

#[test]
fn test_semi_anti_complement_multi_batch() {
    let b1 = batch(&[("id", DataType::Utf8), ("v", DataType::Int64)], vec![str_col(&["a"]), i64_col(&[1])]);
    let b2 = batch(&[("id", DataType::Utf8), ("v", DataType::Int64)], vec![str_col(&["c"]), i64_col(&[3])]);
    let probe = batch(
        &[("id", DataType::Utf8), ("name", DataType::Utf8)],
        vec![str_col(&["a", "b", "c", "d", "e"]), str_col(&["A", "B", "C", "D", "E"])],
    );

    let semi: usize = hash_join(&[b1.clone(), b2.clone()], "id", &[probe.clone()], "id", JoinType::Semi)
        .unwrap().iter().map(|b| b.num_rows()).sum();
    let anti: usize = hash_join(&[b1, b2], "id", &[probe], "id", JoinType::Anti)
        .unwrap().iter().map(|b| b.num_rows()).sum();
    assert_eq!(semi + anti, 5, "semi ({semi}) + anti ({anti}) = probe rows (5)");
}

#[test]
fn test_full_outer_three_way() {
    let a = batch(&[("id", DataType::Utf8), ("a_val", DataType::Int64)], vec![str_col(&["x", "y"]), i64_col(&[1, 2])]);
    let b = batch(&[("id", DataType::Utf8), ("b_val", DataType::Utf8)], vec![str_col(&["y", "z"]), str_col(&["b1", "b2"])]);
    let c = batch(&[("id", DataType::Utf8), ("c_val", DataType::Utf8)], vec![str_col(&["z", "w"]), str_col(&["c1", "c2"])]);

    let step1 = hash_join(&[a], "id", &[b], "id", JoinType::Full).unwrap();
    assert_eq!(step1[0].num_rows(), 3); // x(unmatched) + y(match) + z(unmatched)

    let step2 = hash_join(&step1, "id", &[c], "id", JoinType::Full).unwrap();
    assert!(step2[0].num_rows() >= 3);
}
