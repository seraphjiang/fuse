// SPDX-License-Identifier: Apache-2.0

//! Performance benchmarks for the Fuse engine.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use fuse_engine::cache::{cache_key, QueryCache};
use fuse_engine::join::{extract_join_keys, hash_join, JoinType};
use fuse_engine::ppl::{is_ppl, parse_ppl, ppl_to_sql};
use fuse_engine::{align_batch, sort_batches, union_batches, union_schema};

// ── Helpers ──

fn schema_two_col() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn schema_three_col() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, true),
    ]))
}

fn make_batch(n: usize) -> RecordBatch {
    let ids: Vec<String> = (0..n).map(|i| format!("key_{}", i % 1000)).collect();
    let vals: Vec<i64> = (0..n).map(|i| i as i64).collect();
    RecordBatch::try_new(
        schema_two_col(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap()
}

fn make_probe_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<String> = (0..n).map(|i| format!("key_{}", i % 1000)).collect();
    let names: Vec<String> = (0..n).map(|i| format!("name_{}", i)).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

// ── PPL Benchmarks ──

fn bench_ppl_parse(c: &mut Criterion) {
    let simple = "source = ds.logs | where status >= 500 | head 100";
    let complex = "source = cluster_a.logs, cluster_b.logs, cluster_c.logs | where status >= 500 | stats count() by service | sort - count | head 20";

    let mut group = c.benchmark_group("ppl_parse");
    group.bench_function("simple", |b| {
        b.iter(|| {
            assert!(is_ppl(black_box(simple)));
            parse_ppl(black_box(simple)).unwrap()
        })
    });
    group.bench_function("complex_multi_source", |b| {
        b.iter(|| parse_ppl(black_box(complex)).unwrap())
    });
    group.finish();
}

fn bench_ppl_to_sql(c: &mut Criterion) {
    let simple = "source = ds.logs | where status >= 500 | head 100";
    let complex = "source = cluster_a.logs, cluster_b.logs | stats count() by service | sort - count | head 20";

    let parsed_simple = parse_ppl(simple).unwrap();
    let parsed_complex = parse_ppl(complex).unwrap();

    let mut group = c.benchmark_group("ppl_to_sql");
    group.bench_function("simple", |b| {
        b.iter(|| ppl_to_sql(black_box(&parsed_simple)).unwrap())
    });
    group.bench_function("complex", |b| {
        b.iter(|| ppl_to_sql(black_box(&parsed_complex)).unwrap())
    });
    group.finish();
}

// ── Join Benchmarks ──

fn bench_hash_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_join");
    for size in [1_000, 10_000, 100_000] {
        let build = make_batch(size);
        let probe = make_probe_batch(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                hash_join(
                    black_box(std::slice::from_ref(&build)),
                    "id",
                    black_box(std::slice::from_ref(&probe)),
                    "id",
                    JoinType::Inner,
                )
                .unwrap()
            })
        });
    }
    group.finish();
}

fn bench_extract_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("extract_join_keys");
    for size in [1_000, 10_000, 100_000] {
        let batch = make_batch(size);
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| extract_join_keys(black_box(std::slice::from_ref(&batch)), "id").unwrap())
        });
    }
    group.finish();
}

// ── Merger Benchmarks ──

fn bench_union_and_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("merger");

    let batches: Vec<Vec<RecordBatch>> = (0..4).map(|_| vec![make_batch(10_000)]).collect();
    group.bench_function("union_batches_4x10k", |b| {
        b.iter(|| union_batches(black_box(batches.clone())).unwrap())
    });

    let flat: Vec<RecordBatch> = (0..4).map(|_| make_batch(10_000)).collect();
    group.bench_function("sort_batches_40k", |b| {
        b.iter(|| {
            sort_batches(
                black_box(flat.clone()),
                &[1], // sort by value
                &[false],
                Some(100),
            )
            .unwrap()
        })
    });

    group.finish();
}

fn bench_schema_alignment(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_alignment");

    let s2 = schema_two_col();
    let s3 = schema_three_col();
    let batch = make_batch(10_000);

    group.bench_function("matching_schema", |b| {
        b.iter(|| align_batch(black_box(&batch), black_box(&s2)).unwrap())
    });
    group.bench_function("different_schema", |b| {
        let target = union_schema(&[s2.clone(), s3.clone()]);
        b.iter(|| align_batch(black_box(&batch), black_box(&target)).unwrap())
    });

    group.finish();
}

// ── Cache Benchmarks ──

fn bench_cache_ops(c: &mut Criterion) {
    use std::time::Duration;

    let mut group = c.benchmark_group("cache");

    // Bench put
    group.bench_function("put_1k_entries", |b| {
        let cache = QueryCache::with_capacity(2000);
        b.iter(|| {
            for i in 0u64..1000 {
                cache.put(i, vec![make_batch(10)], Duration::from_secs(60));
            }
        })
    });

    // Bench get (hit)
    group.bench_function("get_hit", |b| {
        let cache = QueryCache::new();
        cache.put(42, vec![make_batch(100)], Duration::from_secs(60));
        b.iter(|| {
            black_box(cache.get(42));
        })
    });

    // Bench get (miss)
    group.bench_function("get_miss", |b| {
        let cache = QueryCache::new();
        b.iter(|| {
            black_box(cache.get(999));
        })
    });

    // Bench cache_key hashing
    group.bench_function("cache_key_hash", |b| {
        b.iter(|| {
            black_box(cache_key(
                "my_connector",
                "SELECT * FROM logs WHERE status >= 500 ORDER BY ts DESC LIMIT 100",
            ));
        })
    });

    // Bench LRU eviction under pressure
    group.bench_function("put_with_eviction", |b| {
        let cache = QueryCache::with_capacity(100);
        // Pre-fill
        for i in 0u64..100 {
            cache.put(i, vec![make_batch(10)], Duration::from_secs(60));
        }
        let mut key = 100u64;
        b.iter(|| {
            cache.put(key, vec![make_batch(10)], Duration::from_secs(60));
            key += 1;
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ppl_parse,
    bench_ppl_to_sql,
    bench_hash_join,
    bench_extract_keys,
    bench_union_and_sort,
    bench_schema_alignment,
    bench_cache_ops,
);
criterion_main!(benches);
