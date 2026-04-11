// SPDX-License-Identifier: Apache-2.0

//! Performance benchmarks for fuse-server modules.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;

use fuse_server::plan_cache::{CachedPlan, PlanCache};
use fuse_server::auto_suggest::{self, ColumnMeta};
use fuse_server::autocomplete;
use fuse_server::anomaly;
use fuse_server::query_advisor;

fn bench_plan_cache_insert_and_get(c: &mut Criterion) {
    let cache = PlanCache::new(300, 10000);
    // Pre-fill
    for i in 0..1000 {
        let plan = CachedPlan::new(vec![("ds".into(), "t".into())], false, false, false, None, 0, vec![]);
        cache.insert(format!("SELECT * FROM t WHERE id = {}", i), plan);
    }

    c.bench_function("plan_cache_get_hit", |b| {
        b.iter(|| black_box(cache.get("SELECT * FROM t WHERE id = 500")))
    });

    c.bench_function("plan_cache_get_miss", |b| {
        b.iter(|| black_box(cache.get("SELECT * FROM nonexistent")))
    });
}

fn bench_auto_suggest(c: &mut Criterion) {
    let columns = vec![
        ColumnMeta { name: "timestamp".into(), data_type: "Utf8".into(), nullable: false },
        ColumnMeta { name: "level".into(), data_type: "Utf8".into(), nullable: false },
        ColumnMeta { name: "message".into(), data_type: "Utf8".into(), nullable: true },
        ColumnMeta { name: "status".into(), data_type: "Int64".into(), nullable: false },
        ColumnMeta { name: "latency_ms".into(), data_type: "Float64".into(), nullable: false },
    ];

    c.bench_function("auto_suggest_5_columns", |b| {
        b.iter(|| black_box(auto_suggest::suggest("cluster_a", "logs", &columns)))
    });
}

fn bench_autocomplete(c: &mut Criterion) {
    let schemas: Vec<autocomplete::SchemaInfo> = (0..20).map(|i| {
        autocomplete::SchemaInfo {
            datasource: format!("ds_{}", i),
            table: format!("table_{}", i),
            columns: vec!["id".into(), "name".into(), "timestamp".into(), "value".into()],
        }
    }).collect();

    c.bench_function("autocomplete_20_schemas", |b| {
        b.iter(|| black_box(autocomplete::complete("SELECT ti", &schemas)))
    });
}

fn bench_anomaly_detection(c: &mut Criterion) {
    let baseline = anomaly::ColumnBaseline {
        column: "latency".into(),
        mean: 100.0,
        stddev: 10.0,
        null_rate: 0.01,
        distinct_count: 50,
    };
    let snapshot = anomaly::CurrentSnapshot { mean: 150.0, null_rate: 0.3, distinct_count: 120 };

    c.bench_function("anomaly_detect", |b| {
        b.iter(|| black_box(anomaly::detect(&snapshot, &baseline)))
    });
}

fn bench_query_advisor(c: &mut Criterion) {
    let caps = fuse_core::connector::ConnectorCapabilities::full();
    let connectors = vec![("opensearch", caps.clone()), ("dynamodb", caps)];

    c.bench_function("query_advisor_complex", |b| {
        b.iter(|| black_box(query_advisor::advise(
            "SELECT * FROM opensearch.logs l JOIN dynamodb.users u ON l.user_id = u.user_id ORDER BY l.timestamp",
            &connectors,
        )))
    });
}

criterion_group!(
    benches,
    bench_plan_cache_insert_and_get,
    bench_auto_suggest,
    bench_autocomplete,
    bench_anomaly_detection,
    bench_query_advisor,
);
criterion_main!(benches);
