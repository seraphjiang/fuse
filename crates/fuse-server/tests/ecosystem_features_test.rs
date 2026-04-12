// SPDX-License-Identifier: Apache-2.0
//! Integration tests for Ecosystem & AI/ML features shipped in overnight run.

// --- Query Auto-Tuner ---

#[test]
fn test_autotuner_index_recommendation() {
    use fuse_server::query_autotuner::*;
    let samples = vec![
        QuerySample { query: "SELECT * FROM ds.logs WHERE status >= 500".into(), datasource: "ds".into(), table: "logs".into(), latency_ms: 5000 },
        QuerySample { query: "SELECT * FROM ds.logs WHERE status = 200".into(), datasource: "ds".into(), table: "logs".into(), latency_ms: 4000 },
        QuerySample { query: "SELECT * FROM ds.logs WHERE status < 300".into(), datasource: "ds".into(), table: "logs".into(), latency_ms: 3000 },
    ];
    let recs = analyze(&samples, 1000);
    assert!(!recs.is_empty());
    assert!(recs.iter().any(|r| matches!(r.recommendation_type, RecommendationType::CreateIndex)));
    assert!(recs.iter().any(|r| r.description.contains("status")));
}

#[test]
fn test_autotuner_missing_limit_recommendation() {
    use fuse_server::query_autotuner::*;
    let samples = vec![
        QuerySample { query: "SELECT * FROM ds.t WHERE x = 1".into(), datasource: "ds".into(), table: "t".into(), latency_ms: 5000 },
        QuerySample { query: "SELECT * FROM ds.t WHERE y = 2".into(), datasource: "ds".into(), table: "t".into(), latency_ms: 6000 },
    ];
    let recs = analyze(&samples, 1000);
    assert!(recs.iter().any(|r| matches!(r.recommendation_type, RecommendationType::AddFilter)));
}

#[test]
fn test_autotuner_fast_queries_no_recommendations() {
    use fuse_server::query_autotuner::*;
    let samples = vec![
        QuerySample { query: "SELECT 1".into(), datasource: "ds".into(), table: "t".into(), latency_ms: 5 },
    ];
    let recs = analyze(&samples, 1000);
    assert!(recs.is_empty());
}

// --- Query Similarity ---

#[test]
fn test_similarity_normalize_strips_literals() {
    use fuse_server::query_similarity::*;
    let n1 = normalize_query("SELECT * FROM t WHERE id = 1");
    let n2 = normalize_query("SELECT * FROM t WHERE id = 999");
    assert_eq!(n1, n2);
}

#[test]
fn test_similarity_different_structure_different_fingerprint() {
    use fuse_server::query_similarity::*;
    let fp1 = fingerprint(&normalize_query("SELECT * FROM t WHERE id = 1"));
    let fp2 = fingerprint(&normalize_query("SELECT name FROM t WHERE status = 'ok'"));
    assert_ne!(fp1, fp2);
}

#[test]
fn test_similarity_grouping() {
    use fuse_server::query_similarity::*;
    let entries = vec![
        QueryEntry { query: "SELECT * FROM t WHERE id = 1".into(), latency_ms: 100, tenant: Some("a".into()) },
        QueryEntry { query: "SELECT * FROM t WHERE id = 2".into(), latency_ms: 200, tenant: Some("b".into()) },
        QueryEntry { query: "SELECT * FROM t WHERE id = 3".into(), latency_ms: 150, tenant: Some("a".into()) },
        QueryEntry { query: "SELECT name FROM t2 LIMIT 10".into(), latency_ms: 50, tenant: None },
    ];
    let groups = find_similar(&entries, 2);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].count, 3);
    assert!(groups[0].tenants.contains(&"a".to_string()));
    assert!(groups[0].tenants.contains(&"b".to_string()));
}

// --- Smart Routing ---

#[test]
fn test_smart_routing_fastest_selection() {
    use fuse_server::smart_routing::SmartRouter;
    let router = SmartRouter::new();
    router.record("fast_ds", 10);
    router.record("fast_ds", 20);
    router.record("slow_ds", 500);
    router.record("slow_ds", 600);
    let fastest = router.fastest(&["fast_ds", "slow_ds"]);
    assert_eq!(fastest, Some("fast_ds".to_string()));
}

#[test]
fn test_smart_routing_stats() {
    use fuse_server::smart_routing::SmartRouter;
    let router = SmartRouter::new();
    for i in 0..10 {
        router.record("ds1", 100 + i * 10);
    }
    let stats = router.stats("ds1").unwrap();
    assert_eq!(stats.sample_count, 10);
    assert!(stats.avg_ms > 0.0);
    assert!(stats.p50_ms > 0);
}

// --- Anomaly Detection (Seasonal + Trend) ---

#[test]
fn test_anomaly_seasonal_deviation() {
    use fuse_server::anomaly::*;
    let historical: Vec<TimeSeriesPoint> = (0..30)
        .map(|i| TimeSeriesPoint { timestamp: i, value: 100.0 + (i as f64 % 5.0) })
        .collect();
    // 300.0 is far outside the range [100..105], should trigger
    let anomalies = detect_seasonal("latency", 300.0, &historical, 3.0);
    assert!(!anomalies.is_empty());
    assert!(anomalies[0].kind == AnomalyKind::SeasonalDeviation);
}

#[test]
fn test_anomaly_trend_break() {
    use fuse_server::anomaly::*;
    // Trend with noise; wider noise band makes stddev robust under parallel execution
    let points: Vec<TimeSeriesPoint> = (0..20)
        .map(|i| TimeSeriesPoint { timestamp: i, value: 100.0 + 10.0 * i as f64 + if i % 2 == 0 { 5.0 } else { -5.0 } })
        .collect();
    // Extreme outlier: expected ~300, actual 2000 — well beyond any threshold
    let anomalies = detect_trend("latency", &points, 2000.0, 3.0);
    assert!(!anomalies.is_empty());
    assert!(anomalies[0].kind == AnomalyKind::TrendBreak);
}

#[test]
fn test_anomaly_no_trend_break_on_expected_value() {
    use fuse_server::anomaly::*;
    let points: Vec<TimeSeriesPoint> = (0..10)
        .map(|i| TimeSeriesPoint { timestamp: i, value: 100.0 + 10.0 * i as f64 })
        .collect();
    // Next expected value ~200
    let anomalies = detect_trend("latency", &points, 200.0, 3.0);
    assert!(anomalies.is_empty());
}

// --- Webhook Retry ---

#[test]
fn test_webhook_retry_config_backoff() {
    use fuse_server::webhook::RetryConfig;
    let cfg = RetryConfig::default();
    assert!(cfg.max_retries > 0);
    let d0 = cfg.backoff_duration(0);
    let d1 = cfg.backoff_duration(1);
    // Exponential: second delay should be longer
    assert!(d1 >= d0);
}

#[test]
fn test_webhook_dlq_operations() {
    use fuse_server::webhook::DeadLetterQueue;
    let dlq = DeadLetterQueue::new(100);
    assert!(dlq.list().is_empty());
    dlq.push(fuse_server::webhook::DeadLetterEntry {
        subscription_id: "wh-1".into(),
        callback_url: "http://example.com".into(),
        payload: serde_json::json!({}),
        attempts: 3,
        last_error: "test error".into(),
        failed_at: 1000,
    });
    assert_eq!(dlq.list().len(), 1);
    assert_eq!(dlq.list()[0].subscription_id, "wh-1");
    dlq.clear();
    assert!(dlq.list().is_empty());
}

// --- CDC Batch ---

#[test]
fn test_cdc_multi_table_tracking() {
    use fuse_server::cdc::*;
    let tracker = CdcTracker::new(100);
    // Register a view that depends on two tables from different datasources
    tracker.register_view("joined_view", vec![
        ("cluster_a".into(), "logs".into()),
        ("dynamodb".into(), "users".into()),
    ]);

    // Change to first source
    let affected = tracker.record_change(ChangeEvent {
        datasource: "cluster_a".into(),
        table: "logs".into(),
        change_type: ChangeType::Insert,
        timestamp: 1000,
    });
    assert_eq!(affected, vec!["joined_view"]);

    tracker.take_pending();

    // Change to second source also triggers
    let affected = tracker.record_change(ChangeEvent {
        datasource: "dynamodb".into(),
        table: "users".into(),
        change_type: ChangeType::Update,
        timestamp: 1001,
    });
    assert_eq!(affected, vec!["joined_view"]);
}

// --- Schema Cache ---

#[test]
fn test_schema_cache_ttl() {
    use fuse_server::schema_cache::SchemaCache;
    let cache = SchemaCache::new(60);
    cache.set("ds.tables".into(), serde_json::json!(["logs", "users"]));
    let val = cache.get("ds.tables");
    assert!(val.is_some());
    assert_eq!(val.unwrap(), serde_json::json!(["logs", "users"]));
    assert!(cache.get("missing").is_none());
}

// --- API Versioning ---

#[test]
fn test_api_version_info() {
    use fuse_server::api_versioning::*;
    let info = ApiVersionInfo {
        current: "v1",
        versions: vec![
            VersionEntry { version: "v1", status: "stable", prefix: "/api/v1/fuse" },
            VersionEntry { version: "v2", status: "beta", prefix: "/api/v2/fuse" },
        ],
    };
    let json = serde_json::to_string(&info).unwrap();
    assert!(json.contains("\"v1\""));
    assert!(json.contains("\"v2\""));
    assert!(json.contains("\"beta\""));
}

// --- Pool Stats ---

#[test]
fn test_pool_stats_tracker() {
    use fuse_server::pool_stats::PoolStatsTracker;
    let tracker = PoolStatsTracker::new();
    tracker.register("pg", 10);
    tracker.acquire("pg");
    tracker.acquire("pg");
    let stats = tracker.get("pg").unwrap();
    assert_eq!(stats.active, 2);
    assert_eq!(stats.max_size, 10);
    tracker.release("pg");
    let stats = tracker.get("pg").unwrap();
    assert_eq!(stats.active, 1);
}
