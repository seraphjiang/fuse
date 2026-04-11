// SPDX-License-Identifier: Apache-2.0
//! Server integration tests — cross-module edge cases.

#[cfg(test)]
mod tests {
    use serde_json::json;

    // Aggregator
    #[test]
    fn test_merge_preserves_column_order() {
        let r = (vec!["b".into(), "a".into()], vec![vec![json!(2), json!(1)]]);
        let (cols, _) = crate::aggregator::merge_results(vec![r]);
        assert_eq!(cols, vec!["b", "a"]);
    }

    // Sorter
    #[test]
    fn test_sort_empty() {
        let mut rows: Vec<Vec<serde_json::Value>> = vec![];
        crate::sorter::sort_by_column(&mut rows, 0, false);
        assert!(rows.is_empty());
    }

    // Distinct
    #[test]
    fn test_distinct_with_nulls() {
        let rows = vec![vec![json!(null)], vec![json!(null)], vec![json!(1)]];
        assert_eq!(crate::distinct::distinct(rows).len(), 2);
    }

    // Formatter
    #[test]
    fn test_csv_empty_rows() {
        let csv = crate::formatter::to_csv(&["a".into()], &[]);
        assert_eq!(csv, "a\n");
    }

    #[test]
    fn test_table_single_row() {
        let table = crate::formatter::to_table(&["x".into()], &[vec![json!(42)]]);
        assert!(table.contains("42"));
    }

    // Fingerprint
    #[test]
    fn test_fingerprint_preserves_keywords() {
        let f = crate::fingerprint::fingerprint("SELECT * FROM t WHERE x = 'val'");
        assert!(f.contains("SELECT"));
        assert!(f.contains("WHERE"));
    }

    // Sanitize
    #[test]
    fn test_sanitize_empty() {
        assert_eq!(crate::sanitize::sanitize_query(""), "");
    }

    #[test]
    fn test_sanitize_no_strings() {
        assert_eq!(crate::sanitize::sanitize_query("SELECT 1"), "SELECT 1");
    }

    // Complexity
    #[test]
    fn test_complexity_simple() {
        let s = crate::complexity::score_query("SELECT * FROM t LIMIT 10");
        assert_eq!(s.level, "simple");
    }

    // Query parser
    #[test]
    fn test_is_read_only_insert() {
        assert!(!crate::query_parser::is_read_only("INSERT INTO t VALUES (1)"));
    }

    // Coercer
    #[test]
    fn test_coerce_bool_to_number() {
        assert_eq!(crate::coercer::to_number(&json!(true)), json!(1));
    }

    // Having
    #[test]
    fn test_having_gte_empty() {
        assert!(crate::having::having_gte(&[], 0, 0.0).is_empty());
    }

    // Set ops
    #[test]
    fn test_semi_join_empty() {
        assert!(crate::set_ops::semi_join(&[], 0, &[], 0).is_empty());
    }

    // Intersect
    #[test]
    fn test_intersect_identical() {
        let rows = vec![vec![json!(1)], vec![json!(2)]];
        assert_eq!(crate::intersect::intersect(&rows, &rows).len(), 2);
    }

    // Window
    #[test]
    fn test_row_number_single() {
        let (_, numbered) = crate::window_fn::add_row_number(&[vec![json!("a")]], "rn");
        assert_eq!(numbered[0][0], json!(1));
    }

    // Grouper
    #[test]
    fn test_group_count_single_value() {
        let rows = vec![vec![json!("x")]; 5];
        let groups = crate::grouper::group_count(&rows, 0);
        assert_eq!(groups[0].1, 5);
    }

    // Joiner
    #[test]
    fn test_hash_join_empty_right() {
        assert!(crate::joiner::hash_join(&[vec![json!(1)]], 0, &[], 0).is_empty());
    }

    // Agg functions
    #[test]
    fn test_count_distinct_all_same() {
        let rows = vec![vec![json!("a")]; 10];
        assert_eq!(crate::agg_functions::count_distinct(&rows, 0), 1);
    }

    // String fn
    #[test]
    fn test_concat_with_numbers() {
        let rows = vec![vec![json!(1), json!(2)]];
        let result = crate::string_fn::concat_columns(&rows, 0, 1, "-");
        assert_eq!(result[0], json!("1-2"));
    }

    // Math fn
    #[test]
    fn test_modulo_zero() {
        let rows = vec![vec![json!(10)]];
        let result = crate::math_fn::modulo(&rows, 0, 3.0);
        assert_eq!(result[0], json!(1.0));
    }

    // Sampling
    #[test]
    fn test_head_tail_exact() {
        let rows: Vec<Vec<serde_json::Value>> = (0..5).map(|i| vec![json!(i)]).collect();
        let (head, tail) = crate::sampling::head_tail(&rows, 5);
        assert_eq!(head.len(), 5);
        assert!(tail.is_empty());
    }
}


#[cfg(test)]
mod boundary_tests {
    use serde_json::json;

    // Null handler
    #[test]
    fn test_null_counts_empty() {
        assert_eq!(crate::null_handler::null_counts(&[], 3), vec![0, 0, 0]);
    }

    // Offset pagination
    #[test]
    fn test_paginate_beyond_end() {
        let rows: Vec<Vec<serde_json::Value>> = vec![vec![json!(1)]];
        assert!(crate::offset_pagination::paginate(&rows, 10, 5).is_empty());
    }

    #[test]
    fn test_page_info_single_page() {
        let info = crate::offset_pagination::page_info(5, 0, 100);
        assert!(!info.has_next);
        assert!(!info.has_prev);
        assert_eq!(info.total_pages, 1);
    }

    // Row limit
    #[test]
    fn test_enforce_limit_zero() {
        let rows = vec![vec![json!(1)]];
        let r = crate::row_limit::enforce_limit(rows, 0);
        assert!(r.truncated);
        assert!(r.rows.is_empty());
    }

    // Pivot
    #[test]
    fn test_pivot_single_value() {
        let rows = vec![vec![json!("k"), json!("col"), json!(42)]];
        let (cols, result) = crate::pivot::pivot(&rows, 0, 1, 2);
        assert_eq!(result.len(), 1);
        assert!(cols.contains(&"col".to_string()));
    }

    // Transpose
    #[test]
    fn test_transpose_single_column() {
        let cols = vec!["x".into()];
        let rows = vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]];
        let (new_cols, new_rows) = crate::transpose::transpose(&cols, &rows);
        assert_eq!(new_cols.len(), 4); // column + row_1 + row_2 + row_3
        assert_eq!(new_rows.len(), 1); // 1 original column
    }

    // Flattener
    #[test]
    fn test_flatten_scalar() {
        let mut out = std::collections::BTreeMap::new();
        crate::flattener::flatten_value("val", &json!(42), &mut out);
        assert_eq!(out["val"], json!(42));
    }

    // Arrow export
    #[test]
    fn test_columnar_all_nulls() {
        let cols = vec!["x".into()];
        let rows = vec![vec![json!(null)], vec![json!(null)]];
        let arrow = crate::arrow_export::to_columnar(&cols, &rows);
        assert_eq!(arrow[0].null_count, 2);
        assert_eq!(arrow[0].data_type, "null");
    }

    // Type infer
    #[test]
    fn test_infer_boolean() {
        assert_eq!(crate::type_infer::infer_type(&json!(true)), crate::type_infer::InferredType::Boolean);
    }

    // Profiler
    #[test]
    fn test_profile_single_column() {
        let profiles = crate::profiler::profile(&["id".into()], &[vec![json!(1)], vec![json!(2)]]);
        assert_eq!(profiles[0].unique_count, 2);
        assert_eq!(profiles[0].data_type, "integer");
    }

    // Column stats
    #[test]
    fn test_stats_single_value() {
        let stats = crate::column_stats::compute_stats(&["x".into()], &[vec![json!(42)]]);
        assert_eq!(stats[0].count, 1);
        assert_eq!(stats[0].null_count, 0);
    }

    // Cache key
    #[test]
    fn test_cache_key_ppl() {
        let k = crate::cache_key::build_key("ppl", "source = t | head 10", None);
        assert!(k.starts_with("ppl:"));
    }

    // Query policy
    #[test]
    fn test_policy_truncate_denied() {
        let p = crate::query_policy::QueryPolicy::with_defaults();
        assert!(matches!(p.check("TRUNCATE TABLE users"), crate::query_policy::PolicyResult::Denied(_)));
    }

    // Circuit breaker
    #[test]
    fn test_circuit_breaker_unknown_ds() {
        let cb = crate::circuit_breaker::CircuitBreaker::new(3, 30);
        assert!(cb.allow("new_ds")); // unknown = closed = allow
    }

    // Rewrite
    #[test]
    fn test_rewrite_preserves_limit() {
        let rules = crate::rewrite::default_rules();
        let result = crate::rewrite::apply_rules("SELECT * FROM t LIMIT 5", &rules);
        assert!(result.contains("LIMIT 5"));
        assert!(!result.contains("LIMIT 10000"));
    }

    // Validate
    #[test]
    fn test_validate_ppl_format() {
        assert!(crate::validate::validate_request("source = t", "ppl", None, None).is_empty());
    }

    // Date fn
    #[test]
    fn test_extract_hour_no_time() {
        let rows = vec![vec![json!("2024-01-15")]];
        assert_eq!(crate::date_fn::extract_hour(&rows, 0)[0], json!(null));
    }

    // Top N
    #[test]
    fn test_extract_top_none() {
        assert_eq!(crate::top_n::extract_top("SELECT * FROM t"), None);
    }

    // Case when
    #[test]
    fn test_case_when_first_match_wins() {
        let case = crate::case_when::CaseWhen::new(json!("default"))
            .when(|_| true, json!("first"))
            .when(|_| true, json!("second"));
        assert_eq!(case.evaluate(&json!("x")), json!("first"));
    }
}


#[cfg(test)]
mod comprehensive_tests {
    use serde_json::json;

    // Response builder
    #[test]
    fn test_response_builder_empty() {
        let resp = crate::response_builder::ResponseBuilder::new("q-1", "sql").build();
        assert_eq!(resp.metadata.total_rows, 0);
        assert!(resp.warnings.is_none());
    }

    // Bookmarks
    #[test]
    fn test_bookmark_search_no_match() {
        let s = crate::bookmarks::BookmarkStore::new();
        assert!(s.search("nonexistent").is_empty());
    }

    // Tags
    #[test]
    fn test_tags_find_empty() {
        let r = crate::tags::TagRegistry::new();
        assert!(r.find_by_tag("missing").is_empty());
    }

    // Templates
    #[test]
    fn test_template_no_params() {
        let t = crate::templates::QueryTemplate {
            name: "simple".into(), template: "SELECT 1".into(),
            params: vec![], description: None,
        };
        assert_eq!(t.render(&std::collections::HashMap::new()).unwrap(), "SELECT 1");
    }

    // Alias
    #[test]
    fn test_alias_overwrite() {
        let r = crate::alias::AliasRegistry::new();
        r.set("logs", "cluster_a");
        r.set("logs", "cluster_b");
        assert_eq!(r.resolve("logs"), "cluster_b");
    }

    // Notifications
    #[test]
    fn test_notification_pending() {
        let hub = crate::notifications::NotificationHub::new();
        let _rx = hub.subscribe("q-1");
        assert_eq!(hub.pending_count(), 1);
    }

    // Access log
    #[test]
    fn test_access_log_empty() {
        let log = crate::access_log::AccessLog::new();
        assert_eq!(log.count(), 0);
        assert!(log.recent(10).is_empty());
    }

    // Timeout tracker
    #[test]
    fn test_timeout_tracker_empty() {
        let t = crate::timeout_tracker::TimeoutTracker::new();
        assert_eq!(t.count(), 0);
    }

    // Cost tracker
    #[test]
    fn test_cost_tracker_empty_tenant() {
        let t = crate::cost_tracker::CostTracker::new();
        assert!(t.for_tenant("nobody").is_empty());
    }

    // Rate monitor
    #[test]
    fn test_rate_monitor_zero_window() {
        let m = crate::rate_monitor::RateMonitor::new(0);
        m.record();
        assert_eq!(m.qps(), 0.0);
    }

    // Pool stats
    #[test]
    fn test_pool_stats_release_unknown() {
        let t = crate::pool_stats::PoolTracker::new();
        t.release("unknown"); // should not panic
        assert!(t.snapshot().is_empty());
    }

    // History analytics
    #[test]
    fn test_analytics_all_errors() {
        let entries = vec![(false, 100, vec!["ds".into()])];
        let a = crate::history_analytics::compute_analytics(&entries);
        assert_eq!(a.success_rate, 0.0);
    }

    // Scheduler
    #[test]
    fn test_scheduler_disable() {
        let reg = crate::scheduler::ScheduleRegistry::new();
        reg.add(crate::scheduler::ScheduledQuery {
            id: "s1".into(), name: "test".into(), query: "SELECT 1".into(),
            format: "sql".into(), cron: "* * * * *".into(), enabled: true,
            last_run: None, last_status: None, run_count: 0,
        });
        reg.set_enabled("s1", false);
        assert!(reg.due_schedules().is_empty());
    }

    // Lineage
    #[test]
    fn test_lineage_single_source() {
        let l = crate::lineage::QueryLineage::new("q-1", vec![("ds", "t")]);
        assert!(!l.is_cross_source());
    }

    // Delivery
    #[test]
    fn test_delivery_buffered_default() {
        let mode = crate::delivery::recommend_delivery(Some(100), Some(1024), 10000, 10_000_000);
        assert_eq!(mode, crate::delivery::DeliveryMode::Buffered);
    }

    // Explain cache
    #[test]
    fn test_explain_cache_len() {
        let c = crate::explain_cache::ExplainCache::new(60, 100);
        c.insert("q1".into(), json!({}));
        c.insert("q2".into(), json!({}));
        assert_eq!(c.len(), 2);
    }

    // Slow query
    #[test]
    fn test_slow_query_custom_threshold() {
        assert!(crate::slow_query::check_slow_query("q", "sql", std::time::Duration::from_millis(50), &[], 0, Some(10)));
    }

    // Pagination
    #[test]
    fn test_pagination_single_page() {
        let p = crate::pagination::PaginationMeta::single_page(5);
        assert!(!p.has_more);
        assert_eq!(p.total_rows, Some(5));
    }

    // Complexity
    #[test]
    fn test_complexity_union() {
        let s = crate::complexity::score_query("SELECT * FROM a.t UNION ALL SELECT * FROM b.t");
        assert!(s.has_union);
    }

    // Fingerprint
    #[test]
    fn test_fingerprint_consistency() {
        let f1 = crate::fingerprint::fingerprint("SELECT * FROM t WHERE id = 1");
        let f2 = crate::fingerprint::fingerprint("SELECT * FROM t WHERE id = 2");
        assert_eq!(f1, f2);
    }

    // Sanitize
    #[test]
    fn test_sanitize_multiple() {
        let s = crate::sanitize::sanitize_query("WHERE a = 'x' AND b = 'y'");
        assert!(!s.contains("x"));
        assert!(!s.contains("y"));
        assert!(s.contains("***"));
    }

    // Query parser
    #[test]
    fn test_extract_tables_multiple() {
        let tables = crate::query_parser::extract_tables("SELECT * FROM a.t1 JOIN b.t2 ON a.t1.id = b.t2.id");
        assert_eq!(tables.len(), 2);
    }

    // Reorder
    #[test]
    fn test_reorder_identity() {
        let cols = vec!["a".into(), "b".into()];
        let rows = vec![vec![json!(1), json!(2)]];
        let (new_cols, new_rows) = crate::reorder::reorder(&cols, &rows, &["a".into(), "b".into()]);
        assert_eq!(new_cols, cols);
        assert_eq!(new_rows, rows);
    }

    // Projector
    #[test]
    fn test_project_all() {
        let cols = vec!["a".into(), "b".into()];
        let rows = vec![vec![json!(1), json!(2)]];
        let (new_cols, _) = crate::projector::project(&rows, &cols, &["a".into(), "b".into()]);
        assert_eq!(new_cols.len(), 2);
    }

    // Union typed
    #[test]
    fn test_union_same_schema() {
        let cols = vec!["x".into()];
        let left = vec![vec![json!(1)]];
        let right = vec![vec![json!(2)]];
        let (c, r) = crate::union_typed::union_aligned(&cols, &left, &cols, &right);
        assert_eq!(c.len(), 1);
        assert_eq!(r.len(), 2);
    }

    // Renamer
    #[test]
    fn test_rename_no_match() {
        let cols = vec!["a".into()];
        let renamed = crate::renamer::rename_columns(&cols, &std::collections::HashMap::new());
        assert_eq!(renamed, cols);
    }

    // Result filter
    #[test]
    fn test_filter_neq() {
        let rows = vec![vec![json!("a")], vec![json!("b")]];
        let result = crate::result_filter::filter_rows(&rows, &[crate::result_filter::FilterOp::Neq(0, json!("a"))]);
        assert_eq!(result.len(), 1);
    }
}
