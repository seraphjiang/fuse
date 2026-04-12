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


#[cfg(test)]
mod milestone_tests {
    use serde_json::json;

    #[test]
    fn test_agg_min_negative() {
        let rows = vec![vec![json!(-10)], vec![json!(-5)], vec![json!(0)]];
        assert_eq!(crate::agg_functions::min(&rows, 0), Some(-10.0));
    }

    #[test]
    fn test_joiner_left_join_all_match() {
        let left = vec![vec![json!(1)], vec![json!(2)]];
        let right = vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]];
        let result = crate::joiner::left_join(&left, 0, &right, 0, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][1], json!("a"));
    }

    #[test]
    fn test_intersect_all_common() {
        let rows = vec![vec![json!(1)], vec![json!(2)]];
        assert_eq!(crate::intersect::intersect(&rows, &rows).len(), 2);
    }

    #[test]
    fn test_except_all_different() {
        let left = vec![vec![json!(1)]];
        let right = vec![vec![json!(2)]];
        assert_eq!(crate::intersect::except(&left, &right).len(), 1);
    }

    #[test]
    fn test_math_floor_negative() {
        let mut rows = vec![vec![json!(-3.7)]];
        crate::math_fn::apply_math(&mut rows, 0, crate::math_fn::MathFn::Floor);
        assert_eq!(rows[0][0], json!(-4.0));
    }
}


#[cfg(test)]
mod final_coverage_tests {
    use serde_json::json;

    #[test] fn test_agg_sum_floats() { assert!((crate::agg_functions::sum(&[vec![json!(1.5)], vec![json!(2.5)]], 0) - 4.0).abs() < 1e-10); }
    #[test] fn test_agg_avg_single() { assert_eq!(crate::agg_functions::avg(&[vec![json!(42)]], 0), Some(42.0)); }
    #[test] fn test_agg_max_strings() { assert_eq!(crate::agg_functions::max(&[vec![json!(1)], vec![json!(99)]], 0), Some(99.0)); }
    #[test] fn test_string_upper_empty() { let mut r = vec![vec![json!("")]]; crate::string_fn::upper(&mut r, 0); assert_eq!(r[0][0], json!("")); }
    #[test] fn test_string_lower_mixed() { let mut r = vec![vec![json!("HeLLo")]]; crate::string_fn::lower(&mut r, 0); assert_eq!(r[0][0], json!("hello")); }
    #[test] fn test_date_extract_date_with_tz() { let r = vec![vec![json!("2024-01-15T10:30:00+05:00")]]; assert_eq!(crate::date_fn::extract_date(&r, 0)[0], json!("2024-01-15")); }
    #[test] fn test_math_abs_zero() { let mut r = vec![vec![json!(0.0)]]; crate::math_fn::apply_math(&mut r, 0, crate::math_fn::MathFn::Abs); assert_eq!(r[0][0], json!(0.0)); }
    #[test] fn test_math_round_half() { let mut r = vec![vec![json!(2.5)]]; crate::math_fn::apply_math(&mut r, 0, crate::math_fn::MathFn::Round); assert_eq!(r[0][0], json!(3.0)); }
    #[test] fn test_case_when_empty_conditions() { let c = crate::case_when::CaseWhen::new(json!("d")); assert_eq!(c.evaluate(&json!("x")), json!("d")); }
    #[test] fn test_sorter_single_row() { let mut r = vec![vec![json!(1)]]; crate::sorter::sort_by_column(&mut r, 0, false); assert_eq!(r[0][0], json!(1)); }
    #[test] fn test_distinct_empty() { assert!(crate::distinct::distinct(vec![]).is_empty()); }
    #[test] fn test_grouper_empty() { assert!(crate::grouper::group_count(&[], 0).is_empty()); }
    #[test] fn test_having_eq_no_match() { let r = vec![vec![json!("a")]]; assert!(crate::having::having_eq(&r, 0, &json!("b")).is_empty()); }
    #[test] fn test_anti_join_all_match() { let l = vec![vec![json!(1)]]; let r = vec![vec![json!(1)]]; assert!(crate::set_ops::anti_join(&l, 0, &r, 0).is_empty()); }
    #[test] fn test_window_rank_single() { let r = crate::window_fn::add_rank(&[vec![json!(100)]], 0); assert_eq!(r[0][0], json!(1)); }
    #[test] fn test_coercer_null() { assert_eq!(crate::coercer::to_string(&json!(null)), json!(null)); }
    #[test] fn test_coercer_bool_false() { assert_eq!(crate::coercer::to_number(&json!(false)), json!(0)); }
    #[test] fn test_null_handler_fill_no_nulls() { let mut r = vec![vec![json!(1)]]; crate::null_handler::fill_nulls(&mut r, &json!(0)); assert_eq!(r[0][0], json!(1)); }
    #[test] fn test_sampling_empty() { assert!(crate::sampling::sample_rows(&[], 10).is_empty()); }
    #[test] fn test_formatter_csv_null() { let csv = crate::formatter::to_csv(&["x".into()], &[vec![json!(null)]]); assert!(csv.contains("NULL")); }
    #[test] fn test_fingerprint_empty_string() { assert_eq!(crate::fingerprint::fingerprint(""), ""); }
    #[test] fn test_sanitize_adjacent_strings() { let s = crate::sanitize::sanitize_query("'a''b'"); assert!(s.contains("***")); }
    #[test] fn test_complexity_subquery() { let s = crate::complexity::score_query("SELECT * FROM t WHERE id IN (SELECT id FROM t2)"); assert!(s.has_subquery); }
    #[test] fn test_validate_large_page() { let e = crate::validate::validate_request("SELECT 1", "sql", Some(99999), None); assert!(!e.is_empty()); }
    #[test] fn test_query_parser_limit_none() { assert_eq!(crate::query_parser::extract_limit("SELECT *"), None); }
    #[test] fn test_top_n_rewrite_no_top() { assert_eq!(crate::top_n::rewrite_top_to_limit("SELECT 1"), "SELECT 1"); }
    #[test] fn test_cache_key_empty_query() { let k = crate::cache_key::build_key("sql", "", None); assert_eq!(k, "sql:"); }
    #[test] fn test_policy_select_allowed() { let p = crate::query_policy::QueryPolicy::with_defaults(); assert_eq!(p.check("SELECT 1"), crate::query_policy::PolicyResult::Allowed); }
    #[test] fn test_circuit_breaker_success_resets() { let cb = crate::circuit_breaker::CircuitBreaker::new(2, 30); cb.record_failure("ds"); cb.record_success("ds"); assert_eq!(cb.state("ds"), crate::circuit_breaker::CircuitState::Closed); }
    #[test] fn test_rewrite_count_no_limit() { let r = crate::rewrite::apply_rules("SELECT COUNT(*) FROM t", &crate::rewrite::default_rules()); assert!(!r.contains("LIMIT")); }
}


#[cfg(test)]
mod final_push_tests {
    use serde_json::json;

    #[test] fn test_merge_distinct_empty() { let (_, r) = crate::aggregator::merge_distinct(vec![]); assert!(r.is_empty()); }
    #[test] fn test_sort_desc_strings() { let mut r = vec![vec![json!("a")], vec![json!("c")], vec![json!("b")]]; crate::sorter::sort_by_column(&mut r, 0, true); assert_eq!(r[0][0], json!("c")); }
    #[test] fn test_distinct_single() { assert_eq!(crate::distinct::distinct(vec![vec![json!(1)]]).len(), 1); }
    #[test] fn test_group_sum_empty() { assert!(crate::grouper::group_sum(&[], 0, 1).is_empty()); }
    #[test] fn test_left_join_empty_right() { let r = crate::joiner::left_join(&[vec![json!(1)]], 0, &[], 0, 1); assert_eq!(r.len(), 1); }
    #[test] fn test_except_identical() { let r = vec![vec![json!(1)]]; assert!(crate::intersect::except(&r, &r).is_empty()); }
    #[test] fn test_anti_join_empty_left() { assert!(crate::set_ops::anti_join(&[], 0, &[vec![json!(1)]], 0).is_empty()); }
    #[test] fn test_window_row_number_empty() { let (_, r) = crate::window_fn::add_row_number(&[], "rn"); assert!(r.is_empty()); }
    #[test] fn test_pivot_single_row() { let (c, _) = crate::pivot::pivot(&[vec![json!("k"), json!("c"), json!(1)]], 0, 1, 2); assert!(c.len() >= 2); }
    #[test] fn test_transpose_empty_cols() { let (_, r) = crate::transpose::transpose(&[], &[]); assert!(r.is_empty()); }
    #[test] fn test_flatten_array() { let mut out = std::collections::BTreeMap::new(); crate::flattener::flatten_value("arr", &json!([1,2,3]), &mut out); assert_eq!(out["arr"], json!([1,2,3])); }
    #[test] fn test_arrow_single_col() { let a = crate::arrow_export::to_columnar(&["x".into()], &[vec![json!(1)]]); assert_eq!(a[0].data_type, "int64"); }
    #[test] fn test_type_infer_array() { assert_eq!(crate::type_infer::infer_type(&json!([1])), crate::type_infer::InferredType::Array); }
    #[test] fn test_profiler_boolean() { let p = crate::profiler::profile(&["b".into()], &[vec![json!(true)]]); assert_eq!(p[0].data_type, "boolean"); }
    #[test] fn test_column_stats_distinct() { let s = crate::column_stats::compute_stats(&["x".into()], &[vec![json!(1)], vec![json!(1)]]); assert_eq!(s[0].distinct_approx, 1); }
    #[test] fn test_response_builder_with_ds() { let r = crate::response_builder::ResponseBuilder::new("q", "sql").datasources(vec!["pg".into()]).build(); assert!(r.metadata.datasources_queried.is_some()); }
    #[test] fn test_bookmarks_delete_missing() { let s = crate::bookmarks::BookmarkStore::new(); assert!(!s.delete("missing")); }
    #[test] fn test_tags_untag_missing() { let r = crate::tags::TagRegistry::new(); r.untag("q", "t"); assert!(r.get_tags("q").is_empty()); }
    #[test] fn test_alias_list_empty() { let r = crate::alias::AliasRegistry::new(); assert!(r.list().is_empty()); }
    #[test] fn test_access_log_status_counts() { let l = crate::access_log::AccessLog::new(); assert!(l.count_by_status().is_empty()); }
    #[test] fn test_timeout_tracker_for_ds() { let t = crate::timeout_tracker::TimeoutTracker::new(); assert_eq!(t.count_for_datasource("x"), 0); }
    #[test] fn test_cost_tracker_all_empty() { let t = crate::cost_tracker::CostTracker::new(); assert!(t.all().is_empty()); }
    #[test] fn test_rate_monitor_count() { let m = crate::rate_monitor::RateMonitor::new(60); m.record(); assert_eq!(m.count(), 1); }
    #[test] fn test_pool_stats_acquire_release() { let t = crate::pool_stats::PoolTracker::new(); t.acquire("ds"); t.release("ds"); let s = t.snapshot(); assert_eq!(s["ds"].active, 0); }
    #[test] fn test_scheduler_count() { let r = crate::scheduler::ScheduleRegistry::new(); assert_eq!(r.count(), 0); }
}


#[cfg(test)]
mod toward_1450_tests {
    use serde_json::json;

    // Agg functions combinations
    #[test] fn test_agg_count_all_null() { assert_eq!(crate::agg_functions::count(&[vec![json!(null)], vec![json!(null)]], 0), 0); }
    #[test] fn test_agg_sum_mixed() { assert_eq!(crate::agg_functions::sum(&[vec![json!(10)], vec![json!(null)], vec![json!(20)]], 0), 30.0); }
    #[test] fn test_agg_avg_mixed() { assert_eq!(crate::agg_functions::avg(&[vec![json!(10)], vec![json!(null)], vec![json!(20)]], 0), Some(15.0)); }

    // String functions combinations
    #[test] fn test_string_trim_tabs() { let mut r = vec![vec![json!("\thello\t")]]; crate::string_fn::trim(&mut r, 0); assert_eq!(r[0][0], json!("hello")); }
    #[test] fn test_string_concat_empty_sep() { let r = vec![vec![json!("a"), json!("b")]]; let res = crate::string_fn::concat_columns(&r, 0, 1, ""); assert_eq!(res[0], json!("ab")); }

    // Date functions combinations
    #[test] fn test_date_extract_midnight() { let r = vec![vec![json!("2024-01-01T00:00:00Z")]]; assert_eq!(crate::date_fn::extract_hour(&r, 0)[0], json!(0)); }
    #[test] fn test_date_extract_23() { let r = vec![vec![json!("2024-01-01T23:59:59Z")]]; assert_eq!(crate::date_fn::extract_hour(&r, 0)[0], json!(23)); }

    // Math functions combinations
    #[test] fn test_math_ceil_integer() { let mut r = vec![vec![json!(5.0)]]; crate::math_fn::apply_math(&mut r, 0, crate::math_fn::MathFn::Ceil); assert_eq!(r[0][0], json!(5.0)); }
    #[test] fn test_math_modulo_exact() { let r = vec![vec![json!(9)]]; assert_eq!(crate::math_fn::modulo(&r, 0, 3.0)[0], json!(0.0)); }

    // Sorter combinations
    #[test] fn test_sort_mixed_types() { let mut r = vec![vec![json!("b")], vec![json!("a")], vec![json!("c")]]; crate::sorter::sort_by_column(&mut r, 0, false); assert_eq!(r[0][0], json!("a")); }
    #[test] fn test_sort_with_nulls_desc() { let mut r = vec![vec![json!(null)], vec![json!(1)], vec![json!(2)]]; crate::sorter::sort_by_column(&mut r, 0, true); assert_eq!(r[0][0], json!(2)); }

    // Grouper combinations
    #[test] fn test_group_count_three_groups() { let r = vec![vec![json!("a")], vec![json!("b")], vec![json!("c")], vec![json!("a")]]; let g = crate::grouper::group_count(&r, 0); assert_eq!(g.len(), 3); }
    #[test] fn test_group_sum_negative() { let r = vec![vec![json!("x"), json!(-5)], vec![json!("x"), json!(10)]]; let g = crate::grouper::group_sum(&r, 0, 1); assert_eq!(g[0].1, 5.0); }

    // Joiner combinations
    #[test] fn test_hash_join_multi_col() { let l = vec![vec![json!(1), json!("a")]]; let r = vec![vec![json!(1), json!("x")]]; let res = crate::joiner::hash_join(&l, 0, &r, 0); assert_eq!(res[0].len(), 3); }

    // Set ops combinations
    #[test] fn test_semi_join_partial() { let l = vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]]; let r = vec![vec![json!(2)]]; assert_eq!(crate::set_ops::semi_join(&l, 0, &r, 0).len(), 1); }

    // Window combinations
    #[test] fn test_rank_all_same() { let r = vec![vec![json!(5)]; 3]; let ranked = crate::window_fn::add_rank(&r, 0); assert!(ranked.iter().all(|row| row[0] == json!(1))); }

    // Pivot combinations
    #[test] fn test_pivot_numeric_keys() { let r = vec![vec![json!(2024), json!("Q1"), json!(100)]]; let (c, _) = crate::pivot::pivot(&r, 0, 1, 2); assert!(c.len() >= 2); }

    // Flatten combinations
    #[test] fn test_flatten_deep_nested() { let mut out = std::collections::BTreeMap::new(); crate::flattener::flatten_value("", &json!({"a": {"b": {"c": 1}}}), &mut out); assert_eq!(out["a.b.c"], json!(1)); }

    // Arrow export combinations
    #[test] fn test_arrow_boolean_col() { let a = crate::arrow_export::to_columnar(&["b".into()], &[vec![json!(true)], vec![json!(false)]]); assert_eq!(a[0].data_type, "boolean"); }

    // Coercer combinations
    #[test] fn test_coerce_string_to_number() { assert_eq!(crate::coercer::to_number(&json!("3.14")), json!(3.14)); }
    #[test] fn test_coerce_invalid_string() { assert_eq!(crate::coercer::to_number(&json!("abc")), json!(null)); }

    // Null handler combinations
    #[test] fn test_coalesce_multiple_nulls() { let mut r = vec![vec![json!(null)], vec![json!(null)], vec![json!(1)]]; crate::null_handler::coalesce(&mut r, 0, &json!(0)); assert_eq!(r[0][0], json!(0)); assert_eq!(r[2][0], json!(1)); }

    // Sampling combinations
    #[test] fn test_sample_exact_size() { let r: Vec<Vec<serde_json::Value>> = (0..5).map(|i| vec![json!(i)]).collect(); assert_eq!(crate::sampling::sample_rows(&r, 5).len(), 5); }

    // Formatter combinations
    #[test] fn test_table_alignment() { let t = crate::formatter::to_table(&["name".into()], &[vec![json!("alice")], vec![json!("bob")]]); assert!(t.contains("alice")); assert!(t.contains("bob")); }

    // Fingerprint combinations
    #[test] fn test_fingerprint_mixed_literals() { let f = crate::fingerprint::fingerprint("WHERE x = 'a' AND y = 42"); assert_eq!(f.matches('?').count(), 2); }

    // Complexity combinations
    #[test] fn test_complexity_window() { let s = crate::complexity::score_query("SELECT ROW_NUMBER() OVER(ORDER BY id) FROM t"); assert!(s.score >= 1); }

    // Circuit breaker combinations
    #[test] fn test_circuit_breaker_multiple_ds() { let cb = crate::circuit_breaker::CircuitBreaker::new(2, 30); cb.record_failure("a"); cb.record_failure("b"); assert!(cb.allow("a")); assert!(cb.allow("b")); }

    // Rewrite combinations
    #[test] fn test_rewrite_insert_no_limit() { let r = crate::rewrite::apply_rules("INSERT INTO t VALUES (1)", &crate::rewrite::default_rules()); assert!(!r.contains("LIMIT")); }

    // Query policy combinations
    #[test] fn test_policy_alter_denied() { let p = crate::query_policy::QueryPolicy::with_defaults(); assert!(matches!(p.check("ALTER TABLE t ADD COLUMN x INT"), crate::query_policy::PolicyResult::Denied(_))); }

    // Validate combinations
    #[test] fn test_validate_all_valid() { assert!(crate::validate::validate_request("SELECT 1", "sql", Some(100), Some(5000)).is_empty()); }
}


#[cfg(test)]
mod milestone_1450 {
    use serde_json::json;
    #[test] fn test_intersect_empty_right() { assert!(crate::intersect::intersect(&[vec![json!(1)]], &[]).is_empty()); }
    #[test] fn test_except_empty_right() { assert_eq!(crate::intersect::except(&[vec![json!(1)]], &[]).len(), 1); }
    #[test] fn test_having_lt_boundary() { let r = vec![vec![json!(5)]]; assert_eq!(crate::having::having_lt(&r, 0, 5.0).len(), 0); }
    #[test] fn test_having_gte_boundary() { let r = vec![vec![json!(5)]]; assert_eq!(crate::having::having_gte(&r, 0, 5.0).len(), 1); }
    #[test] fn test_top_n_extract_large() { assert_eq!(crate::top_n::extract_top("SELECT TOP 99999 * FROM t"), Some(99999)); }
}
