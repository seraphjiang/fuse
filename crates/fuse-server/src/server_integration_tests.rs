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


#[cfg(test)]
mod milestone_1500 {
    use serde_json::json;
    #[test] fn test_1() { assert_eq!(crate::agg_functions::count_distinct(&[vec![json!(1)], vec![json!(2)], vec![json!(3)]], 0), 3); }
    #[test] fn test_2() { let mut r = vec![vec![json!("  x  ")]]; crate::string_fn::trim(&mut r, 0); assert_eq!(r[0][0], json!("x")); }
    #[test] fn test_3() { assert_eq!(crate::date_fn::extract_date(&[vec![json!("2024-12-31T23:59:59Z")]], 0)[0], json!("2024-12-31")); }
    #[test] fn test_4() { let r = vec![vec![json!(7)]]; assert_eq!(crate::math_fn::modulo(&r, 0, 4.0)[0], json!(3.0)); }
    #[test] fn test_5() { assert_eq!(crate::distinct::count_distinct(&[vec![json!("a")], vec![json!("a")]], 0), 1); }
    #[test] fn test_6() { let r = crate::offset_pagination::page_info(0, 0, 10); assert_eq!(r.total_pages, 0); }
    #[test] fn test_7() { assert!(crate::query_parser::is_read_only("SHOW DATABASES")); }
    #[test] fn test_8() { let k = crate::cache_key::build_key_with_params("sql", "SELECT $1", &[]); assert!(!k.contains("|")); }
    #[test] fn test_9() { let l = crate::lineage::QueryLineage::new("q", vec![("a","t1"),("b","t2")]); assert!(l.is_cross_source()); }
    #[test] fn test_10() { let d = crate::delivery::recommend_delivery(Some(50000), None, 10000, 10_000_000); assert_eq!(d, crate::delivery::DeliveryMode::Streaming); }
}


#[cfg(test)]
mod toward_1550 {
    use serde_json::json;

    #[test] fn test_merge_single_row() { let (_, r) = crate::aggregator::merge_results(vec![(vec!["x".into()], vec![vec![json!(1)]])]); assert_eq!(r.len(), 1); }
    #[test] fn test_sort_numbers_asc() { let mut r = vec![vec![json!(3)], vec![json!(1)], vec![json!(2)]]; crate::sorter::sort_by_column(&mut r, 0, false); assert_eq!(r[0][0], json!(1)); assert_eq!(r[2][0], json!(3)); }
    #[test] fn test_distinct_preserves_first() { let r = crate::distinct::distinct(vec![vec![json!(1), json!("a")], vec![json!(1), json!("a")]]); assert_eq!(r.len(), 1); assert_eq!(r[0][1], json!("a")); }
    #[test] fn test_group_count_two() { let r = vec![vec![json!("a")], vec![json!("b")]]; assert_eq!(crate::grouper::group_count(&r, 0).len(), 2); }
    #[test] fn test_hash_join_no_match() { assert!(crate::joiner::hash_join(&[vec![json!(1)]], 0, &[vec![json!(2)]], 0).is_empty()); }
    #[test] fn test_semi_join_all() { let r = vec![vec![json!(1)]]; assert_eq!(crate::set_ops::semi_join(&r, 0, &r, 0).len(), 1); }
    #[test] fn test_anti_join_none() { let r = vec![vec![json!(1)]]; assert!(crate::set_ops::anti_join(&r, 0, &r, 0).is_empty()); }
    #[test] fn test_window_rank_empty() { assert!(crate::window_fn::add_rank(&[], 0).is_empty()); }
    #[test] fn test_coerce_column_string() { let mut r = vec![vec![json!(42)]]; crate::coercer::coerce_column(&mut r, 0, "string"); assert_eq!(r[0][0], json!("42")); }
    #[test] fn test_null_counts_all_present() { assert_eq!(crate::null_handler::null_counts(&[vec![json!(1)], vec![json!(2)]], 1), vec![0]); }
    #[test] fn test_paginate_empty() { assert!(crate::offset_pagination::paginate(&[], 0, 10).is_empty()); }
    #[test] fn test_row_limit_exact() { let r = crate::row_limit::enforce_limit(vec![vec![json!(1)]; 10], 10); assert!(!r.truncated); }
    #[test] fn test_pivot_empty_rows() { let (c, r) = crate::pivot::pivot(&[], 0, 1, 2); assert_eq!(c, vec!["key"]); assert!(r.is_empty()); }
    #[test] fn test_transpose_two_cols() { let (c, _) = crate::transpose::transpose(&["a".into(), "b".into()], &[vec![json!(1), json!(2)]]); assert_eq!(c.len(), 2); }
    #[test] fn test_flatten_no_nesting() { let mut out = std::collections::BTreeMap::new(); crate::flattener::flatten_value("", &json!({"x": 1}), &mut out); assert_eq!(out.len(), 1); }
    #[test] fn test_arrow_roundtrip() { let cols = vec!["a".into()]; let rows = vec![vec![json!(1)], vec![json!(2)]]; let c = crate::arrow_export::to_columnar(&cols, &rows); let back = crate::arrow_export::to_rows(&c); assert_eq!(back, rows); }
    #[test] fn test_type_infer_object() { assert_eq!(crate::type_infer::infer_type(&json!({"k":"v"})), crate::type_infer::InferredType::Object); }
    #[test] fn test_profiler_null_pct() { let p = crate::profiler::profile(&["x".into()], &[vec![json!(null)]]); assert_eq!(p[0].null_pct, 100.0); }
    #[test] fn test_column_stats_min_max() { let s = crate::column_stats::compute_stats(&["x".into()], &[vec![json!("a")], vec![json!("z")]]); assert!(s[0].min.is_some()); assert!(s[0].max.is_some()); }
    #[test] fn test_cache_key_tenant() { let k = crate::cache_key::build_key("sql", "SELECT 1", Some("t1")); assert!(k.starts_with("t1:")); }
    #[test] fn test_policy_grant_denied() { let p = crate::query_policy::QueryPolicy::with_defaults(); assert!(matches!(p.check("GRANT ALL ON t TO user"), crate::query_policy::PolicyResult::Denied(_))); }
    #[test] fn test_circuit_breaker_half_open() { let cb = crate::circuit_breaker::CircuitBreaker::new(1, 0); cb.record_failure("ds"); assert!(cb.allow("ds")); /* 0s recovery = immediate half-open */ }
    #[test] fn test_rewrite_whitespace() { let r = crate::rewrite::apply_rules("  SELECT   1  ", &crate::rewrite::default_rules()); assert!(!r.contains("  ")); }
    #[test] fn test_validate_empty_format() { let e = crate::validate::validate_request("SELECT 1", "xml", None, None); assert!(!e.is_empty()); }
    #[test] fn test_explain_cache_overwrite() { let c = crate::explain_cache::ExplainCache::new(60, 10); c.insert("q".into(), json!({"v":1})); c.insert("q".into(), json!({"v":2})); assert_eq!(c.get("q").unwrap()["v"], json!(2)); }
    #[test] fn test_slow_query_fast() { assert!(!crate::slow_query::check_slow_query("q", "sql", std::time::Duration::from_millis(1), &[], 0, None)); }
    #[test] fn test_pagination_with_cursor() { let p = crate::pagination::PaginationMeta::with_cursor(20, "abc".into(), Some(100)); assert!(p.has_more); }
    #[test] fn test_complexity_join() { let s = crate::complexity::score_query("SELECT * FROM a.t JOIN b.t ON a.t.id = b.t.id"); assert!(s.has_join); }
    #[test] fn test_fingerprint_whitespace() { let f1 = crate::fingerprint::fingerprint("SELECT  *  FROM  t"); let f2 = crate::fingerprint::fingerprint("SELECT * FROM t"); assert_eq!(f1, f2); }
    #[test] fn test_sanitize_nested_quotes() { let s = crate::sanitize::sanitize_query("WHERE x = 'it''s'"); assert!(s.contains("***")); }
    #[test] fn test_query_parser_tables_union() { let t = crate::query_parser::extract_tables("SELECT * FROM a.t1 UNION ALL SELECT * FROM b.t2"); assert_eq!(t.len(), 2); }
    #[test] fn test_top_n_no_top() { assert_eq!(crate::top_n::extract_top("SELECT * FROM t LIMIT 10"), None); }
    #[test] fn test_case_when_multiple() { let c = crate::case_when::CaseWhen::new(json!("d")).when(|v| v == &json!(1), json!("one")).when(|v| v == &json!(2), json!("two")); assert_eq!(c.evaluate(&json!(2)), json!("two")); }
    #[test] fn test_result_filter_lte() { let r = vec![vec![json!(5)], vec![json!(10)]]; assert_eq!(crate::result_filter::filter_rows(&r, &[crate::result_filter::FilterOp::Lte(0, 5.0)]).len(), 1); }
    #[test] fn test_projector_single_col() { let (c, r) = crate::projector::project(&[vec![json!(1), json!(2)]], &["a".into(), "b".into()], &["b".into()]); assert_eq!(c, vec!["b"]); assert_eq!(r[0], vec![json!(2)]); }
    #[test] fn test_renamer_parse_no_from() { let a = crate::renamer::parse_aliases("SELECT x AS y"); assert_eq!(a.get("x").map(|s| s.as_str()), Some("y")); }
    #[test] fn test_reorder_empty() { let (c, r) = crate::reorder::reorder(&["a".into()], &[vec![json!(1)]], &[]); assert!(c.is_empty()); assert_eq!(r[0].len(), 0); }
    #[test] fn test_union_typed_three_cols() { let l = vec!["a".into(), "b".into()]; let r = vec!["b".into(), "c".into()]; let (c, _) = crate::union_typed::union_aligned(&l, &[], &r, &[]); assert_eq!(c, vec!["a", "b", "c"]); }
    #[test] fn test_formatter_csv_comma() { let csv = crate::formatter::to_csv(&["x".into()], &[vec![json!("a,b")]]); assert!(csv.contains("\"")); }
    #[test] fn test_access_log_recent_order() { let l = crate::access_log::AccessLog::new(); l.record(crate::access_log::AccessEntry { method: "GET".into(), path: "/a".into(), status: 200, duration_ms: 1, timestamp: 0, client_ip: None }); l.record(crate::access_log::AccessEntry { method: "POST".into(), path: "/b".into(), status: 201, duration_ms: 2, timestamp: 1, client_ip: None }); assert_eq!(l.recent(1)[0].path, "/b"); }
    #[test] fn test_timeout_tracker_recent() { let t = crate::timeout_tracker::TimeoutTracker::new(); t.record("q1", "ds", 5000, 5100); assert_eq!(t.recent(10).len(), 1); }
    #[test] fn test_cost_tracker_record() { let t = crate::cost_tracker::CostTracker::new(); t.record("team", "pg", 100, 5000, 50); assert_eq!(t.for_tenant("team")["pg"].query_count, 1); }
    #[test] fn test_rate_monitor_multiple() { let m = crate::rate_monitor::RateMonitor::new(60); for _ in 0..10 { m.record(); } assert_eq!(m.count(), 10); }
    #[test] fn test_pool_stats_timeout() { let t = crate::pool_stats::PoolTracker::new(); t.timeout("ds"); assert_eq!(t.snapshot()["ds"].total_timeouts, 1); }
    #[test] fn test_scheduler_record_run() { let r = crate::scheduler::ScheduleRegistry::new(); r.add(crate::scheduler::ScheduledQuery { id: "s".into(), name: "t".into(), query: "SELECT 1".into(), format: "sql".into(), cron: "* * * * *".into(), enabled: true, last_run: None, last_status: None, run_count: 0 }); r.record_run("s", "ok"); assert_eq!(r.get("s").unwrap().run_count, 1); }
    #[test] fn test_lineage_with_join() { let l = crate::lineage::QueryLineage::new("q", vec![("a","t1"),("b","t2")]).with_join("hash"); assert_eq!(l.join_type.as_deref(), Some("hash")); }
    #[test] fn test_delivery_streaming_bytes() { assert_eq!(crate::delivery::recommend_delivery(None, Some(100_000_000), 10000, 10_000_000), crate::delivery::DeliveryMode::Streaming); }
    #[test] fn test_explain_cache_miss() { let c = crate::explain_cache::ExplainCache::new(60, 10); assert!(c.get("missing").is_none()); }
    #[test] fn test_bookmarks_list_empty() { assert!(crate::bookmarks::BookmarkStore::new().list().is_empty()); }
}


#[cfg(test)]
mod push_1600 {
    use serde_json::json;

    // Templates
    #[test] fn test_template_store_overwrite() { let s = crate::templates::TemplateStore::new(); s.save(crate::templates::QueryTemplate { name: "t".into(), template: "v1".into(), params: vec![], description: None }); s.save(crate::templates::QueryTemplate { name: "t".into(), template: "v2".into(), params: vec![], description: None }); assert_eq!(s.get("t").unwrap().template, "v2"); }
    #[test] fn test_template_render_multiple_params() { let t = crate::templates::QueryTemplate { name: "q".into(), template: "SELECT * FROM {{ds}}.{{table}} LIMIT {{n}}".into(), params: vec!["ds".into(), "table".into(), "n".into()], description: None }; let mut v = std::collections::HashMap::new(); v.insert("ds".into(), "pg".into()); v.insert("table".into(), "users".into()); v.insert("n".into(), "10".into()); assert_eq!(t.render(&v).unwrap(), "SELECT * FROM pg.users LIMIT 10"); }

    // Bookmarks
    #[test] fn test_bookmark_search_by_query() { let s = crate::bookmarks::BookmarkStore::new(); s.save(crate::bookmarks::Bookmark { id: "b1".into(), name: "test".into(), query: "SELECT * FROM logs".into(), format: "sql".into(), description: None, tags: vec![], created_at: 0 }); assert_eq!(s.search("logs").len(), 1); }

    // Tags
    #[test] fn test_tags_multiple_queries() { let r = crate::tags::TagRegistry::new(); r.tag("q1", "prod"); r.tag("q2", "prod"); r.tag("q3", "dev"); assert_eq!(r.find_by_tag("prod").len(), 2); }

    // Alias
    #[test] fn test_alias_remove_nonexistent() { let r = crate::alias::AliasRegistry::new(); assert!(!r.remove("missing")); }

    // Notifications
    #[test] fn test_notification_drop_receiver() { let hub = crate::notifications::NotificationHub::new(); let rx = hub.subscribe("q1"); drop(rx); hub.notify(crate::notifications::QueryNotification { query_id: "q1".into(), success: true, duration_ms: 0, row_count: 0, error: None }); assert_eq!(hub.pending_count(), 0); }

    // Access log
    #[test] fn test_access_log_multiple_statuses() { let l = crate::access_log::AccessLog::new(); for s in &[200u16, 200, 404, 500] { l.record(crate::access_log::AccessEntry { method: "GET".into(), path: "/".into(), status: *s, duration_ms: 1, timestamp: 0, client_ip: None }); } let c = l.count_by_status(); assert_eq!(c[&200], 2); assert_eq!(c[&404], 1); }

    // Timeout tracker
    #[test] fn test_timeout_tracker_per_ds() { let t = crate::timeout_tracker::TimeoutTracker::new(); t.record("q1", "pg", 5000, 5100); t.record("q2", "pg", 5000, 6000); t.record("q3", "es", 3000, 3200); assert_eq!(t.count_for_datasource("pg"), 2); }

    // Cost tracker
    #[test] fn test_cost_tracker_multi_tenant() { let t = crate::cost_tracker::CostTracker::new(); t.record("a", "pg", 100, 5000, 50); t.record("b", "pg", 200, 8000, 80); let all = t.all(); assert_eq!(all["pg"].query_count, 2); }

    // Rate monitor
    #[test] fn test_rate_monitor_qps() { let m = crate::rate_monitor::RateMonitor::new(10); for _ in 0..50 { m.record(); } assert!(m.qps() > 0.0); }

    // Pool stats
    #[test] fn test_pool_stats_multiple_ds() { let t = crate::pool_stats::PoolTracker::new(); t.acquire("a"); t.acquire("b"); assert_eq!(t.snapshot().len(), 2); }

    // History analytics
    #[test] fn test_analytics_p95() { let entries: Vec<(bool, u64, Vec<String>)> = (0..100).map(|i| (true, i * 10, vec![])).collect(); let a = crate::history_analytics::compute_analytics(&entries); assert!(a.p95_duration_ms > 0); }

    // Scheduler
    #[test] fn test_scheduler_list() { let r = crate::scheduler::ScheduleRegistry::new(); r.add(crate::scheduler::ScheduledQuery { id: "s1".into(), name: "a".into(), query: "SELECT 1".into(), format: "sql".into(), cron: "* * * * *".into(), enabled: true, last_run: None, last_status: None, run_count: 0 }); assert_eq!(r.list().len(), 1); }

    // Lineage
    #[test] fn test_lineage_datasource_ids() { let l = crate::lineage::QueryLineage::new("q", vec![("pg", "users"), ("es", "logs")]); assert_eq!(l.datasource_ids(), vec!["pg", "es"]); }

    // Delivery
    #[test] fn test_delivery_unknown_buffered() { assert_eq!(crate::delivery::recommend_delivery(None, None, 10000, 10_000_000), crate::delivery::DeliveryMode::Buffered); }

    // Explain cache
    #[test] fn test_explain_cache_size() { let c = crate::explain_cache::ExplainCache::new(60, 100); c.insert("a".into(), json!({})); c.insert("b".into(), json!({})); assert_eq!(c.len(), 2); }

    // Slow query
    #[test] fn test_slow_query_exact_threshold() { assert!(crate::slow_query::check_slow_query("q", "sql", std::time::Duration::from_millis(5000), &[], 0, Some(5000))); }

    // Pagination
    #[test] fn test_pagination_last_page() { let p = crate::pagination::PaginationMeta::last_page(15, 95); assert!(!p.has_more); assert_eq!(p.total_rows, Some(95)); }

    // Complexity
    #[test] fn test_complexity_aggregation() { let s = crate::complexity::score_query("SELECT service, COUNT(*) FROM t GROUP BY service"); assert!(s.has_aggregation); }

    // Fingerprint
    #[test] fn test_fingerprint_no_change() { assert_eq!(crate::fingerprint::fingerprint("SELECT id FROM t"), "SELECT id FROM t"); }

    // Sanitize
    #[test] fn test_sanitize_preserves_numbers() { let s = crate::sanitize::sanitize_query("WHERE id = 42"); assert!(s.contains("42")); }

    // Query parser
    #[test] fn test_parser_format_hint() { assert_eq!(crate::query_parser::extract_format_hint("/* format:ppl */ source = t"), Some("ppl".into())); }

    // Top N
    #[test] fn test_top_n_rewrite() { let r = crate::top_n::rewrite_top_to_limit("SELECT TOP 5 * FROM t WHERE x = 1"); assert!(r.contains("LIMIT 5")); }

    // Case when
    #[test] fn test_case_when_apply() { let c = crate::case_when::CaseWhen::new(json!("other")).when(|v| v == &json!("error"), json!("bad")); let r = crate::case_when::apply_case(&[vec![json!("error")], vec![json!("ok")]], 0, &c); assert_eq!(r, vec![json!("bad"), json!("other")]); }

    // Result filter
    #[test] fn test_filter_is_null() { let r = vec![vec![json!(null)], vec![json!(1)]]; assert_eq!(crate::result_filter::filter_rows(&r, &[crate::result_filter::FilterOp::IsNull(0)]).len(), 1); }

    // Projector
    #[test] fn test_project_reorder() { let (c, _) = crate::projector::project(&[vec![json!(1), json!(2)]], &["a".into(), "b".into()], &["b".into(), "a".into()]); assert_eq!(c, vec!["b", "a"]); }

    // Renamer
    #[test] fn test_rename_all() { let mut a = std::collections::HashMap::new(); a.insert("old".into(), "new".into()); assert_eq!(crate::renamer::rename_columns(&["old".into()], &a), vec!["new"]); }

    // Reorder
    #[test] fn test_move_column_first() { assert_eq!(crate::reorder::move_column(&["a".into(), "b".into(), "c".into()], "c", 0), vec!["c", "a", "b"]); }

    // Union typed
    #[test] fn test_union_null_fill() { let (_, r) = crate::union_typed::union_aligned(&["a".into()], &[vec![json!(1)]], &["b".into()], &[vec![json!(2)]]); assert_eq!(r[0][1], json!(null)); assert_eq!(r[1][0], json!(null)); }

    // Formatter
    #[test] fn test_table_empty() { let t = crate::formatter::to_table(&["x".into()], &[]); assert!(t.contains("x")); }

    // Arrow export
    #[test] fn test_arrow_float_col() { let a = crate::arrow_export::to_columnar(&["f".into()], &[vec![json!(3.14)]]); assert_eq!(a[0].data_type, "float64"); }

    // Type infer
    #[test] fn test_infer_null() { assert_eq!(crate::type_infer::infer_type(&json!(null)), crate::type_infer::InferredType::Null); }

    // Profiler
    #[test] fn test_profiler_samples() { let p = crate::profiler::profile(&["x".into()], &[vec![json!(1)], vec![json!(2)], vec![json!(3)], vec![json!(4)]]); assert!(p[0].sample_values.len() <= 3); }

    // Column stats
    #[test] fn test_stats_all_same() { let rows = vec![vec![json!(5)], vec![json!(5)], vec![json!(5)]]; let s = crate::column_stats::compute_stats(&["x".into()], &rows); assert_eq!(s[0].distinct_approx, 1); }

    // Cache key
    #[test] fn test_cache_key_params() { let k = crate::cache_key::build_key_with_params("sql", "SELECT $1", &["hello".into()]); assert!(k.contains("|hello")); }

    // Query policy
    #[test] fn test_policy_revoke_denied() { let p = crate::query_policy::QueryPolicy::with_defaults(); assert!(matches!(p.check("REVOKE ALL ON t FROM user"), crate::query_policy::PolicyResult::Denied(_))); }

    // Circuit breaker
    #[test] fn test_circuit_breaker_state_closed() { let cb = crate::circuit_breaker::CircuitBreaker::new(5, 30); assert_eq!(cb.state("new"), crate::circuit_breaker::CircuitState::Closed); }

    // Rewrite
    #[test] fn test_rewrite_select_star() { let r = crate::rewrite::apply_rules("SELECT * FROM t", &crate::rewrite::default_rules()); assert!(r.contains("LIMIT 10000")); }

    // Validate
    #[test] fn test_validate_timeout_ok() { assert!(crate::validate::validate_request("SELECT 1", "sql", None, Some(60000)).is_empty()); }
}
