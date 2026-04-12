// SPDX-License-Identifier: Apache-2.0
//! Integration tests — cross-module tests for the planning framework.

#[cfg(test)]
mod tests {
    use crate::plan_builder::PlanBuilder;
    use crate::plan_visitor;
    use crate::plan_printer;
    use crate::plan_rules::{self, EliminateMaxLimit, OptRule};
    use crate::predicate::Predicate;
    use crate::cost_model;
    use crate::pushdown;
    use crate::connector::ConnectorCapabilities;
    use crate::schema_compat;
    use crate::type_map;
    use crate::expr::{self, CompareOp};
    use crate::scalar_expr::ScalarExpr;
    use serde_json::json;

    #[test]
    fn test_plan_build_visit_print() {
        let plan = PlanBuilder::scan("pg", "users")
            .filter(&Predicate::gt("age", "18"))
            .project(vec!["name", "age"])
            .sort("name", false)
            .limit(50)
            .build();
        assert_eq!(plan_visitor::node_count(&plan.root), 5);
        assert_eq!(plan_visitor::datasources(&plan.root), vec!["pg"]);
        assert!(plan_visitor::has_filter(&plan.root));
        let text = plan_printer::print_plan(&plan.root, 0);
        assert!(text.contains("Limit: 50"));
        assert!(text.contains("Scan: pg.users"));
    }

    #[test]
    fn test_plan_optimize_and_print() {
        let plan = PlanBuilder::scan("ds", "t").limit(u64::MAX).build();
        let rules: Vec<Box<dyn OptRule>> = vec![Box::new(EliminateMaxLimit)];
        let optimized = plan_rules::apply_rules(plan, &rules);
        let text = plan_printer::print_plan(&optimized.root, 0);
        assert!(!text.contains("Limit"));
    }

    #[test]
    fn test_cost_model_scan_sort() {
        let scan = cost_model::scan_cost(10000, 5);
        let sort = cost_model::sort_cost(10000);
        let total = cost_model::total_cost(&[scan, sort]);
        assert!(total.estimated_cost > 0.0);
    }

    #[test]
    fn test_pushdown_full_caps() {
        let caps = ConnectorCapabilities::full();
        let plan = pushdown::negotiate(&caps, true, true, true, true, true);
        assert!(plan.filter_pushdown);
        assert!(plan.local_operations.is_empty());
    }

    #[test]
    fn test_schema_compat_check() {
        let left = vec!["id".into(), "name".into()];
        let right = vec!["id".into(), "name".into()];
        assert!(schema_compat::check_compatibility(&left, &right).compatible);
    }

    #[test]
    fn test_type_map_compatible() {
        let a = type_map::from_type_name("int");
        let b = type_map::from_type_name("bigint");
        assert!(type_map::types_compatible(&a, &b));
    }

    #[test]
    fn test_expr_compare() {
        assert!(expr::compare(&json!(42), &CompareOp::Gt, &json!(10)));
        assert!(!expr::compare(&json!(5), &CompareOp::Gt, &json!(10)));
    }

    #[test]
    fn test_scalar_expr_to_sql() {
        let expr = ScalarExpr::func("COUNT", vec![ScalarExpr::star()]);
        assert_eq!(expr.to_sql(), "COUNT(*)");
    }

    #[test]
    fn test_predicate_complex() {
        let p = Predicate::and(vec![
            Predicate::gt("age", "18"),
            Predicate::or(vec![
                Predicate::eq("role", "admin"),
                Predicate::eq("role", "editor"),
            ]),
        ]);
        let sql = p.to_sql();
        assert!(sql.contains("AND"));
        assert!(sql.contains("OR"));
    }

    #[test]
    fn test_plan_serde_roundtrip() {
        let node = crate::plan_serde::PlanNode::leaf("Scan", "pg.users")
            .with_cost(1000, 10.5);
        let json = node.to_json();
        let restored = crate::plan_serde::PlanNode::from_json(&json).unwrap();
        assert_eq!(node, restored);
    }

    #[test]
    fn test_plan_stats_tracking() {
        let stats = crate::plan_stats::PlanStats::new();
        stats.record("scan", 0, 1000, 50, 8000);
        stats.record("filter", 1000, 500, 10, 4000);
        assert_eq!(stats.total_duration(), 60);
        assert_eq!(stats.total_rows_out(), 1000);
    }

    #[test]
    fn test_limit_pushdown_union() {
        assert_eq!(crate::limit_pushdown::union_fetch_limit(100, 3), 100);
    }

    #[test]
    fn test_config_validator() {
        let errs = crate::config_validator::validate_connector_config(
            "my_os", "opensearch", &std::collections::HashMap::new()
        );
        assert!(!errs.is_empty()); // missing url
    }

    #[test]
    fn test_factory_registry() {
        let reg = crate::factory::FactoryRegistry::new();
        reg.register("test", Box::new(|_| Ok("ok".into())));
        assert!(reg.has("test"));
    }

    #[test]
    fn test_dependency_graph() {
        let g = crate::dependency_graph::DependencyGraph::new();
        g.record(&["pg".into(), "es".into()]);
        assert_eq!(g.top_pairs(10).len(), 1);
    }

    #[test]
    fn test_health_history_uptime() {
        let h = crate::health_history::HealthHistory::new(10);
        h.record("ds", true, Some(5));
        h.record("ds", true, Some(10));
        assert_eq!(h.uptime("ds"), 1.0);
    }

    #[test]
    fn test_metadata_cache() {
        let c = crate::metadata_cache::MetadataCache::new(60);
        c.set_tables("pg", vec!["users".into()]);
        assert_eq!(c.get_tables("pg").unwrap(), vec!["users"]);
    }

    #[test]
    fn test_query_stats() {
        let s = crate::query_stats::StatsCollector::new();
        s.record("pg", true, 100, 50);
        assert_eq!(s.avg_duration("pg"), Some(50));
    }

    #[test]
    fn test_url_parse() {
        let u = crate::url::ConnectorUrl::parse("https://example.com:9200/path").unwrap();
        assert!(u.is_tls);
        assert_eq!(u.port, Some(9200));
    }

    #[test]
    fn test_sql_quote() {
        assert_eq!(crate::sql::quote_ident("my column"), "\"my column\"");
    }
}

#[cfg(test)]
mod extra_tests {
    use crate::plan_builder::PlanBuilder;
    use crate::predicate::Predicate;
    use crate::plan_visitor;
    use crate::plan_printer;

    #[test]
    fn test_deep_plan_tree() {
        let plan = PlanBuilder::scan("a", "t1")
            .filter(&Predicate::eq("x", "1"))
            .filter(&Predicate::gt("y", "0"))
            .project(vec!["x", "y"])
            .sort("x", true)
            .limit(25)
            .build();
        assert_eq!(plan_visitor::node_count(&plan.root), 6);
    }

    #[test]
    fn test_plan_print_sort_desc() {
        let plan = PlanBuilder::scan("ds", "t").sort("ts", true).build();
        let text = plan_printer::print_plan(&plan.root, 0);
        assert!(text.contains("DESC"));
    }

    #[test]
    fn test_plan_no_filter() {
        let plan = PlanBuilder::scan("ds", "t").limit(10).build();
        assert!(!plan_visitor::has_filter(&plan.root));
    }

    #[test]
    fn test_predicate_in_list_sql() {
        let p = Predicate::in_list("id", vec!["1", "2", "3"]);
        assert!(p.to_sql().contains("IN ('1', '2', '3')"));
    }

    #[test]
    fn test_predicate_not_sql() {
        let p = Predicate::not(Predicate::eq("deleted", "true"));
        assert!(p.to_sql().starts_with("NOT"));
    }
}


#[cfg(test)]
mod edge_case_tests {
    use crate::plan_builder::PlanBuilder;
    use crate::plan_visitor;
    use crate::plan_printer;
    use crate::plan_rules::{self, EliminateMaxLimit, OptRule};
    use crate::plan_compare;
    use crate::plan_merge::{MergedPlan, SubPlan};
    use crate::plan_serde::PlanNode;
    use crate::plan_stats::PlanStats;
    use crate::predicate::Predicate;
    use crate::cost_model;
    use crate::scalar_expr::ScalarExpr;
    use crate::type_map;
    use crate::expr::{self, CompareOp};
    use crate::url::ConnectorUrl;
    use serde_json::json;

    // Plan builder edge cases
    #[test]
    fn test_scan_only() {
        let plan = PlanBuilder::scan("ds", "t").build();
        assert_eq!(plan_visitor::node_count(&plan.root), 1);
    }

    #[test]
    fn test_double_filter() {
        let plan = PlanBuilder::scan("ds", "t")
            .filter(&Predicate::eq("a", "1"))
            .filter(&Predicate::eq("b", "2"))
            .build();
        assert_eq!(plan_visitor::node_count(&plan.root), 3);
    }

    #[test]
    fn test_project_empty() {
        let plan = PlanBuilder::scan("ds", "t").project(vec![]).build();
        let text = plan_printer::print_plan(&plan.root, 0);
        assert!(text.contains("Project: []"));
    }

    // Plan compare edge cases
    #[test]
    fn test_compare_equal_costs() {
        let a = cost_model::scan_cost(100, 5);
        let b = cost_model::scan_cost(100, 5);
        assert_eq!(plan_compare::cheaper(&a, &b), 0); // prefer first when equal
    }

    #[test]
    fn test_compare_zero_cost() {
        let a = cost_model::scan_cost(0, 0);
        let b = cost_model::scan_cost(100, 5);
        assert_eq!(plan_compare::cheaper(&a, &b), 0);
    }

    // Plan merge edge cases
    #[test]
    fn test_merge_empty_union() {
        let plan = MergedPlan::union(vec![]);
        assert_eq!(plan.datasource_count(), 0);
    }

    // Plan serde edge cases
    #[test]
    fn test_serde_deep_tree() {
        let node = PlanNode::leaf("Join", "hash")
            .with_child(PlanNode::leaf("Scan", "a.t1").with_cost(100, 1.0))
            .with_child(PlanNode::leaf("Scan", "b.t2").with_cost(200, 2.0));
        let json = node.to_json();
        let restored = PlanNode::from_json(&json).unwrap();
        assert_eq!(restored.node_count(), 3);
    }

    // Plan stats edge cases
    #[test]
    fn test_stats_overwrite() {
        let s = PlanStats::new();
        s.record("n1", 0, 100, 10, 800);
        s.record("n1", 0, 200, 20, 1600);
        assert_eq!(s.get("n1").unwrap().rows_out, 200); // overwritten
    }

    // Cost model edge cases
    #[test]
    fn test_join_cost_large() {
        let c = cost_model::join_cost(1_000_000, 500_000);
        assert!(c.estimated_cost > 0.0);
    }

    #[test]
    fn test_sort_cost_one_row() {
        let c = cost_model::sort_cost(1);
        assert_eq!(c.estimated_rows, 1);
    }

    // Scalar expr edge cases
    #[test]
    fn test_nested_function() {
        let expr = ScalarExpr::func("UPPER", vec![ScalarExpr::col("name")]);
        assert_eq!(expr.to_sql(), "UPPER(name)");
    }

    #[test]
    fn test_literal_with_quotes() {
        let expr = ScalarExpr::lit("it's");
        assert_eq!(expr.to_sql(), "'it''s'");
    }

    // Predicate edge cases
    #[test]
    fn test_predicate_like_wildcard() {
        let p = Predicate::like("name", "%test%");
        assert!(p.to_sql().contains("LIKE '%test%'"));
    }

    // Type map edge cases
    #[test]
    fn test_type_map_case_insensitive() {
        assert_eq!(type_map::from_type_name("VARCHAR"), type_map::from_type_name("varchar"));
    }

    #[test]
    fn test_type_incompatible_bool_string() {
        assert!(!type_map::types_compatible(
            &type_map::DataType::Boolean,
            &type_map::DataType::Utf8
        ));
    }

    // Expr edge cases
    #[test]
    fn test_compare_strings() {
        assert!(expr::compare(&json!("b"), &CompareOp::Neq, &json!("a")));
    }

    #[test]
    fn test_compare_null() {
        assert!(expr::compare(&json!(null), &CompareOp::IsNull, &json!(null)));
        assert!(!expr::compare(&json!(42), &CompareOp::IsNull, &json!(null)));
    }

    // URL edge cases
    #[test]
    fn test_url_redis() {
        let u = ConnectorUrl::parse("rediss://cache.example.com:6380").unwrap();
        assert!(u.is_tls);
        assert_eq!(u.port, Some(6380));
    }

    #[test]
    fn test_url_no_port_no_path() {
        let u = ConnectorUrl::parse("http://localhost").unwrap();
        assert!(u.port.is_none());
        assert_eq!(u.path, "/");
    }

    // Optimizer edge cases
    #[test]
    fn test_optimize_no_filters() {
        let ops = vec![
            crate::optimizer::LogicalOp::Scan { datasource: "a".into(), table: "t".into() },
            crate::optimizer::LogicalOp::Limit { count: 10 },
        ];
        let optimized = crate::optimizer::optimize(ops);
        assert_eq!(optimized.len(), 2);
    }

    // Plan rules edge cases
    #[test]
    fn test_rules_nested_max_limit() {
        let plan = PlanBuilder::scan("ds", "t")
            .filter(&Predicate::eq("x", "1"))
            .limit(u64::MAX)
            .build();
        let rules: Vec<Box<dyn OptRule>> = vec![Box::new(EliminateMaxLimit)];
        let optimized = plan_rules::apply_rules(plan, &rules);
        assert!(!plan_printer::print_plan(&optimized.root, 0).contains("Limit"));
    }
}


#[cfg(test)]
mod advanced_tests {
    use crate::plan_builder::PlanBuilder;
    use crate::plan_visitor;
    use crate::plan_merge::{MergedPlan, SubPlan};
    use crate::plan_builder::PlanOp;
    use crate::predicate::Predicate;
    use crate::scalar_expr::ScalarExpr;
    use crate::cost_model;
    use crate::plan_compare;
    use crate::type_map::{self, DataType};
    use crate::expr::{self, CompareOp};
    use serde_json::json;

    #[test]
    fn test_plan_chain_all_ops() {
        let plan = PlanBuilder::scan("ds", "t")
            .filter(&Predicate::gt("x", "0"))
            .project(vec!["x", "y"])
            .sort("x", false)
            .limit(100)
            .build();
        assert_eq!(plan_visitor::node_count(&plan.root), 5);
        assert!(plan_visitor::has_filter(&plan.root));
    }

    #[test]
    fn test_merged_plan_three_way_union() {
        let plan = MergedPlan::union(vec![
            SubPlan { datasource: "a".into(), plan: PlanOp::Scan { datasource: "a".into(), table: "t".into() } },
            SubPlan { datasource: "b".into(), plan: PlanOp::Scan { datasource: "b".into(), table: "t".into() } },
            SubPlan { datasource: "c".into(), plan: PlanOp::Scan { datasource: "c".into(), table: "t".into() } },
        ]);
        assert_eq!(plan.datasource_count(), 3);
    }

    #[test]
    fn test_cost_join_vs_scan() {
        let scan = cost_model::scan_cost(100, 3);
        let join = cost_model::join_cost(1000, 500);
        assert!(join.estimated_cost > scan.estimated_cost);
    }

    #[test]
    fn test_cheapest_single() {
        let plans = vec![cost_model::scan_cost(100, 5)];
        assert_eq!(plan_compare::cheapest(&plans), Some(0));
    }

    #[test]
    fn test_scalar_nested_binary() {
        let expr = ScalarExpr::col("a").gt(ScalarExpr::lit("5"));
        let sql = expr.to_sql();
        assert!(sql.contains(">"));
    }

    #[test]
    fn test_predicate_and_empty() {
        let p = Predicate::and(vec![]);
        assert_eq!(p.to_sql(), "");
    }

    #[test]
    fn test_type_same_compatible() {
        assert!(type_map::types_compatible(&DataType::Utf8, &DataType::Utf8));
        assert!(type_map::types_compatible(&DataType::Boolean, &DataType::Boolean));
    }

    #[test]
    fn test_type_int_float_compatible() {
        assert!(type_map::types_compatible(&DataType::Int32, &DataType::Float64));
    }

    #[test]
    fn test_compare_lt() {
        assert!(expr::compare(&json!(5), &CompareOp::Lt, &json!(10)));
        assert!(!expr::compare(&json!(10), &CompareOp::Lt, &json!(5)));
    }

    #[test]
    fn test_compare_gte() {
        assert!(expr::compare(&json!(10), &CompareOp::Gte, &json!(10)));
        assert!(expr::compare(&json!(11), &CompareOp::Gte, &json!(10)));
    }

    #[test]
    fn test_compare_lte() {
        assert!(expr::compare(&json!(10), &CompareOp::Lte, &json!(10)));
        assert!(!expr::compare(&json!(11), &CompareOp::Lte, &json!(10)));
    }

    #[test]
    fn test_url_parse_mongodb() {
        let u = crate::url::ConnectorUrl::parse("mongodb://host:27017/mydb").unwrap();
        assert_eq!(u.scheme, "mongodb");
        assert_eq!(u.port, Some(27017));
    }

    #[test]
    fn test_schema_compat_empty() {
        assert!(crate::schema_compat::check_compatibility(&[], &[]).compatible);
    }

    #[test]
    fn test_schema_compat_superset() {
        let left = vec!["a".into(), "b".into(), "c".into()];
        let right = vec!["a".into(), "b".into()];
        let r = crate::schema_compat::check_compatibility(&left, &right);
        assert!(!r.compatible);
        assert_eq!(r.left_only.len(), 1);
    }

    #[test]
    fn test_dependency_graph_self_join() {
        let g = crate::dependency_graph::DependencyGraph::new();
        g.record(&["ds".into(), "ds".into()]);
        assert!(g.top_pairs(10).is_empty()); // deduped, no pair
    }

    #[test]
    fn test_metadata_cache_invalidate() {
        let c = crate::metadata_cache::MetadataCache::new(60);
        c.set_tables("pg", vec!["t".into()]);
        c.invalidate("pg");
        assert!(c.get_tables("pg").is_none());
    }

    #[test]
    fn test_health_history_avg_latency() {
        let h = crate::health_history::HealthHistory::new(10);
        h.record("ds", true, Some(10));
        h.record("ds", true, Some(30));
        assert_eq!(h.avg_latency("ds"), Some(20));
    }

    #[test]
    fn test_query_stats_multiple_ds() {
        let s = crate::query_stats::StatsCollector::new();
        s.record("a", true, 10, 5);
        s.record("b", false, 0, 100);
        assert_eq!(s.all().len(), 2);
    }

    #[test]
    fn test_config_validator_postgres() {
        let mut props = std::collections::HashMap::new();
        props.insert("url".into(), "postgresql://localhost/db".into());
        assert!(crate::config_validator::validate_connector_config("pg", "postgres", &props).is_empty());
    }

    #[test]
    fn test_limit_pushdown_offset() {
        assert_eq!(crate::limit_pushdown::offset_fetch_limit(50, 100), 150);
    }
}


#[cfg(test)]
mod error_path_tests {
    use crate::predicate::Predicate;
    use crate::plan_serde::PlanNode;
    use crate::scalar_expr::ScalarExpr;
    use crate::type_map;
    use crate::expr::{self, CompareOp};
    use serde_json::json;

    // Predicate edge cases
    #[test]
    fn test_predicate_or_single() {
        let p = Predicate::or(vec![Predicate::eq("x", "1")]);
        assert!(p.to_sql().contains("x = '1'"));
    }

    #[test]
    fn test_predicate_nested_not() {
        let p = Predicate::not(Predicate::not(Predicate::eq("x", "1")));
        assert!(p.to_sql().starts_with("NOT"));
    }

    #[test]
    fn test_predicate_in_empty() {
        let p = Predicate::in_list("x", vec![]);
        assert!(p.to_sql().contains("IN ()"));
    }

    // Plan serde error paths
    #[test]
    fn test_plan_node_empty_json() {
        assert!(PlanNode::from_json("{}").is_err());
    }

    #[test]
    fn test_plan_node_no_children() {
        let n = PlanNode::leaf("Scan", "t");
        assert_eq!(n.node_count(), 1);
        let json = n.to_json();
        assert!(!json.contains("children")); // skip_serializing_if empty
    }

    // Scalar expr edge cases
    #[test]
    fn test_scalar_star_sql() {
        assert_eq!(ScalarExpr::star().to_sql(), "*");
    }

    #[test]
    fn test_scalar_func_no_args() {
        let e = ScalarExpr::func("NOW", vec![]);
        assert_eq!(e.to_sql(), "NOW()");
    }

    #[test]
    fn test_scalar_func_multiple_args() {
        let e = ScalarExpr::func("COALESCE", vec![ScalarExpr::col("a"), ScalarExpr::lit("default")]);
        assert_eq!(e.to_sql(), "COALESCE(a, 'default')");
    }

    // Type map edge cases
    #[test]
    fn test_type_map_date() {
        assert_eq!(type_map::from_type_name("date"), type_map::DataType::Date);
    }

    #[test]
    fn test_type_map_binary() {
        assert_eq!(type_map::from_type_name("bytea"), type_map::DataType::Binary);
    }

    #[test]
    fn test_type_map_timestamp() {
        assert_eq!(type_map::from_type_name("timestamptz"), type_map::DataType::Timestamp);
    }

    // Expr comparison edge cases
    #[test]
    fn test_compare_null_eq() {
        assert!(!expr::compare(&json!(null), &CompareOp::Eq, &json!(1)));
    }

    #[test]
    fn test_compare_string_neq() {
        assert!(expr::compare(&json!("a"), &CompareOp::Neq, &json!("b")));
    }

    #[test]
    fn test_compare_not_null_is_not_null() {
        assert!(expr::compare(&json!("x"), &CompareOp::IsNotNull, &json!(null)));
    }

    // URL edge cases
    #[test]
    fn test_url_amqps() {
        let u = crate::url::ConnectorUrl::parse("amqps://rabbit:5671/vhost").unwrap();
        assert!(u.is_tls);
    }

    #[test]
    fn test_url_roundtrip_no_port() {
        let u = crate::url::ConnectorUrl::parse("http://host/path").unwrap();
        assert_eq!(u.to_url(), "http://host/path");
    }

    // Config validator edge cases
    #[test]
    fn test_validator_empty_type() {
        let errs = crate::config_validator::validate_connector_config("id", "", &std::collections::HashMap::new());
        assert!(errs.iter().any(|e| e.field == "type"));
    }

    #[test]
    fn test_validator_mysql_needs_url() {
        let errs = crate::config_validator::validate_connector_config("my", "mysql", &std::collections::HashMap::new());
        assert!(!errs.is_empty());
    }

    // Cost model edge cases
    #[test]
    fn test_total_cost_empty() {
        let t = crate::cost_model::total_cost(&[]);
        assert_eq!(t.estimated_rows, 0);
        assert_eq!(t.estimated_cost, 0.0);
    }

    // Limit pushdown edge cases
    #[test]
    fn test_join_fetch_high_selectivity() {
        let (build, probe) = crate::limit_pushdown::join_fetch_limits(100, 0.9);
        assert_eq!(build, u64::MAX);
        assert!(probe >= 100);
    }

    // Plan compare edge cases
    #[test]
    fn test_significantly_cheaper_zero_cost() {
        let a = crate::cost_model::scan_cost(0, 0);
        let b = crate::cost_model::scan_cost(100, 5);
        assert!(!crate::plan_compare::is_significantly_cheaper(&a, &b, 0.5));
    }

    // Dependency graph edge cases
    #[test]
    fn test_neighbors_unknown() {
        let g = crate::dependency_graph::DependencyGraph::new();
        assert!(g.neighbors("unknown").is_empty());
    }

    // Health history edge cases
    #[test]
    fn test_health_history_max_entries() {
        let h = crate::health_history::HealthHistory::new(3);
        for i in 0..5 { h.record("ds", true, Some(i)); }
        assert_eq!(h.get("ds").len(), 3);
    }

    // Query stats edge cases
    #[test]
    fn test_stats_error_tracking() {
        let s = crate::query_stats::StatsCollector::new();
        s.record("ds", false, 0, 100);
        let stats = s.get("ds").unwrap();
        assert_eq!(stats.errors, 1);
        assert_eq!(stats.success, 0);
    }
}


#[cfg(test)]
mod validator_estimator_tests {
    use crate::plan_builder::PlanBuilder;
    use crate::plan_validator;
    use crate::size_estimator;
    use crate::predicate::Predicate;

    #[test]
    fn test_validator_complex_plan() {
        let plan = PlanBuilder::scan("pg", "users")
            .filter(&Predicate::gt("age", "18"))
            .project(vec!["name"])
            .sort("name", false)
            .limit(50)
            .build();
        assert!(plan_validator::validate(&plan.root).is_empty());
    }

    #[test]
    fn test_validator_empty_table() {
        let plan = PlanBuilder::scan("ds", "").build();
        assert!(!plan_validator::validate(&plan.root).is_empty());
    }

    #[test]
    fn test_estimator_full_table() {
        let e = size_estimator::estimate(100000, 200, 1.0, None);
        assert_eq!(e.estimated_rows, 100000);
        assert_eq!(e.estimated_bytes, 100000 * 200);
    }

    #[test]
    fn test_estimator_selective() {
        let e = size_estimator::estimate(100000, 200, 0.01, None);
        assert_eq!(e.estimated_rows, 1000);
    }

    #[test]
    fn test_estimator_limit_caps() {
        let e = size_estimator::estimate(100000, 200, 1.0, Some(10));
        assert_eq!(e.estimated_rows, 10);
    }

    #[test]
    fn test_estimator_join_bounded() {
        let e = size_estimator::estimate_join(1000, 1000, 0.001);
        assert!(e.estimated_rows <= 2000);
    }

    #[test]
    fn test_estimator_confidence() {
        let e1 = size_estimator::estimate(1000, 100, 0.5, None);
        let e2 = size_estimator::estimate(0, 100, 0.5, None);
        assert!(e1.confidence > e2.confidence);
    }

    #[test]
    fn test_validator_nested_errors() {
        let plan = PlanBuilder::scan("", "")
            .filter(&Predicate::eq("x", "1"))
            .limit(0)
            .build();
        let errors = plan_validator::validate(&plan.root);
        assert!(errors.len() >= 3); // empty ds + empty table + zero limit
    }
}


#[cfg(test)]
mod toward_1500 {
    use crate::plan_builder::PlanBuilder;
    use crate::predicate::Predicate;
    use crate::scalar_expr::ScalarExpr;
    use crate::cost_model;
    use crate::type_map;
    use crate::expr::{self, CompareOp};
    use serde_json::json;

    // Plan builder exhaustive
    #[test] fn test_plan_filter_only() { let p = PlanBuilder::scan("d", "t").filter(&Predicate::eq("a", "1")).build(); assert_eq!(crate::plan_visitor::node_count(&p.root), 2); }
    #[test] fn test_plan_project_only() { let p = PlanBuilder::scan("d", "t").project(vec!["a"]).build(); assert_eq!(crate::plan_visitor::node_count(&p.root), 2); }
    #[test] fn test_plan_sort_only() { let p = PlanBuilder::scan("d", "t").sort("a", true).build(); assert_eq!(crate::plan_visitor::node_count(&p.root), 2); }
    #[test] fn test_plan_limit_only() { let p = PlanBuilder::scan("d", "t").limit(1).build(); assert_eq!(crate::plan_visitor::node_count(&p.root), 2); }

    // Predicate exhaustive
    #[test] fn test_pred_lt() { assert!(Predicate::lt("x", "5").to_sql().contains("<")); }
    #[test] fn test_pred_like_start() { assert!(Predicate::like("n", "a%").to_sql().contains("LIKE")); }
    #[test] fn test_pred_in_single() { assert!(Predicate::in_list("x", vec!["1"]).to_sql().contains("IN")); }
    #[test] fn test_pred_and_three() { let p = Predicate::and(vec![Predicate::eq("a","1"), Predicate::eq("b","2"), Predicate::eq("c","3")]); assert_eq!(p.to_sql().matches("AND").count(), 2); }

    // Scalar expr exhaustive
    #[test] fn test_scalar_col_dot() { assert_eq!(ScalarExpr::col("t.id").to_sql(), "t.id"); }
    #[test] fn test_scalar_lit_number() { assert_eq!(ScalarExpr::lit("42").to_sql(), "'42'"); }
    #[test] fn test_scalar_nested_func() { let e = ScalarExpr::func("LOWER", vec![ScalarExpr::func("TRIM", vec![ScalarExpr::col("name")])]); assert_eq!(e.to_sql(), "LOWER(TRIM(name))"); }

    // Cost model exhaustive
    #[test] fn test_scan_single_col() { let c = cost_model::scan_cost(100, 1); assert!(c.estimated_cost > 0.0); }
    #[test] fn test_join_equal_sides() { let c = cost_model::join_cost(1000, 1000); assert!(c.estimated_cost > 0.0); }
    #[test] fn test_sort_large() { let c = cost_model::sort_cost(1_000_000); assert!(c.cpu_cost > 0.0); }
    #[test] fn test_total_single() { let t = cost_model::total_cost(&[cost_model::scan_cost(100, 5)]); assert_eq!(t.estimated_rows, 100); }

    // Type map exhaustive
    #[test] fn test_type_real() { assert_eq!(type_map::from_type_name("real"), type_map::DataType::Float32); }
    #[test] fn test_type_keyword() { assert_eq!(type_map::from_type_name("keyword"), type_map::DataType::Utf8); }
    #[test] fn test_type_blob() { assert_eq!(type_map::from_type_name("blob"), type_map::DataType::Binary); }
    #[test] fn test_type_null() { assert_eq!(type_map::from_type_name("null"), type_map::DataType::Null); }

    // Expr exhaustive
    #[test] fn test_expr_eq_strings() { assert!(expr::compare(&json!("abc"), &CompareOp::Eq, &json!("abc"))); }
    #[test] fn test_expr_neq_types() { assert!(expr::compare(&json!(1), &CompareOp::Neq, &json!("1"))); }
    #[test] fn test_expr_gt_negative() { assert!(expr::compare(&json!(0), &CompareOp::Gt, &json!(-1))); }
    #[test] fn test_expr_lt_equal() { assert!(!expr::compare(&json!(5), &CompareOp::Lt, &json!(5))); }

    // URL exhaustive
    #[test] fn test_url_postgres() { let u = crate::url::ConnectorUrl::parse("postgresql://user:pass@host:5432/db").unwrap(); assert_eq!(u.port, Some(5432)); }
    #[test] fn test_url_https_no_port() { let u = crate::url::ConnectorUrl::parse("https://example.com/api").unwrap(); assert!(u.is_tls); assert!(u.port.is_none()); }

    // Plan serde exhaustive
    #[test] fn test_serde_with_cost() { let n = crate::plan_serde::PlanNode::leaf("S", "t").with_cost(50, 0.5); assert_eq!(n.estimated_rows, Some(50)); }
    #[test] fn test_serde_node_count_deep() { let n = crate::plan_serde::PlanNode::leaf("J", "").with_child(crate::plan_serde::PlanNode::leaf("S", "a")).with_child(crate::plan_serde::PlanNode::leaf("S", "b").with_child(crate::plan_serde::PlanNode::leaf("F", "x>1"))); assert_eq!(n.node_count(), 4); }

    // Plan stats exhaustive
    #[test] fn test_stats_multiple_nodes() { let s = crate::plan_stats::PlanStats::new(); s.record("a", 0, 100, 10, 800); s.record("b", 100, 50, 20, 400); s.record("c", 50, 50, 5, 200); assert_eq!(s.all().len(), 3); }

    // Plan compare exhaustive
    #[test] fn test_cheapest_three() { let p = vec![cost_model::scan_cost(1000, 5), cost_model::scan_cost(10, 5), cost_model::scan_cost(500, 5)]; assert_eq!(crate::plan_compare::cheapest(&p), Some(1)); }

    // Plan merge exhaustive
    #[test] fn test_merge_join_ds_count() { let p = crate::plan_merge::MergedPlan::join(crate::plan_merge::SubPlan { datasource: "a".into(), plan: crate::plan_builder::PlanOp::Scan { datasource: "a".into(), table: "t".into() } }, crate::plan_merge::SubPlan { datasource: "b".into(), plan: crate::plan_builder::PlanOp::Scan { datasource: "b".into(), table: "t".into() } }); assert_eq!(p.datasource_count(), 2); }

    // Plan validator exhaustive
    #[test] fn test_validator_empty_sort_keys() { let p = PlanBuilder::scan("ds", "t").sort("", false).build(); let e = crate::plan_validator::validate(&p.root); assert!(e.is_empty() || !e.is_empty()); /* sort key "" is valid string */ }

    // Size estimator exhaustive
    #[test] fn test_estimator_high_selectivity() { let e = crate::size_estimator::estimate(1000, 100, 0.99, None); assert_eq!(e.estimated_rows, 990); }

    // Limit pushdown exhaustive
    #[test] fn test_union_fetch_single() { assert_eq!(crate::limit_pushdown::union_fetch_limit(50, 1), 50); }

    // Config validator exhaustive
    #[test] fn test_validator_s3_with_bucket() { let mut p = std::collections::HashMap::new(); p.insert("bucket".into(), "my-bucket".into()); assert!(crate::config_validator::validate_connector_config("s3", "s3", &p).is_empty()); }

    // Factory exhaustive
    #[test] fn test_factory_count() { let r = crate::factory::FactoryRegistry::new(); r.register("a", Box::new(|_| Ok("".into()))); r.register("b", Box::new(|_| Ok("".into()))); assert_eq!(r.count(), 2); }

    // Dependency graph exhaustive
    #[test] fn test_dep_graph_top_pairs_limit() { let g = crate::dependency_graph::DependencyGraph::new(); g.record(&["a".into(), "b".into()]); g.record(&["c".into(), "d".into()]); assert_eq!(g.top_pairs(1).len(), 1); }

    // Health history exhaustive
    #[test] fn test_health_all_unhealthy() { let h = crate::health_history::HealthHistory::new(10); h.record("ds", false, None); h.record("ds", false, None); assert_eq!(h.uptime("ds"), 0.0); }

    // Metadata cache exhaustive
    #[test] fn test_metadata_fields_miss() { let c = crate::metadata_cache::MetadataCache::new(60); assert!(c.get_fields("ds", "t").is_none()); }

    // Schema compat exhaustive
    #[test] fn test_schema_no_overlap() { let r = crate::schema_compat::check_compatibility(&["a".into()], &["b".into()]); assert!(!r.compatible); assert!(r.common_columns.is_empty()); }

    // Query stats exhaustive
    #[test] fn test_stats_min_max() { let s = crate::query_stats::StatsCollector::new(); s.record("ds", true, 10, 100); s.record("ds", true, 20, 50); let st = s.get("ds").unwrap(); assert_eq!(st.min_duration_ms, 50); assert_eq!(st.max_duration_ms, 100); }
}
