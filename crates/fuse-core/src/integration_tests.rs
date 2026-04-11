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
