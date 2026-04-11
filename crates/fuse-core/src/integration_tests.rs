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
