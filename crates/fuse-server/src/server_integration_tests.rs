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
