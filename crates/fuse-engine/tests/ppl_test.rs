// SPDX-License-Identifier: Apache-2.0

//! Integration tests for PPL parsing and PPL-to-SQL translation.

use fuse_engine::ppl::{is_ppl, parse_ppl, ppl_to_sql, PplCommand};

#[test]
fn test_ppl_complex_pipeline() {
    // source = ds.logs | where status >= 500 | stats count() by service | sort - count() | head 10
    let q = parse_ppl(
        "source = ds.logs | where status >= 500 | stats count() by service | sort - count() | head 10",
    )
    .unwrap();

    assert_eq!(q.sources.len(), 1);
    assert_eq!(q.sources[0].datasource.as_deref(), Some("ds"));
    assert_eq!(q.sources[0].table, "logs");
    assert_eq!(q.commands.len(), 4);
    assert!(matches!(&q.commands[0], PplCommand::Where(w) if w == "status >= 500"));
    assert!(matches!(&q.commands[1], PplCommand::Stats { aggs, by } if aggs.len() == 1 && by == &["service"]));
    assert!(matches!(&q.commands[2], PplCommand::Sort(fields) if fields.len() == 1 && fields[0].descending));
    assert!(matches!(&q.commands[3], PplCommand::Head(10)));
}

#[test]
fn test_ppl_to_sql_complex_pipeline() {
    let q = parse_ppl(
        "source = ds.logs | where status >= 500 | stats count() by service | sort - count() | head 10",
    )
    .unwrap();
    let sql = ppl_to_sql(&q).unwrap();

    assert!(sql.contains("SELECT service, count(*)"));
    assert!(sql.contains("FROM ds.logs"));
    assert!(sql.contains("WHERE status >= 500"));
    assert!(sql.contains("GROUP BY service"));
    assert!(sql.contains("ORDER BY count() DESC"));
    assert!(sql.contains("LIMIT 10"));
}

#[test]
fn test_ppl_to_sql_no_filter() {
    let q = parse_ppl("source = prod.metrics | head 5").unwrap();
    let sql = ppl_to_sql(&q).unwrap();
    assert_eq!(sql, "SELECT * FROM prod.metrics LIMIT 5");
}

#[test]
fn test_ppl_to_sql_single_source_no_commands() {
    let q = parse_ppl("source = cluster.logs").unwrap();
    let sql = ppl_to_sql(&q).unwrap();
    assert_eq!(sql, "SELECT * FROM cluster.logs");
}

#[test]
fn test_ppl_to_sql_fields_projection() {
    let q = parse_ppl("source = ds.logs | fields host, status, message").unwrap();
    let sql = ppl_to_sql(&q).unwrap();
    assert!(sql.contains("SELECT host, status, message"));
    assert!(sql.contains("FROM ds.logs"));
}

#[test]
fn test_ppl_to_sql_multi_source_with_stats() {
    let q = parse_ppl(
        "source = a.logs, b.logs | where level = 'ERROR' | stats count() by service",
    )
    .unwrap();
    let sql = ppl_to_sql(&q).unwrap();
    assert!(sql.contains("UNION ALL"));
    assert!(sql.contains("FROM a.logs"));
    assert!(sql.contains("FROM b.logs"));
    assert!(sql.contains("GROUP BY service"));
}

#[test]
fn test_ppl_is_not_sql() {
    assert!(!is_ppl("SELECT * FROM logs"));
    assert!(!is_ppl("INSERT INTO logs VALUES (1)"));
}

#[test]
fn test_ppl_parse_error_no_source() {
    assert!(parse_ppl("SELECT * FROM logs").is_err());
}

#[test]
fn test_ppl_parse_error_empty_dedup() {
    assert!(parse_ppl("source = logs | dedup").is_err());
}

#[test]
fn test_ppl_parse_error_unknown_command() {
    assert!(parse_ppl("source = logs | foobar x").is_err());
}

#[test]
fn test_ppl_head_default() {
    let q = parse_ppl("source = logs | head").unwrap();
    assert!(matches!(&q.commands[0], PplCommand::Head(10)));
}

#[test]
fn test_ppl_dedup_single_field() {
    let q = parse_ppl("source = logs | dedup trace_id").unwrap();
    if let PplCommand::Dedup(fields) = &q.commands[0] {
        assert_eq!(fields, &["trace_id"]);
    } else {
        panic!("Expected Dedup");
    }
}

// ── PPL eval command ──

#[test]
fn test_ppl_eval_single_expr() {
    let q = parse_ppl("source = ds.logs | eval duration_s = duration_ms / 1000").unwrap();
    if let PplCommand::Eval(exprs) = &q.commands[0] {
        assert_eq!(exprs.len(), 1);
        assert_eq!(exprs[0].alias, "duration_s");
    } else {
        panic!("Expected Eval");
    }
}

#[test]
fn test_ppl_eval_multiple_exprs() {
    let q = parse_ppl("source = ds.logs | eval a = x + 1, b = y * 2").unwrap();
    if let PplCommand::Eval(exprs) = &q.commands[0] {
        assert_eq!(exprs.len(), 2);
        assert_eq!(exprs[0].alias, "a");
        assert_eq!(exprs[1].alias, "b");
    } else {
        panic!("Expected Eval");
    }
}

// ── PPL rename command ──

#[test]
fn test_ppl_rename_single() {
    let q = parse_ppl("source = ds.logs | rename timestamp as ts").unwrap();
    if let PplCommand::Rename(renames) = &q.commands[0] {
        assert_eq!(renames.len(), 1);
        assert_eq!(renames[0].old_name, "timestamp");
        assert_eq!(renames[0].new_name, "ts");
    } else {
        panic!("Expected Rename");
    }
}

#[test]
fn test_ppl_rename_multiple() {
    let q = parse_ppl("source = ds.logs | rename timestamp as ts, message as msg").unwrap();
    if let PplCommand::Rename(renames) = &q.commands[0] {
        assert_eq!(renames.len(), 2);
    } else {
        panic!("Expected Rename");
    }
}

// ── PPL top/rare commands ──

#[test]
fn test_ppl_top() {
    let q = parse_ppl("source = ds.logs | top 5 service").unwrap();
    if let PplCommand::Top { n, fields } = &q.commands[0] {
        assert_eq!(*n, 5);
        assert_eq!(fields, &["service"]);
    } else {
        panic!("Expected Top");
    }
}

#[test]
fn test_ppl_rare() {
    let q = parse_ppl("source = ds.logs | rare status").unwrap();
    if let PplCommand::Rare { fields } = &q.commands[0] {
        assert_eq!(fields, &["status"]);
    } else {
        panic!("Expected Rare");
    }
}

// ── PPL edge cases ──

#[test]
fn test_ppl_fields_exclude() {
    let q = parse_ppl("source = ds.logs | fields - password, secret").unwrap();
    if let PplCommand::Fields { include, names } = &q.commands[0] {
        assert!(!include);
        assert_eq!(names, &["password", "secret"]);
    } else {
        panic!("Expected Fields");
    }
}

#[test]
fn test_ppl_chained_pipeline() {
    let q = parse_ppl(
        "source = ds.logs | where status >= 400 | eval is_error = status >= 500 | dedup trace_id | fields service, status, is_error | head 20"
    ).unwrap();
    assert_eq!(q.commands.len(), 5);
    assert!(matches!(&q.commands[0], PplCommand::Where(_)));
    assert!(matches!(&q.commands[1], PplCommand::Eval(_)));
    assert!(matches!(&q.commands[2], PplCommand::Dedup(_)));
    assert!(matches!(&q.commands[3], PplCommand::Fields { include: true, .. }));
    assert!(matches!(&q.commands[4], PplCommand::Head(20)));
}

#[test]
fn test_ppl_sort_ascending() {
    let q = parse_ppl("source = ds.logs | sort + timestamp").unwrap();
    if let PplCommand::Sort(fields) = &q.commands[0] {
        assert_eq!(fields.len(), 1);
        assert!(!fields[0].descending);
    } else {
        panic!("Expected Sort");
    }
}

#[test]
fn test_ppl_sort_multiple_fields() {
    let q = parse_ppl("source = ds.logs | sort - status, + timestamp").unwrap();
    if let PplCommand::Sort(fields) = &q.commands[0] {
        assert_eq!(fields.len(), 2);
        assert!(fields[0].descending);
        assert!(!fields[1].descending);
    } else {
        panic!("Expected Sort");
    }
}

#[test]
fn test_ppl_dedup_multiple_fields() {
    let q = parse_ppl("source = ds.logs | dedup service, host").unwrap();
    if let PplCommand::Dedup(fields) = &q.commands[0] {
        assert_eq!(fields, &["service", "host"]);
    } else {
        panic!("Expected Dedup");
    }
}

#[test]
fn test_ppl_stats_multiple_aggs() {
    let q = parse_ppl("source = ds.logs | stats count(), avg(latency), max(latency) by service").unwrap();
    if let PplCommand::Stats { aggs, by } = &q.commands[0] {
        assert_eq!(aggs.len(), 3);
        assert_eq!(by, &["service"]);
    } else {
        panic!("Expected Stats");
    }
}

#[test]
fn test_ppl_is_ppl_positive() {
    assert!(is_ppl("source = ds.logs"));
    assert!(is_ppl("source = ds.logs | head 10"));
    assert!(is_ppl("  source = ds.logs  "));
}
