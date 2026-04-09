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
