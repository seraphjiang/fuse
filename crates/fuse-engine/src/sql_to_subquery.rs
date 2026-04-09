// SPDX-License-Identifier: Apache-2.0

//! Translates a SQL string (produced by DataFusion's unparser) into a
//! [`SubQuery`] that can be sent to a [`FederatedConnector`].

use datafusion::sql::sqlparser::ast::{
    self, Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    Ident, LimitClause, ObjectName, OrderBy, OrderByKind, SelectItem, SetExpr,
    Statement, TableFactor, Value,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use fuse_core::connector::{
    AggFunction, AggregationExpr, ComparisonOp, FilterExpr, ScalarValue, SortExpr, SubQuery,
};
use fuse_core::error::ConnectorError;

/// Parse a SQL string into a SubQuery.
pub fn sql_to_subquery(sql: &str) -> Result<SubQuery, ConnectorError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| ConnectorError::QueryFailed(format!("SQL parse error: {e}")))?;

    let stmt = statements
        .into_iter()
        .next()
        .ok_or_else(|| ConnectorError::QueryFailed("empty SQL".into()))?;

    let query = match stmt {
        Statement::Query(q) => *q,
        _ => return Err(ConnectorError::QueryFailed("expected SELECT query".into())),
    };

    let select = match *query.body {
        SetExpr::Select(s) => *s,
        _ => return Err(ConnectorError::QueryFailed("expected simple SELECT".into())),
    };

    // Check for subquery in FROM clause and merge inner filters
    let inner_filter = extract_inner_filter(&select.from);

    let table = extract_table_name(&select.from)?;
    let (projections, aggregations) = extract_projections_and_aggs(&select.projection);
    let outer_filter = select.selection.as_ref().and_then(|e| translate_expr(e));

    // Merge inner and outer filters with AND
    let filter = match (outer_filter, inner_filter) {
        (Some(o), Some(i)) => Some(FilterExpr::And(Box::new(o), Box::new(i))),
        (Some(f), None) | (None, Some(f)) => Some(f),
        (None, None) => None,
    };

    let group_by = extract_group_by(&select.group_by);
    let having = select.having.as_ref().and_then(|e| translate_expr(e));
    let sort = query
        .order_by
        .as_ref()
        .map(extract_order_by)
        .unwrap_or_default();
    let limit = query
        .limit_clause
        .as_ref()
        .and_then(extract_limit);

    Ok(SubQuery {
        table,
        projections,
        filter,
        aggregations,
        group_by,
        having,
        sort,
        limit,
        passthrough: None,
    })
}

/// Extract filter from inner subquery in FROM clause (if Derived).
fn extract_inner_filter(from: &[ast::TableWithJoins]) -> Option<FilterExpr> {
    let twj = from.first()?;
    match &twj.relation {
        TableFactor::Derived { subquery, .. } => {
            let inner = match *subquery.body.clone() {
                SetExpr::Select(s) => *s,
                _ => return None,
            };
            inner.selection.as_ref().and_then(|e| translate_expr(e))
        }
        _ => None,
    }
}

fn extract_table_name(from: &[ast::TableWithJoins]) -> Result<String, ConnectorError> {
    let twj = from
        .first()
        .ok_or_else(|| ConnectorError::QueryFailed("no FROM clause".into()))?;

    match &twj.relation {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
        TableFactor::Derived { subquery, .. } => {
            // Extract table name from inner subquery
            let inner = match *subquery.body.clone() {
                SetExpr::Select(s) => *s,
                _ => return Err(ConnectorError::QueryFailed("unsupported subquery body".into())),
            };
            extract_table_name(&inner.from)
        }
        _ => Err(ConnectorError::QueryFailed("unsupported FROM clause".into())),
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|id| id.value.clone())
        .unwrap_or_default()
}

fn ident_to_string(id: &Ident) -> String {
    id.value.clone()
}

fn extract_projections_and_aggs(items: &[SelectItem]) -> (Vec<String>, Vec<AggregationExpr>) {
    let mut projections = Vec::new();
    let mut aggregations = Vec::new();

    for item in items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                if let Some(agg) = try_extract_agg(expr, None) {
                    aggregations.push(agg);
                } else if let Some(col) = expr_to_column_name(expr) {
                    projections.push(col);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let alias_str = ident_to_string(alias);
                if let Some(agg) = try_extract_agg(expr, Some(alias_str)) {
                    aggregations.push(agg);
                } else if let Some(col) = expr_to_column_name(expr) {
                    projections.push(col);
                }
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
        }
    }

    (projections, aggregations)
}

fn try_extract_agg(expr: &SqlExpr, alias: Option<String>) -> Option<AggregationExpr> {
    let f = match expr {
        SqlExpr::Function(f) => f,
        _ => return None,
    };

    let func_name = f.name.to_string().to_lowercase();
    let is_distinct = matches!(
        &f.args,
        FunctionArguments::List(arg_list) if arg_list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct)
    );
    let agg_fn = match (func_name.as_str(), is_distinct) {
        ("count", true) => AggFunction::CountDistinct,
        ("count", false) => AggFunction::Count,
        ("sum", _) => AggFunction::Sum,
        ("avg", _) => AggFunction::Avg,
        ("min", _) => AggFunction::Min,
        ("max", _) => AggFunction::Max,
        _ => return None,
    };

    let field = match &f.args {
        FunctionArguments::List(arg_list) => arg_list.args.first().and_then(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => expr_to_column_name(e),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
            _ => None,
        }),
        _ => None,
    };

    let alias = alias.unwrap_or_else(|| match &field {
        Some(f) => format!("{}_{}", func_name, f),
        None => func_name.clone(),
    });

    Some(AggregationExpr {
        function: agg_fn,
        field,
        alias,
    })
}

fn expr_to_column_name(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(id) => Some(ident_to_string(id)),
        SqlExpr::CompoundIdentifier(ids) => ids.last().map(ident_to_string),
        SqlExpr::Function(f) => Some(f.to_string()),
        _ => None,
    }
}

fn translate_expr(expr: &SqlExpr) -> Option<FilterExpr> {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => translate_binary_op(left, op, right),
        SqlExpr::UnaryOp {
            op: ast::UnaryOperator::Not,
            expr: inner,
        } => {
            let inner = translate_expr(inner)?;
            Some(FilterExpr::Not(Box::new(inner)))
        }
        SqlExpr::IsNull(inner) => {
            let col = expr_to_column_name(inner)?;
            Some(FilterExpr::IsNull(col))
        }
        SqlExpr::IsNotNull(inner) => {
            let col = expr_to_column_name(inner)?;
            Some(FilterExpr::IsNotNull(col))
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let col = expr_to_column_name(expr)?;
            let values: Vec<ScalarValue> = list.iter().filter_map(sql_expr_to_scalar).collect();
            let in_expr = FilterExpr::In { field: col, values };
            if *negated {
                Some(FilterExpr::Not(Box::new(in_expr)))
            } else {
                Some(in_expr)
            }
        }
        SqlExpr::Like {
            expr,
            pattern,
            negated,
            ..
        } => {
            let field = expr_to_column_name(expr)?;
            let value = sql_expr_to_scalar(pattern)?;
            let like_expr = FilterExpr::Comparison {
                field,
                op: ComparisonOp::Like,
                value,
            };
            if *negated {
                Some(FilterExpr::Not(Box::new(like_expr)))
            } else {
                Some(like_expr)
            }
        }
        SqlExpr::ILike {
            expr,
            pattern,
            negated,
            ..
        } => {
            let field = expr_to_column_name(expr)?;
            let value = sql_expr_to_scalar(pattern)?;
            let ilike_expr = FilterExpr::Comparison {
                field,
                op: ComparisonOp::ILike,
                value,
            };
            if *negated {
                Some(FilterExpr::Not(Box::new(ilike_expr)))
            } else {
                Some(ilike_expr)
            }
        }
        SqlExpr::Nested(inner) => translate_expr(inner),
        SqlExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let field = expr_to_column_name(expr)?;
            let low_val = sql_expr_to_scalar(low)?;
            let high_val = sql_expr_to_scalar(high)?;
            let between = FilterExpr::And(
                Box::new(FilterExpr::Comparison {
                    field: field.clone(),
                    op: ComparisonOp::Gte,
                    value: low_val,
                }),
                Box::new(FilterExpr::Comparison {
                    field,
                    op: ComparisonOp::Lte,
                    value: high_val,
                }),
            );
            if *negated {
                Some(FilterExpr::Not(Box::new(between)))
            } else {
                Some(between)
            }
        }
        _ => None,
    }
}

fn translate_binary_op(
    left: &SqlExpr,
    op: &ast::BinaryOperator,
    right: &SqlExpr,
) -> Option<FilterExpr> {
    match op {
        ast::BinaryOperator::And => {
            let l = translate_expr(left);
            let r = translate_expr(right);
            match (l, r) {
                (Some(l), Some(r)) => Some(FilterExpr::And(Box::new(l), Box::new(r))),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        ast::BinaryOperator::Or => {
            let l = translate_expr(left);
            let r = translate_expr(right);
            match (l, r) {
                (Some(l), Some(r)) => Some(FilterExpr::Or(Box::new(l), Box::new(r))),
                // If either side of OR is untranslatable, we can't push down
                // the filter — dropping one side would widen results incorrectly
                _ => None,
            }
        }
        _ => {
            let comp_op = match op {
                ast::BinaryOperator::Eq => ComparisonOp::Eq,
                ast::BinaryOperator::NotEq => ComparisonOp::Neq,
                ast::BinaryOperator::Lt => ComparisonOp::Lt,
                ast::BinaryOperator::LtEq => ComparisonOp::Lte,
                ast::BinaryOperator::Gt => ComparisonOp::Gt,
                ast::BinaryOperator::GtEq => ComparisonOp::Gte,
                _ => return None,
            };

            if let (Some(field), Some(value)) =
                (expr_to_column_name(left), sql_expr_to_scalar(right))
            {
                return Some(FilterExpr::Comparison {
                    field,
                    op: comp_op,
                    value,
                });
            }
            if let (Some(value), Some(field)) =
                (sql_expr_to_scalar(left), expr_to_column_name(right))
            {
                let reversed_op = match comp_op {
                    ComparisonOp::Lt => ComparisonOp::Gt,
                    ComparisonOp::Lte => ComparisonOp::Gte,
                    ComparisonOp::Gt => ComparisonOp::Lt,
                    ComparisonOp::Gte => ComparisonOp::Lte,
                    other => other,
                };
                return Some(FilterExpr::Comparison {
                    field,
                    op: reversed_op,
                    value,
                });
            }
            None
        }
    }
}

fn sql_expr_to_scalar(expr: &SqlExpr) -> Option<ScalarValue> {
    match expr {
        SqlExpr::Value(vws) => value_to_scalar(&vws.value),
        SqlExpr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr: inner,
        } => match sql_expr_to_scalar(inner)? {
            ScalarValue::Int64(n) => Some(ScalarValue::Int64(-n)),
            ScalarValue::Float64(f) => Some(ScalarValue::Float64(-f)),
            _ => None,
        },
        _ => None,
    }
}

fn value_to_scalar(v: &Value) -> Option<ScalarValue> {
    match v {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Some(ScalarValue::Int64(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Some(ScalarValue::Float64(f))
            } else {
                None
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            Some(ScalarValue::Utf8(s.clone()))
        }
        Value::Boolean(b) => Some(ScalarValue::Boolean(*b)),
        Value::Null => Some(ScalarValue::Null),
        _ => None,
    }
}

fn extract_group_by(group_by: &GroupByExpr) -> Vec<String> {
    match group_by {
        GroupByExpr::Expressions(exprs, _) => {
            exprs.iter().filter_map(|e| expr_to_column_name(e)).collect()
        }
        GroupByExpr::All(_) => vec![],
    }
}

fn extract_order_by(order_by: &OrderBy) -> Vec<SortExpr> {
    match &order_by.kind {
        OrderByKind::Expressions(exprs) => exprs
            .iter()
            .filter_map(|o| {
                let field = expr_to_column_name(&o.expr)?;
                let descending = o.options.asc.map(|asc| !asc).unwrap_or(false);
                Some(SortExpr { field, descending })
            })
            .collect(),
        OrderByKind::All(_) => vec![],
    }
}

fn extract_limit(clause: &LimitClause) -> Option<u64> {
    match clause {
        LimitClause::LimitOffset { limit, .. } => {
            limit.as_ref().and_then(|e| expr_to_u64(e))
        }
        LimitClause::OffsetCommaLimit { limit, .. } => expr_to_u64(limit),
    }
}

fn expr_to_u64(expr: &SqlExpr) -> Option<u64> {
    match expr {
        SqlExpr::Value(vws) => match &vws.value {
            Value::Number(n, _) => n.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let sq = sql_to_subquery("SELECT * FROM logs").unwrap();
        assert_eq!(sq.table, "logs");
        assert!(sq.projections.is_empty());
        assert!(sq.filter.is_none());
        assert!(sq.limit.is_none());
    }

    #[test]
    fn test_select_columns() {
        let sq = sql_to_subquery("SELECT service, status FROM logs").unwrap();
        assert_eq!(sq.projections, vec!["service", "status"]);
    }

    #[test]
    fn test_where_string_eq() {
        let sq = sql_to_subquery("SELECT * FROM logs WHERE service = 'api'").unwrap();
        let f = sq.filter.unwrap();
        match f {
            FilterExpr::Comparison { field, op, value } => {
                assert_eq!(field, "service");
                assert!(matches!(op, ComparisonOp::Eq));
                assert!(matches!(value, ScalarValue::Utf8(s) if s == "api"));
            }
            _ => panic!("expected Comparison"),
        }
    }

    #[test]
    fn test_where_int_gte() {
        let sq = sql_to_subquery("SELECT * FROM logs WHERE status >= 500").unwrap();
        let f = sq.filter.unwrap();
        match f {
            FilterExpr::Comparison { op, value, .. } => {
                assert!(matches!(op, ComparisonOp::Gte));
                assert!(matches!(value, ScalarValue::Int64(500)));
            }
            _ => panic!("expected Comparison"),
        }
    }

    #[test]
    fn test_and_filter() {
        let sq = sql_to_subquery("SELECT * FROM logs WHERE status >= 500 AND service = 'x'").unwrap();
        assert!(matches!(sq.filter.unwrap(), FilterExpr::And(_, _)));
    }

    #[test]
    fn test_or_filter() {
        let sq = sql_to_subquery("SELECT * FROM logs WHERE status = 500 OR status = 503").unwrap();
        assert!(matches!(sq.filter.unwrap(), FilterExpr::Or(_, _)));
    }

    #[test]
    fn test_order_by() {
        let sq = sql_to_subquery("SELECT * FROM logs ORDER BY status DESC, service ASC").unwrap();
        assert_eq!(sq.sort.len(), 2);
        assert_eq!(sq.sort[0].field, "status");
        assert!(sq.sort[0].descending);
        assert_eq!(sq.sort[1].field, "service");
        assert!(!sq.sort[1].descending);
    }

    #[test]
    fn test_limit() {
        let sq = sql_to_subquery("SELECT * FROM logs LIMIT 42").unwrap();
        assert_eq!(sq.limit, Some(42));
    }

    #[test]
    fn test_group_by() {
        let sq = sql_to_subquery("SELECT service, COUNT(*) FROM logs GROUP BY service").unwrap();
        assert_eq!(sq.group_by, vec!["service"]);
        assert!(!sq.aggregations.is_empty());
    }

    #[test]
    fn test_count_star_agg() {
        let sq = sql_to_subquery("SELECT COUNT(*) AS cnt FROM logs").unwrap();
        assert_eq!(sq.aggregations.len(), 1);
        assert!(matches!(sq.aggregations[0].function, AggFunction::Count));
        assert_eq!(sq.aggregations[0].alias, "cnt");
    }

    #[test]
    fn test_avg_agg() {
        let sq = sql_to_subquery("SELECT AVG(duration_ms) AS avg_dur FROM logs").unwrap();
        assert!(matches!(sq.aggregations[0].function, AggFunction::Avg));
        assert_eq!(sq.aggregations[0].field.as_deref(), Some("duration_ms"));
    }

    #[test]
    fn test_count_distinct_agg() {
        let sq = sql_to_subquery("SELECT COUNT(DISTINCT service) AS unique_svc FROM logs").unwrap();
        assert!(matches!(sq.aggregations[0].function, AggFunction::CountDistinct));
        assert_eq!(sq.aggregations[0].field.as_deref(), Some("service"));
        assert_eq!(sq.aggregations[0].alias, "unique_svc");
    }

    #[test]
    fn test_count_distinct_without_alias() {
        let sq = sql_to_subquery("SELECT COUNT(DISTINCT user_id) FROM logs").unwrap();
        assert!(matches!(sq.aggregations[0].function, AggFunction::CountDistinct));
        assert_eq!(sq.aggregations[0].field.as_deref(), Some("user_id"));
    }

    #[test]
    fn test_dotted_table_name() {
        // SQL parser treats "cluster_a" as schema, "logs" as table
        let sq = sql_to_subquery("SELECT * FROM cluster_a.logs").unwrap();
        assert_eq!(sq.table, "logs");
    }

    #[test]
    fn test_not_a_select_fails() {
        assert!(sql_to_subquery("INSERT INTO logs VALUES (1)").is_err());
    }

    #[test]
    fn test_empty_sql_fails() {
        assert!(sql_to_subquery("").is_err());
    }

    #[test]
    fn test_invalid_sql_fails() {
        assert!(sql_to_subquery("NOT VALID SQL AT ALL").is_err());
    }

    #[test]
    fn test_between() {
        let sq = sql_to_subquery("SELECT * FROM logs WHERE ts BETWEEN '2024-01-01' AND '2024-12-31'").unwrap();
        let f = sq.filter.unwrap();
        // BETWEEN translates to AND(>=, <=)
        if let FilterExpr::And(left, right) = f {
            if let FilterExpr::Comparison { field, op, .. } = *left {
                assert_eq!(field, "ts");
                assert!(matches!(op, ComparisonOp::Gte));
            } else {
                panic!("expected Comparison");
            }
            if let FilterExpr::Comparison { field, op, .. } = *right {
                assert_eq!(field, "ts");
                assert!(matches!(op, ComparisonOp::Lte));
            } else {
                panic!("expected Comparison");
            }
        } else {
            panic!("expected And");
        }
    }

    #[test]
    fn test_not_between() {
        let sq = sql_to_subquery("SELECT * FROM logs WHERE status NOT BETWEEN 400 AND 499").unwrap();
        let f = sq.filter.unwrap();
        assert!(matches!(f, FilterExpr::Not(_)));
    }

    #[test]
    fn test_and_with_untranslatable_preserves_other_side() {
        // CASE WHEN is untranslatable, but status = 200 should survive
        let sq = sql_to_subquery(
            "SELECT * FROM logs WHERE status = 200 AND CASE WHEN status > 0 THEN 1 ELSE 0 END = 1",
        ).unwrap();
        let f = sq.filter.unwrap();
        if let FilterExpr::Comparison { field, op, .. } = f {
            assert_eq!(field, "status");
            assert!(matches!(op, ComparisonOp::Eq));
        } else {
            panic!("expected Comparison, got {:?}", f);
        }
    }

    #[test]
    fn test_or_with_untranslatable_drops_both() {
        // OR with untranslatable side must drop entire filter (can't safely keep one side)
        let sq = sql_to_subquery(
            "SELECT * FROM logs WHERE status = 200 OR CASE WHEN status > 0 THEN 1 ELSE 0 END = 1",
        ).unwrap();
        assert!(sq.filter.is_none());
    }

    #[test]
    fn test_having_clause() {
        let sq = sql_to_subquery(
            "SELECT host, COUNT(*) FROM logs GROUP BY host HAVING COUNT(*) > 10",
        ).unwrap();
        assert!(sq.having.is_some());
        assert_eq!(sq.group_by, vec!["host"]);
    }

    #[test]
    fn test_having_with_comparison() {
        let sq = sql_to_subquery(
            "SELECT status, COUNT(*) AS cnt FROM logs GROUP BY status HAVING COUNT(*) >= 5",
        ).unwrap();
        let h = sq.having.unwrap();
        // The HAVING filter should be a comparison
        assert!(matches!(h, FilterExpr::Comparison { .. }));
    }

    #[test]
    fn test_no_having() {
        let sq = sql_to_subquery("SELECT host FROM logs GROUP BY host").unwrap();
        assert!(sq.having.is_none());
    }

    // ── HAVING verification tests (tester) ──

    #[test]
    fn test_having_and_compound() {
        // HAVING with AND — both conditions should be preserved
        let sq = sql_to_subquery(
            "SELECT host, COUNT(*) AS c, AVG(duration) AS d FROM logs GROUP BY host HAVING COUNT(*) > 5 AND AVG(duration) > 100",
        ).unwrap();
        let h = sq.having.unwrap();
        assert!(matches!(h, FilterExpr::And(_, _)));
    }

    #[test]
    fn test_having_or() {
        let sq = sql_to_subquery(
            "SELECT status, COUNT(*) FROM logs GROUP BY status HAVING COUNT(*) > 10 OR status = 500",
        ).unwrap();
        let h = sq.having.unwrap();
        assert!(matches!(h, FilterExpr::Or(_, _)));
    }

    #[test]
    fn test_having_without_group_by_still_parses() {
        // SQL allows HAVING without GROUP BY (implicit single group)
        let sq = sql_to_subquery(
            "SELECT COUNT(*) FROM logs HAVING COUNT(*) > 0",
        ).unwrap();
        assert!(sq.having.is_some());
        assert!(sq.group_by.is_empty());
    }

    #[test]
    fn test_having_preserves_where_separately() {
        // WHERE and HAVING should be independent
        let sq = sql_to_subquery(
            "SELECT host, COUNT(*) FROM logs WHERE status >= 400 GROUP BY host HAVING COUNT(*) > 3",
        ).unwrap();
        assert!(sq.filter.is_some(), "WHERE should be in filter");
        assert!(sq.having.is_some(), "HAVING should be separate");
        // Verify they're different expressions
        if let FilterExpr::Comparison { field, .. } = sq.filter.unwrap() {
            assert_eq!(field, "status");
        } else {
            panic!("WHERE filter should be on status");
        }
    }

    #[test]
    fn test_having_with_limit_and_sort() {
        let sq = sql_to_subquery(
            "SELECT host, COUNT(*) AS cnt FROM logs GROUP BY host HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 10",
        ).unwrap();
        assert!(sq.having.is_some());
        assert_eq!(sq.limit, Some(10));
        assert!(!sq.sort.is_empty());
    }

    #[test]
    fn test_subquery_extracts_table() {
        let sq = sql_to_subquery(
            "SELECT * FROM (SELECT host, status FROM logs) AS sub",
        ).unwrap();
        assert_eq!(sq.table, "logs");
    }

    #[test]
    fn test_subquery_merges_inner_filter() {
        let sq = sql_to_subquery(
            "SELECT * FROM (SELECT * FROM logs WHERE status > 400) AS sub WHERE host = 'h1'",
        ).unwrap();
        // Both inner (status > 400) and outer (host = 'h1') should be merged with AND
        let f = sq.filter.unwrap();
        assert!(matches!(f, FilterExpr::And(_, _)));
    }

    #[test]
    fn test_subquery_inner_filter_only() {
        let sq = sql_to_subquery(
            "SELECT * FROM (SELECT * FROM logs WHERE status = 200) AS sub",
        ).unwrap();
        let f = sq.filter.unwrap();
        assert!(matches!(f, FilterExpr::Comparison { .. }));
    }

    #[test]
    fn test_subquery_outer_filter_only() {
        let sq = sql_to_subquery(
            "SELECT * FROM (SELECT * FROM logs) AS sub WHERE host = 'h1'",
        ).unwrap();
        let f = sq.filter.unwrap();
        assert!(matches!(f, FilterExpr::Comparison { .. }));
    }

    #[test]
    fn test_in_list_filter() {
        let sq = sql_to_subquery(
            "SELECT * FROM logs WHERE status IN (200, 201, 204)",
        ).unwrap();
        let f = sq.filter.unwrap();
        if let FilterExpr::In { field, values } = f {
            assert_eq!(field, "status");
            assert_eq!(values.len(), 3);
        } else {
            panic!("expected In, got {:?}", f);
        }
    }

    #[test]
    fn test_not_in_list_filter() {
        let sq = sql_to_subquery(
            "SELECT * FROM logs WHERE host NOT IN ('h1', 'h2')",
        ).unwrap();
        let f = sq.filter.unwrap();
        // NOT IN wraps In in Not
        assert!(matches!(f, FilterExpr::Not(_)));
        if let FilterExpr::Not(inner) = f {
            assert!(matches!(*inner, FilterExpr::In { .. }));
        }
    }

    #[test]
    fn test_in_list_with_strings() {
        let sq = sql_to_subquery(
            "SELECT * FROM logs WHERE host IN ('web-01', 'web-02')",
        ).unwrap();
        let f = sq.filter.unwrap();
        if let FilterExpr::In { field, values } = f {
            assert_eq!(field, "host");
            assert_eq!(values.len(), 2);
        } else {
            panic!("expected In, got {:?}", f);
        }
    }

    // ── Subquery verification tests (tester) ──

    #[test]
    fn test_subquery_nested_two_levels() {
        let sq = sql_to_subquery(
            "SELECT * FROM (SELECT * FROM (SELECT * FROM logs) AS inner_q) AS outer_q",
        ).unwrap();
        assert_eq!(sq.table, "logs");
    }

    #[test]
    fn test_subquery_preserves_outer_limit() {
        let sq = sql_to_subquery(
            "SELECT * FROM (SELECT * FROM logs WHERE status >= 500) AS sub LIMIT 10",
        ).unwrap();
        assert_eq!(sq.limit, Some(10));
        assert!(sq.filter.is_some());
    }

    #[test]
    fn test_subquery_inner_and_outer_both_and() {
        let sq = sql_to_subquery(
            "SELECT * FROM (SELECT * FROM logs WHERE status > 400 AND host = 'h1') AS sub WHERE service = 'api'",
        ).unwrap();
        let f = sq.filter.unwrap();
        assert!(matches!(f, FilterExpr::And(_, _)));
    }

    #[test]
    fn test_subquery_with_projections() {
        let sq = sql_to_subquery(
            "SELECT host, status FROM (SELECT * FROM logs WHERE status = 200) AS sub",
        ).unwrap();
        assert_eq!(sq.table, "logs");
        assert!(sq.projections.contains(&"host".to_string()));
        assert!(sq.projections.contains(&"status".to_string()));
    }

    #[test]
    fn test_subquery_no_filter_either_side() {
        let sq = sql_to_subquery(
            "SELECT * FROM (SELECT * FROM logs) AS sub",
        ).unwrap();
        assert_eq!(sq.table, "logs");
        assert!(sq.filter.is_none());
    }

    #[test]
    fn test_like_filter() {
        let sq = sql_to_subquery(
            "SELECT * FROM logs WHERE host LIKE 'web-%'",
        ).unwrap();
        let f = sq.filter.unwrap();
        if let FilterExpr::Comparison { field, op, .. } = f {
            assert_eq!(field, "host");
            assert!(matches!(op, ComparisonOp::Like));
        } else {
            panic!("expected Comparison, got {:?}", f);
        }
    }

    #[test]
    fn test_ilike_filter() {
        let sq = sql_to_subquery(
            "SELECT * FROM logs WHERE host ILIKE '%WEB%'",
        ).unwrap();
        let f = sq.filter.unwrap();
        if let FilterExpr::Comparison { field, op, .. } = f {
            assert_eq!(field, "host");
            assert!(matches!(op, ComparisonOp::ILike));
        } else {
            panic!("expected Comparison, got {:?}", f);
        }
    }

    #[test]
    fn test_not_like_filter() {
        let sq = sql_to_subquery(
            "SELECT * FROM logs WHERE host NOT LIKE 'test-%'",
        ).unwrap();
        let f = sq.filter.unwrap();
        assert!(matches!(f, FilterExpr::Not(_)));
    }
}
