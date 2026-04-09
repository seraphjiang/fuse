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

    let table = extract_table_name(&select.from)?;
    let (projections, aggregations) = extract_projections_and_aggs(&select.projection);
    let filter = select.selection.as_ref().and_then(|e| translate_expr(e));
    let group_by = extract_group_by(&select.group_by);
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
        sort,
        limit,
        passthrough: None,
    })
}

fn extract_table_name(from: &[ast::TableWithJoins]) -> Result<String, ConnectorError> {
    let twj = from
        .first()
        .ok_or_else(|| ConnectorError::QueryFailed("no FROM clause".into()))?;

    match &twj.relation {
        TableFactor::Table { name, .. } => Ok(object_name_to_string(name)),
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
    let agg_fn = match func_name.as_str() {
        "count" => AggFunction::Count,
        "sum" => AggFunction::Sum,
        "avg" => AggFunction::Avg,
        "min" => AggFunction::Min,
        "max" => AggFunction::Max,
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
        SqlExpr::Nested(inner) => translate_expr(inner),
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
            let l = translate_expr(left)?;
            let r = translate_expr(right)?;
            Some(FilterExpr::And(Box::new(l), Box::new(r)))
        }
        ast::BinaryOperator::Or => {
            let l = translate_expr(left)?;
            let r = translate_expr(right)?;
            Some(FilterExpr::Or(Box::new(l), Box::new(r)))
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
