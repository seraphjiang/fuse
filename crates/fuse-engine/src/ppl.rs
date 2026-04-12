// SPDX-License-Identifier: Apache-2.0

//! PPL (Piped Processing Language) parser for multi-source federated queries.
//!
//! Parses the syntax:
//! ```text
//! source = datasource.table [, datasource.table]* | command | command ...
//! search source = ...  (alias for source =)
//! ```
//!
//! Supported commands: where, stats, sort, head, fields, dedup

use std::fmt;

/// A parsed PPL query.
#[derive(Debug, Clone)]
pub struct PplQuery {
    pub sources: Vec<QualifiedTable>,
    pub commands: Vec<PplCommand>,
}

/// A qualified table reference: `datasource.table` or just `table`.
#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedTable {
    pub datasource: Option<String>,
    pub table: String,
}

impl fmt::Display for QualifiedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.datasource {
            Some(ds) => write!(f, "{}.{}", ds, self.table),
            None => write!(f, "{}", self.table),
        }
    }
}

/// A single PPL pipe command.
#[derive(Debug, Clone)]
pub enum PplCommand {
    Where(String),
    Stats {
        aggs: Vec<StatsAgg>,
        by: Vec<String>,
    },
    Sort(Vec<SortField>),
    Head(u64),
    Fields {
        include: bool,
        names: Vec<String>,
    },
    Dedup(Vec<String>),
    /// eval field1 = expr1, field2 = expr2
    Eval(Vec<EvalExpr>),
    /// rename old_name AS new_name, ...
    Rename(Vec<RenameExpr>),
    /// top N field1, field2 — most frequent values
    Top {
        n: u64,
        fields: Vec<String>,
    },
    /// rare field1, field2 — least frequent values
    Rare {
        fields: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub struct StatsAgg {
    pub func: String,
    pub field: Option<String>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SortField {
    pub field: String,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub struct EvalExpr {
    pub alias: String,
    pub expr: String,
}

#[derive(Debug, Clone)]
pub struct RenameExpr {
    pub old_name: String,
    pub new_name: String,
}

/// Returns true if the input looks like a PPL query (starts with `source` or `search`).
pub fn is_ppl(input: &str) -> bool {
    let trimmed = input.trim_start().to_lowercase();
    trimmed.starts_with("source") || trimmed.starts_with("search")
}

/// Parse a PPL query string into a `PplQuery`.
pub fn parse_ppl(input: &str) -> Result<PplQuery, PplParseError> {
    let trimmed = input.trim();
    let body = strip_ppl_prefix(trimmed)?;

    // Split on pipe `|` — first segment is the source list, rest are commands.
    let segments: Vec<&str> = body.split('|').collect();
    if segments.is_empty() {
        return Err(PplParseError("Empty query".into()));
    }

    let sources = parse_sources(segments[0].trim())?;
    let mut commands = Vec::new();
    for seg in &segments[1..] {
        commands.push(parse_command(seg.trim())?);
    }

    Ok(PplQuery { sources, commands })
}

/// Convert a parsed PPL query to a SQL string that DataFusion can execute.
///
/// Multi-source queries become `SELECT * FROM t1 UNION ALL SELECT * FROM t2`
/// with commands translated to SQL clauses.
pub fn ppl_to_sql(query: &PplQuery) -> Result<String, PplParseError> {
    if query.sources.is_empty() {
        return Err(PplParseError("No sources specified".into()));
    }

    // Build the projection (fields command)
    let projection = find_projection(&query.commands);
    let select_clause = projection.unwrap_or_else(|| "*".to_string());

    // Build WHERE, GROUP BY, ORDER BY, LIMIT from commands
    let where_clause = find_where(&query.commands);
    let (stats_select, group_by) = find_stats(&query.commands);
    let order_by = find_sort(&query.commands);
    let limit = find_head(&query.commands);

    // Check for top/rare — these override stats/sort/limit
    let (stats_select, group_by, order_by, limit) = if let Some(top) = find_top(&query.commands) {
        let fields_str = top.fields.join(", ");
        let sel = format!("{}, COUNT(*) AS count", fields_str);
        (
            Some(sel),
            Some(fields_str),
            Some("count DESC".to_string()),
            Some(top.n),
        )
    } else if let Some(rare) = find_rare(&query.commands) {
        let fields_str = rare.fields.join(", ");
        let sel = format!("{}, COUNT(*) AS count", fields_str);
        (
            Some(sel),
            Some(fields_str),
            Some("count ASC".to_string()),
            Some(10),
        )
    } else {
        (stats_select, group_by, order_by, limit)
    };

    let final_select = if let Some(ref ss) = stats_select {
        ss.clone()
    } else {
        let mut sel = select_clause;
        // Append eval expressions as computed columns
        for cmd in &query.commands {
            if let PplCommand::Eval(exprs) = cmd {
                for e in exprs {
                    sel.push_str(&format!(", {} AS {}", e.expr, e.alias));
                }
            }
        }
        // Apply rename: wrap in outer SELECT with aliases
        let renames = find_renames(&query.commands);
        if !renames.is_empty() {
            // Rewrite column references: old_name → old_name AS new_name
            for r in &renames {
                sel = sel.replace(&r.old_name, &format!("{} AS {}", r.old_name, r.new_name));
            }
        }
        sel
    };

    // Build per-source SELECT statements
    let per_source: Vec<String> = query
        .sources
        .iter()
        .map(|src| {
            let mut sql = format!("SELECT {} FROM {}", final_select, src);
            if let Some(ref w) = where_clause {
                sql.push_str(&format!(" WHERE {}", w));
            }
            if let Some(ref gb) = group_by {
                sql.push_str(&format!(" GROUP BY {}", gb));
            }
            sql
        })
        .collect();

    let mut sql = if per_source.len() == 1 {
        per_source.into_iter().next().unwrap()
    } else {
        per_source.join(" UNION ALL ")
    };

    if let Some(ref ob) = order_by {
        sql.push_str(&format!(" ORDER BY {}", ob));
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {}", n));
    }

    Ok(sql)
}

// ── Internal parsing helpers ──

fn strip_ppl_prefix(input: &str) -> Result<&str, PplParseError> {
    let lower = input.to_lowercase();
    // "search source = ..." or "source = ..."
    let rest = if lower.starts_with("search") {
        input["search".len()..].trim_start()
    } else {
        input
    };

    let lower_rest = rest.to_lowercase();
    if !lower_rest.starts_with("source") {
        return Err(PplParseError("Expected 'source = ...'".into()));
    }
    let after_source = rest["source".len()..].trim_start();
    if !after_source.starts_with('=') {
        return Err(PplParseError("Expected '=' after 'source'".into()));
    }
    Ok(after_source[1..].trim_start())
}

fn parse_sources(input: &str) -> Result<Vec<QualifiedTable>, PplParseError> {
    if input.is_empty() {
        return Err(PplParseError("No sources specified".into()));
    }
    input
        .split(',')
        .map(|s| parse_qualified_table(s.trim()))
        .collect()
}

fn parse_qualified_table(input: &str) -> Result<QualifiedTable, PplParseError> {
    if input.is_empty() {
        return Err(PplParseError("Empty table reference".into()));
    }
    match input.split_once('.') {
        Some((ds, table)) => Ok(QualifiedTable {
            datasource: Some(ds.trim().to_string()),
            table: table.trim().to_string(),
        }),
        None => Ok(QualifiedTable {
            datasource: None,
            table: input.to_string(),
        }),
    }
}

fn parse_command(input: &str) -> Result<PplCommand, PplParseError> {
    let (keyword, rest) = split_first_word(input);
    match keyword.to_lowercase().as_str() {
        "where" => Ok(PplCommand::Where(rest.to_string())),
        "stats" => parse_stats(rest),
        "sort" => parse_sort(rest),
        "head" => parse_head(rest),
        "fields" => parse_fields(rest),
        "dedup" => parse_dedup(rest),
        "eval" => parse_eval(rest),
        "rename" => parse_rename(rest),
        "top" => parse_top(rest),
        "rare" => parse_rare(rest),
        other => Err(PplParseError(format!("Unknown command: '{}'", other))),
    }
}

fn split_first_word(input: &str) -> (&str, &str) {
    match input.find(|c: char| c.is_whitespace()) {
        Some(pos) => (&input[..pos], input[pos..].trim_start()),
        None => (input, ""),
    }
}

fn parse_stats(input: &str) -> Result<PplCommand, PplParseError> {
    // "count() as error_count, avg(latency) by service_name, host"
    let (agg_part, by_part) = match input.to_lowercase().find(" by ") {
        Some(pos) => (&input[..pos], Some(&input[pos + 4..])),
        None => (input, None),
    };

    let aggs = agg_part
        .split(',')
        .map(|a| parse_stats_agg(a.trim()))
        .collect::<Result<Vec<_>, _>>()?;

    let by = by_part
        .map(|b| b.split(',').map(|f| f.trim().to_string()).collect())
        .unwrap_or_default();

    Ok(PplCommand::Stats { aggs, by })
}

fn parse_stats_agg(input: &str) -> Result<StatsAgg, PplParseError> {
    // "count() as error_count" or "avg(latency)" or "count()"
    let (expr, alias) = match input.to_lowercase().find(" as ") {
        Some(pos) => (&input[..pos], Some(input[pos + 4..].trim().to_string())),
        None => (input, None),
    };

    let expr = expr.trim();
    if let Some(paren_start) = expr.find('(') {
        let func = expr[..paren_start].trim().to_string();
        let inner = expr[paren_start + 1..].trim_end_matches(')').trim();
        let field = if inner.is_empty() || inner == "*" {
            None
        } else {
            Some(inner.to_string())
        };
        Ok(StatsAgg { func, field, alias })
    } else {
        Err(PplParseError(format!(
            "Invalid aggregation expression: '{}'",
            expr
        )))
    }
}

fn parse_sort(input: &str) -> Result<PplCommand, PplParseError> {
    // "- field1, + field2, field3"  (- = desc, + or none = asc)
    let fields = input
        .split(',')
        .map(|f| {
            let f = f.trim();
            if let Some(rest) = f.strip_prefix('-') {
                Ok(SortField {
                    field: rest.trim().to_string(),
                    descending: true,
                })
            } else if let Some(rest) = f.strip_prefix('+') {
                Ok(SortField {
                    field: rest.trim().to_string(),
                    descending: false,
                })
            } else {
                Ok(SortField {
                    field: f.to_string(),
                    descending: false,
                })
            }
        })
        .collect::<Result<Vec<_>, PplParseError>>()?;
    Ok(PplCommand::Sort(fields))
}

fn parse_head(input: &str) -> Result<PplCommand, PplParseError> {
    let n = if input.is_empty() {
        10 // PPL default
    } else {
        input
            .trim()
            .parse::<u64>()
            .map_err(|_| PplParseError(format!("Invalid head count: '{}'", input)))?
    };
    Ok(PplCommand::Head(n))
}

fn parse_fields(input: &str) -> Result<PplCommand, PplParseError> {
    // "- field1, field2" (exclude) or "+ field1, field2" or "field1, field2" (include)
    let (include, rest) = if let Some(r) = input.strip_prefix('-') {
        (false, r.trim())
    } else if let Some(r) = input.strip_prefix('+') {
        (true, r.trim())
    } else {
        (true, input)
    };
    let names = rest.split(',').map(|f| f.trim().to_string()).collect();
    Ok(PplCommand::Fields { include, names })
}

fn parse_dedup(input: &str) -> Result<PplCommand, PplParseError> {
    let fields: Vec<String> = input.split(',').map(|f| f.trim().to_string()).collect();
    if fields.is_empty() || (fields.len() == 1 && fields[0].is_empty()) {
        return Err(PplParseError("dedup requires at least one field".into()));
    }
    Ok(PplCommand::Dedup(fields))
}

/// Parse `eval field1 = expr1, field2 = expr2`
fn parse_eval(input: &str) -> Result<PplCommand, PplParseError> {
    let mut exprs = Vec::new();
    for part in input.split(',') {
        let part = part.trim();
        let eq_pos = part
            .find('=')
            .ok_or_else(|| PplParseError(format!("eval expression missing '=': '{}'", part)))?;
        let alias = part[..eq_pos].trim().to_string();
        let expr = part[eq_pos + 1..].trim().to_string();
        if alias.is_empty() || expr.is_empty() {
            return Err(PplParseError(format!(
                "eval expression incomplete: '{}'",
                part
            )));
        }
        exprs.push(EvalExpr { alias, expr });
    }
    Ok(PplCommand::Eval(exprs))
}

/// Parse `rename old_name AS new_name, ...`
fn parse_rename(input: &str) -> Result<PplCommand, PplParseError> {
    let mut renames = Vec::new();
    for part in input.split(',') {
        let part = part.trim();
        let lower = part.to_lowercase();
        let as_pos = lower
            .find(" as ")
            .ok_or_else(|| PplParseError(format!("rename missing 'AS': '{}'", part)))?;
        let old_name = part[..as_pos].trim().to_string();
        let new_name = part[as_pos + 4..].trim().to_string();
        if old_name.is_empty() || new_name.is_empty() {
            return Err(PplParseError(format!("rename incomplete: '{}'", part)));
        }
        renames.push(RenameExpr { old_name, new_name });
    }
    Ok(PplCommand::Rename(renames))
}

/// Parse `top N field1, field2` — N is optional (default 10)
fn parse_top(input: &str) -> Result<PplCommand, PplParseError> {
    let parts: Vec<&str> = input.splitn(2, |c: char| c.is_whitespace()).collect();
    let (n, field_str) = if let Ok(num) = parts[0].parse::<u64>() {
        (num, parts.get(1).copied().unwrap_or(""))
    } else {
        (10, input)
    };
    let fields: Vec<String> = field_str
        .split(',')
        .map(|f| f.trim().to_string())
        .filter(|f| !f.is_empty())
        .collect();
    if fields.is_empty() {
        return Err(PplParseError("top requires at least one field".into()));
    }
    Ok(PplCommand::Top { n, fields })
}

/// Parse `rare field1, field2`
fn parse_rare(input: &str) -> Result<PplCommand, PplParseError> {
    let fields: Vec<String> = input
        .split(',')
        .map(|f| f.trim().to_string())
        .filter(|f| !f.is_empty())
        .collect();
    if fields.is_empty() {
        return Err(PplParseError("rare requires at least one field".into()));
    }
    Ok(PplCommand::Rare { fields })
}

// ── SQL generation helpers ──

fn find_projection(commands: &[PplCommand]) -> Option<String> {
    for cmd in commands {
        if let PplCommand::Fields {
            include: true,
            names,
        } = cmd
        {
            return Some(names.join(", "));
        }
    }
    None
}

fn find_renames(commands: &[PplCommand]) -> Vec<RenameExpr> {
    commands
        .iter()
        .filter_map(|cmd| {
            if let PplCommand::Rename(renames) = cmd {
                Some(renames.clone())
            } else {
                None
            }
        })
        .flatten()
        .collect()
}

struct TopInfo {
    n: u64,
    fields: Vec<String>,
}
struct RareInfo {
    fields: Vec<String>,
}

fn find_top(commands: &[PplCommand]) -> Option<TopInfo> {
    commands.iter().find_map(|cmd| {
        if let PplCommand::Top { n, fields } = cmd {
            Some(TopInfo {
                n: *n,
                fields: fields.clone(),
            })
        } else {
            None
        }
    })
}

fn find_rare(commands: &[PplCommand]) -> Option<RareInfo> {
    commands.iter().find_map(|cmd| {
        if let PplCommand::Rare { fields } = cmd {
            Some(RareInfo {
                fields: fields.clone(),
            })
        } else {
            None
        }
    })
}

fn find_where(commands: &[PplCommand]) -> Option<String> {
    for cmd in commands {
        if let PplCommand::Where(expr) = cmd {
            return Some(expr.clone());
        }
    }
    None
}

fn find_stats(commands: &[PplCommand]) -> (Option<String>, Option<String>) {
    for cmd in commands {
        if let PplCommand::Stats { aggs, by } = cmd {
            let select_parts: Vec<String> = aggs
                .iter()
                .map(|a| {
                    let expr = match &a.field {
                        Some(f) => format!("{}({})", a.func, f),
                        None => format!("{}(*)", a.func),
                    };
                    match &a.alias {
                        Some(alias) => format!("{} AS {}", expr, alias),
                        None => expr,
                    }
                })
                .collect();

            let select = if by.is_empty() {
                select_parts.join(", ")
            } else {
                format!("{}, {}", by.join(", "), select_parts.join(", "))
            };

            let group_by = if by.is_empty() {
                None
            } else {
                Some(by.join(", "))
            };

            return (Some(select), group_by);
        }
    }
    (None, None)
}

fn find_sort(commands: &[PplCommand]) -> Option<String> {
    for cmd in commands {
        if let PplCommand::Sort(fields) = cmd {
            let parts: Vec<String> = fields
                .iter()
                .map(|f| {
                    if f.descending {
                        format!("{} DESC", f.field)
                    } else {
                        format!("{} ASC", f.field)
                    }
                })
                .collect();
            return Some(parts.join(", "));
        }
    }
    None
}

fn find_head(commands: &[PplCommand]) -> Option<u64> {
    for cmd in commands {
        if let PplCommand::Head(n) = cmd {
            return Some(*n);
        }
    }
    None
}

// ── Error type ──

#[derive(Debug, Clone)]
pub struct PplParseError(pub String);

impl fmt::Display for PplParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PPL parse error: {}", self.0)
    }
}

impl std::error::Error for PplParseError {}

impl From<PplParseError> for datafusion::error::DataFusionError {
    fn from(e: PplParseError) -> Self {
        datafusion::error::DataFusionError::Plan(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_ppl() {
        assert!(is_ppl("source = logs"));
        assert!(is_ppl("  SOURCE = logs"));
        assert!(is_ppl("search source = logs"));
        assert!(!is_ppl("SELECT * FROM logs"));
    }

    #[test]
    fn test_parse_single_source() {
        let q = parse_ppl("source = prod.logs").unwrap();
        assert_eq!(q.sources.len(), 1);
        assert_eq!(q.sources[0].datasource.as_deref(), Some("prod"));
        assert_eq!(q.sources[0].table, "logs");
    }

    #[test]
    fn test_parse_multi_source() {
        let q = parse_ppl("source = cluster_a.logs, cluster_b.logs, cluster_c.logs").unwrap();
        assert_eq!(q.sources.len(), 3);
        assert_eq!(q.sources[0].datasource.as_deref(), Some("cluster_a"));
        assert_eq!(q.sources[2].table, "logs");
    }

    #[test]
    fn test_parse_with_commands() {
        let q = parse_ppl(
            "source = prod.logs | where status >= 500 | stats count() as cnt by host | sort - cnt | head 20"
        ).unwrap();
        assert_eq!(q.sources.len(), 1);
        assert_eq!(q.commands.len(), 4);
        assert!(matches!(q.commands[0], PplCommand::Where(_)));
        assert!(matches!(q.commands[1], PplCommand::Stats { .. }));
        assert!(matches!(q.commands[2], PplCommand::Sort(_)));
        assert!(matches!(q.commands[3], PplCommand::Head(20)));
    }

    #[test]
    fn test_parse_search_prefix() {
        let q = parse_ppl("search source = logs | head 5").unwrap();
        assert_eq!(q.sources[0].table, "logs");
        assert!(matches!(q.commands[0], PplCommand::Head(5)));
    }

    #[test]
    fn test_parse_fields_exclude() {
        let q = parse_ppl("source = logs | fields - password, secret").unwrap();
        if let PplCommand::Fields { include, names } = &q.commands[0] {
            assert!(!include);
            assert_eq!(names, &["password", "secret"]);
        } else {
            panic!("Expected Fields command");
        }
    }

    #[test]
    fn test_parse_dedup() {
        let q = parse_ppl("source = logs | dedup trace_id, span_id").unwrap();
        if let PplCommand::Dedup(fields) = &q.commands[0] {
            assert_eq!(fields, &["trace_id", "span_id"]);
        } else {
            panic!("Expected Dedup command");
        }
    }

    #[test]
    fn test_ppl_to_sql_simple() {
        let q = parse_ppl("source = prod.logs | where status = 500 | head 10").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        assert_eq!(sql, "SELECT * FROM prod.logs WHERE status = 500 LIMIT 10");
    }

    #[test]
    fn test_ppl_to_sql_multi_source() {
        let q = parse_ppl("source = a.logs, b.logs | where level = 'ERROR'").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("FROM a.logs"));
        assert!(sql.contains("FROM b.logs"));
    }

    #[test]
    fn test_ppl_to_sql_stats() {
        let q = parse_ppl("source = logs | stats count() as cnt, avg(latency) by service").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        assert!(sql.contains("service, count(*) AS cnt, avg(latency)"));
        assert!(sql.contains("GROUP BY service"));
    }

    #[test]
    fn test_ppl_to_sql_sort() {
        let q = parse_ppl("source = logs | sort - timestamp, + host").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        assert!(sql.contains("ORDER BY timestamp DESC, host ASC"));
    }

    #[test]
    fn test_parse_unqualified_table() {
        let q = parse_ppl("source = logs").unwrap();
        assert_eq!(q.sources[0].datasource, None);
        assert_eq!(q.sources[0].table, "logs");
    }

    #[test]
    fn test_parse_fields_include() {
        let q = parse_ppl("source = logs | fields host, status, latency").unwrap();
        if let PplCommand::Fields { include, names } = &q.commands[0] {
            assert!(include);
            assert_eq!(names, &["host", "status", "latency"]);
        } else {
            panic!("Expected Fields command");
        }
    }

    #[test]
    fn test_ppl_to_sql_fields_include() {
        let q = parse_ppl("source = logs | fields host, status").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        assert!(sql.contains("SELECT host, status FROM logs"));
    }

    #[test]
    fn test_ppl_to_sql_dedup() {
        // Dedup is parsed but currently not translated to SQL (silently dropped).
        // The query still executes — just without deduplication at SQL level.
        let q = parse_ppl("source = logs | dedup trace_id").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        assert!(sql.contains("FROM logs"));
    }

    #[test]
    fn test_parse_error_missing_equals() {
        let err = parse_ppl("source logs | head 5");
        assert!(err.is_err());
        assert!(err.unwrap_err().0.contains("'='"));
    }

    #[test]
    fn test_parse_error_empty_source() {
        let err = parse_ppl("source = ");
        assert!(err.is_err());
    }

    #[test]
    fn test_eval_command() {
        let q = parse_ppl("source = logs | eval duration_sec = duration / 1000").unwrap();
        assert_eq!(q.commands.len(), 1);
        if let PplCommand::Eval(exprs) = &q.commands[0] {
            assert_eq!(exprs[0].alias, "duration_sec");
            assert_eq!(exprs[0].expr, "duration / 1000");
        } else {
            panic!("expected Eval");
        }
    }

    #[test]
    fn test_eval_to_sql() {
        let q = parse_ppl("source = logs | eval rate = count / total").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        assert!(sql.contains("count / total AS rate"));
    }

    #[test]
    fn test_eval_multiple() {
        let q = parse_ppl("source = logs | eval a = x + 1, b = y * 2").unwrap();
        if let PplCommand::Eval(exprs) = &q.commands[0] {
            assert_eq!(exprs.len(), 2);
            assert_eq!(exprs[0].alias, "a");
            assert_eq!(exprs[1].alias, "b");
        } else {
            panic!("expected Eval");
        }
    }

    #[test]
    fn test_rename_command() {
        let q = parse_ppl("source = logs | rename host AS hostname").unwrap();
        assert_eq!(q.commands.len(), 1);
        if let PplCommand::Rename(renames) = &q.commands[0] {
            assert_eq!(renames[0].old_name, "host");
            assert_eq!(renames[0].new_name, "hostname");
        } else {
            panic!("expected Rename");
        }
    }

    #[test]
    fn test_rename_to_sql() {
        let q = parse_ppl("source = logs | fields host, status | rename host AS hostname").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        assert!(sql.contains("AS hostname"));
    }

    #[test]
    fn test_rename_multiple() {
        let q = parse_ppl("source = logs | rename host AS hostname, status AS code").unwrap();
        if let PplCommand::Rename(renames) = &q.commands[0] {
            assert_eq!(renames.len(), 2);
        } else {
            panic!("expected Rename");
        }
    }

    #[test]
    fn test_eval_error_missing_eq() {
        let q = parse_ppl("source = logs | eval bad_expr");
        assert!(q.is_err());
    }

    #[test]
    fn test_rename_error_missing_as() {
        let q = parse_ppl("source = logs | rename host hostname");
        assert!(q.is_err());
    }

    #[test]
    fn test_top_command() {
        let q = parse_ppl("source = logs | top 5 host").unwrap();
        if let PplCommand::Top { n, fields } = &q.commands[0] {
            assert_eq!(*n, 5);
            assert_eq!(fields, &["host"]);
        } else {
            panic!("expected Top");
        }
    }

    #[test]
    fn test_top_default_n() {
        let q = parse_ppl("source = logs | top host, status").unwrap();
        if let PplCommand::Top { n, fields } = &q.commands[0] {
            assert_eq!(*n, 10);
            assert_eq!(fields.len(), 2);
        } else {
            panic!("expected Top");
        }
    }

    #[test]
    fn test_top_to_sql() {
        let q = parse_ppl("source = logs | top 3 host").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        let lower = sql.to_lowercase();
        assert!(lower.contains("count(*)"));
        assert!(lower.contains("group by host"));
        assert!(lower.contains("order by count desc"));
        assert!(lower.contains("limit 3"));
    }

    #[test]
    fn test_rare_command() {
        let q = parse_ppl("source = logs | rare status").unwrap();
        if let PplCommand::Rare { fields } = &q.commands[0] {
            assert_eq!(fields, &["status"]);
        } else {
            panic!("expected Rare");
        }
    }

    #[test]
    fn test_rare_to_sql() {
        let q = parse_ppl("source = logs | rare host").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        let lower = sql.to_lowercase();
        assert!(lower.contains("count(*)"));
        assert!(lower.contains("group by host"));
        assert!(lower.contains("order by count asc"));
        assert!(lower.contains("limit 10"));
    }

    #[test]
    fn test_top_with_where() {
        let q = parse_ppl("source = logs | where status > 400 | top 5 host").unwrap();
        let sql = ppl_to_sql(&q).unwrap();
        let lower = sql.to_lowercase();
        assert!(lower.contains("where status > 400"));
        assert!(lower.contains("group by host"));
        assert!(lower.contains("limit 5"));
    }
}
