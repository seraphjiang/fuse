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

    let final_select = if let Some(ref ss) = stats_select {
        ss.clone()
    } else {
        select_clause
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
}
