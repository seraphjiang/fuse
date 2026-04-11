// SPDX-License-Identifier: Apache-2.0

//! Query auto-complete — schema-aware SQL/PPL completion suggestions.
//!
//! Given a partial query and available schemas, suggests table names,
//! column names, SQL keywords, and functions.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Completion {
    pub label: String,
    pub kind: CompletionKind,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CompletionKind {
    Keyword,
    Table,
    Column,
    Function,
}

static SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
    "ON", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS", "NULL",
    "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET", "AS", "DISTINCT",
    "UNION", "ALL", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
    "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "EXISTS", "CASE", "WHEN",
    "THEN", "ELSE", "END", "COUNT", "SUM", "AVG", "MIN", "MAX",
    "EXPLAIN", "ANALYZE", "PREPARE", "EXECUTE",
];

static SQL_FUNCTIONS: &[&str] = &[
    "count", "sum", "avg", "min", "max", "coalesce", "nullif",
    "upper", "lower", "trim", "length", "substring", "concat",
    "now", "date_trunc", "extract", "cast", "row_number", "rank",
    "lag", "lead", "first_value", "last_value",
];

/// Available schema info for completions.
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub datasource: String,
    pub table: String,
    pub columns: Vec<String>,
}

/// Generate completions for a partial query.
pub fn complete(partial: &str, schemas: &[SchemaInfo]) -> Vec<Completion> {
    let last_word = partial.split_whitespace().last().unwrap_or("").to_lowercase();
    if last_word.is_empty() {
        return vec![];
    }

    let mut completions = Vec::new();

    // Keyword completions
    for kw in SQL_KEYWORDS {
        if kw.to_lowercase().starts_with(&last_word) {
            completions.push(Completion {
                label: kw.to_string(),
                kind: CompletionKind::Keyword,
                detail: None,
            });
        }
    }

    // Function completions
    for func in SQL_FUNCTIONS {
        if func.starts_with(&last_word) {
            completions.push(Completion {
                label: format!("{}()", func),
                kind: CompletionKind::Function,
                detail: Some("function".into()),
            });
        }
    }

    // Table completions (datasource.table)
    for schema in schemas {
        let fqn = format!("{}.{}", schema.datasource, schema.table);
        if fqn.to_lowercase().starts_with(&last_word) || schema.table.to_lowercase().starts_with(&last_word) {
            completions.push(Completion {
                label: fqn,
                kind: CompletionKind::Table,
                detail: Some(schema.datasource.clone()),
            });
        }

        // Column completions
        for col in &schema.columns {
            if col.to_lowercase().starts_with(&last_word) {
                completions.push(Completion {
                    label: col.clone(),
                    kind: CompletionKind::Column,
                    detail: Some(format!("{}.{}", schema.datasource, schema.table)),
                });
            }
        }
    }

    completions.truncate(20); // cap suggestions
    completions
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schemas() -> Vec<SchemaInfo> {
        vec![SchemaInfo {
            datasource: "cluster_a".into(),
            table: "logs".into(),
            columns: vec!["timestamp".into(), "level".into(), "message".into()],
        }]
    }

    #[test]
    fn test_keyword_completion() {
        let c = complete("SEL", &[]);
        assert!(c.iter().any(|c| c.label == "SELECT" && c.kind == CompletionKind::Keyword));
    }

    #[test]
    fn test_table_completion() {
        let c = complete("FROM cl", &schemas());
        assert!(c.iter().any(|c| c.label == "cluster_a.logs" && c.kind == CompletionKind::Table));
    }

    #[test]
    fn test_column_completion() {
        let c = complete("WHERE le", &schemas());
        assert!(c.iter().any(|c| c.label == "level" && c.kind == CompletionKind::Column));
    }

    #[test]
    fn test_function_completion() {
        let c = complete("SELECT cou", &[]);
        assert!(c.iter().any(|c| c.label == "count()" && c.kind == CompletionKind::Function));
    }

    #[test]
    fn test_empty_input() {
        assert!(complete("", &schemas()).is_empty());
    }

    #[test]
    fn test_no_match() {
        let c = complete("zzzzz", &schemas());
        assert!(c.is_empty());
    }

    #[test]
    fn test_max_completions() {
        let c = complete("a", &schemas()); // matches many keywords
        assert!(c.len() <= 20);
    }
}
