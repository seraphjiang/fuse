// SPDX-License-Identifier: Apache-2.0

//! SQL identifier quoting utilities for pushdown connectors.
//!
//! Prevents identifier injection by quoting table names, column names,
//! and aliases before interpolation into generated SQL strings.

/// Quote a SQL identifier with double quotes (ANSI SQL standard).
/// Used by PostgreSQL, DuckDB, and other ANSI-compliant databases.
/// Escapes embedded double quotes by doubling them.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a table reference. Passes through subquery expressions (starting with `(`)
/// and already-quoted identifiers unchanged; quotes plain identifiers.
pub fn quote_table(name: &str) -> String {
    if name.starts_with('(') || name.starts_with('"') {
        name.to_string()
    } else {
        quote_ident(name)
    }
}

/// Quote a table reference with backticks. Passes through subquery expressions unchanged.
pub fn quote_table_backtick(name: &str) -> String {
    if name.starts_with('(') || name.starts_with('`') {
        name.to_string()
    } else {
        quote_ident_backtick(name)
    }
}

/// Quote a SQL identifier with backticks.
/// Used by ClickHouse and MySQL.
/// Escapes embedded backticks by doubling them.
pub fn quote_ident_backtick(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("users"), "\"users\"");
    }

    #[test]
    fn test_quote_ident_with_space() {
        assert_eq!(quote_ident("my table"), "\"my table\"");
    }

    #[test]
    fn test_quote_ident_with_embedded_quote() {
        assert_eq!(quote_ident("col\"name"), "\"col\"\"name\"");
    }

    #[test]
    fn test_quote_ident_reserved_word() {
        assert_eq!(quote_ident("select"), "\"select\"");
    }

    #[test]
    fn test_quote_ident_backtick_simple() {
        assert_eq!(quote_ident_backtick("events"), "`events`");
    }

    #[test]
    fn test_quote_ident_backtick_with_embedded() {
        assert_eq!(quote_ident_backtick("col`name"), "`col``name`");
    }

    #[test]
    fn test_quote_ident_empty() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn test_quote_ident_backtick_empty() {
        assert_eq!(quote_ident_backtick(""), "``");
    }

    #[test]
    fn test_quote_table_simple() {
        assert_eq!(quote_table("users"), "\"users\"");
    }

    #[test]
    fn test_quote_table_subquery_passthrough() {
        let expr = "(SELECT 1) t";
        assert_eq!(quote_table(expr), expr);
    }

    #[test]
    fn test_quote_table_already_quoted() {
        assert_eq!(quote_table("\"users\""), "\"users\"");
    }

    #[test]
    fn test_quote_table_backtick_simple() {
        assert_eq!(quote_table_backtick("events"), "`events`");
    }

    #[test]
    fn test_quote_table_backtick_subquery_passthrough() {
        let expr = "(SELECT 1) t";
        assert_eq!(quote_table_backtick(expr), expr);
    }
}
