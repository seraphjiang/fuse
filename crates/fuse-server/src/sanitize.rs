// SPDX-License-Identifier: Apache-2.0
//! Query sanitizer — redact sensitive values before logging.

/// Redact string literals in SQL/PPL queries for safe logging.
/// Replaces quoted string values with '***' to prevent credential leakage.
pub fn sanitize_query(query: &str) -> String {
    let mut result = String::with_capacity(query.len());
    let mut chars = query.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            result.push_str("'***'");
            // Skip until closing quote
            while let Some(inner) = chars.next() {
                if inner == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next(); // escaped quote, continue
                    } else {
                        break;
                    }
                }
            }
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_strings() {
        assert_eq!(sanitize_query("SELECT 1"), "SELECT 1");
    }

    #[test]
    fn test_redacts_string_literal() {
        assert_eq!(
            sanitize_query("SELECT * FROM t WHERE name = 'secret_value'"),
            "SELECT * FROM t WHERE name = '***'"
        );
    }

    #[test]
    fn test_multiple_strings() {
        assert_eq!(
            sanitize_query("WHERE a = 'x' AND b = 'y'"),
            "WHERE a = '***' AND b = '***'"
        );
    }

    #[test]
    fn test_escaped_quotes() {
        assert_eq!(
            sanitize_query("WHERE name = 'it''s a test'"),
            "WHERE name = '***'"
        );
    }

    #[test]
    fn test_preserves_structure() {
        let q = "SELECT id, name FROM ds.users WHERE role = 'admin' ORDER BY id";
        let s = sanitize_query(q);
        assert!(s.contains("SELECT id, name"));
        assert!(s.contains("ORDER BY id"));
        assert!(!s.contains("admin"));
    }
}
