// SPDX-License-Identifier: Apache-2.0
//! Query fingerprinting — normalize queries to identify patterns.
//!
//! Replaces literal values with placeholders to group similar queries.

/// Generate a fingerprint by normalizing a SQL query.
/// Replaces string literals with '?' and numbers with '?'.
pub fn fingerprint(query: &str) -> String {
    let mut result = String::with_capacity(query.len());
    let mut chars = query.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\'' {
            result.push('?');
            while let Some(inner) = chars.next() {
                if inner == '\'' {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
        } else if c.is_ascii_digit()
            && !result.ends_with(|ch: char| ch.is_ascii_alphanumeric() || ch == '_')
        {
            result.push('?');
            while chars
                .peek()
                .map(|ch| ch.is_ascii_digit() || *ch == '.')
                .unwrap_or(false)
            {
                chars.next();
            }
        } else {
            result.push(c);
        }
    }

    // Normalize whitespace
    result.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_replacement() {
        assert_eq!(
            fingerprint("SELECT * FROM t WHERE name = 'alice'"),
            "SELECT * FROM t WHERE name = ?"
        );
    }

    #[test]
    fn test_number_replacement() {
        assert_eq!(
            fingerprint("SELECT * FROM t WHERE id = 42 LIMIT 10"),
            "SELECT * FROM t WHERE id = ? LIMIT ?"
        );
    }

    #[test]
    fn test_same_pattern() {
        let f1 = fingerprint("SELECT * FROM t WHERE name = 'alice' AND age = 30");
        let f2 = fingerprint("SELECT * FROM t WHERE name = 'bob' AND age = 25");
        assert_eq!(f1, f2);
    }

    #[test]
    fn test_preserves_structure() {
        let f = fingerprint("SELECT a, b FROM ds.table WHERE x = 'val' ORDER BY a");
        assert!(f.contains("SELECT a, b FROM ds.table"));
        assert!(f.contains("ORDER BY a"));
    }

    #[test]
    fn test_whitespace_normalization() {
        let f = fingerprint("SELECT  *   FROM   t");
        assert_eq!(f, "SELECT * FROM t");
    }
}

#[cfg(test)]
mod extra_tests {
    use super::*;

    #[test]
    fn test_fingerprint_insert() {
        let f = fingerprint("INSERT INTO t VALUES ('a', 42)");
        assert!(f.contains("?"));
        assert!(!f.contains("42"));
    }

    #[test]
    fn test_fingerprint_multiple_strings() {
        let f = fingerprint("WHERE a = 'x' AND b = 'y' AND c = 'z'");
        assert_eq!(f.matches('?').count(), 3);
    }

    #[test]
    fn test_fingerprint_no_literals() {
        assert_eq!(fingerprint("SELECT id FROM t"), "SELECT id FROM t");
    }

    #[test]
    fn test_fingerprint_float() {
        let f = fingerprint("WHERE x > 3.14");
        assert!(f.contains("?"));
    }

    #[test]
    fn test_fingerprint_empty() {
        assert_eq!(fingerprint(""), "");
    }
}
