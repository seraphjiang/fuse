// SPDX-License-Identifier: Apache-2.0
//! Query parser utilities — extract SQL components.

/// Extract table references from a simple SQL query.
pub fn extract_tables(query: &str) -> Vec<String> {
    let upper = query.to_uppercase();
    let mut tables = Vec::new();
    for keyword in &["FROM ", "JOIN "] {
        let mut pos = 0;
        while let Some(idx) = upper[pos..].find(keyword) {
            let start = pos + idx + keyword.len();
            if let Some(token) = query[start..].split_whitespace().next() {
                let clean = token.trim_end_matches([',', ')', ';']);
                if !clean.is_empty() && clean.contains('.') {
                    tables.push(clean.to_string());
                }
            }
            pos = start;
        }
    }
    tables
}

/// Extract LIMIT value from SQL.
pub fn extract_limit(query: &str) -> Option<u64> {
    let upper = query.to_uppercase();
    if let Some(pos) = upper.rfind("LIMIT ") {
        let after = query[pos + 6..].trim();
        after.split_whitespace().next()?.parse().ok()
    } else {
        None
    }
}

/// Check if query is read-only (SELECT/EXPLAIN only).
pub fn is_read_only(query: &str) -> bool {
    let trimmed = query.trim().to_uppercase();
    trimmed.starts_with("SELECT") || trimmed.starts_with("EXPLAIN") || trimmed.starts_with("SHOW")
}

/// Extract format hint from query comment (e.g., /* format:ppl */).
pub fn extract_format_hint(query: &str) -> Option<String> {
    if let Some(start) = query.find("/* format:") {
        let rest = &query[start + 10..];
        if let Some(end) = rest.find("*/") {
            return Some(rest[..end].trim().to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_tables() {
        let tables = extract_tables("SELECT * FROM ds1.t1 JOIN ds2.t2 ON ds1.t1.id = ds2.t2.id");
        assert_eq!(tables, vec!["ds1.t1", "ds2.t2"]);
    }

    #[test]
    fn test_extract_tables_single() {
        let tables = extract_tables("SELECT * FROM cluster_a.logs WHERE status >= 500");
        assert_eq!(tables, vec!["cluster_a.logs"]);
    }

    #[test]
    fn test_extract_limit() {
        assert_eq!(extract_limit("SELECT * FROM t LIMIT 100"), Some(100));
        assert_eq!(extract_limit("SELECT * FROM t"), None);
    }

    #[test]
    fn test_is_read_only() {
        assert!(is_read_only("SELECT * FROM t"));
        assert!(is_read_only("EXPLAIN SELECT 1"));
        assert!(!is_read_only("DROP TABLE t"));
        assert!(!is_read_only("INSERT INTO t VALUES (1)"));
    }

    #[test]
    fn test_format_hint() {
        assert_eq!(extract_format_hint("/* format:ppl */ source = t"), Some("ppl".into()));
        assert_eq!(extract_format_hint("SELECT 1"), None);
    }

    #[test]
    fn test_extract_tables_no_qualified() {
        let tables = extract_tables("SELECT * FROM local_table");
        assert!(tables.is_empty()); // no dot = not qualified
    }

    #[test]
    fn test_limit_with_offset() {
        assert_eq!(extract_limit("SELECT * FROM t LIMIT 50 OFFSET 10"), Some(50));
    }

    #[test]
    fn test_read_only_show() {
        assert!(is_read_only("SHOW TABLES"));
    }

    #[test]
    fn test_extract_tables_union() {
        let tables = extract_tables("SELECT * FROM ds1.t1 UNION ALL SELECT * FROM ds2.t2");
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_extract_tables_subquery() {
        let tables = extract_tables("SELECT * FROM ds.t WHERE id IN (SELECT id FROM ds2.t2)");
        assert_eq!(tables.len(), 2);
    }
}
