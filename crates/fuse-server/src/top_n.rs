// SPDX-License-Identifier: Apache-2.0
//! TOP N support — extract and apply TOP clause from SQL queries.

/// Extract TOP N value from a SQL query.
pub fn extract_top(query: &str) -> Option<u64> {
    let upper = query.trim().to_uppercase();
    if let Some(pos) = upper.find("SELECT TOP ") {
        let after = &query[pos + 11..];
        after.trim().split_whitespace().next()?.parse().ok()
    } else {
        None
    }
}

/// Rewrite TOP N to LIMIT N for DataFusion compatibility.
pub fn rewrite_top_to_limit(query: &str) -> String {
    let upper = query.to_uppercase();
    if let Some(pos) = upper.find("SELECT TOP ") {
        let before = &query[..pos + 7]; // "SELECT "
        let after = &query[pos + 11..];
        let rest = after.trim();
        // Skip the number
        if let Some(space_pos) = rest.find(|c: char| !c.is_ascii_digit()) {
            let n = &rest[..space_pos];
            let remainder = &rest[space_pos..];
            format!("{}{} LIMIT {}", before, remainder.trim(), n)
        } else {
            query.to_string()
        }
    } else {
        query.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_top() {
        assert_eq!(extract_top("SELECT TOP 10 * FROM t"), Some(10));
        assert_eq!(extract_top("SELECT * FROM t"), None);
    }

    #[test]
    fn test_rewrite_top() {
        let result = rewrite_top_to_limit("SELECT TOP 10 * FROM t WHERE x = 1");
        assert!(result.contains("LIMIT 10"));
        assert!(!result.contains("TOP"));
    }

    #[test]
    fn test_no_top_passthrough() {
        let q = "SELECT * FROM t LIMIT 5";
        assert_eq!(rewrite_top_to_limit(q), q);
    }

    #[test]
    fn test_extract_top_large() {
        assert_eq!(extract_top("SELECT TOP 1000 id FROM t"), Some(1000));
    }
}
