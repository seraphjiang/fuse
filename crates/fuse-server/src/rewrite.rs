// SPDX-License-Identifier: Apache-2.0
//! Query rewrite rules — transform queries before execution.

/// A rewrite rule that transforms a query string.
pub struct RewriteRule {
    pub name: &'static str,
    pub apply: fn(&str) -> String,
}

/// Apply all rewrite rules to a query.
pub fn apply_rules(query: &str, rules: &[RewriteRule]) -> String {
    let mut q = query.to_string();
    for rule in rules {
        q = (rule.apply)(&q);
    }
    q
}

/// Default rewrite rules.
pub fn default_rules() -> Vec<RewriteRule> {
    vec![
        RewriteRule { name: "normalize_whitespace", apply: normalize_whitespace },
        RewriteRule { name: "add_default_limit", apply: add_default_limit },
    ]
}

fn normalize_whitespace(query: &str) -> String {
    query.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn add_default_limit(query: &str) -> String {
    let upper = query.to_uppercase();
    if !upper.contains("LIMIT") && upper.starts_with("SELECT") && !upper.contains("COUNT(") {
        format!("{} LIMIT 10000", query.trim_end_matches(';').trim())
    } else {
        query.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_whitespace() {
        assert_eq!(normalize_whitespace("SELECT  *   FROM  t"), "SELECT * FROM t");
    }

    #[test]
    fn test_add_default_limit() {
        assert_eq!(add_default_limit("SELECT * FROM t"), "SELECT * FROM t LIMIT 10000");
    }

    #[test]
    fn test_no_limit_on_count() {
        assert_eq!(add_default_limit("SELECT COUNT(*) FROM t"), "SELECT COUNT(*) FROM t");
    }

    #[test]
    fn test_existing_limit_preserved() {
        assert_eq!(add_default_limit("SELECT * FROM t LIMIT 5"), "SELECT * FROM t LIMIT 5");
    }

    #[test]
    fn test_apply_rules() {
        let rules = default_rules();
        let result = apply_rules("SELECT  *   FROM  t", &rules);
        assert_eq!(result, "SELECT * FROM t LIMIT 10000");
    }
}
