// SPDX-License-Identifier: Apache-2.0
//! Query allowlist/denylist — restrict permitted SQL patterns.

/// Check result for a query against the policy.
#[derive(Debug, PartialEq)]
pub enum PolicyResult {
    Allowed,
    Denied(String),
}

/// Query policy with deny patterns.
pub struct QueryPolicy {
    deny_patterns: Vec<String>,
}

impl Default for QueryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryPolicy {
    pub fn new() -> Self {
        Self { deny_patterns: Vec::new() }
    }

    /// Add default deny patterns (DDL, dangerous operations).
    pub fn with_defaults() -> Self {
        let mut p = Self::new();
        for pattern in &["DROP ", "TRUNCATE ", "ALTER ", "CREATE INDEX", "GRANT ", "REVOKE "] {
            p.deny_patterns.push(pattern.to_string());
        }
        p
    }

    pub fn deny(&mut self, pattern: &str) {
        self.deny_patterns.push(pattern.to_uppercase());
    }

    pub fn check(&self, query: &str) -> PolicyResult {
        let upper = query.trim().to_uppercase();
        for pattern in &self.deny_patterns {
            if upper.contains(pattern) {
                return PolicyResult::Denied(format!("query matches deny pattern: {}", pattern.trim()));
            }
        }
        PolicyResult::Allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_allowed() {
        let p = QueryPolicy::with_defaults();
        assert_eq!(p.check("SELECT * FROM t"), PolicyResult::Allowed);
    }

    #[test]
    fn test_drop_denied() {
        let p = QueryPolicy::with_defaults();
        assert!(matches!(p.check("DROP TABLE users"), PolicyResult::Denied(_)));
    }

    #[test]
    fn test_case_insensitive() {
        let p = QueryPolicy::with_defaults();
        assert!(matches!(p.check("drop table users"), PolicyResult::Denied(_)));
    }

    #[test]
    fn test_custom_deny() {
        let mut p = QueryPolicy::new();
        p.deny("DELETE ");
        assert!(matches!(p.check("DELETE FROM t"), PolicyResult::Denied(_)));
        assert_eq!(p.check("SELECT * FROM t"), PolicyResult::Allowed);
    }

    #[test]
    fn test_empty_policy_allows_all() {
        let p = QueryPolicy::new();
        assert_eq!(p.check("DROP TABLE x"), PolicyResult::Allowed);
    }
}
