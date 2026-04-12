// SPDX-License-Identifier: Apache-2.0
//! Cache key builder — standardize cache key construction.

/// Build a cache key from query components.
pub fn build_key(format: &str, query: &str, tenant: Option<&str>) -> String {
    let normalized = query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    match tenant {
        Some(t) => format!("{}:{}:{}", t, format, normalized),
        None => format!("{}:{}", format, normalized),
    }
}

/// Build a cache key with parameter binding.
pub fn build_key_with_params(format: &str, query: &str, params: &[String]) -> String {
    let base = build_key(format, query, None);
    if params.is_empty() {
        base
    } else {
        format!("{}|{}", base, params.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_key() {
        let k = build_key("sql", "SELECT * FROM t", None);
        assert_eq!(k, "sql:select * from t");
    }

    #[test]
    fn test_key_with_tenant() {
        let k = build_key("sql", "SELECT 1", Some("team_a"));
        assert!(k.starts_with("team_a:sql:"));
    }

    #[test]
    fn test_whitespace_normalized() {
        let k1 = build_key("sql", "SELECT  *   FROM  t", None);
        let k2 = build_key("sql", "SELECT * FROM t", None);
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_case_normalized() {
        let k1 = build_key("sql", "SELECT * FROM T", None);
        let k2 = build_key("sql", "select * from t", None);
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_with_params() {
        let k = build_key_with_params("sql", "SELECT $1", &["42".into()]);
        assert!(k.contains("|42"));
    }
}
