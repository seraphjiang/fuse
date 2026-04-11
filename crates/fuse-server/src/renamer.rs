// SPDX-License-Identifier: Apache-2.0
//! Column renamer — rename columns in query results.

use std::collections::HashMap;

/// Rename columns in a result set.
pub fn rename_columns(columns: &[String], aliases: &HashMap<String, String>) -> Vec<String> {
    columns.iter().map(|c| aliases.get(c).cloned().unwrap_or_else(|| c.clone())).collect()
}

/// Parse column aliases from SQL AS clauses.
/// e.g., "SELECT count(*) AS total, name AS user_name" → {count(*): total, name: user_name}
pub fn parse_aliases(query: &str) -> HashMap<String, String> {
    let mut aliases = HashMap::new();
    // Extract SELECT ... FROM portion
    let upper = query.to_uppercase();
    let select_part = if let Some(from_pos) = upper.find(" FROM ") {
        &query[..from_pos]
    } else {
        query
    };
    for part in select_part.split(',') {
        let trimmed = part.trim();
        if let Some(as_pos) = trimmed.to_uppercase().rfind(" AS ") {
            let original = trimmed[..as_pos].trim();
            let alias = trimmed[as_pos + 4..].trim().trim_matches('"').to_string();
            if !alias.is_empty() && !original.is_empty() {
                let col = original.split_whitespace().last().unwrap_or(original).to_string();
                aliases.insert(col, alias);
            }
        }
    }
    aliases
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rename() {
        let cols = vec!["count(*)".into(), "name".into()];
        let mut aliases = HashMap::new();
        aliases.insert("count(*)".into(), "total".into());
        let renamed = rename_columns(&cols, &aliases);
        assert_eq!(renamed, vec!["total", "name"]);
    }

    #[test]
    fn test_no_aliases() {
        let cols = vec!["a".into(), "b".into()];
        let renamed = rename_columns(&cols, &HashMap::new());
        assert_eq!(renamed, vec!["a", "b"]);
    }

    #[test]
    fn test_parse_aliases() {
        let aliases = parse_aliases("SELECT count(*) AS total, name AS user_name FROM t");
        assert_eq!(aliases.get("count(*)").map(|s| s.as_str()), Some("total"));
        assert_eq!(aliases.get("name").map(|s| s.as_str()), Some("user_name"));
    }

    #[test]
    fn test_parse_no_aliases() {
        let aliases = parse_aliases("SELECT id, name FROM t");
        assert!(aliases.is_empty());
    }
}
