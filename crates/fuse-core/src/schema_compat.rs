// SPDX-License-Identifier: Apache-2.0
//! Schema compatibility checker — compare schemas across datasources.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SchemaCompat {
    pub compatible: bool,
    pub common_columns: Vec<String>,
    pub left_only: Vec<String>,
    pub right_only: Vec<String>,
}

/// Compare two column lists for compatibility (e.g., for UNION).
pub fn check_compatibility(left: &[String], right: &[String]) -> SchemaCompat {
    let left_set: std::collections::HashSet<&str> = left.iter().map(|s| s.as_str()).collect();
    let right_set: std::collections::HashSet<&str> = right.iter().map(|s| s.as_str()).collect();

    let common: Vec<String> = left_set
        .intersection(&right_set)
        .map(|s| s.to_string())
        .collect();
    let left_only: Vec<String> = left_set
        .difference(&right_set)
        .map(|s| s.to_string())
        .collect();
    let right_only: Vec<String> = right_set
        .difference(&left_set)
        .map(|s| s.to_string())
        .collect();

    SchemaCompat {
        compatible: left_only.is_empty() && right_only.is_empty(),
        common_columns: common,
        left_only,
        right_only,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identical_schemas() {
        let cols = vec!["id".into(), "name".into()];
        let r = check_compatibility(&cols, &cols);
        assert!(r.compatible);
        assert_eq!(r.common_columns.len(), 2);
    }

    #[test]
    fn test_different_schemas() {
        let left = vec!["id".into(), "name".into()];
        let right = vec!["id".into(), "email".into()];
        let r = check_compatibility(&left, &right);
        assert!(!r.compatible);
        assert_eq!(r.common_columns.len(), 1);
        assert_eq!(r.left_only.len(), 1);
        assert_eq!(r.right_only.len(), 1);
    }

    #[test]
    fn test_subset() {
        let left = vec!["id".into(), "name".into(), "age".into()];
        let right = vec!["id".into(), "name".into()];
        let r = check_compatibility(&left, &right);
        assert!(!r.compatible);
        assert_eq!(r.left_only.len(), 1);
        assert!(r.right_only.is_empty());
    }

    #[test]
    fn test_empty() {
        let r = check_compatibility(&[], &[]);
        assert!(r.compatible);
    }
}
