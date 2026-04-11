// SPDX-License-Identifier: Apache-2.0
//! Query request validation — check parameters before execution.

/// Validation error.
#[derive(Debug, PartialEq)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
}

/// Validate a query request.
pub fn validate_request(
    query: &str,
    format: &str,
    page_size: Option<usize>,
    timeout_ms: Option<u64>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    if query.trim().is_empty() {
        errors.push(ValidationError {
            field: "query".into(),
            message: "query cannot be empty".into(),
        });
    }

    if query.len() > 1_000_000 {
        errors.push(ValidationError {
            field: "query".into(),
            message: "query exceeds maximum length (1MB)".into(),
        });
    }

    if !matches!(format, "sql" | "ppl") {
        errors.push(ValidationError {
            field: "format".into(),
            message: format!("unsupported format '{}', use 'sql' or 'ppl'", format),
        });
    }

    if let Some(ps) = page_size {
        if ps == 0 || ps > 10_000 {
            errors.push(ValidationError {
                field: "page_size".into(),
                message: "page_size must be between 1 and 10000".into(),
            });
        }
    }

    if let Some(t) = timeout_ms {
        if t > 300_000 {
            errors.push(ValidationError {
                field: "timeout_ms".into(),
                message: "timeout_ms cannot exceed 300000 (5 minutes)".into(),
            });
        }
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_request() {
        assert!(validate_request("SELECT 1", "sql", None, None).is_empty());
    }

    #[test]
    fn test_empty_query() {
        let errs = validate_request("", "sql", None, None);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "query");
    }

    #[test]
    fn test_bad_format() {
        let errs = validate_request("SELECT 1", "graphql", None, None);
        assert_eq!(errs[0].field, "format");
    }

    #[test]
    fn test_page_size_zero() {
        let errs = validate_request("SELECT 1", "sql", Some(0), None);
        assert_eq!(errs[0].field, "page_size");
    }

    #[test]
    fn test_timeout_too_large() {
        let errs = validate_request("SELECT 1", "sql", None, Some(999_999));
        assert_eq!(errs[0].field, "timeout_ms");
    }

    #[test]
    fn test_multiple_errors() {
        let errs = validate_request("", "xml", Some(0), Some(999_999));
        assert_eq!(errs.len(), 4);
    }
}
