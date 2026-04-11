// SPDX-License-Identifier: Apache-2.0
//! Connector config validation — check configs before registration.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ConfigError {
    pub field: String,
    pub message: String,
}

/// Validate a connector config has required fields.
pub fn validate_connector_config(
    id: &str,
    connector_type: &str,
    properties: &std::collections::HashMap<String, String>,
) -> Vec<ConfigError> {
    let mut errors = Vec::new();

    if id.is_empty() {
        errors.push(ConfigError { field: "id".into(), message: "connector id cannot be empty".into() });
    }

    if connector_type.is_empty() {
        errors.push(ConfigError { field: "type".into(), message: "connector type cannot be empty".into() });
    }

    // Type-specific validation
    match connector_type {
        "opensearch" | "elasticsearch" => {
            if !properties.contains_key("url") {
                errors.push(ConfigError { field: "url".into(), message: format!("{} requires 'url'", connector_type) });
            }
        }
        "postgres" | "mysql" => {
            if !properties.contains_key("url") {
                errors.push(ConfigError { field: "url".into(), message: "SQL connector requires 'url'".into() });
            }
        }
        "dynamodb" => {} // uses IAM, no required fields
        "s3" => {
            if !properties.contains_key("bucket") {
                errors.push(ConfigError { field: "bucket".into(), message: "S3 connector requires 'bucket'".into() });
            }
        }
        _ => {} // unknown types pass through
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_valid_opensearch() {
        let mut props = HashMap::new();
        props.insert("url".into(), "https://example.com".into());
        assert!(validate_connector_config("my_os", "opensearch", &props).is_empty());
    }

    #[test]
    fn test_missing_url() {
        let errs = validate_connector_config("my_os", "opensearch", &HashMap::new());
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "url");
    }

    #[test]
    fn test_empty_id() {
        let errs = validate_connector_config("", "opensearch", &HashMap::new());
        assert!(errs.iter().any(|e| e.field == "id"));
    }

    #[test]
    fn test_s3_requires_bucket() {
        let errs = validate_connector_config("my_s3", "s3", &HashMap::new());
        assert!(errs.iter().any(|e| e.field == "bucket"));
    }

    #[test]
    fn test_dynamodb_no_required() {
        assert!(validate_connector_config("ddb", "dynamodb", &HashMap::new()).is_empty());
    }

    #[test]
    fn test_unknown_type_passes() {
        assert!(validate_connector_config("x", "custom", &HashMap::new()).is_empty());
    }
}
