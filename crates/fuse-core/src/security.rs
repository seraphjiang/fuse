// SPDX-License-Identifier: Apache-2.0

//! RBAC and field-level security for federated query results.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use serde::Deserialize;

use crate::error::FuseError;

// ── Config ──

/// Security configuration loaded from `[security]` in fuse.toml.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SecurityConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, rename = "policy")]
    pub policies: Vec<PolicyConfig>,
}

/// A single field-level access policy from config.
#[derive(Debug, Clone, Deserialize)]
pub struct PolicyConfig {
    pub datasource: String,
    #[serde(default)]
    pub index: Option<String>,
    #[serde(default)]
    pub deny_fields: Vec<String>,
    #[serde(default)]
    pub mask_fields: Vec<String>,
    #[serde(default)]
    pub allowed_roles: Vec<String>,
}

// ── Policy types ──

/// What to do with a field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldPolicy {
    Allow,
    Deny,
    Mask,
}

/// User context for authorization decisions.
#[derive(Debug, Clone, Default)]
pub struct UserContext {
    pub username: String,
    pub roles: Vec<String>,
}

// ── Policy engine ──

/// Evaluates access policies against user context and query targets.
pub struct PolicyEngine {
    policies: Vec<PolicyConfig>,
}

impl PolicyEngine {
    pub fn new(config: &SecurityConfig) -> Self {
        Self {
            policies: config.policies.clone(),
        }
    }

    /// Find all policies that apply to a given datasource + index.
    fn matching_policies(&self, datasource: &str, index: &str) -> Vec<&PolicyConfig> {
        self.policies
            .iter()
            .filter(|p| {
                p.datasource == datasource
                    && p.index.as_ref().is_none_or(|i| i == index)
            })
            .collect()
    }

    /// Determine the policy for each field given user context.
    pub fn evaluate(
        &self,
        datasource: &str,
        index: &str,
        fields: &[String],
        user: &UserContext,
    ) -> HashMap<String, FieldPolicy> {
        let matching = self.matching_policies(datasource, index);
        let mut result: HashMap<String, FieldPolicy> = fields
            .iter()
            .map(|f| (f.clone(), FieldPolicy::Allow))
            .collect();

        for policy in &matching {
            // Skip policy if user has an allowed role
            if !policy.allowed_roles.is_empty()
                && user.roles.iter().any(|r| policy.allowed_roles.contains(r))
            {
                continue;
            }

            for field in &policy.deny_fields {
                if let Some(fp) = result.get_mut(field) {
                    *fp = FieldPolicy::Deny;
                }
            }
            for field in &policy.mask_fields {
                if let Some(fp) = result.get_mut(field) {
                    if *fp != FieldPolicy::Deny {
                        *fp = FieldPolicy::Mask;
                    }
                }
            }
        }

        result
    }
}

// ── Result filter ──

/// Post-query filter that removes denied fields and masks restricted fields.
pub struct ResultFilter {
    engine: PolicyEngine,
}

impl ResultFilter {
    pub fn new(engine: PolicyEngine) -> Self {
        Self { engine }
    }

    /// Apply field-level security to a set of RecordBatches.
    pub fn filter_batches(
        &self,
        batches: Vec<RecordBatch>,
        datasource: &str,
        index: &str,
        user: &UserContext,
    ) -> Result<Vec<RecordBatch>, FuseError> {
        if batches.is_empty() {
            return Ok(batches);
        }

        let schema = batches[0].schema();
        let field_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let policies = self.engine.evaluate(datasource, index, &field_names, user);

        // Build new schema: skip denied fields
        let kept: Vec<(usize, &Field, &FieldPolicy)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                let policy = policies.get(f.name()).unwrap_or(&FieldPolicy::Allow);
                if *policy == FieldPolicy::Deny {
                    None
                } else {
                    Some((i, f.as_ref(), policy))
                }
            })
            .collect();

        let new_schema = Arc::new(Schema::new(
            kept.iter().map(|(_, f, _)| (*f).clone()).collect::<Vec<_>>(),
        ));

        batches
            .into_iter()
            .map(|batch| {
                let columns: Vec<Arc<dyn Array>> = kept
                    .iter()
                    .map(|(i, _, policy)| {
                        let col = batch.column(*i);
                        if **policy == FieldPolicy::Mask {
                            mask_column(col)
                        } else {
                            col.clone()
                        }
                    })
                    .collect();
                RecordBatch::try_new(new_schema.clone(), columns)
                    .map_err(FuseError::Arrow)
            })
            .collect()
    }
}

/// Replace all non-null values in a column with "****".
fn mask_column(col: &Arc<dyn Array>) -> Arc<dyn Array> {
    let masked: Vec<Option<&str>> = (0..col.len())
        .map(|i| if col.is_null(i) { None } else { Some("****") })
        .collect();
    Arc::new(StringArray::from(masked))
}

// ── Datasource-level RBAC (#921) ──

/// Permission level for datasource access.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatasourcePermission {
    Read,
    Write,
    Admin,
}

/// A datasource access rule: which roles can access which datasources.
#[derive(Debug, Clone, Deserialize)]
pub struct DatasourceAccessRule {
    pub datasource: String,
    pub roles: Vec<String>,
    pub permissions: Vec<DatasourcePermission>,
}

/// Evaluates datasource-level access control.
pub struct DatasourceRbac {
    rules: Vec<DatasourceAccessRule>,
}

impl DatasourceRbac {
    pub fn new(rules: Vec<DatasourceAccessRule>) -> Self {
        Self { rules }
    }

    /// Check if a user has the given permission on a datasource.
    pub fn check(
        &self,
        user: &UserContext,
        datasource: &str,
        required: &DatasourcePermission,
    ) -> bool {
        // If no rules defined, allow all (open by default)
        if self.rules.is_empty() {
            return true;
        }
        self.rules.iter().any(|rule| {
            rule.datasource == datasource
                && rule.permissions.contains(required)
                && rule.roles.iter().any(|r| user.roles.contains(r))
        })
    }

    /// Filter a list of datasource IDs to only those the user can read.
    pub fn filter_readable(&self, user: &UserContext, datasources: &[String]) -> Vec<String> {
        datasources
            .iter()
            .filter(|ds| self.check(user, ds, &DatasourcePermission::Read))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod rbac_tests {
    use super::*;

    fn rules() -> Vec<DatasourceAccessRule> {
        vec![
            DatasourceAccessRule {
                datasource: "prod_logs".into(),
                roles: vec!["admin".into(), "analyst".into()],
                permissions: vec![DatasourcePermission::Read],
            },
            DatasourceAccessRule {
                datasource: "prod_logs".into(),
                roles: vec!["admin".into()],
                permissions: vec![DatasourcePermission::Write, DatasourcePermission::Admin],
            },
            DatasourceAccessRule {
                datasource: "dev_logs".into(),
                roles: vec!["admin".into(), "analyst".into(), "developer".into()],
                permissions: vec![DatasourcePermission::Read, DatasourcePermission::Write],
            },
        ]
    }

    #[test]
    fn test_analyst_can_read_prod() {
        let rbac = DatasourceRbac::new(rules());
        let user = UserContext { username: "alice".into(), roles: vec!["analyst".into()] };
        assert!(rbac.check(&user, "prod_logs", &DatasourcePermission::Read));
    }

    #[test]
    fn test_analyst_cannot_write_prod() {
        let rbac = DatasourceRbac::new(rules());
        let user = UserContext { username: "alice".into(), roles: vec!["analyst".into()] };
        assert!(!rbac.check(&user, "prod_logs", &DatasourcePermission::Write));
    }

    #[test]
    fn test_admin_can_write_prod() {
        let rbac = DatasourceRbac::new(rules());
        let user = UserContext { username: "bob".into(), roles: vec!["admin".into()] };
        assert!(rbac.check(&user, "prod_logs", &DatasourcePermission::Write));
    }

    #[test]
    fn test_developer_can_write_dev() {
        let rbac = DatasourceRbac::new(rules());
        let user = UserContext { username: "carol".into(), roles: vec!["developer".into()] };
        assert!(rbac.check(&user, "dev_logs", &DatasourcePermission::Write));
    }

    #[test]
    fn test_unknown_datasource_denied() {
        let rbac = DatasourceRbac::new(rules());
        let user = UserContext { username: "bob".into(), roles: vec!["admin".into()] };
        assert!(!rbac.check(&user, "secret_db", &DatasourcePermission::Read));
    }

    #[test]
    fn test_no_roles_denied() {
        let rbac = DatasourceRbac::new(rules());
        let user = UserContext { username: "nobody".into(), roles: vec![] };
        assert!(!rbac.check(&user, "prod_logs", &DatasourcePermission::Read));
    }

    #[test]
    fn test_empty_rules_allows_all() {
        let rbac = DatasourceRbac::new(vec![]);
        let user = UserContext { username: "anyone".into(), roles: vec![] };
        assert!(rbac.check(&user, "anything", &DatasourcePermission::Read));
    }

    #[test]
    fn test_filter_readable() {
        let rbac = DatasourceRbac::new(rules());
        let user = UserContext { username: "carol".into(), roles: vec!["developer".into()] };
        let all = vec!["prod_logs".into(), "dev_logs".into(), "secret_db".into()];
        let readable = rbac.filter_readable(&user, &all);
        assert_eq!(readable, vec!["dev_logs"]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn test_config() -> SecurityConfig {
        SecurityConfig {
            enabled: true,
            policies: vec![PolicyConfig {
                datasource: "prod".into(),
                index: Some("logs".into()),
                deny_fields: vec!["ssn".into()],
                mask_fields: vec!["email".into()],
                allowed_roles: vec!["admin".into()],
            }],
        }
    }

    #[test]
    fn test_regular_user_gets_restrictions() {
        let engine = PolicyEngine::new(&test_config());
        let user = UserContext {
            username: "analyst".into(),
            roles: vec!["analyst".into()],
        };
        let fields = vec!["name".into(), "email".into(), "ssn".into()];
        let result = engine.evaluate("prod", "logs", &fields, &user);

        assert_eq!(result["name"], FieldPolicy::Allow);
        assert_eq!(result["email"], FieldPolicy::Mask);
        assert_eq!(result["ssn"], FieldPolicy::Deny);
    }

    #[test]
    fn test_admin_bypasses_restrictions() {
        let engine = PolicyEngine::new(&test_config());
        let user = UserContext {
            username: "boss".into(),
            roles: vec!["admin".into()],
        };
        let fields = vec!["name".into(), "email".into(), "ssn".into()];
        let result = engine.evaluate("prod", "logs", &fields, &user);

        assert_eq!(result["name"], FieldPolicy::Allow);
        assert_eq!(result["email"], FieldPolicy::Allow);
        assert_eq!(result["ssn"], FieldPolicy::Allow);
    }

    #[test]
    fn test_different_datasource_no_restrictions() {
        let engine = PolicyEngine::new(&test_config());
        let user = UserContext {
            username: "analyst".into(),
            roles: vec![],
        };
        let fields = vec!["ssn".into(), "email".into()];
        let result = engine.evaluate("staging", "logs", &fields, &user);

        assert_eq!(result["ssn"], FieldPolicy::Allow);
        assert_eq!(result["email"], FieldPolicy::Allow);
    }

    #[test]
    fn test_filter_batches() {
        let engine = PolicyEngine::new(&test_config());
        let filter = ResultFilter::new(engine);
        let user = UserContext {
            username: "analyst".into(),
            roles: vec!["analyst".into()],
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
            Field::new("ssn", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(StringArray::from(vec!["a@example.com", "b@example.com"])),
                Arc::new(StringArray::from(vec!["123-45-6789", "987-65-4321"])),
            ],
        )
        .unwrap();

        let result = filter
            .filter_batches(vec![batch], "prod", "logs", &user)
            .unwrap();

        assert_eq!(result.len(), 1);
        let b = &result[0];
        // ssn should be removed
        assert_eq!(b.num_columns(), 2);
        assert_eq!(b.schema().field(0).name(), "name");
        assert_eq!(b.schema().field(1).name(), "email");
        // email should be masked
        let email_col = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(email_col.value(0), "****");
        assert_eq!(email_col.value(1), "****");
    }

    #[test]
    fn test_empty_policies_allows_everything() {
        let config = SecurityConfig {
            enabled: true,
            policies: vec![],
        };
        let engine = PolicyEngine::new(&config);
        let user = UserContext {
            username: "anyone".into(),
            roles: vec![],
        };
        let fields = vec!["ssn".into(), "email".into(), "name".into()];
        let result = engine.evaluate("prod", "logs", &fields, &user);
        assert!(result.values().all(|p| *p == FieldPolicy::Allow));
    }

    #[test]
    fn test_overlapping_policies_deny_wins_over_mask() {
        let config = SecurityConfig {
            enabled: true,
            policies: vec![
                PolicyConfig {
                    datasource: "prod".into(),
                    index: Some("logs".into()),
                    deny_fields: vec![],
                    mask_fields: vec!["secret".into()],
                    allowed_roles: vec![],
                },
                PolicyConfig {
                    datasource: "prod".into(),
                    index: Some("logs".into()),
                    deny_fields: vec!["secret".into()],
                    mask_fields: vec![],
                    allowed_roles: vec![],
                },
            ],
        };
        let engine = PolicyEngine::new(&config);
        let user = UserContext {
            username: "analyst".into(),
            roles: vec![],
        };
        let fields = vec!["secret".into()];
        let result = engine.evaluate("prod", "logs", &fields, &user);
        assert_eq!(result["secret"], FieldPolicy::Deny);
    }

    #[test]
    fn test_filter_batches_empty_input() {
        let engine = PolicyEngine::new(&test_config());
        let filter = ResultFilter::new(engine);
        let user = UserContext {
            username: "analyst".into(),
            roles: vec![],
        };
        let result = filter.filter_batches(vec![], "prod", "logs", &user).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_mask_column_with_nulls() {
        let col: Arc<dyn Array> = Arc::new(StringArray::from(vec![
            Some("real"),
            None,
            Some("data"),
        ]));
        let masked = mask_column(&col);
        let arr = masked.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "****");
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), "****");
    }
}
