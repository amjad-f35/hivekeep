//! Actor grants as the Credential Access Boundary rules a downscoped GCS
//! token carries: one rule per grant, limited to its bucket and an
//! object-name prefix.

use serde::{Deserialize, Serialize};

use super::actorscope::{Access, Grant};
use crate::store::bucket_prefix;

const ROLE_OBJECT_VIEWER: &str = "inRole:roles/storage.objectViewer";
/// objectAdmin, not objectCreator: an upload overwrites on retry and deletes
/// parts after a compose.
const ROLE_OBJECT_ADMIN: &str = "inRole:roles/storage.objectAdmin";
/// The most rules one boundary may carry.
pub const MAX_RULES: usize = 10;

/// When a rule applies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvailabilityCondition {
    /// A CEL expression.
    pub expression: String,
}

/// One access boundary rule, in the STS options' JSON form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessBoundaryRule {
    /// The bucket, as a full resource name.
    pub available_resource: String,
    /// The role granted.
    pub available_permissions: Vec<String>,
    /// The object-name condition.
    pub availability_condition: AvailabilityCondition,
}

/// Quotes the two characters that would end or alter a CEL single-quoted
/// literal. A storage location may contain a quote, so it is escaped rather
/// than refused.
pub fn cel_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Builds the rules. Each prefix must be `gs://`; the condition's path ends
/// in `/`, so it cannot spill into a sibling that merely starts the same
/// way. No list term is added: the node plugin never lists.
pub fn from_grants(grants: &[Grant]) -> Result<Vec<AccessBoundaryRule>, String> {
    grants
        .iter()
        .map(|g| {
            if !g.prefix.as_str().starts_with("gs://") {
                return Err(format!(
                    "prefix {:?} is not a gs:// prefix",
                    g.prefix.as_str()
                ));
            }
            let (bucket, path) = bucket_prefix(&g.prefix).map_err(|e| e.to_string())?;
            let role = match g.access {
                Access::ReadOnly => ROLE_OBJECT_VIEWER,
                Access::ReadWrite => ROLE_OBJECT_ADMIN,
            };
            Ok(AccessBoundaryRule {
                available_resource: format!("//storage.googleapis.com/projects/_/buckets/{bucket}"),
                available_permissions: vec![role.to_string()],
                availability_condition: AvailabilityCondition {
                    expression: format!(
                        "resource.name.startsWith('projects/_/buckets/{}/objects/{}')",
                        cel_escape(&bucket),
                        cel_escape(&path)
                    ),
                },
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::actorscope::tests::prefix;
    use hivekeep_core::snapshot::StoragePrefix;

    #[test]
    fn from_grants_builds_one_rule_per_grant() {
        let grants = [
            Grant {
                prefix: prefix("gs://snaps/hivekeep-demo/hives/team-a/actors/9f1e"),
                access: Access::ReadWrite,
            },
            Grant {
                prefix: prefix("gs://snaps/hivekeep-demo/hives/hivekeep-golden/tags/7a3c"),
                access: Access::ReadOnly,
            },
        ];
        let want = vec![
            AccessBoundaryRule {
                available_resource: "//storage.googleapis.com/projects/_/buckets/snaps".into(),
                available_permissions: vec!["inRole:roles/storage.objectAdmin".into()],
                availability_condition: AvailabilityCondition {
                    expression: "resource.name.startsWith('projects/_/buckets/snaps/objects/hivekeep-demo/hives/team-a/actors/9f1e/')".into(),
                },
            },
            AccessBoundaryRule {
                available_resource: "//storage.googleapis.com/projects/_/buckets/snaps".into(),
                available_permissions: vec!["inRole:roles/storage.objectViewer".into()],
                availability_condition: AvailabilityCondition {
                    expression: "resource.name.startsWith('projects/_/buckets/snaps/objects/hivekeep-demo/hives/hivekeep-golden/tags/7a3c/')".into(),
                },
            },
        ];
        assert_eq!(from_grants(&grants).unwrap(), want);
    }

    #[test]
    fn from_grants_refuses() {
        for (name, g) in [
            (
                "not gs",
                Grant {
                    prefix: prefix("s3://b/p"),
                    access: Access::ReadOnly,
                },
            ),
            (
                "zero prefix",
                Grant {
                    prefix: StoragePrefix::default(),
                    access: Access::ReadOnly,
                },
            ),
        ] {
            assert!(from_grants(&[g]).is_err(), "{name}");
        }
    }

    #[test]
    fn from_grants_escapes_the_cel_literal() {
        let rules = from_grants(&[Grant {
            prefix: prefix("gs://snaps/tenant's/hives/team-a/actors/9f1e"),
            access: Access::ReadWrite,
        }])
        .unwrap();
        assert_eq!(
            rules[0].availability_condition.expression,
            r"resource.name.startsWith('projects/_/buckets/snaps/objects/tenant\'s/hives/team-a/actors/9f1e/')"
        );
        assert_eq!(cel_escape(r"a\b'c"), r"a\\b\'c");
    }
}
