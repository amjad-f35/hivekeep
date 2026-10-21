//! IAM policies as `gcloud ... get-iam-policy --format=json` prints them,
//! and the idempotent edits made to them.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Binding {
    pub role: String,
    #[serde(default)]
    pub members: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    #[serde(default)]
    pub bindings: Vec<Binding>,
    /// etag, version and anything else the API returns, sent back unchanged
    /// so concurrent edits are detected.
    #[serde(flatten)]
    pub rest: BTreeMap<String, Value>,
}

impl Policy {
    /// The unconditional members of `role`.
    pub fn members(&self, role: &str) -> Vec<String> {
        self.bindings
            .iter()
            .filter(|b| b.role == role && b.condition.is_none())
            .flat_map(|b| b.members.iter().cloned())
            .collect()
    }

    /// Whether `member` holds `role` without a condition. Conditional
    /// bindings are ignored: they may not apply to what Hivekeep touches.
    pub fn has_unconditional_binding(&self, role: &str, member: &str) -> bool {
        self.bindings.iter().any(|b| {
            b.condition.is_none() && b.role == role && b.members.iter().any(|m| m == member)
        })
    }

    /// Adds `member` to the unconditional binding of `role`, creating it if
    /// needed, and reports whether anything changed.
    pub fn add_binding(&mut self, role: &str, member: &str) -> bool {
        for b in &mut self.bindings {
            if b.role != role || b.condition.is_some() {
                continue;
            }
            if b.members.iter().any(|m| m == member) {
                return false;
            }
            b.members.push(member.to_string());
            return true;
        }
        self.bindings.push(Binding {
            role: role.into(),
            members: vec![member.into()],
            condition: None,
        });
        true
    }
}

/// Grants `member` the project role; reports whether the policy changed.
pub fn add_project_iam_binding(policy: &mut Policy, role: &str, member: &str) -> bool {
    policy.add_binding(role, member)
}

/// Grants `member` the role on a service account; reports whether the policy
/// changed.
pub fn add_service_account_binding(policy: &mut Policy, role: &str, member: &str) -> bool {
    policy.add_binding(role, member)
}
