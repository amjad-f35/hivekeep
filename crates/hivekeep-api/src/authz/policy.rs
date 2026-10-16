//! The built-in authorizer over stored access policies.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{Store, StoreError};
use tonic::Status;

use super::model::{Grants, Object, Role};
use super::Authorizer;
use crate::context::{Principal, PrincipalKind};

/// SPIFFE IDs of the system's own components. Each holds its role when it
/// calls over mTLS.
#[derive(Debug, Clone, Default)]
pub struct SystemPrincipals {
    /// The worker controller.
    pub controller: String,
    /// The egress gateway.
    pub egress_gateway: String,
    /// The ingress router.
    pub ingress_router: String,
    /// Node keepers.
    pub keeper: String,
    /// The API server pod's sidecars.
    pub api_server_sidecar: String,
}

impl SystemPrincipals {
    /// Refuses a missing ID, or one that is not a SPIFFE ID. One ID may
    /// hold several roles.
    pub fn validate(&self) -> Result<(), String> {
        for (role, id) in [
            ("controller", &self.controller),
            ("egress_gateway", &self.egress_gateway),
            ("ingress_router", &self.ingress_router),
            ("keeper", &self.keeper),
            ("api_server_sidecar", &self.api_server_sidecar),
        ] {
            if id.is_empty() {
                return Err(format!("invalid {role} SPIFFE ID {id:?}: empty"));
            }
            hivekeep_core::spiffe::validate_spiffe_id(id)
                .map_err(|e| format!("invalid {role} SPIFFE ID {id:?}: {e}"))?;
        }
        Ok(())
    }

    fn roles(&self) -> HashMap<String, Vec<Role>> {
        let mut out: HashMap<String, Vec<Role>> = HashMap::new();
        for (id, role) in [
            (&self.controller, Role::Controller),
            (&self.egress_gateway, Role::EgressGateway),
            (&self.ingress_router, Role::IngressRouter),
            (&self.keeper, Role::Keeper),
            (&self.api_server_sidecar, Role::ApiServerSidecar),
        ] {
            if !id.is_empty() {
                out.entry(id.clone()).or_default().push(role);
            }
        }
        out
    }
}

/// Evaluates the stored global and Hive policies. Bootstrap owners are
/// global owners while configured, whatever the stored policy says, so they
/// can always create or repair it.
pub struct PolicyAuthorizer {
    store: Arc<dyn Store>,
    bootstrap_owners: HashSet<String>,
    system: HashMap<String, Vec<Role>>,
}

/// The binding member string of a principal id.
pub fn member_of(id: &str) -> String {
    format!("user:{id}")
}

fn roles_of<'a>(policy: &'a pb::AccessPolicy, member: &str) -> impl Iterator<Item = Role> + 'a {
    let member = member.to_string();
    policy
        .bindings
        .iter()
        .filter(move |b| b.members.iter().any(|m| m == &member))
        .filter_map(|b| Role::parse(&b.role))
}

impl PolicyAuthorizer {
    /// An authorizer reading policies from `store`.
    pub fn new(
        store: Arc<dyn Store>,
        bootstrap_owners: impl IntoIterator<Item = String>,
        system: SystemPrincipals,
    ) -> Self {
        PolicyAuthorizer {
            store,
            bootstrap_owners: bootstrap_owners.into_iter().collect(),
            system: system.roles(),
        }
    }

    /// The roles `p` holds for `object`.
    pub async fn grants(&self, p: &Principal, object: &Object) -> Result<Grants, Status> {
        let member = member_of(&p.id);
        let mut g = Grants::default();
        if self.bootstrap_owners.contains(&p.id) {
            g.global.insert(Role::Owner);
        }
        if p.kind == PrincipalKind::Mtls {
            if let Some(roles) = self.system.get(&p.id) {
                g.global.extend(roles.iter().copied());
            }
        }
        match self.store.get_global_access_policy().await {
            Ok(pol) => g.global.extend(
                roles_of(&pol, &member).filter(|r| matches!(r, Role::Owner | Role::Viewer)),
            ),
            Err(StoreError::NotFound) => {}
            Err(e) => {
                return Err(Status::unavailable(format!(
                    "authz: reading global access policy: {e}"
                )))
            }
        }
        if let Some(hive) = object.hive() {
            match self.store.get_hive_access_policy(hive).await {
                Ok(pol) => g.hive.extend(roles_of(&pol, &member)),
                Err(StoreError::NotFound) => {}
                Err(e) => {
                    return Err(Status::unavailable(format!(
                        "authz: reading access policy of hive {hive}: {e}"
                    )))
                }
            }
        }
        Ok(g)
    }
}

#[async_trait]
impl Authorizer for PolicyAuthorizer {
    async fn check(&self, p: &Principal, relation: &str, object: &Object) -> Result<(), Status> {
        if self.grants(p, object).await?.allows(relation, object) {
            Ok(())
        } else {
            Err(Status::permission_denied(format!(
                "permission denied: principal {:?} lacks {relation:?} on {:?}",
                member_of(&p.id),
                object.to_string()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authz::model::rel;
    use hivekeep_store::{AccessPolicyStore, HiveStore, MemoryStore};

    fn policy(bindings: &[(&str, &str)]) -> pb::AccessPolicy {
        pb::AccessPolicy {
            metadata: Some(pb::ResourceMetadata {
                name: "default".into(),
                ..Default::default()
            }),
            bindings: bindings
                .iter()
                .map(|(r, m)| pb::Binding {
                    role: r.to_string(),
                    members: vec![m.to_string()],
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn evaluates_stored_policies() {
        let store = Arc::new(MemoryStore::new());
        store
            .create_hive(&pb::Hive {
                metadata: Some(pb::ResourceMetadata {
                    name: "h".into(),
                    ..Default::default()
                }),
            })
            .await
            .unwrap();
        store
            .create_global_access_policy(&policy(&[("viewer", "user:gv")]))
            .await
            .unwrap();
        store
            .create_hive_access_policy("h", &policy(&[("editor", "user:ed")]))
            .await
            .unwrap();
        let sys = SystemPrincipals {
            ingress_router: "spiffe://router".into(),
            ..Default::default()
        };
        let a = PolicyAuthorizer::new(store, ["boot".to_string()], sys);
        let actor = Object::Actor {
            hive: "h".into(),
            name: "a".into(),
        };

        assert!(a
            .check(&Principal::jwt("ed", "i"), rel::CAN_RESUME, &actor)
            .await
            .is_ok());
        assert!(a
            .check(
                &Principal::jwt("ed", "i"),
                rel::CAN_CREATE_HIVE,
                &Object::Global
            )
            .await
            .is_err());
        assert!(a
            .check(&Principal::jwt("gv", "i"), rel::CAN_GET, &actor)
            .await
            .is_ok());
        let denied = a
            .check(&Principal::jwt("gv", "i"), rel::CAN_DELETE, &actor)
            .await
            .unwrap_err();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
        assert!(a
            .check(
                &Principal::jwt("boot", "i"),
                rel::CAN_CREATE_HIVE,
                &Object::Global
            )
            .await
            .is_ok());
        assert!(a
            .check(&Principal::jwt("nobody", "i"), rel::CAN_GET, &actor)
            .await
            .is_err());
        // System roles apply only over mTLS.
        assert!(a
            .check(&Principal::mtls("spiffe://router"), rel::CAN_RESUME, &actor)
            .await
            .is_ok());
        assert!(a
            .check(
                &Principal::jwt("spiffe://router", "i"),
                rel::CAN_RESUME,
                &actor
            )
            .await
            .is_err());
    }
}
