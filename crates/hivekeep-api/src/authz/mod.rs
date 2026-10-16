//! Authorization of Control RPCs.
//!
//! Each RPC requires one or more relations on objects derived from its
//! request (see [`checks`]). An [`Authorizer`] decides them.
//! [`PolicyAuthorizer`] evaluates the stored global and Hive
//! [`AccessPolicy`](hivekeep_proto::api::v1::AccessPolicy) bindings, the
//! server's bootstrap owners and the roles of the system's own components;
//! [`AllowAll`] permits everything for development.

pub mod checks;
pub mod model;
pub mod policy;

use async_trait::async_trait;
use tonic::Status;

use crate::context::{CallContext, Principal};

pub use checks::Check;
pub use model::Object;
pub use policy::{PolicyAuthorizer, SystemPrincipals};

/// Decides whether a principal holds a relation on an object.
#[async_trait]
pub trait Authorizer: Send + Sync {
    /// `Ok` when `principal` has `relation` on `object`; PERMISSION_DENIED
    /// otherwise.
    async fn check(
        &self,
        principal: &Principal,
        relation: &str,
        object: &Object,
    ) -> Result<(), Status>;
}

/// Permits every authenticated request.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

#[async_trait]
impl Authorizer for AllowAll {
    async fn check(&self, _: &Principal, _: &str, _: &Object) -> Result<(), Status> {
        Ok(())
    }
}

/// Runs `checks` for the caller. System calls pass; a request without a
/// principal is UNAUTHENTICATED.
pub async fn authorize(
    authz: &dyn Authorizer,
    cc: &CallContext,
    checks: Result<Vec<Check>, Status>,
) -> Result<(), Status> {
    if cc.system {
        return Ok(());
    }
    let Some(p) = cc.principal.as_ref().filter(|p| !p.id.is_empty()) else {
        return Err(Status::unauthenticated(
            "unauthenticated: missing principal in context",
        ));
    };
    for c in checks? {
        authz.check(p, c.relation, &c.object).await?;
    }
    Ok(())
}
