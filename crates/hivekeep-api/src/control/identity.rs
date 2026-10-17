//! Actor identity RPCs: JWTs and SPIFFE certificates, for the egress
//! gateway.

use std::time::{Duration, SystemTime};

use hivekeep_core::resource::ActorRef;
use hivekeep_core::validation::api as v;
use hivekeep_identity::actor_cert::{mint_actor_certificate, ActorCertKind};
use hivekeep_identity::extension::ActorIdentity;
use hivekeep_identity::jwt::claims::Claims;
use hivekeep_identity::IdentityError;
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::StoreError;

use super::{invalid, ControlService};
use crate::authz::{checks, model::rel};
use crate::context::CallContext;
use crate::error::{status, Error, Result};
use crate::fields;

/// Maps a minting failure: a refused or malformed request is the caller's
/// fault, anything else is internal.
pub(crate) fn mint_error(e: IdentityError, what: &str) -> Error {
    match e {
        IdentityError::Csr(m) => status::invalid_argument(format!("Failed to verify CSR: {m}")),
        IdentityError::Refused(m) => status::invalid_argument(m),
        e => status::internal(format!("while signing {what}: {e}")),
    }
}

impl ControlService {
    async fn load_actor_for_mint(&self, r: Option<&pb::ObjectRef>) -> Result<pb::Actor> {
        let r = r
            .map(ActorRef::from_object_ref)
            .unwrap_or_else(|| ActorRef::new("", ""));
        self.store().get_actor(&r).await.map_err(|e| match e {
            StoreError::NotFound => status::not_found("actor not found"),
            e => Error::from(e).context("while retrieving actor"),
        })
    }

    pub(crate) async fn mint_actor_jwt(
        &self,
        cc: &CallContext,
        req: pb::MintActorJwtRequest,
    ) -> Result<pb::MintActorJwtResponse> {
        self.authorize(
            cc,
            checks::actor_ref(rel::CAN_MINT_ACTOR_JWT, req.actor.as_ref()),
        )
        .await?;
        invalid(v::validate_mint_actor_jwt_request(&self.op(cc), &req))?;
        // The Actor need not be running: tokens are needed during resume.
        let actor = self.load_actor_for_mint(req.actor.as_ref()).await?;
        if req.audiences.is_empty() {
            return Err(status::invalid_argument(
                "at least one audience must be requested",
            ));
        }
        let id = &self.inner.identity;
        let lifetime = Duration::from_secs(req.expiration_seconds.max(0) as u64);
        let claims = Claims::for_actor(
            &id.jwt_issuer,
            fields::actor_hive(&actor),
            fields::actor_name(&actor),
            fields::actor_uid(&actor),
            req.audiences.clone(),
            lifetime,
            SystemTime::now(),
        )
        .map_err(|e| mint_error(e, "actor JWT"))?;
        let token = id
            .jwt_signer
            .sign_jwt(&claims)
            .map_err(|e| mint_error(e, "actor JWT"))?;
        Ok(pb::MintActorJwtResponse {
            actor_jwt: token,
            expire_time: Some(claims.expiration.into()),
        })
    }

    pub(crate) async fn mint_actor_certificate(
        &self,
        cc: &CallContext,
        req: pb::MintActorCertificateRequest,
    ) -> Result<pb::MintActorCertificateResponse> {
        self.authorize(
            cc,
            checks::actor_ref(rel::CAN_MINT_ACTOR_CERTIFICATE, req.actor.as_ref()),
        )
        .await?;
        invalid(v::validate_mint_actor_certificate_request(
            &self.op(cc),
            &req,
        ))?;
        // A proof-of-possession credential may not be bootstrapped from a
        // bearer token: the caller must have presented a certificate.
        if !cc.system && cc.peer_certs.as_ref().and_then(|c| c.leaf()).is_none() {
            return Err(status::unauthenticated("could not verify peer certificate"));
        }
        let actor = self.load_actor_for_mint(req.actor.as_ref()).await?;
        if fields::actor_uid(&actor) != req.actor_uid {
            return Err(status::aborted(
                "conflict; actor has been deleted and recreated",
            ));
        }
        let identity = ActorIdentity {
            hive: fields::actor_hive(&actor).into(),
            actor_name: fields::actor_name(&actor).into(),
            actor_uid: fields::actor_uid(&actor).into(),
        };
        let chain = mint_actor_certificate(
            self.inner.identity.ca.as_ref(),
            &identity,
            ActorCertKind::Actor,
            &req.certificate_signing_request,
            SystemTime::now(),
        )
        .map_err(|e| mint_error(e, "certificate"))?;
        Ok(pb::MintActorCertificateResponse {
            actor_certificates: chain,
        })
    }
}
