//! The calls the egress handler makes: the control plane (Actor state,
//! egress policy, Actor JWTs) and the credential provider.

use futures::future::BoxFuture;
use hivekeep_proto::api::v1 as pb;
use tonic::transport::Channel;

use crate::credprovider::v1 as cp;

/// Control-plane calls of the egress gateway.
pub trait EgressApi: Send + Sync + 'static {
    /// The Actor.
    fn get_actor(
        &self,
        req: pb::GetActorRequest,
    ) -> BoxFuture<'static, Result<pb::Actor, tonic::Status>>;
    /// The Actor's egress policy (`NotFound` when it has none).
    fn get_actor_egress_policy(
        &self,
        req: pb::GetActorEgressPolicyRequest,
    ) -> BoxFuture<'static, Result<pb::EgressPolicy, tonic::Status>>;
    /// Mints a JWT for the Actor.
    fn mint_actor_jwt(
        &self,
        req: pb::MintActorJwtRequest,
    ) -> BoxFuture<'static, Result<pb::MintActorJwtResponse, tonic::Status>>;
}

impl EgressApi for pb::control_client::ControlClient<Channel> {
    fn get_actor(
        &self,
        req: pb::GetActorRequest,
    ) -> BoxFuture<'static, Result<pb::Actor, tonic::Status>> {
        let mut c = self.clone();
        Box::pin(async move {
            pb::control_client::ControlClient::get_actor(&mut c, req)
                .await
                .map(|r| r.into_inner())
        })
    }
    fn get_actor_egress_policy(
        &self,
        req: pb::GetActorEgressPolicyRequest,
    ) -> BoxFuture<'static, Result<pb::EgressPolicy, tonic::Status>> {
        let mut c = self.clone();
        Box::pin(async move {
            pb::control_client::ControlClient::get_actor_egress_policy(&mut c, req)
                .await
                .map(|r| r.into_inner())
        })
    }
    fn mint_actor_jwt(
        &self,
        req: pb::MintActorJwtRequest,
    ) -> BoxFuture<'static, Result<pb::MintActorJwtResponse, tonic::Status>> {
        let mut c = self.clone();
        Box::pin(async move {
            pb::control_client::ControlClient::mint_actor_jwt(&mut c, req)
                .await
                .map(|r| r.into_inner())
        })
    }
}

/// A secret backend.
pub trait CredentialProvider: Send + Sync + 'static {
    /// Resolves a credential URI for an Actor.
    fn fetch_secret(
        &self,
        req: cp::FetchSecretRequest,
    ) -> BoxFuture<'static, Result<cp::FetchSecretResponse, tonic::Status>>;
}

impl CredentialProvider for cp::credential_provider_client::CredentialProviderClient<Channel> {
    fn fetch_secret(
        &self,
        req: cp::FetchSecretRequest,
    ) -> BoxFuture<'static, Result<cp::FetchSecretResponse, tonic::Status>> {
        let mut c = self.clone();
        Box::pin(async move {
            cp::credential_provider_client::CredentialProviderClient::fetch_secret(&mut c, req)
                .await
                .map(|r| r.into_inner())
        })
    }
}
