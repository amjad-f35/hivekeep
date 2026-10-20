//! The `CredentialProvider` service backed by Kubernetes Secrets.
//!
//! Authorization runs in three steps: the caller names a real actor (its
//! SPIFFE ID parses), the actor's hive is granted the namespace, and — once
//! the Secret is read — a label-narrowed grant admits that Secret. While
//! authorization is enforced a missing Secret is refused exactly like one
//! failing the label check, so callers cannot probe which names exist.

use std::collections::BTreeMap;

use hivekeep_core::spiffe::actor_ref_from_spiffe_id;
use hivekeep_k8s::api_errors::{is_forbidden, is_not_found};
use k8s_openapi::api::core::v1::Secret;
use kube::Api;
use tonic::{Request, Response, Status};

use crate::nsauthz::NamespaceAuthorizer;
use crate::pb::v1::credential_provider_server::CredentialProvider;
use crate::pb::v1::{FetchSecretRequest, FetchSecretResponse};
use crate::secret_uri::{parse_uri, SecretRef, PROVIDER_NAME};

/// Resolves `hk-secret://k8s.io/...` URIs by reading Secrets from the
/// cluster the provider runs in.
#[derive(Clone)]
pub struct KubeSecretProvider {
    client: kube::Client,
    /// `None` disables authorization; only tests do that, since the binary
    /// always requires a policy file.
    ns_auth: Option<NamespaceAuthorizer>,
}

impl KubeSecretProvider {
    /// A provider reading through `client`, enforcing `ns_auth` when set.
    pub fn new(client: kube::Client, ns_auth: Option<NamespaceAuthorizer>) -> Self {
        KubeSecretProvider { client, ns_auth }
    }

    /// The enforced hive → namespaces policy, for the status page. `None`
    /// when authorization is disabled.
    pub fn grants(&self) -> Option<BTreeMap<String, Vec<String>>> {
        self.ns_auth.as_ref().map(NamespaceAuthorizer::grants)
    }

    /// Resolves one URI to its Secret value.
    pub async fn fetch(&self, req: &FetchSecretRequest) -> Result<FetchSecretResponse, Status> {
        let r = parse_uri(&req.uri).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let hive = self.authorize(&req.actor_spiffe_id, &r.namespace)?;

        tracing::info!(
            provider = PROVIDER_NAME,
            namespace = %r.namespace,
            secret = %r.name,
            actor = %req.actor_spiffe_id,
            "Resolving credential"
        );

        let secret = match Api::<Secret>::namespaced(self.client.clone(), &r.namespace)
            .get(&r.name)
            .await
        {
            Ok(s) => s,
            Err(e) if is_not_found(&e) => {
                return Err(match &hive {
                    None => {
                        Status::not_found(format!("secret {}/{} not found", r.namespace, r.name))
                    }
                    Some(h) => denied(h, &r),
                })
            }
            Err(e) if is_forbidden(&e) => {
                return Err(Status::permission_denied(format!(
                    "not permitted to read secret {}/{}",
                    r.namespace, r.name
                )))
            }
            Err(e) => {
                return Err(Status::unavailable(format!(
                    "reading secret {}/{}: {e}",
                    r.namespace, r.name
                )))
            }
        };

        if let (Some(auth), Some(h)) = (&self.ns_auth, &hive) {
            if !auth.allowed_secret(h, &r.namespace, secret.metadata.labels.as_ref()) {
                tracing::warn!(
                    hive = %h,
                    namespace = %r.namespace,
                    secret = %r.name,
                    "Credential request denied: Secret does not match the grant"
                );
                return Err(denied(h, &r));
            }
        }

        let value = secret
            .data
            .and_then(|mut d| d.remove(&r.key))
            .ok_or_else(|| {
                Status::not_found(format!(
                    "secret {}/{}: key {:?} not present",
                    r.namespace, r.name, r.key
                ))
            })?;
        Ok(FetchSecretResponse {
            opaque_bytes: value.0,
        })
    }

    /// The checks that need nothing from Kubernetes: the caller is an
    /// actor, and its hive is granted `namespace`. Returns the hive, or
    /// `None` when authorization is disabled.
    fn authorize(&self, actor_spiffe_id: &str, namespace: &str) -> Result<Option<String>, Status> {
        let Some(auth) = &self.ns_auth else {
            return Ok(None);
        };
        let actor = actor_ref_from_spiffe_id(actor_spiffe_id).map_err(|e| {
            tracing::warn!(err = %e, "Credential request denied: unusable actor identity");
            Status::permission_denied(format!(
                "actor identity is required and must be a valid actor SPIFFE URI: {e}"
            ))
        })?;
        if !auth.allowed(&actor.hive, namespace) {
            tracing::warn!(
                hive = %actor.hive,
                namespace,
                "Credential request denied: hive not permitted for namespace"
            );
            return Err(Status::permission_denied(format!(
                "hive {:?} is not permitted to resolve secrets in namespace {namespace:?}",
                actor.hive
            )));
        }
        Ok(Some(actor.hive))
    }
}

/// The refusal shared by a missing Secret and a label mismatch, identical
/// but for the requested name so neither reveals which case applied.
fn denied(hive: &str, r: &SecretRef) -> Status {
    Status::permission_denied(format!(
        "hive {hive:?} is not permitted to resolve secret {}/{}",
        r.namespace, r.name
    ))
}

#[tonic::async_trait]
impl CredentialProvider for KubeSecretProvider {
    async fn fetch_secret(
        &self,
        request: Request<FetchSecretRequest>,
    ) -> Result<Response<FetchSecretResponse>, Status> {
        self.fetch(request.get_ref()).await.map(Response::new)
    }
}

#[cfg(test)]
#[path = "provider_tests.rs"]
mod tests;
