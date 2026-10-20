//! `postgres.podcert.hivekeep.io/identity`: PostgreSQL client-login
//! certificates for the control plane, backed by a local CA pool.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hivekeep_identity::ca::CertificateAuthority;
use hivekeep_k8s::cluster_trust_bundle::ClusterTrustBundle;
use hivekeep_k8s::install_defaults::API_SERVER_SERVICE_ACCOUNT;
use hivekeep_node::clock::SharedClock;
use hivekeep_store::postgres::setup::{OWNER_USER, READ_WRITE_USER};

use crate::error::SignerError;
use crate::pod_certificate::{
    public_key, PcrClient, PodCertificateRequest, REASON_INVALID_USER_CONFIG,
};
use crate::signer::issue::{capped_lifetime, requested_lifetime, sign_leaf};
use crate::signer::status::{with_denied, with_issued};
use crate::signer::trust_bundle::primary_bundle;
use crate::signer::SignerImpl;

/// The signer name.
pub const NAME: &str = "postgres.podcert.hivekeep.io/identity";

/// The user annotation naming the login a request asks for.
pub const USERNAME_ANNOTATION: &str = "postgres.podcert.hivekeep.io/username";

/// Prefix of the signer's ClusterTrustBundles.
pub const CTB_PREFIX: &str = "postgres.podcert.hivekeep.io:identity:";

/// Denial reason for requests from any other service account.
pub const REASON_UNAUTHORIZED_SERVICE_ACCOUNT: &str = "UnauthorizedServiceAccount";

/// Shortest lifetime a request may allow.
pub const MIN_LIFETIME: Duration = Duration::from_secs(60 * 60);

/// Issues login certificates for the bundled application users to the
/// control plane's service account in one namespace.
pub struct PostgresSigner {
    namespace: String,
    ca: Arc<dyn CertificateAuthority>,
    pcrs: Arc<PcrClient>,
    clock: SharedClock,
}

impl PostgresSigner {
    /// A signer for requests from `namespace`'s control plane service
    /// account, issuing from `ca` and answering through `pcrs`.
    pub fn new(
        namespace: impl Into<String>,
        ca: Arc<dyn CertificateAuthority>,
        pcrs: Arc<PcrClient>,
        clock: SharedClock,
    ) -> Self {
        PostgresSigner {
            namespace: namespace.into(),
            ca,
            pcrs,
            clock,
        }
    }

    async fn deny(
        &self,
        pcr: &PodCertificateRequest,
        reason: &str,
        message: &str,
    ) -> Result<(), SignerError> {
        self.pcrs
            .update_status(&with_denied(pcr, reason, message, self.clock.now()))
            .await
            .map_err(|e| SignerError::UpdateStatus(Box::new(e)))?;
        Ok(())
    }

    /// The login `pcr` may have, or why it is denied. Authorization rests
    /// on the namespace and service account, which kube-apiserver attests;
    /// the annotations are untrusted and may only pick one of the bundled
    /// application logins, never the administrator.
    fn authorize<'a>(
        &self,
        pcr: &'a PodCertificateRequest,
    ) -> Result<&'a str, (&'static str, String)> {
        if pcr.metadata.namespace.as_deref() != Some(self.namespace.as_str())
            || pcr.spec.service_account_name != API_SERVER_SERVICE_ACCOUNT
        {
            return Err((
                REASON_UNAUTHORIZED_SERVICE_ACCOUNT,
                format!(
                    "only {}/{API_SERVER_SERVICE_ACCOUNT} may request PostgreSQL login certificates",
                    self.namespace
                ),
            ));
        }
        let annotations = pcr.spec.unverified_user_annotations.as_ref();
        let username = annotations
            .and_then(|a| a.get(USERNAME_ANNOTATION))
            .map(String::as_str);
        match username {
            Some(u) if annotations.map_or(0, |a| a.len()) == 1 && (u == OWNER_USER || u == READ_WRITE_USER) => Ok(u),
            _ => Err((
                REASON_INVALID_USER_CONFIG,
                "request must contain only the username annotation naming a bundled application login".into(),
            )),
        }
    }
}

#[async_trait]
impl SignerImpl for PostgresSigner {
    fn signer_name(&self) -> &str {
        NAME
    }

    fn desired_cluster_trust_bundles(&self) -> Result<Vec<ClusterTrustBundle>, SignerError> {
        Ok(vec![primary_bundle(CTB_PREFIX, NAME, self.ca.as_ref())?])
    }

    async fn make_cert(&self, pcr: &PodCertificateRequest) -> Result<(), SignerError> {
        if pcr.spec.signer_name != NAME {
            return Err(SignerError::UnexpectedSigner(pcr.spec.signer_name.clone()));
        }
        let username = match self.authorize(pcr) {
            Ok(u) => u.to_string(),
            Err((reason, message)) => return self.deny(pcr, reason, &message).await,
        };
        let subject = public_key(pcr)?;
        if requested_lifetime(pcr).is_some_and(|l| l < MIN_LIFETIME) {
            return Err(SignerError::LifetimeTooShort);
        }
        let now = self.clock.now();
        // The requester's CSR subject and extensions never shape the login.
        let issued = sign_leaf(
            self.ca.as_ref(),
            &subject,
            capped_lifetime(pcr),
            now.into(),
            |leaf| {
                leaf.common_name = Some(username);
                leaf.client_auth = true;
            },
        )?;
        self.pcrs
            .update_status(&with_issued(pcr, &issued, now))
            .await
            .map_err(|e| SignerError::UpdateStatus(Box::new(e)))?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "postgres_signer_tests.rs"]
mod tests;
