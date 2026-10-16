//! Authenticating node keepers, the only callers of `WorkerService`.
//!
//! The TLS layer has already verified the client chain against the
//! pod-identity CA, so the identity extensions read here are trustworthy:
//! only the pod-identity signer can mint a certificate naming a node.

use hivekeep_identity::extension::{IdentityExtension, PodIdentity};
use tonic::Status;

use crate::context::CallContext;
use crate::peer_cert;

/// A verified keeper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeeperCaller {
    /// The keeper's pod.
    pub pod_name: String,
    /// The node it runs on.
    pub node_name: String,
}

fn denied() -> Status {
    // Uniform: a caller learns it is not a keeper, and nothing about why.
    Status::permission_denied("caller is not permitted")
}

/// Verifies the call came over mTLS from a keeper presenting
/// `keeper_spiffe_id`, returning the node its certificate names.
pub fn authenticate(cc: &CallContext, keeper_spiffe_id: &str) -> Result<KeeperCaller, Status> {
    let Some(leaf) = cc.peer_certs.as_ref().and_then(|c| c.leaf()) else {
        return Err(Status::unauthenticated("could not verify peer certificate"));
    };
    match peer_cert::uri_sans(leaf).first() {
        Some(id) if id == keeper_spiffe_id => {}
        other => {
            tracing::warn!(uri = ?other, expected = keeper_spiffe_id, "denied: caller is not a keeper");
            return Err(denied());
        }
    }
    match PodIdentity::from_certificate(leaf) {
        Ok(Some(id)) => Ok(KeeperCaller {
            pod_name: id.pod_name,
            node_name: id.node_name,
        }),
        Ok(None) => {
            tracing::warn!("denied: certificate has no PodIdentity extension");
            Err(denied())
        }
        Err(e) => {
            tracing::warn!(error = %e, "denied: malformed PodIdentity extension");
            Err(denied())
        }
    }
}

/// Helpers that mint pod-identity certificates, for tests and local
/// development setups.
pub mod testing {
    use std::time::{Duration, SystemTime};

    use hivekeep_identity::ca::{Ca, CaPool};
    use hivekeep_identity::extension::PodIdentity;
    use hivekeep_identity::key::{KeyAlgorithm, PrivateKey};
    use hivekeep_identity::pem;
    use hivekeep_identity::pod_cert::{mint_pod_certificate, service_account_spiffe_id};

    /// A throwaway pod-identity CA.
    pub fn pod_ca() -> CaPool {
        CaPool::new(
            vec![Ca::generate("pod", KeyAlgorithm::EcdsaP256, Duration::from_secs(86400)).unwrap()],
            "",
        )
    }

    /// A DER certificate chain for a pod of `service_account` in
    /// `namespace` on `node`.
    pub fn pod_cert(
        ca: &CaPool,
        namespace: &str,
        service_account: &str,
        node: &str,
    ) -> Vec<Vec<u8>> {
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        let id = PodIdentity {
            namespace: namespace.into(),
            service_account_name: service_account.into(),
            service_account_uid: uuid::Uuid::new_v4().to_string(),
            pod_name: format!("{service_account}-{node}"),
            pod_uid: uuid::Uuid::new_v4().to_string(),
            node_name: node.into(),
            node_uid: uuid::Uuid::new_v4().to_string(),
        };
        let issued = mint_pod_certificate(
            ca,
            &id,
            &key.public_key(),
            Duration::from_secs(3600 * 24),
            SystemTime::now(),
        )
        .unwrap();
        pem::decode_certificates(issued.cert_chain_pem.as_bytes()).unwrap()
    }

    /// The SPIFFE ID `pod_cert` certificates carry.
    pub fn spiffe_id(namespace: &str, service_account: &str) -> String {
        service_account_spiffe_id(namespace, service_account)
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::context::{request_as, Principal};

    #[test]
    fn authenticates_keepers_only() {
        let ca = pod_ca();
        let want = spiffe_id("hivekeep-system", "keeper");
        let keeper = pod_cert(&ca, "hivekeep-system", "keeper", "node-1");
        let cc = CallContext::of(&request_as((), Some(Principal::mtls(&want)), Some(keeper)));
        let caller = authenticate(&cc, &want).unwrap();
        assert_eq!(caller.node_name, "node-1");

        let other = pod_cert(&ca, "default", "app", "node-1");
        let cc = CallContext::of(&request_as((), None, Some(other)));
        assert_eq!(
            authenticate(&cc, &want).unwrap_err().code(),
            tonic::Code::PermissionDenied
        );

        let cc = CallContext::of(&request_as((), Some(Principal::jwt("x", "i")), None));
        assert_eq!(
            authenticate(&cc, &want).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }
}
