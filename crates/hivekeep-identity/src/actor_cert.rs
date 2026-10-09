//! Certificates for Actors and for the workers acting on their behalf.
//!
//! The caller (the API server) decides whether the identity may be issued:
//! that the Actor exists, that the CSR's sender is entitled to it, and that
//! the uid it named is the live incarnation. This module only builds and
//! signs the certificate.

use std::time::{Duration, SystemTime};

use hivekeep_core::resource::ActorRef;
use hivekeep_core::spiffe::{actor_spiffe_id, worker_for_actor_spiffe_id};

use crate::ca::{CertificateAuthority, LeafSpec};
use crate::csr::verified_public_key;
use crate::error::{IdentityError, Result};
use crate::extension::{ActorIdentity, IdentityExtension};

/// Lifetime of an Actor certificate.
pub const ACTOR_CERT_TTL: Duration = Duration::from_secs(60 * 60);

/// How far validity is backdated, to tolerate clock skew.
pub const BACKDATE: Duration = Duration::from_secs(5 * 60);

/// Whose identity a certificate carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActorCertKind {
    /// The Actor itself: `spiffe://<actor trust domain>/actor/<hive>/<name>`.
    Actor,
    /// The tunnel a worker runs for the Actor:
    /// `spiffe://<actor trust domain>/worker-for-actor/<hive>/<name>`.
    WorkerForActor,
}

impl ActorCertKind {
    /// The SPIFFE ID minted for `actor`.
    pub fn spiffe_id(self, actor: &ActorRef) -> String {
        match self {
            ActorCertKind::Actor => actor_spiffe_id(actor),
            ActorCertKind::WorkerForActor => worker_for_actor_spiffe_id(actor),
        }
    }
}

/// The certificate profile for `identity`, valid from `now - BACKDATE` to
/// `now + ACTOR_CERT_TTL`, usable as both TLS client and server.
pub fn actor_leaf_spec(
    identity: &ActorIdentity,
    kind: ActorCertKind,
    now: SystemTime,
) -> Result<LeafSpec> {
    identity.validate()?;
    let actor = ActorRef::new(identity.hive.clone(), identity.actor_name.clone());
    if !actor.is_valid() {
        return Err(IdentityError::Refused(format!(
            "{}/{} is not a valid actor reference",
            identity.hive, identity.actor_name
        )));
    }
    let mut leaf = LeafSpec::new(now - BACKDATE, now + ACTOR_CERT_TTL);
    leaf.uris.push(kind.spiffe_id(&actor));
    leaf.client_auth = true;
    leaf.server_auth = true;
    leaf.extensions.push(identity.to_extension()?);
    Ok(leaf)
}

/// Verifies `csr_der` and signs a certificate of `kind` for `identity`.
/// Returns the chain, leaf first, in DER.
pub fn mint_actor_certificate(
    ca: &dyn CertificateAuthority,
    identity: &ActorIdentity,
    kind: ActorCertKind,
    csr_der: &[u8],
    now: SystemTime,
) -> Result<Vec<Vec<u8>>> {
    let subject = verified_public_key(csr_der)?;
    ca.sign(&actor_leaf_spec(identity, kind, now)?, &subject)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ca::tests::test_pool;
    use crate::csr::tests::csr_for;
    use crate::key::{KeyAlgorithm, PrivateKey};
    use x509_parser::extensions::GeneralName;
    use x509_parser::prelude::{FromDer, X509Certificate};

    fn identity() -> ActorIdentity {
        ActorIdentity {
            hive: "team-a".into(),
            actor_name: "agent-1".into(),
            actor_uid: "6b1f9d0c-4a2e-4d38-9c77-5e0a1b2c3d4e".into(),
        }
    }

    #[test]
    fn mints_both_kinds() {
        let pool = test_pool();
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        let now = SystemTime::now();
        for (kind, want) in [
            (
                ActorCertKind::Actor,
                "spiffe://hivekeep-actor.local/actor/team-a/agent-1",
            ),
            (
                ActorCertKind::WorkerForActor,
                "spiffe://hivekeep-actor.local/worker-for-actor/team-a/agent-1",
            ),
        ] {
            let chain =
                mint_actor_certificate(&pool, &identity(), kind, &csr_for(&key), now).unwrap();
            assert_eq!(chain.len(), 1);
            let (_, cert) = X509Certificate::from_der(&chain[0]).unwrap();
            let san = cert.subject_alternative_name().unwrap().unwrap();
            // The CSR's own SAN is ignored; only the minted identity appears.
            assert_eq!(san.value.general_names.len(), 1);
            assert!(matches!(san.value.general_names[0], GeneralName::URI(u) if u == want));
            assert_eq!(cert.public_key().raw, key.public_key().spki_der());
            let lifetime =
                cert.validity().not_after.timestamp() - cert.validity().not_before.timestamp();
            assert_eq!(lifetime as u64, (ACTOR_CERT_TTL + BACKDATE).as_secs());
            let eku = cert.extended_key_usage().unwrap().unwrap().value;
            assert!(eku.client_auth && eku.server_auth);
            assert_eq!(
                ActorIdentity::from_certificate(&chain[0]).unwrap(),
                Some(identity())
            );
        }
    }

    #[test]
    fn refuses_bad_input() {
        let pool = test_pool();
        let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
        let now = SystemTime::now();
        assert!(matches!(
            mint_actor_certificate(&pool, &identity(), ActorCertKind::Actor, b"junk", now),
            Err(IdentityError::Csr(_))
        ));
        let mut missing_uid = identity();
        missing_uid.actor_uid.clear();
        assert!(mint_actor_certificate(
            &pool,
            &missing_uid,
            ActorCertKind::Actor,
            &csr_for(&key),
            now
        )
        .is_err());
        let mut bad_name = identity();
        bad_name.actor_name = "Not/A-Name".into();
        assert!(matches!(
            mint_actor_certificate(&pool, &bad_name, ActorCertKind::Actor, &csr_for(&key), now),
            Err(IdentityError::Refused(_))
        ));
    }
}
