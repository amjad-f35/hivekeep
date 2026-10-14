//! The Actor certificate the tunnel presents, minted by the node's keeper
//! for a key that never leaves this process.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::RwLock;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::sign::CertifiedKey;
use tokio::time::Instant;

use super::{ActorCertificateSource, ClientCertificateSource, MintError};
use crate::tunnel::keeper_dial;

/// Attempts per mint when the keeper is unavailable.
const MINT_ATTEMPTS: u32 = 4;

/// Where the keeper is and which Actor to mint for.
#[derive(Debug, Clone, Default)]
pub struct BrokerConfig {
    /// The keeper's node-local socket.
    pub socket_path: PathBuf,
    /// The worker's credential bundle (Pod certificate and key).
    pub credential_bundle_path: PathBuf,
    /// CAs the keeper's certificate chains to.
    pub trust_bundle_path: PathBuf,
    /// The Actor's hive.
    pub actor_hive: String,
    /// The Actor's name.
    pub actor_name: String,
    /// The Actor's UID.
    pub actor_uid: String,
    /// The SPIFFE ID the keeper must present.
    pub keeper_spiffe_id: String,
}

/// Mints and holds the Actor's tunnel certificate.
pub struct BrokerCertificateSource {
    socket_path: PathBuf,
    actor_hive: String,
    actor_name: String,
    actor_uid: String,
    tls: Arc<rustls::ClientConfig>,
    key: rcgen::KeyPair,
    current: RwLock<Option<(Arc<CertifiedKey>, SystemTime)>>,
}

impl BrokerCertificateSource {
    /// A source for `cfg` with a fresh P-256 key.
    pub fn new(cfg: BrokerConfig) -> Result<Self, String> {
        if cfg.socket_path.as_os_str().is_empty()
            || cfg.credential_bundle_path.as_os_str().is_empty()
            || cfg.trust_bundle_path.as_os_str().is_empty()
        {
            return Err("credential broker socket, credentials, trust bundle are required".into());
        }
        if cfg.actor_hive.is_empty() || cfg.actor_name.is_empty() || cfg.actor_uid.is_empty() {
            return Err("actor information is required".into());
        }
        if cfg.keeper_spiffe_id.is_empty() {
            return Err("egress: expected keeper SPIFFE ID is required".into());
        }
        let tls = keeper_dial::tls_config(
            &cfg.credential_bundle_path,
            &cfg.trust_bundle_path,
            &cfg.keeper_spiffe_id,
        )
        .map_err(|e| format!("egress: {e}"))?;
        let key = rcgen::KeyPair::generate()
            .map_err(|e| format!("egress: generate actor private key: {e}"))?;
        Ok(BrokerCertificateSource {
            socket_path: cfg.socket_path,
            actor_hive: cfg.actor_hive,
            actor_name: cfg.actor_name,
            actor_uid: cfg.actor_uid,
            tls,
            key,
            current: RwLock::new(None),
        })
    }

    /// Mints a certificate for the Actor's key, retrying while the keeper
    /// is unavailable, and returns its expiry.
    pub async fn mint_worker_certificate(&self) -> Result<SystemTime, MintError> {
        self.mint_until(None).await
    }

    /// Like [`Self::mint_worker_certificate`], giving up retries at
    /// `deadline` with the last error.
    pub async fn mint_until(&self, deadline: Option<Instant>) -> Result<SystemTime, MintError> {
        let csr = rcgen::CertificateParams::default()
            .serialize_request(&self.key)
            .map_err(|e| MintError::other(format!("egress: create actor CSR: {e}")))?
            .der()
            .to_vec();
        let mut attempt = 1;
        loop {
            let err = match self.mint_once(&csr).await {
                Ok(t) => return Ok(t),
                Err(e) => e,
            };
            if attempt >= MINT_ATTEMPTS || err.code != Some(tonic::Code::Unavailable) {
                return Err(err);
            }
            let wake = Instant::now() + Duration::from_millis(500 * u64::from(attempt));
            if deadline.is_some_and(|d| wake > d) {
                return Err(err);
            }
            tokio::time::sleep_until(wake).await;
            attempt += 1;
        }
    }

    async fn mint_once(&self, csr: &[u8]) -> Result<SystemTime, MintError> {
        let channel = keeper_dial::connect(&self.socket_path, self.tls.clone())
            .await
            .map_err(|e| MintError {
                code: Some(tonic::Code::Unavailable),
                message: format!(
                    "egress: connecting to keeper: {}",
                    keeper_dial::error_chain(&e)
                ),
            })?;
        let resp = pb::worker_support_client::WorkerSupportClient::new(channel)
            .mint_actor_certificate(pb::MintActorCertificateRequest {
                actor_hive: self.actor_hive.clone(),
                actor_name: self.actor_name.clone(),
                actor_uid: self.actor_uid.clone(),
                certificate_signing_request: csr.to_vec(),
            })
            .await
            .map_err(|s| MintError {
                code: Some(s.code()),
                message: format!(
                    "egress: mint actor certificate: {}",
                    keeper_dial::error_chain(&s)
                ),
            })?
            .into_inner();
        let chain = resp.actor_certificates;
        let leaf_der = chain.first().ok_or_else(|| {
            MintError::other("egress: credential broker returned no actor certificate")
        })?;
        let (_, leaf) = x509_parser::parse_x509_certificate(leaf_der)
            .map_err(|e| MintError::other(format!("egress: parse actor certificate: {e}")))?;
        if leaf.tbs_certificate.subject_pki.raw
            != rcgen::PublicKeyData::subject_public_key_info(&self.key).as_slice()
        {
            return Err(MintError::other(
                "egress: actor certificate does not match private key",
            ));
        }
        let now = SystemTime::now();
        let not_before = SystemTime::UNIX_EPOCH
            + Duration::from_secs(leaf.validity().not_before.timestamp().max(0) as u64);
        let not_after = SystemTime::UNIX_EPOCH
            + Duration::from_secs(leaf.validity().not_after.timestamp().max(0) as u64);
        if now < not_before || not_after <= now {
            return Err(MintError::other(
                "egress: credential broker returned an invalid actor certificate lifetime",
            ));
        }
        let client_auth =
            matches!(leaf.extended_key_usage(), Ok(Some(eku)) if eku.value.client_auth);
        if !client_auth {
            return Err(MintError::other(
                "egress: actor certificate cannot authenticate a TLS client",
            ));
        }
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key.serialize_der()));
        let signer = rustls::crypto::ring::sign::any_supported_type(&key)
            .map_err(|e| MintError::other(format!("egress: actor key: {e}")))?;
        let certs = chain
            .iter()
            .map(|d| CertificateDer::from(d.clone()))
            .collect();
        *self.current.write() = Some((Arc::new(CertifiedKey::new(certs, signer)), not_after));
        Ok(not_after)
    }

    /// The URI SANs of the current certificate (for diagnostics).
    pub fn current_uris(&self) -> Vec<String> {
        self.current
            .read()
            .as_ref()
            .and_then(|(c, _)| c.cert.first().map(|d| keeper_dial::uri_sans(d)))
            .unwrap_or_default()
    }
}

impl ClientCertificateSource for BrokerCertificateSource {
    fn current(&self) -> Option<Arc<CertifiedKey>> {
        let cur = self.current.read();
        let (cert, not_after) = cur.as_ref()?;
        (*not_after > SystemTime::now()).then(|| cert.clone())
    }
}

impl ActorCertificateSource for BrokerCertificateSource {
    fn mint(&self) -> BoxFuture<'_, Result<SystemTime, MintError>> {
        Box::pin(self.mint_worker_certificate())
    }
}

#[cfg(test)]
#[path = "credential_tests.rs"]
pub(crate) mod tests;
