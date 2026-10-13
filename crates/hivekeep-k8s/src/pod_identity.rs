//! Pod-identity certificates for processes that do not run in a pod.
//!
//! [`request_certificate`] creates a PodCertificateRequest naming a
//! ServiceAccount, waits for the pod-identity signer to issue it, and
//! deletes it again. The private key never leaves this process.
//!
//! The pod, ServiceAccount and node the request names need not exist:
//! kube-apiserver only checks them for requests a kubelet creates, and the
//! signer takes the identity from the request. `requester` names the node
//! and prefixes the request's name, so the request shows who made it.

use std::time::Duration;

use k8s_openapi::api::certificates::v1beta1::{PodCertificateRequest, PodCertificateRequestSpec};
use kube::api::{DeleteParams, PostParams};
use kube::Client;

use hivekeep_identity::key::{KeyAlgorithm, PrivateKey};
use hivekeep_identity::pod_cert::POD_IDENTITY_SIGNER;

use crate::cert_api::{self, VersionedApi};

/// Lifetime asked for.
const MAX_EXPIRATION_SECONDS: i32 = 24 * 60 * 60;
/// How often the request is re-read.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// An issued certificate and its key.
pub struct PodCredential {
    /// The certificate chain, leaf first, DER.
    pub chain: Vec<Vec<u8>>,
    /// The private key of the leaf.
    pub key: PrivateKey,
}

/// Why no certificate was obtained.
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// Key or CSR generation failed.
    #[error("generating key: {0}")]
    Key(String),
    /// The cluster serves no PodCertificateRequest API.
    #[error(transparent)]
    Discover(#[from] cert_api::DiscoverError),
    /// The request could not be created.
    #[error("creating PodCertificateRequest in {namespace}: {source}")]
    Create {
        /// The namespace.
        namespace: String,
        /// The API error.
        #[source]
        source: Box<kube::Error>,
    },
    /// The request was not issued in time, or was denied or failed.
    #[error("waiting for PodCertificateRequest {namespace}/{name} to be issued: {reason}")]
    Wait {
        /// The namespace.
        namespace: String,
        /// The request name.
        name: String,
        /// What went wrong.
        reason: String,
    },
    /// The issued chain is unusable.
    #[error("PodCertificateRequest {namespace}/{name}: {reason}")]
    BadChain {
        /// The namespace.
        namespace: String,
        /// The request name.
        name: String,
        /// What is wrong with it.
        reason: String,
    },
}

fn stub_csr(key: &PrivateKey) -> Result<Vec<u8>, RequestError> {
    let pair =
        rcgen::KeyPair::try_from(key.pkcs8_der()).map_err(|e| RequestError::Key(e.to_string()))?;
    let csr = rcgen::CertificateParams::default()
        .serialize_request(&pair)
        .map_err(|e| RequestError::Key(e.to_string()))?;
    Ok(csr.der().to_vec())
}

/// Requests a pod-identity certificate for `service_account` in
/// `namespace`, waiting up to `timeout` for it to be issued.
pub async fn request_certificate(
    client: Client,
    requester: &str,
    namespace: &str,
    service_account: &str,
    timeout: Duration,
) -> Result<PodCredential, RequestError> {
    let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
        .map_err(|e| RequestError::Key(e.to_string()))?;
    let csr = stub_csr(&key)?;
    let version =
        cert_api::discover(&client, "podcertificaterequests", "PodCertificateRequest").await?;
    let api = VersionedApi::<PodCertificateRequest>::namespaced(client, namespace, version);

    let mut pcr = PodCertificateRequest {
        spec: PodCertificateRequestSpec {
            signer_name: POD_IDENTITY_SIGNER.into(),
            pod_name: requester.into(),
            pod_uid: uuid::Uuid::new_v4().to_string(),
            service_account_name: service_account.into(),
            service_account_uid: uuid::Uuid::new_v4().to_string(),
            node_name: requester.into(),
            node_uid: uuid::Uuid::new_v4().to_string(),
            max_expiration_seconds: Some(MAX_EXPIRATION_SECONDS),
            stub_pkc_s10_request: k8s_openapi::ByteString(csr),
            ..Default::default()
        },
        ..Default::default()
    };
    pcr.metadata.generate_name = Some(format!("{requester}-"));
    pcr.metadata.namespace = Some(namespace.into());
    let created = api
        .create(&PostParams::default(), &pcr)
        .await
        .map_err(|e| RequestError::Create {
            namespace: namespace.into(),
            source: Box::new(e),
        })?;
    let name = created.metadata.name.clone().unwrap_or_default();

    let result = wait_issued(&api, namespace, &name, timeout).await;
    // A leftover request is harmless (kube-apiserver garbage-collects old
    // ones), so a failed delete is not an error.
    let _ = tokio::time::timeout(
        Duration::from_secs(30),
        api.delete(&name, &DeleteParams::default()),
    )
    .await;
    let chain_pem = result?;

    let bad = |reason: String| RequestError::BadChain {
        namespace: namespace.into(),
        name: name.clone(),
        reason,
    };
    let chain = hivekeep_identity::pem::decode_certificates(chain_pem.as_bytes())
        .map_err(|e| bad(e.to_string()))?;
    let Some(leaf) = chain.first() else {
        return Err(bad("issued with no certificates".into()));
    };
    let (_, cert) = x509_parser::parse_x509_certificate(leaf)
        .map_err(|e| bad(format!("parsing the issued leaf certificate: {e}")))?;
    if cert.public_key().raw != key.public_key().spki_der() {
        return Err(bad(
            "the issued certificate does not carry the requested public key".into(),
        ));
    }
    Ok(PodCredential { chain, key })
}

async fn wait_issued(
    api: &VersionedApi<PodCertificateRequest>,
    namespace: &str,
    name: &str,
    timeout: Duration,
) -> Result<String, RequestError> {
    let fail = |reason: String| RequestError::Wait {
        namespace: namespace.into(),
        name: name.into(),
        reason,
    };
    let poll = async {
        loop {
            let pcr = api.get(name).await.map_err(|e| fail(e.to_string()))?;
            let status = pcr.status.unwrap_or_default();
            for c in status
                .conditions
                .iter()
                .flatten()
                .filter(|c| c.status == "True")
            {
                match c.type_.as_str() {
                    "Issued" => return Ok(status.certificate_chain.clone().unwrap_or_default()),
                    "Denied" | "Failed" => {
                        return Err(fail(format!("{}: {}: {}", c.type_, c.reason, c.message)))
                    }
                    _ => {}
                }
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    };
    tokio::time::timeout(timeout, poll)
        .await
        .map_err(|_| fail("timed out".into()))?
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::SystemTime;

    use parking_lot::Mutex;
    use serde_json::{json, Value};

    use super::*;
    use crate::fake::{FakeApiServer, Reaction};
    use hivekeep_identity::ca::{Ca, LeafSpec};

    const NAMESPACE: &str = "ns";
    const SERVICE_ACCOUNT: &str = "client";

    /// Answers PodCertificateRequests the way the pod-identity signer does,
    /// signing the stub CSR's key with a throwaway CA.
    struct FakeSigner {
        ca: Ca,
        denied: bool,
        got: Mutex<Option<Value>>,
    }

    impl FakeSigner {
        fn new(denied: bool) -> Arc<Self> {
            Arc::new(FakeSigner {
                ca: Ca::generate("test", KeyAlgorithm::EcdsaP256, Duration::from_secs(3600))
                    .unwrap(),
                denied,
                got: Mutex::new(None),
            })
        }

        fn status(&self, spec: &Value) -> Value {
            *self.got.lock() = Some(spec.clone());
            if self.denied {
                return json!({"conditions": [{"type": "Denied", "status": "True", "reason": "UnsupportedKeyType", "message": "no", "lastTransitionTime": "2026-01-01T00:00:00Z"}]});
            }
            let csr: k8s_openapi::ByteString =
                serde_json::from_value(spec["stubPKCS10Request"].clone()).unwrap();
            let pk = hivekeep_identity::csr::verified_public_key(&csr.0).unwrap();
            let now = SystemTime::now();
            let mut leaf = LeafSpec::new(
                now - Duration::from_secs(60),
                now + Duration::from_secs(3600),
            );
            leaf.client_auth = true;
            let der = self.ca.sign(&leaf, &pk).unwrap();
            json!({
                "conditions": [{"type": "Issued", "status": "True", "reason": "Reason", "message": "Issued", "lastTransitionTime": "2026-01-01T00:00:00Z"}],
                "certificateChain": hivekeep_identity::pem::encode("CERTIFICATE", &der),
            })
        }

        /// Serves PodCertificateRequest at `version` and answers every
        /// request created through it.
        fn install(self: &Arc<Self>, fake: &FakeApiServer, version: &str) {
            fake.serve(
                &format!("certificates.k8s.io/{version}"),
                &[("podcertificaterequests", "PodCertificateRequest", true)],
            );
            let this = self.clone();
            fake.prepend_reactor("create", "podcertificaterequests", move |a| {
                let mut body = a.body.clone().unwrap();
                let prefix = body["metadata"]["generateName"]
                    .as_str()
                    .unwrap()
                    .to_string();
                body["metadata"]["name"] = json!(format!("{prefix}abcde"));
                body["status"] = this.status(&body["spec"]);
                Reaction::Mutate(body)
            });
        }
    }

    #[tokio::test]
    async fn request_certificate_each_version() {
        for version in ["v1", "v1beta1"] {
            let fake = FakeApiServer::new();
            let signer = FakeSigner::new(false);
            signer.install(&fake, version);
            let cred = request_certificate(
                fake.client(),
                "test",
                NAMESPACE,
                SERVICE_ACCOUNT,
                Duration::from_secs(1),
            )
            .await
            .unwrap_or_else(|e| panic!("{version}: {e}"));
            let got = signer.got.lock().clone().unwrap();
            assert_eq!(got["signerName"], POD_IDENTITY_SIGNER);
            assert_eq!(got["serviceAccountName"], SERVICE_ACCOUNT);
            let (_, leaf) = x509_parser::parse_x509_certificate(&cred.chain[0]).unwrap();
            assert_eq!(
                leaf.public_key().raw,
                cred.key.public_key().spki_der(),
                "key matches leaf"
            );
            assert!(
                fake.list::<PodCertificateRequest>().is_empty(),
                "{version}: requests left behind"
            );
            assert!(fake
                .actions()
                .iter()
                .filter(|a| a.resource == "podcertificaterequests")
                .all(|a| a.version == version));
        }
    }

    #[tokio::test]
    async fn request_certificate_errors() {
        type Setup = Box<dyn Fn(&FakeApiServer)>;
        let cases: Vec<(&str, Setup, &str)> = vec![
            (
                "denied",
                Box::new(|f| FakeSigner::new(true).install(f, "v1")),
                "Denied",
            ),
            (
                "never issued",
                Box::new(|f| {
                    f.serve(
                        "certificates.k8s.io/v1",
                        &[("podcertificaterequests", "PodCertificateRequest", true)],
                    )
                }),
                "to be issued",
            ),
            ("not served", Box::new(|_| {}), "neither v1 nor v1beta1"),
        ];
        for (name, setup, want) in cases {
            let fake = FakeApiServer::new();
            setup(&fake);
            let err = request_certificate(
                fake.client(),
                "test",
                NAMESPACE,
                SERVICE_ACCOUNT,
                Duration::from_millis(100),
            )
            .await
            .err()
            .unwrap_or_else(|| panic!("{name}: succeeded"));
            assert!(err.to_string().contains(want), "{name}: {err}");
        }
    }
}
