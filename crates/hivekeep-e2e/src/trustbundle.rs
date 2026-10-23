//! The egress MITM trust bundle and other ClusterTrustBundles the suites
//! read.
//!
//! The controller derives the `egress-mitm.hivekeep.io` bundle from the egress
//! CA pool Secret and reverts hand-written bundle contents, so suites
//! provision the pool and let the reconciler publish the bundle, exercising
//! the whole chain (pool, reconciler, bundle, projection into the sandbox).

use std::time::{Duration, Instant};

use base64::Engine as _;
use hivekeep_identity::ca::{Ca, CaPool};
use hivekeep_identity::key::KeyAlgorithm;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::ByteString;
use kube::api::{Api, DeleteParams, PostParams};

use crate::clients::Clients;
use crate::env::system_namespace;
use crate::harness::T;
use crate::kube_util;

/// The reconciler-owned ClusterTrustBundle behind the allowlisted
/// [`crate::probe::EGRESS_TRUST_BUNDLE_NAME`].
pub const EGRESS_TRUST_BUNDLE_OBJECT_NAME: &str = "egress-mitm.hivekeep.io:mitm:primary-bundle";
/// The egress CA pool Secret. Not prefixed: the installer creates it under
/// this fixed name and the reconciler looks it up the same way.
pub const EGRESS_CA_POOL_SECRET_NAME: &str = "egress-mitm-ca-pool";
/// The pool's key in that Secret.
pub const EGRESS_CA_POOL_SECRET_KEY: &str = "pool";

const CA_VALIDITY: Duration = Duration::from_secs(365 * 24 * 3600);

/// Makes sure the egress trust bundle exists, then waits until the
/// reconciler-published bundle is non-empty. It provisions a pool only when
/// there is none and never replaces one it finds: the pool is cluster-wide
/// and the egress gate mounts the one the install created.
pub async fn ensure_egress_trust_bundle(t: &T, clients: &Clients) {
    create_egress_trust_pool(t, clients, new_egress_trust_pool(t)).await;
    wait_for_egress_trust_bundle(t, clients, "").await;
}

/// Adds a fresh CA named `id` to the egress CA pool, waits for the derived
/// bundle, and returns it: what a trustBundle projection must then deliver,
/// in any order (see [`same_certificates`]). The new CA is trusted but never
/// signs, so every Actor keeps trusting the leaves the gate mints and suites
/// doing TLS through the gate can run alongside. The pool is put back as
/// found when the test ends.
pub async fn add_egress_ca(t: &T, clients: &Clients, id: &str) -> String {
    ensure_egress_trust_bundle(t, clients).await;
    let secrets: Api<Secret> = Api::namespaced(clients.kube(), &system_namespace());
    let mut secret = secrets
        .get(EGRESS_CA_POOL_SECRET_NAME)
        .await
        .unwrap_or_else(|e| t.fatal(format!("reading CA pool secret: {e}")));
    let data = secret.data.get_or_insert_with(Default::default);
    let before = data
        .get(EGRESS_CA_POOL_SECRET_KEY)
        .map(|b| b.0.clone())
        .unwrap_or_default();
    let (pool, bundle) = add_ca(&before, id)
        .unwrap_or_else(|e| t.fatal(format!("adding CA {id:?} to the egress pool: {e}")));
    data.insert(EGRESS_CA_POOL_SECRET_KEY.into(), ByteString(pool));
    secrets
        .replace(EGRESS_CA_POOL_SECRET_NAME, &PostParams::default(), &secret)
        .await
        .unwrap_or_else(|e| t.fatal(format!("updating CA pool secret: {e}")));
    let id = id.to_string();
    t.cleanup(move |t| async move {
        let restore = async {
            let mut secret = secrets.get(EGRESS_CA_POOL_SECRET_NAME).await?;
            secret
                .data
                .get_or_insert_with(Default::default)
                .insert(EGRESS_CA_POOL_SECRET_KEY.into(), ByteString(before));
            secrets
                .replace(EGRESS_CA_POOL_SECRET_NAME, &PostParams::default(), &secret)
                .await
        };
        if let Err(e) = restore.await {
            t.log(format!(
                "cleanup: restoring CA pool secret failed, CA {id:?} left in the pool: {e}"
            ));
        }
    });
    wait_for_egress_trust_bundle(t, clients, &bundle).await;
    bundle
}

/// Adds a fresh CA named `id` to a serialized pool without changing which
/// CA signs, and returns the new pool and the bundle the reconciler derives
/// from it (every root, in pool order). The pool is edited as JSON so its
/// signer selection survives exactly as it was.
pub fn add_ca(pool_bytes: &[u8], id: &str) -> Result<(Vec<u8>, String), String> {
    CaPool::from_json(pool_bytes).map_err(|e| format!("parsing the pool: {e}"))?;
    let mut wire: serde_json::Value =
        serde_json::from_slice(pool_bytes).map_err(|e| format!("parsing the pool: {e}"))?;
    let ca = Ca::generate(id, KeyAlgorithm::EcdsaP256, CA_VALIDITY)
        .map_err(|e| format!("generating the CA: {e}"))?;
    let entry = serde_json::from_slice::<serde_json::Value>(
        &CaPool::new(vec![ca], "")
            .to_json()
            .map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?["CAs"][0]
        .clone();
    // Last, because a pool that names no signer signs with its first CA.
    match wire.get_mut("CAs").and_then(|v| v.as_array_mut()) {
        Some(cas) => cas.push(entry),
        None => wire["CAs"] = serde_json::Value::Array(vec![entry]),
    }
    let out = serde_json::to_vec(&wire).map_err(|e| format!("marshaling the pool: {e}"))?;
    let pool = CaPool::from_json(&out).map_err(|e| format!("re-reading the pool: {e}"))?;
    let bundle = pool.cas().iter().map(Ca::root_pem).collect::<String>();
    Ok((out, bundle))
}

/// Whether two bundles are the same PEM CERTIFICATE blocks and nothing
/// else, in any order (the keeper shuffles a bundle's certificates every
/// time it writes one).
pub fn same_certificates(a: &str, b: &str) -> bool {
    match (certificate_blocks(a), certificate_blocks(b)) {
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}

fn canonical_block(der: &[u8]) -> String {
    hivekeep_identity::pem::encode(hivekeep_identity::pem::CERTIFICATE, der)
}

/// Splits `bundle` into its PEM blocks, sorted, each in canonical encoding.
/// `None` if the bundle holds anything else: other block types, headers,
/// non-canonical wrapping, or text around or between the blocks.
pub fn certificate_blocks(bundle: &str) -> Option<Vec<String>> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----\n";
    const END: &str = "-----END CERTIFICATE-----\n";
    let mut blocks = Vec::new();
    let mut rest = bundle;
    while !rest.is_empty() {
        let body_start = rest.strip_prefix(BEGIN)?;
        let end = body_start.find(END)?;
        let body = &body_start[..end];
        if body.contains(':') {
            return None;
        }
        let der = base64::engine::general_purpose::STANDARD
            .decode(body.replace('\n', ""))
            .ok()?;
        let encoded = canonical_block(&der);
        if !rest.starts_with(&encoded) {
            return None;
        }
        rest = &rest[encoded.len()..];
        blocks.push(encoded);
    }
    blocks.sort();
    Some(blocks)
}

/// A fresh single-CA pool Secret, the shape the admin tooling writes for
/// the egress MITM CA.
pub fn new_egress_trust_pool(t: &T) -> Secret {
    let ca = Ca::generate("mitm", KeyAlgorithm::EcdsaP256, CA_VALIDITY)
        .unwrap_or_else(|e| t.fatal(format!("generating CA for the egress pool: {e}")));
    let pool = CaPool::new(vec![ca.clone()], "")
        .to_json()
        .unwrap_or_else(|e| t.fatal(format!("marshaling the egress pool: {e}")));
    let mut secret = Secret {
        type_: Some("kubernetes.io/tls".into()),
        data: Some(
            [
                (EGRESS_CA_POOL_SECRET_KEY.to_string(), ByteString(pool)),
                (
                    "tls.crt".to_string(),
                    ByteString(ca.root_pem().into_bytes()),
                ),
                ("tls.key".to_string(), ByteString(ca.key_pem().into_bytes())),
            ]
            .into(),
        ),
        ..Default::default()
    };
    secret.metadata.name = Some(EGRESS_CA_POOL_SECRET_NAME.into());
    secret.metadata.namespace = Some(system_namespace());
    secret
}

/// Creates `secret`, tolerating a pool that already exists so concurrent
/// first users cannot clobber each other. Only the creator deletes it when
/// its test ends (whereupon the reconciler deletes the bundle).
async fn create_egress_trust_pool(t: &T, clients: &Clients, secret: Secret) {
    let secrets: Api<Secret> = Api::namespaced(clients.kube(), &system_namespace());
    match secrets.create(&PostParams::default(), &secret).await {
        Ok(_) => {
            t.cleanup(move |_| async move {
                let _ = secrets
                    .delete(EGRESS_CA_POOL_SECRET_NAME, &DeleteParams::default())
                    .await;
            });
        }
        Err(e) if kube_util::is_already_exists(&e) => {}
        Err(e) => t.fatal(format!(
            "creating CA pool secret {}/{EGRESS_CA_POOL_SECRET_NAME}: {e}",
            system_namespace()
        )),
    }
}

/// Polls the reconciler-owned bundle until it matches `want`, or is merely
/// non-empty when `want` is empty. This reads the API server while the
/// keeper resolves from its informer cache; Actor start latency dwarfs
/// watch delivery, but it is the first suspect if a rotation assertion
/// flakes.
async fn wait_for_egress_trust_bundle(t: &T, clients: &Clients, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = String::new();
    while Instant::now() < deadline {
        match kube_util::get_trust_bundle(&clients.kube(), EGRESS_TRUST_BUNDLE_OBJECT_NAME).await {
            Ok(b) => {
                if same_certificates(&b.trust_bundle, want)
                    || (want.is_empty() && !b.trust_bundle.is_empty())
                {
                    return;
                }
                last = b.trust_bundle;
            }
            Err(e) => last = format!("<{e}>"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let shown: String = last.chars().take(80).collect();
    t.fatal(format!(
        "timed out waiting for ClusterTrustBundle {EGRESS_TRUST_BUNDLE_OBJECT_NAME:?} to carry the pool's root certificates (last observed: {shown:?}...); is the controller's egress trust reconciler running?"
    ))
}

/// The service-DNS signer's roots as PEM, for a fetch that verifies a
/// [`crate::serverpod::service_dns_tls`] server's own certificate rather
/// than a leaf the egress gate minted.
pub async fn service_dns_trust_bundle(t: &T, clients: &Clients) -> String {
    let name = crate::install::SERVICE_DNS_TRUST_BUNDLE;
    let b = kube_util::get_trust_bundle(&clients.kube(), name)
        .await
        .unwrap_or_else(|e| t.fatal(format!("getting ClusterTrustBundle {name:?}: {e}")));
    if certificate_blocks(&b.trust_bundle).is_none_or(|blocks| blocks.is_empty()) {
        let shown: String = b.trust_bundle.chars().take(80).collect();
        t.fatal(format!(
            "ClusterTrustBundle {name:?} holds no certificates: {shown:?}"
        ));
    }
    b.trust_bundle
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_identity::ca::{CertificateAuthority, LeafSpec};
    use hivekeep_identity::key::PrivateKey;
    use std::time::SystemTime;

    fn generate_ca(id: &str) -> Ca {
        Ca::generate(id, KeyAlgorithm::EcdsaP256, Duration::from_secs(3600)).unwrap()
    }

    #[test]
    fn add_ca_keeps_the_signer() {
        for (name, active) in [("named signer", "install"), ("first CA signs", "")] {
            let signer = generate_ca("install");
            let before = CaPool::new(vec![signer.clone()], active).to_json().unwrap();
            let (after, bundle) = add_ca(&before, "added").unwrap();
            let pool = CaPool::from_json(&after).unwrap();
            assert_eq!(pool.cas().len(), 2, "{name}");
            let want = signer.root_pem() + &pool.cas()[1].root_pem();
            assert!(
                same_certificates(&bundle, &want),
                "{name}: bundle {bundle:?}"
            );

            let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
            let now = SystemTime::now();
            let chain = pool
                .sign(
                    &LeafSpec::new(now, now + Duration::from_secs(3600)),
                    &key.public_key(),
                )
                .unwrap();
            let (_, leaf) = x509_parser::parse_x509_certificate(&chain[0]).unwrap();
            let (_, root) = x509_parser::parse_x509_certificate(signer.root_der()).unwrap();
            assert!(
                leaf.verify_signature(Some(root.public_key())).is_ok(),
                "{name}: the new pool no longer signs with the original CA"
            );
        }
    }

    #[test]
    fn same_certificates_cases() {
        let ca = generate_ca("a");
        let a = ca.root_pem();
        let b = generate_ca("b").root_pem();
        let with_header = a.replacen(
            "-----BEGIN CERTIFICATE-----\n",
            "-----BEGIN CERTIFICATE-----\nk: v\n\n",
            1,
        );
        let key = hivekeep_identity::pem::encode("PRIVATE KEY", b"not a key");
        let sanitized =
            hivekeep_identity::pem::sanitize_certificate_bundle(format!("{a}{b}").as_bytes(), 1)
                .unwrap();
        let cases: Vec<(&str, String, String, bool)> = vec![
            ("same", format!("{a}{b}"), format!("{a}{b}"), true),
            ("reordered", format!("{a}{b}"), format!("{b}{a}"), true),
            (
                "as the keeper writes it",
                sanitized,
                format!("{a}{b}"),
                true,
            ),
            ("one missing", format!("{a}{b}"), a.clone(), false),
            ("duplicated", format!("{a}{a}"), a.clone(), false),
            ("different", a.clone(), b.clone(), false),
            ("empty", String::new(), a.clone(), false),
            (
                "text between blocks",
                format!("{a}junk\n{b}"),
                format!("{a}{b}"),
                false,
            ),
            ("other block type", format!("{a}{key}"), a.clone(), false),
            ("header", with_header, a.clone(), false),
        ];
        for (name, x, y, want) in cases {
            assert_eq!(same_certificates(&x, &y), want, "{name}");
        }
    }
}
