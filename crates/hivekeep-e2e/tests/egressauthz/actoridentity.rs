//! Minting the Actor credential the egress probe presents.
//!
//! The gate's front door requires a client certificate signed by the
//! actor-identity CA, and its external authorization then looks the
//! certified Actor up in the control plane. Getting through needs both: a
//! leaf this pool signs, and an Actor the API agrees is running. The suite
//! mints the first half and never supplies the second.

use std::time::{Duration, SystemTime};

use hivekeep_core::resource::ActorRef;
use hivekeep_core::spiffe::worker_for_actor_spiffe_id;
use hivekeep_e2e::env::system_namespace;
use hivekeep_e2e::harness::T;
use hivekeep_identity::ca::{Ca, CaPool, LeafSpec};
use hivekeep_identity::key::{KeyAlgorithm, PrivateKey};
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::ByteString;
use kube::api::{Api, PostParams};

/// The Secret the API signs Actor certificates from, and its key.
pub const ACTOR_ID_CA_SECRET: &str = "actor-id-ca-pool";
pub const ACTOR_ID_CA_SECRET_KEY: &str = "pool";
/// The hive named in the credential. No such hive is created: nothing looks
/// it up until external authorization does, which must come back empty.
pub const PROBE_HIVE: &str = "hivekeep-egressauthz-e2e";
/// Matches what the API mints. A credential outliving the real one would
/// hide an expiry bug in the gate rather than reproduce it.
pub const ACTOR_CERTIFICATE_LIFETIME: Duration = Duration::from_secs(3600);
/// Where the probe pod finds the minted credential; in step with
/// fixtures/testserver/egressprobe.yaml.tmpl.
pub const UNKNOWN_ACTOR_CREDENTIAL_SECRET: &str = "egressprobe-unknown-actor";
pub const UNKNOWN_ACTOR_CREDENTIAL_PATH: &str = "/run/actor-identity-unknown/credential-bundle.pem";
pub const CREDENTIAL_BUNDLE_KEY: &str = "credential-bundle.pem";
/// The probe's own workload identity: a valid Hivekeep credential that is
/// not an Actor, mounted so the suite can show the gate refusing it.
pub const POD_IDENTITY_CREDENTIAL_PATH: &str =
    "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem";

/// The CA that signs Actor certificates, straight from the API's Secret.
/// The first CA signs, the same choice the API makes.
pub async fn actor_identity_ca(t: &T) -> Ca {
    let ns = system_namespace();
    let secret = Api::<Secret>::namespaced(hivekeep_e2e::suite::clients().kube(), &ns)
        .get(ACTOR_ID_CA_SECRET)
        .await
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "reading actor-identity CA pool secret {ns}/{ACTOR_ID_CA_SECRET}: {e}"
            ))
        });
    let data = secret
        .data
        .unwrap_or_default()
        .get(ACTOR_ID_CA_SECRET_KEY)
        .map(|b| b.0.clone())
        .unwrap_or_default();
    let pool = CaPool::from_json(&data)
        .unwrap_or_else(|e| t.fatal(format!("parsing actor-identity CA pool from {ns}/{ACTOR_ID_CA_SECRET} key {ACTOR_ID_CA_SECRET_KEY:?}: {e}")));
    pool.cas().first().cloned().unwrap_or_else(|| {
        t.fatal(format!(
            "actor-identity CA pool {ns}/{ACTOR_ID_CA_SECRET} contains no CA"
        ))
    })
}

/// A client credential for `hive/name` in the shape a worker's tunnel gets
/// from the API: the PKCS#8 key first, then the chain leaf first.
pub fn mint_actor_credential(t: &T, ca: &Ca, hive: &str, name: &str) -> Vec<u8> {
    let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256)
        .unwrap_or_else(|e| t.fatal(format!("generating actor key: {e}")));
    let now = SystemTime::now();
    let mut leaf = LeafSpec::new(
        now - Duration::from_secs(300),
        now + ACTOR_CERTIFICATE_LIFETIME,
    );
    leaf.uris = vec![worker_for_actor_spiffe_id(&ActorRef::new(hive, name))];
    leaf.client_auth = true;
    let der = ca.sign(&leaf, &key.public_key()).unwrap_or_else(|e| {
        t.fatal(format!(
            "signing the actor certificate for {hive}/{name}: {e}"
        ))
    });
    let mut bundle = key.to_pem();
    bundle.push_str(&hivekeep_identity::pem::encode(
        hivekeep_identity::pem::CERTIFICATE,
        &der,
    ));
    bundle.into_bytes()
}

/// Puts a minted bundle where the probe pod can mount it.
pub async fn write_credential_secret(t: &T, ns: &str, name: &str, bundle: Vec<u8>) {
    let mut secret = Secret {
        data: Some([(CREDENTIAL_BUNDLE_KEY.to_string(), ByteString(bundle))].into()),
        ..Default::default()
    };
    secret.metadata.name = Some(name.into());
    secret.metadata.namespace = Some(ns.into());
    if let Err(e) = Api::<Secret>::namespaced(hivekeep_e2e::suite::clients().kube(), ns)
        .create(&PostParams::default(), &secret)
        .await
    {
        t.fatal(format!("creating credential secret {ns}/{name}: {e}"));
    }
}

/// Mints the one credential the probe mounts: a leaf the real
/// actor-identity CA signed, naming an Actor the control plane has never
/// heard of (scoped by the probe's namespace so it cannot collide).
pub async fn provision_probe_credentials(t: &T, ns: &str) {
    let ca = actor_identity_ca(t).await;
    let bundle = mint_actor_credential(t, &ca, PROBE_HIVE, &format!("no-such-actor-{ns}"));
    write_credential_secret(t, ns, UNKNOWN_ACTOR_CREDENTIAL_SECRET, bundle).await;
}
