use std::collections::BTreeMap;
use std::time::Duration;

use hivekeep_identity::ca::Ca;
use hivekeep_identity::key::KeyAlgorithm;
use hivekeep_k8s::cert_api::CertVersion;
use hivekeep_k8s::fake::FakeApiServer;
use hivekeep_k8s::install_defaults::SYSTEM_NAMESPACE;
use k8s_openapi::api::certificates::v1beta1::ClusterTrustBundleSpec;
use k8s_openapi::ByteString;
use kube::api::{Api, PostParams};

use super::*;

/// A pool of fresh CAs named `ids`, in that order.
fn ca_pool(ids: &[&str]) -> CaPool {
    let cas = ids
        .iter()
        .map(|id| Ca::generate(*id, KeyAlgorithm::Ed25519, Duration::from_secs(24 * 3600)).unwrap())
        .collect();
    CaPool::new(cas, "")
}

fn pool_data(pool: &CaPool) -> BTreeMap<String, ByteString> {
    BTreeMap::from([(
        EGRESS_MITM_CA_POOL_KEY.to_string(),
        ByteString(pool.to_json().unwrap()),
    )])
}

/// The pool Secret, shaped the way the admin tooling writes it.
fn secret_for(pool: &CaPool) -> Secret {
    let (ns, name) = egress_mitm_ca_pool_ref(SYSTEM_NAMESPACE);
    let mut s = Secret {
        data: Some(pool_data(pool)),
        ..Default::default()
    };
    s.metadata.namespace = Some(ns);
    s.metadata.name = Some(name);
    s
}

fn root_pem(pool: &CaPool) -> String {
    pool.cas().iter().map(|c| c.root_pem()).collect()
}

fn reconciler(fake: &FakeApiServer, version: CertVersion) -> EgressMitmTrustReconciler {
    let client = fake.client();
    EgressMitmTrustReconciler::new(
        client.clone(),
        Reader::live_namespaced(client.clone()),
        ClusterTrustBundleClient::with_version(client, version, None),
    )
}

async fn reconcile_pool(fake: &FakeApiServer) -> Result<Action> {
    reconcile_pool_at(fake, CertVersion::V1).await
}

async fn reconcile_pool_at(fake: &FakeApiServer, version: CertVersion) -> Result<Action> {
    let (ns, name) = egress_mitm_ca_pool_ref(SYSTEM_NAMESPACE);
    reconciler(fake, version)
        .reconcile(&ObjectKey::new(ns, name))
        .await
}

fn trust_bundle(fake: &FakeApiServer) -> Option<ClusterTrustBundle> {
    fake.get::<ClusterTrustBundle>("", EGRESS_MITM_TRUST_BUNDLE_NAME)
}

fn secrets(fake: &FakeApiServer) -> Api<Secret> {
    Api::namespaced(fake.client(), SYSTEM_NAMESPACE)
}

async fn replace_pool_data(fake: &FakeApiServer, data: BTreeMap<String, ByteString>) {
    let api = secrets(fake);
    let mut current = api.get(EGRESS_MITM_CA_POOL_SECRET).await.unwrap();
    current.data = Some(data);
    api.replace(EGRESS_MITM_CA_POOL_SECRET, &PostParams::default(), &current)
        .await
        .unwrap();
}

#[tokio::test]
async fn egress_mitm_trust_publishes_every_root() {
    let fake = FakeApiServer::new();
    let pool = ca_pool(&["mitm", "mitm-next"]);
    fake.insert(&secret_for(&pool));
    reconcile_pool(&fake).await.unwrap();
    let ctb = trust_bundle(&fake).expect("no ClusterTrustBundle was created");
    assert_eq!(
        ctb.spec.signer_name.as_deref(),
        Some(EGRESS_MITM_SIGNER_NAME)
    );
    assert_eq!(
        ctb.metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(CANARYING_LABEL))
            .map(String::as_str),
        Some("live")
    );
    // Both roots, in pool order: the outgoing root still signs leaves.
    assert_eq!(ctb.spec.trust_bundle, root_pem(&pool));
}

#[tokio::test]
async fn egress_mitm_trust_publishes_no_private_key() {
    let fake = FakeApiServer::new();
    fake.insert(&secret_for(&ca_pool(&["mitm"])));
    reconcile_pool(&fake).await.unwrap();
    let ctb = trust_bundle(&fake).expect("no ClusterTrustBundle was created");
    let blocks =
        hivekeep_identity::pem::decode_all(ctb.spec.trust_bundle.as_bytes()).expect("only PEM");
    assert!(!blocks.is_empty());
    for b in blocks {
        assert_eq!(
            b.label, "CERTIFICATE",
            "only CERTIFICATE blocks belong in the bundle"
        );
    }
}

#[tokio::test]
async fn egress_mitm_trust_follows_pool_rotation() {
    let fake = FakeApiServer::new();
    fake.insert(&secret_for(&ca_pool(&["mitm"])));
    reconcile_pool(&fake).await.unwrap();
    let rotated = ca_pool(&["mitm", "mitm-next"]);
    replace_pool_data(&fake, pool_data(&rotated)).await;
    reconcile_pool(&fake).await.unwrap();
    let ctb = trust_bundle(&fake).expect("the ClusterTrustBundle disappeared");
    assert_eq!(ctb.spec.trust_bundle, root_pem(&rotated));
}

#[tokio::test]
async fn egress_mitm_trust_recreates_deleted_bundle() {
    let fake = FakeApiServer::new();
    let pool = ca_pool(&["mitm"]);
    fake.insert(&secret_for(&pool));
    reconcile_pool(&fake).await.unwrap();
    assert!(
        trust_bundle(&fake).is_some(),
        "no ClusterTrustBundle was created"
    );
    ClusterTrustBundleClient::with_version(fake.client(), CertVersion::V1, None)
        .delete(EGRESS_MITM_TRUST_BUNDLE_NAME, &DeleteParams::default())
        .await
        .unwrap();
    reconcile_pool(&fake).await.unwrap();
    let got = trust_bundle(&fake).expect("the ClusterTrustBundle was not recreated");
    assert_eq!(got.spec.trust_bundle, root_pem(&pool));
}

#[tokio::test]
async fn egress_mitm_trust_deletes_bundle_when_pool_is_gone() {
    let fake = FakeApiServer::new();
    fake.insert(&secret_for(&ca_pool(&["mitm"])));
    reconcile_pool(&fake).await.unwrap();
    secrets(&fake)
        .delete(EGRESS_MITM_CA_POOL_SECRET, &DeleteParams::default())
        .await
        .unwrap();
    reconcile_pool(&fake).await.unwrap();
    assert!(
        trust_bundle(&fake).is_none(),
        "the ClusterTrustBundle outlived its CA pool"
    );
}

fn foreign_bundle(signer: &str) -> ClusterTrustBundle {
    let mut b = ClusterTrustBundle {
        spec: ClusterTrustBundleSpec {
            signer_name: Some(signer.into()),
            trust_bundle: String::new(),
        },
        ..Default::default()
    };
    b.metadata.name = Some(EGRESS_MITM_TRUST_BUNDLE_NAME.into());
    b
}

#[tokio::test]
async fn egress_mitm_trust_leaves_foreign_bundle_alone() {
    let fake = FakeApiServer::new();
    fake.insert(&foreign_bundle("someone.else.example/identity"));
    // No pool Secret exists, so this takes the delete path.
    assert!(
        reconcile_pool(&fake).await.is_err(),
        "deleted a ClusterTrustBundle of another signer"
    );
    assert!(
        trust_bundle(&fake).is_some(),
        "the foreign ClusterTrustBundle was deleted"
    );
}

#[tokio::test]
async fn egress_mitm_trust_keeps_last_good_bundle_on_bad_pool() {
    let cases: Vec<(&str, BTreeMap<String, ByteString>)> = vec![
        (
            "missing key",
            BTreeMap::from([("not-pool".into(), ByteString(b"{}".to_vec()))]),
        ),
        (
            "unparseable",
            BTreeMap::from([("pool".into(), ByteString(b"not json".to_vec()))]),
        ),
        (
            "no CAs",
            BTreeMap::from([("pool".into(), ByteString(br#"{"CAs":[]}"#.to_vec()))]),
        ),
    ];
    for (name, data) in cases {
        let fake = FakeApiServer::new();
        let pool = ca_pool(&["mitm"]);
        fake.insert(&secret_for(&pool));
        reconcile_pool(&fake).await.unwrap();
        replace_pool_data(&fake, data).await;
        assert!(
            reconcile_pool(&fake).await.is_err(),
            "{name}: accepted an unreadable pool; it should fail and requeue"
        );
        let ctb = trust_bundle(&fake)
            .unwrap_or_else(|| panic!("{name}: the bundle was removed by an unreadable pool"));
        assert_eq!(
            ctb.spec.trust_bundle,
            root_pem(&pool),
            "{name}: rewritten from an unreadable pool"
        );
    }
}

#[tokio::test]
async fn egress_mitm_trust_api_versions() {
    for version in [CertVersion::V1beta1, CertVersion::V1] {
        let fake = FakeApiServer::new();
        let pool = ca_pool(&["initial"]);
        fake.insert(&secret_for(&pool));
        let r = reconciler(&fake, version);
        let key = {
            let (ns, name) = egress_mitm_ca_pool_ref(SYSTEM_NAMESPACE);
            ObjectKey::new(ns, name)
        };
        let bundles = ClusterTrustBundleClient::with_version(fake.client(), version, None);
        r.reconcile(&key).await.unwrap();
        let bundle = bundles.get(EGRESS_MITM_TRUST_BUNDLE_NAME).await.unwrap();
        assert_eq!(bundle.spec.trust_bundle, root_pem(&pool), "{version:?}");
        assert_eq!(
            bundle.spec.signer_name.as_deref(),
            Some(EGRESS_MITM_SIGNER_NAME),
            "{version:?}"
        );
        assert_eq!(
            bundle
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(CANARYING_LABEL))
                .map(String::as_str),
            Some("live"),
            "{version:?}"
        );
        // Every bundle request went to the discovered version.
        for a in fake
            .actions()
            .iter()
            .filter(|a| a.resource == "clustertrustbundles")
        {
            assert_eq!(a.version, version.as_str(), "{version:?}: {a:?}");
        }

        let next = ca_pool(&["rotated"]);
        replace_pool_data(&fake, pool_data(&next)).await;
        r.reconcile(&key).await.unwrap();
        assert_eq!(
            bundles
                .get(EGRESS_MITM_TRUST_BUNDLE_NAME)
                .await
                .unwrap()
                .spec
                .trust_bundle,
            root_pem(&next),
            "{version:?}: rotation"
        );

        secrets(&fake)
            .delete(EGRESS_MITM_CA_POOL_SECRET, &DeleteParams::default())
            .await
            .unwrap();
        r.reconcile(&key).await.unwrap();
        let err = bundles
            .get(EGRESS_MITM_TRUST_BUNDLE_NAME)
            .await
            .expect_err("bundle survived pool deletion");
        assert!(
            hivekeep_k8s::api_errors::is_not_found(&err),
            "{version:?}: {err}"
        );

        bundles
            .create(
                &PostParams::default(),
                &foreign_bundle("other.example/signer"),
            )
            .await
            .unwrap();
        assert!(
            r.reconcile(&key).await.is_err(),
            "{version:?}: did not reject deleting a foreign bundle"
        );
        assert_eq!(
            bundles
                .get(EGRESS_MITM_TRUST_BUNDLE_NAME)
                .await
                .unwrap()
                .spec
                .signer_name
                .as_deref(),
            Some("other.example/signer"),
            "{version:?}: foreign bundle changed"
        );
    }
}

#[test]
fn ca_pool_ref() {
    assert_eq!(
        egress_mitm_ca_pool_ref("ns"),
        ("ns".to_string(), "egress-mitm-ca-pool".to_string())
    );
}
