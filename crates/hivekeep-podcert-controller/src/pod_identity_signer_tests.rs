use std::time::Duration;

use hivekeep_identity::extension::IdentityExtension;
use hivekeep_identity::pem;
use hivekeep_k8s::fake::{FakeApiServer, Reaction};
use hivekeep_node::clock::MockClock;
use k8s_openapi::ByteString;
use x509_parser::extensions::{GeneralName, ParsedExtension};

use super::*;
use crate::signer::status::time;
use crate::signer::trust_bundle::CANARYING_LABEL;
use crate::test_support::{
    beta_client, pool, spki, start_time, stub_csr, subject_key, test_ca, verified_leaf,
    PCR_RESOURCE,
};

fn make_pcr(
    namespace: &str,
    pod: &str,
    service_account: &str,
    max_expiration_seconds: i32,
) -> PodCertificateRequest {
    let mut pcr = PodCertificateRequest::default();
    pcr.metadata.namespace = Some(namespace.into());
    pcr.metadata.name = Some("req-1".into());
    let spec = &mut pcr.spec;
    spec.signer_name = NAME.into();
    spec.pod_name = pod.into();
    spec.pod_uid = "pod-uid-1".into();
    spec.service_account_name = service_account.into();
    spec.service_account_uid = "sa-uid-1".into();
    spec.node_name = "node-1".into();
    spec.node_uid = "node-uid-1".into();
    spec.max_expiration_seconds = Some(max_expiration_seconds);
    pcr
}

fn identity(namespace: &str, pod: &str, service_account: &str) -> PodIdentity {
    PodIdentity {
        namespace: namespace.into(),
        service_account_name: service_account.into(),
        service_account_uid: "sa-uid-1".into(),
        pod_name: pod.into(),
        pod_uid: "pod-uid-1".into(),
        node_name: "node-1".into(),
        node_uid: "node-uid-1".into(),
    }
}

fn key_ids(
    cert: &x509_parser::certificate::X509Certificate<'_>,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let (mut aki, mut ski) = (None, None);
    for ext in cert.extensions() {
        match ext.parsed_extension() {
            ParsedExtension::AuthorityKeyIdentifier(a) => {
                aki = a.key_identifier.as_ref().map(|k| k.0.to_vec())
            }
            ParsedExtension::SubjectKeyIdentifier(s) => ski = Some(s.0.to_vec()),
            _ => {}
        }
    }
    (aki, ski)
}

#[tokio::test(start_paused = true)]
async fn make_cert() {
    let hour = Duration::from_secs(3600);
    for (name, namespace, pod, sa, max, lifetime) in [
        (
            "keeper in hivekeep-system",
            "hivekeep-system",
            "keeper-abcde",
            "keeper",
            86400,
            24 * hour,
        ),
        // Worker pods host the tunnel ingress server, so they serve TLS
        // while running as the hive namespace's default service account.
        (
            "worker pod also serves",
            "hivekeep-demo-counter",
            "counter-abcde",
            "default",
            86400,
            24 * hour,
        ),
        (
            "ordinary workload",
            "default",
            "myapp-0",
            "default",
            86400,
            24 * hour,
        ),
        (
            "requested lifetime capped at 24h",
            "default",
            "myapp-0",
            "default",
            7 * 86400,
            24 * hour,
        ),
        (
            "shorter requested lifetime honored",
            "default",
            "myapp-0",
            "default",
            3600,
            hour,
        ),
    ] {
        let ca = test_ca("test-ca");
        let key = subject_key();
        let mut pcr = make_pcr(namespace, pod, sa, max);
        pcr.spec.stub_pkc_s10_request = ByteString(stub_csr(&key));

        // Only the request exists: the signer must not need the Pod.
        let fake = FakeApiServer::new();
        let pcr = fake.insert(&pcr);
        let now = start_time();
        let signer = PodIdentitySigner::new(
            pool(std::slice::from_ref(&ca)),
            beta_client(&fake).await,
            Arc::new(MockClock::new(now)),
        );
        signer
            .make_cert(&pcr)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));

        let got: PodCertificateRequest = fake.get(namespace, "req-1").unwrap();
        let status = got.status.unwrap();
        let conds = status.conditions.unwrap();
        assert_eq!(conds.len(), 1, "{name}");
        assert_eq!(conds[0].type_, "Issued", "{name}");

        let chain = pem::decode_certificates(status.certificate_chain.unwrap().as_bytes()).unwrap();
        assert_eq!(
            chain.len(),
            1,
            "{name}: expected exactly one certificate (no intermediates)"
        );
        let leaf = verified_leaf(&chain[0], ca.root_der());
        assert_eq!(
            leaf.public_key().raw,
            spki(&key),
            "{name}: wrong subject key"
        );

        let eku = leaf.extended_key_usage().unwrap().unwrap().value;
        assert!(eku.client_auth && eku.server_auth, "{name}: {eku:?}");

        let want_not_before = now - chrono::Duration::minutes(2);
        let want_not_after = want_not_before + chrono::Duration::from_std(lifetime).unwrap();
        let want_refresh = want_not_after - chrono::Duration::minutes(30);
        assert_eq!(
            leaf.validity().not_before.timestamp(),
            want_not_before.timestamp(),
            "{name}"
        );
        assert_eq!(
            leaf.validity().not_after.timestamp(),
            want_not_after.timestamp(),
            "{name}"
        );
        assert_eq!(status.not_before, Some(time(want_not_before)), "{name}");
        assert_eq!(status.not_after, Some(time(want_not_after)), "{name}");
        assert_eq!(status.begin_refresh_at, Some(time(want_refresh)), "{name}");

        let san = leaf.subject_alternative_name().unwrap().unwrap();
        let want_uri = format!("spiffe://cluster.local/ns/{namespace}/sa/{sa}");
        assert_eq!(
            san.value.general_names,
            vec![GeneralName::URI(&want_uri)],
            "{name}"
        );

        let (_, root) = x509_parser::prelude::FromDer::from_der(ca.root_der()).unwrap();
        let (aki, _) = key_ids(&leaf);
        let (_, root_ski) = key_ids(&root);
        assert!(
            aki.is_some() && aki == root_ski,
            "{name}: AKI {aki:?} != root SKI {root_ski:?}"
        );

        let got_identity = PodIdentity::from_certificate(&chain[0]).unwrap();
        assert_eq!(got_identity, Some(identity(namespace, pod, sa)), "{name}");
    }
}

#[tokio::test]
async fn make_cert_errors() {
    for (name, omit_key, fail_update) in [
        ("no key material in PCR", true, false),
        ("status update fails", false, true),
    ] {
        let mut pcr = make_pcr("hivekeep-system", "keeper-abcde", "keeper", 86400);
        if !omit_key {
            pcr.spec.stub_pkc_s10_request = ByteString(stub_csr(&subject_key()));
        }
        let fake = FakeApiServer::new();
        let pcr = fake.insert(&pcr);
        if fail_update {
            fake.prepend_reactor("update", PCR_RESOURCE, |_| {
                Reaction::fail(500, "InternalError", "injected update failure")
            });
        }
        let signer = PodIdentitySigner::new(
            pool(&[test_ca("test-ca")]),
            beta_client(&fake).await,
            Arc::new(MockClock::new(start_time())),
        );
        assert!(
            signer.make_cert(&pcr).await.is_err(),
            "{name}: got nil error"
        );
        let got: PodCertificateRequest = fake.get("hivekeep-system", "req-1").unwrap();
        assert!(
            got.status
                .is_none_or(|s| s.conditions.is_none() && s.certificate_chain.is_none()),
            "{name}: status updated despite error"
        );
    }
}

#[tokio::test]
async fn desired_cluster_trust_bundles() {
    let cas = [test_ca("test-ca-1"), test_ca("test-ca-2")];
    let fake = FakeApiServer::new();
    let signer = PodIdentitySigner::new(
        pool(&cas),
        beta_client(&fake).await,
        Arc::new(MockClock::new(start_time())),
    );
    let ctbs = signer.desired_cluster_trust_bundles().unwrap();
    assert_eq!(ctbs.len(), 1);
    let ctb = &ctbs[0];
    assert_eq!(
        ctb.metadata.name.as_deref(),
        Some("podidentity.podcert.hivekeep.io:identity:primary-bundle")
    );
    assert_eq!(ctb.spec.signer_name.as_deref(), Some(NAME));
    assert_eq!(
        ctb.metadata.labels.as_ref().unwrap()[CANARYING_LABEL],
        "live"
    );
    let want: String = cas.iter().map(|c| c.root_pem()).collect();
    assert_eq!(ctb.spec.trust_bundle, want);
}
