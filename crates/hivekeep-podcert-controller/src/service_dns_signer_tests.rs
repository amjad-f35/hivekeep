use std::collections::BTreeMap;
use std::time::Duration;

use hivekeep_identity::pem;
use hivekeep_k8s::fake::FakeApiServer;
use hivekeep_node::clock::MockClock;
use k8s_openapi::api::core::v1::ServiceSpec;
use k8s_openapi::ByteString;
use x509_parser::extensions::GeneralName;

use super::*;
use crate::signer::trust_bundle::CANARYING_LABEL;
use crate::test_support::{
    beta_client, pool, start_time, stub_csr, subject_key, test_ca, verified_leaf,
};

fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn service(name: &str, type_: &str, selector: &[(&str, &str)]) -> Service {
    let mut s = Service::default();
    s.metadata.namespace = Some("ns".into());
    s.metadata.name = Some(name.into());
    s.spec = Some(ServiceSpec {
        type_: Some(type_.into()),
        selector: (!selector.is_empty()).then(|| labels(selector)),
        ..Default::default()
    });
    s
}

fn pod(name: &str, uid: &str, pod_labels: &[(&str, &str)]) -> Pod {
    let mut p = Pod::default();
    p.metadata.namespace = Some("ns".into());
    p.metadata.name = Some(name.into());
    p.metadata.uid = Some(uid.into());
    p.metadata.labels = Some(labels(pod_labels));
    p
}

fn request() -> PodCertificateRequest {
    let mut pcr = PodCertificateRequest::default();
    pcr.metadata.namespace = Some("ns".into());
    pcr.metadata.name = Some("req".into());
    pcr.spec.signer_name = NAME.into();
    pcr.spec.pod_name = "api-0".into();
    pcr.spec.pod_uid = "uid-0".into();
    pcr.spec.max_expiration_seconds = Some(3600);
    pcr.spec.stub_pkc_s10_request = ByteString(stub_csr(&subject_key()));
    pcr
}

async fn signer(fake: &FakeApiServer, ca: &hivekeep_identity::ca::Ca) -> ServiceDnsSigner {
    ServiceDnsSigner::new(
        fake.client(),
        pool(std::slice::from_ref(ca)),
        beta_client(fake).await,
        Arc::new(MockClock::new(start_time())),
    )
}

#[tokio::test]
async fn issues_names_of_selecting_services() {
    let fake = FakeApiServer::new();
    fake.insert(&pod("api-0", "uid-0", &[("app", "api"), ("tier", "web")]));
    fake.insert(&pod("other", "uid-1", &[("app", "other")]));
    fake.insert(&service("api", "ClusterIP", &[("app", "api")]));
    fake.insert(&service(
        "web",
        "NodePort",
        &[("tier", "web"), ("app", "api")],
    ));
    fake.insert(&service("lb", "LoadBalancer", &[("app", "api")]));
    fake.insert(&service("external", "ExternalName", &[("app", "api")]));
    fake.insert(&service("headless-manual", "ClusterIP", &[]));
    fake.insert(&service("other", "ClusterIP", &[("app", "other")]));
    let ca = test_ca("ca");
    let s = signer(&fake, &ca).await;
    let pcr = fake.insert(&request());
    s.make_cert(&pcr).await.unwrap();

    let got: PodCertificateRequest = fake.get("ns", "req").unwrap();
    let status = got.status.unwrap();
    assert_eq!(status.conditions.unwrap()[0].type_, "Issued");
    let chain = pem::decode_certificates(status.certificate_chain.unwrap().as_bytes()).unwrap();
    let leaf = verified_leaf(&chain[0], ca.root_der());
    let mut names: Vec<String> = leaf
        .subject_alternative_name()
        .unwrap()
        .unwrap()
        .value
        .general_names
        .iter()
        .map(|n| match n {
            GeneralName::DNSName(d) => d.to_string(),
            other => panic!("unexpected SAN {other:?}"),
        })
        .collect();
    names.sort();
    assert_eq!(names, ["api.ns.svc", "lb.ns.svc", "web.ns.svc"]);
    assert!(
        leaf.subject().iter_common_name().next().is_none(),
        "requested subject leaked"
    );
    let eku = leaf.extended_key_usage().unwrap().unwrap().value;
    assert!(eku.client_auth && eku.server_auth);
    let v = leaf.validity();
    assert_eq!(
        v.not_after.timestamp() - v.not_before.timestamp(),
        Duration::from_secs(3600).as_secs() as i64
    );
}

#[tokio::test]
async fn unselected_pods_are_retried_not_issued() {
    let fake = FakeApiServer::new();
    // Same name, different incarnation.
    fake.insert(&pod("api-0", "uid-old", &[("app", "api")]));
    fake.insert(&service("api", "ClusterIP", &[("app", "api")]));
    let ca = test_ca("ca");
    let s = signer(&fake, &ca).await;
    let pcr = fake.insert(&request());
    let err = s.make_cert(&pcr).await.unwrap_err();
    assert!(matches!(err, SignerError::NotSelected { .. }), "{err}");
    let got: PodCertificateRequest = fake.get("ns", "req").unwrap();
    assert!(got.status.is_none_or(|s| s.conditions.is_none()));
}

#[tokio::test]
async fn desired_cluster_trust_bundles() {
    let fake = FakeApiServer::new();
    let ca = test_ca("ca");
    let s = signer(&fake, &ca).await;
    let ctbs = s.desired_cluster_trust_bundles().unwrap();
    assert_eq!(ctbs.len(), 1);
    assert_eq!(
        ctbs[0].metadata.name.as_deref(),
        Some("servicedns.podcert.hivekeep.io:identity:primary-bundle")
    );
    assert_eq!(ctbs[0].spec.signer_name.as_deref(), Some(NAME));
    assert_eq!(
        ctbs[0].metadata.labels.as_ref().unwrap()[CANARYING_LABEL],
        "live"
    );
    assert_eq!(ctbs[0].spec.trust_bundle, ca.root_pem());
}
