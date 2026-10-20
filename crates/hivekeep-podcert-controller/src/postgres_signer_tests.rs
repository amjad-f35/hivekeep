use hivekeep_identity::pem;
use hivekeep_k8s::fake::{FakeApiServer, Reaction};
use hivekeep_k8s::install_defaults::SYSTEM_NAMESPACE;
use hivekeep_node::clock::MockClock;
use k8s_openapi::ByteString;

use super::*;
use crate::signer::status::time;
use crate::signer::trust_bundle::CANARYING_LABEL;
use crate::test_support::{
    beta_client, pool, spki, start_time, stub_csr, subject_key, test_ca, verified_leaf,
    PCR_RESOURCE,
};

type Mutate = fn(&mut PodCertificateRequest);

#[derive(Default)]
struct Case {
    name: &'static str,
    namespace: Option<&'static str>,
    mutate: Option<Mutate>,
    want_denied: bool,
    want_error: bool,
    fail_update: bool,
    lifetime_hours: u64,
    want_username: Option<&'static str>,
}

fn annotations(p: &mut PodCertificateRequest) -> &mut std::collections::BTreeMap<String, String> {
    p.spec.unverified_user_annotations.get_or_insert_default()
}

const RELOCATED: &str = "team-a-hivekeep";

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "runtime login",
            lifetime_hours: 24,
            ..Default::default()
        },
        Case {
            name: "owner login",
            lifetime_hours: 24,
            want_username: Some(OWNER_USER),
            mutate: Some(|p| {
                annotations(p).insert(USERNAME_ANNOTATION.into(), OWNER_USER.into());
            }),
            ..Default::default()
        },
        Case {
            name: "relocated runtime login",
            namespace: Some(RELOCATED),
            lifetime_hours: 24,
            mutate: Some(|p| p.metadata.namespace = Some(RELOCATED.into())),
            ..Default::default()
        },
        Case {
            name: "relocated owner login",
            namespace: Some(RELOCATED),
            lifetime_hours: 24,
            want_username: Some(OWNER_USER),
            mutate: Some(|p| {
                p.metadata.namespace = Some(RELOCATED.into());
                annotations(p).insert(USERNAME_ANNOTATION.into(), OWNER_USER.into());
            }),
            ..Default::default()
        },
        Case {
            name: "default namespace denied after relocation",
            namespace: Some(RELOCATED),
            want_denied: true,
            ..Default::default()
        },
        Case {
            name: "wrong service account after relocation",
            namespace: Some(RELOCATED),
            want_denied: true,
            mutate: Some(|p| {
                p.metadata.namespace = Some(RELOCATED.into());
                p.spec.service_account_name = "default".into();
            }),
            ..Default::default()
        },
        Case {
            name: "missing username",
            want_denied: true,
            mutate: Some(|p| p.spec.unverified_user_annotations = None),
            ..Default::default()
        },
        Case {
            name: "administrator username",
            want_denied: true,
            mutate: Some(|p| {
                annotations(p).insert(USERNAME_ANNOTATION.into(), "postgres".into());
            }),
            ..Default::default()
        },
        Case {
            name: "unknown username",
            want_denied: true,
            mutate: Some(|p| {
                annotations(p).insert(USERNAME_ANNOTATION.into(), "other".into());
            }),
            ..Default::default()
        },
        Case {
            name: "unknown annotation",
            want_denied: true,
            mutate: Some(|p| {
                annotations(p).insert("other.example/username".into(), OWNER_USER.into());
            }),
            ..Default::default()
        },
        Case {
            name: "denial status update error",
            want_error: true,
            fail_update: true,
            mutate: Some(|p| p.spec.unverified_user_annotations = None),
            ..Default::default()
        },
        Case {
            name: "short lifetime",
            lifetime_hours: 1,
            mutate: Some(|p| p.spec.max_expiration_seconds = Some(3600)),
            ..Default::default()
        },
        Case {
            name: "capped lifetime",
            lifetime_hours: 24,
            mutate: Some(|p| p.spec.max_expiration_seconds = Some(7 * 86400)),
            ..Default::default()
        },
        Case {
            name: "default lifetime",
            lifetime_hours: 24,
            mutate: Some(|p| p.spec.max_expiration_seconds = None),
            ..Default::default()
        },
        Case {
            name: "wrong namespace",
            want_denied: true,
            mutate: Some(|p| p.metadata.namespace = Some("other".into())),
            ..Default::default()
        },
        Case {
            name: "wrong service account",
            want_denied: true,
            mutate: Some(|p| p.spec.service_account_name = "keeper".into()),
            ..Default::default()
        },
        Case {
            name: "owner from wrong namespace",
            want_denied: true,
            mutate: Some(|p| {
                p.metadata.namespace = Some("other".into());
                annotations(p).insert(USERNAME_ANNOTATION.into(), OWNER_USER.into());
            }),
            ..Default::default()
        },
        Case {
            name: "owner from wrong service account",
            want_denied: true,
            mutate: Some(|p| {
                p.spec.service_account_name = "default".into();
                annotations(p).insert(USERNAME_ANNOTATION.into(), OWNER_USER.into());
            }),
            ..Default::default()
        },
        Case {
            name: "wrong signer",
            want_error: true,
            mutate: Some(|p| {
                p.spec.signer_name = "podidentity.podcert.hivekeep.io/identity".into()
            }),
            ..Default::default()
        },
        Case {
            name: "invalid key",
            want_error: true,
            mutate: Some(|p| p.spec.stub_pkc_s10_request = ByteString(b"invalid".to_vec())),
            ..Default::default()
        },
        Case {
            name: "invalid lifetime",
            want_error: true,
            mutate: Some(|p| p.spec.max_expiration_seconds = Some(0)),
            ..Default::default()
        },
        Case {
            name: "status update error",
            want_error: true,
            fail_update: true,
            ..Default::default()
        },
    ]
}

fn base_request(key: &rcgen::KeyPair) -> PodCertificateRequest {
    let mut pcr = PodCertificateRequest::default();
    pcr.metadata.namespace = Some(SYSTEM_NAMESPACE.into());
    pcr.metadata.name = Some("req-1".into());
    let spec = &mut pcr.spec;
    spec.signer_name = NAME.into();
    spec.pod_name = "api-1".into();
    spec.pod_uid = "pod-1".into();
    spec.service_account_name = API_SERVER_SERVICE_ACCOUNT.into();
    spec.max_expiration_seconds = Some(86400);
    // Requester-supplied subject and names must never control the login.
    spec.stub_pkc_s10_request = ByteString(stub_csr(key));
    spec.unverified_user_annotations =
        Some([(USERNAME_ANNOTATION.to_string(), READ_WRITE_USER.to_string())].into());
    pcr
}

#[tokio::test(start_paused = true)]
async fn make_cert() {
    for tc in cases() {
        let name = tc.name;
        let ca = test_ca("postgres-test");
        let key = subject_key();
        let mut pcr = base_request(&key);
        if let Some(mutate) = tc.mutate {
            mutate(&mut pcr);
        }
        let fake = FakeApiServer::new();
        let pcr = fake.insert(&pcr);
        let client = beta_client(&fake).await;
        if tc.fail_update {
            fake.prepend_reactor("update", PCR_RESOURCE, |_| {
                Reaction::fail(500, "InternalError", "update failed")
            });
        }
        let now = start_time();
        let signer = PostgresSigner::new(
            tc.namespace.unwrap_or(SYSTEM_NAMESPACE),
            pool(std::slice::from_ref(&ca)),
            client,
            Arc::new(MockClock::new(now)),
        );
        let result = signer.make_cert(&pcr).await;
        assert_eq!(result.is_err(), tc.want_error, "{name}: {result:?}");

        let got: PodCertificateRequest = fake
            .get(pcr.metadata.namespace.as_deref().unwrap(), "req-1")
            .unwrap();
        let status = got.status.unwrap_or_default();
        if tc.want_error {
            assert!(
                status.certificate_chain.is_none() && status.conditions.is_none(),
                "{name}: failed request received certificate or status"
            );
            continue;
        }
        let conds = status.conditions.clone().unwrap_or_default();
        let want_type = if tc.want_denied { "Denied" } else { "Issued" };
        assert!(
            conds.len() == 1 && conds[0].type_ == want_type && conds[0].status == "True",
            "{name}: unexpected conditions {conds:?}"
        );
        if tc.want_denied {
            assert!(
                status.certificate_chain.is_none(),
                "{name}: unauthorized pod received certificate"
            );
            continue;
        }

        let chain = pem::decode_certificates(status.certificate_chain.as_ref().unwrap().as_bytes())
            .unwrap();
        let leaf = verified_leaf(&chain[0], ca.root_der());
        let cn: Vec<&str> = leaf
            .subject()
            .iter_common_name()
            .map(|a| a.as_str().unwrap())
            .collect();
        assert_eq!(cn, [tc.want_username.unwrap_or(READ_WRITE_USER)], "{name}");
        let eku = leaf.extended_key_usage().unwrap().unwrap().value;
        assert!(
            eku.client_auth && !eku.server_auth && eku.other.is_empty(),
            "{name}: {eku:?}"
        );
        assert!(
            leaf.subject_alternative_name().unwrap().is_none(),
            "{name}: requested names leaked"
        );
        assert_eq!(
            leaf.public_key().raw,
            spki(&key),
            "{name}: wrong public key"
        );
        let v = leaf.validity();
        assert_eq!(
            v.not_after.timestamp() - v.not_before.timestamp(),
            (tc.lifetime_hours * 3600) as i64,
            "{name}"
        );

        let at = |ts: i64| time(chrono::DateTime::from_timestamp(ts, 0).unwrap());
        assert_eq!(
            status.not_before,
            Some(at(v.not_before.timestamp())),
            "{name}"
        );
        assert_eq!(
            status.not_after,
            Some(at(v.not_after.timestamp())),
            "{name}"
        );
        assert_eq!(
            status.begin_refresh_at,
            Some(at(v.not_after.timestamp() - 1800)),
            "{name}"
        );
    }
}

#[tokio::test]
async fn desired_cluster_trust_bundles() {
    let cas = [test_ca("old"), test_ca("new")];
    let fake = FakeApiServer::new();
    let signer = PostgresSigner::new(
        SYSTEM_NAMESPACE,
        pool(&cas),
        beta_client(&fake).await,
        Arc::new(MockClock::new(start_time())),
    );
    let bundles = signer.desired_cluster_trust_bundles().unwrap();
    assert_eq!(signer.signer_name(), NAME);
    assert_eq!(bundles.len(), 1);
    let bundle = &bundles[0];
    assert_eq!(
        bundle.metadata.name.as_deref(),
        Some("postgres.podcert.hivekeep.io:identity:primary-bundle")
    );
    assert_eq!(bundle.spec.signer_name.as_deref(), Some(NAME));
    assert_eq!(
        bundle.metadata.labels.as_ref().unwrap()[CANARYING_LABEL],
        "live"
    );
    let want: String = cas.iter().map(|c| c.root_pem()).collect();
    assert_eq!(
        bundle.spec.trust_bundle, want,
        "trust bundle does not contain both roots"
    );
}
