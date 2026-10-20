use hivekeep_k8s::fake::FakeApiServer;
use hivekeep_k8s::install_defaults::SYSTEM_NAMESPACE;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

use super::super::events::RecordingEventRecorder;
use super::super::test_support::make_worker_pool;
use super::*;

#[tokio::test]
async fn build_network_policy_relocated_namespace() {
    let r = NetworkPolicyReconciler::live(
        FakeApiServer::new().client(),
        "hivekeep-test",
        Arc::new(RecordingEventRecorder::new()),
    );
    let np = r.build_network_policy(&make_worker_pool("pool", "default", 2, "worker:v1"));
    let ingress = np.spec.as_ref().unwrap().ingress.as_ref().unwrap();
    assert_eq!(ingress.len(), 1);
    let from = ingress[0].from.as_ref().unwrap();
    assert_eq!(from.len(), 1);
    let got = from[0]
        .namespace_selector
        .as_ref()
        .unwrap()
        .match_labels
        .as_ref()
        .unwrap()["kubernetes.io/metadata.name"]
        .clone();
    assert_eq!(got, "hivekeep-test");
}

#[tokio::test]
async fn network_policy_takeover() {
    let cases = [
        ("labeled for another pool", "other-pool", "", true),
        ("unlabeled", "", "", true),
        (
            "controlled by an earlier pool with the same name",
            "netpol-takeover",
            "earlier-uid",
            true,
        ),
        ("orphaned policy of this pool", "netpol-takeover", "", false),
        (
            "controlled by this pool with an edited label",
            "edited",
            "netpol-takeover-uid",
            false,
        ),
    ];
    for (name, lbl, owner_uid, want_err) in cases {
        let fake = FakeApiServer::new();
        // The pool is never stored, so only this test reconciles it.
        let mut wp = make_worker_pool("netpol-takeover", "default", 1, "worker:v1");
        wp.metadata.uid = Some("netpol-takeover-uid".into());

        // Applied as the controller's field manager, as an earlier
        // reconcile would.
        let mut pre = NetworkPolicy {
            metadata: ObjectMeta {
                name: Some(network_policy_name(&wp.name_any())),
                namespace: Some("default".into()),
                ..Default::default()
            },
            spec: Some(NetworkPolicySpec {
                policy_types: Some(vec!["Ingress".into()]),
                ..Default::default()
            }),
        };
        if !lbl.is_empty() {
            pre.metadata.labels = Some(label(WORKER_POOL_LABEL, lbl));
        }
        if !owner_uid.is_empty() {
            pre.metadata.owner_references = Some(vec![OwnerReference {
                api_version: "hivekeep.io/v1".into(),
                kind: "WorkerPool".into(),
                name: wp.name_any(),
                uid: owner_uid.into(),
                controller: Some(true),
                block_owner_deletion: None,
            }]);
        }
        let api: Api<NetworkPolicy> = Api::namespaced(fake.client(), "default");
        let np = api
            .patch(
                &pre.name_any(),
                &PatchParams::apply(NETWORK_POLICY_FIELD_OWNER),
                &Patch::Apply(&pre),
            )
            .await
            .unwrap();

        let rec = Arc::new(RecordingEventRecorder::new());
        let r = NetworkPolicyReconciler::live(fake.client(), SYSTEM_NAMESPACE, rec.clone());
        let res = r.reconcile_impl(&wp).await;
        assert_eq!(res.is_err(), want_err, "{name}: {res:?}");
        let want_events: Vec<String> = match &res {
            Err(e) => vec![format!(
                "Warning {EVENT_REASON_NETWORK_POLICY_CONFLICT} {e}"
            )],
            Ok(()) => vec![],
        };
        assert_eq!(rec.drain(), want_events, "{name}");

        let got = api.get(&np.name_any()).await.unwrap();
        if want_err {
            assert_eq!(
                got.metadata.resource_version, np.metadata.resource_version,
                "{name}: NetworkPolicy was rewritten"
            );
            continue;
        }
        let owner = controller_of(&got.metadata).unwrap_or_else(|| panic!("{name}: no controller"));
        assert_eq!(owner.uid, "netpol-takeover-uid", "{name}");
        assert_eq!(
            got.labels().get(WORKER_POOL_LABEL).map(String::as_str),
            Some("netpol-takeover"),
            "{name}"
        );
    }
}
