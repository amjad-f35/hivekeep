use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use hivekeep_k8s::cert_api::CertVersion;
use hivekeep_k8s::fake::FakeApiServer;
use k8s_openapi::api::certificates::v1beta1::ClusterTrustBundleSpec;
use parking_lot::Mutex;

use super::*;
use crate::signer::status::with_denied;
use crate::test_support::{beta_client, start_time, wait_until, FixedHasher};

const SIGNER: &str = "example.com/signer";

#[derive(Default)]
struct FakeSigner {
    bundles: Vec<ClusterTrustBundle>,
    fail: AtomicBool,
    made: Mutex<Vec<String>>,
}

#[async_trait]
impl SignerImpl for FakeSigner {
    fn signer_name(&self) -> &str {
        SIGNER
    }

    fn desired_cluster_trust_bundles(&self) -> Result<Vec<ClusterTrustBundle>, SignerError> {
        Ok(self.bundles.clone())
    }

    async fn make_cert(&self, pcr: &PodCertificateRequest) -> Result<(), SignerError> {
        self.made.lock().push(pcr.name_any());
        if self.fail.load(Ordering::SeqCst) {
            return Err(SignerError::NoPublicKey);
        }
        Ok(())
    }
}

fn bundle(trust: &str, labels: Option<&[(&str, &str)]>) -> ClusterTrustBundle {
    let mut b = ClusterTrustBundle {
        spec: ClusterTrustBundleSpec {
            signer_name: Some(SIGNER.into()),
            trust_bundle: trust.into(),
        },
        ..Default::default()
    };
    b.metadata.name = Some("example.com:signer:bundle".into());
    b.metadata.labels = labels.map(|l| {
        l.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    });
    b
}

async fn controller(
    fake: &FakeApiServer,
    signer: Arc<FakeSigner>,
    assigned: bool,
) -> Arc<Controller> {
    fake.serve(
        &CertVersion::V1.api_version(),
        &[("clustertrustbundles", "ClusterTrustBundle", false)],
    );
    let bundles = ClusterTrustBundleClient::new(fake.client(), None)
        .await
        .unwrap();
    Controller::new(
        signer,
        Arc::new(FixedHasher(assigned)),
        beta_client(fake).await,
        Arc::new(bundles),
    )
}

#[tokio::test]
async fn ensure_bundles() {
    let want = bundle("root-a", Some(&[("k", "v")]));
    for (name, existing, want_verb) in [
        ("missing", None, Some("create")),
        (
            "matching",
            Some(bundle("root-a", Some(&[("k", "v")]))),
            None,
        ),
        (
            "stale bundle",
            Some(bundle("root-old", Some(&[("k", "v")]))),
            Some("update"),
        ),
        ("stale labels", Some(bundle("root-a", None)), Some("update")),
    ] {
        let fake = FakeApiServer::new();
        let signer = Arc::new(FakeSigner {
            bundles: vec![want.clone()],
            ..Default::default()
        });
        let c = controller(&fake, signer, true).await;
        if let Some(existing) = existing {
            fake.insert(&existing);
        }
        fake.clear_actions();
        c.ensure_bundles().await;

        let writes: Vec<String> = fake
            .actions()
            .into_iter()
            .filter(|a| a.verb == "create" || a.verb == "update")
            .map(|a| a.verb)
            .collect();
        match want_verb {
            None => assert!(writes.is_empty(), "{name}: writes = {writes:?}, want none"),
            Some(v) => assert_eq!(writes, [v], "{name}"),
        }
        let got: ClusterTrustBundle = fake.get("", "example.com:signer:bundle").unwrap();
        assert_eq!(got.spec, want.spec, "{name}");
        assert_eq!(
            got.labels().get("k").map(String::as_str),
            Some("v"),
            "{name}"
        );
    }
}

#[tokio::test]
async fn ensure_bundles_only_on_the_owning_replica() {
    let fake = FakeApiServer::new();
    let signer = Arc::new(FakeSigner {
        bundles: vec![bundle("root-a", None)],
        ..Default::default()
    });
    let c = controller(&fake, signer, false).await;
    fake.clear_actions();
    c.ensure_bundles().await;
    assert!(fake.actions().is_empty());
}

fn request(name: &str, signer: &str) -> PodCertificateRequest {
    let mut pcr = PodCertificateRequest::default();
    pcr.metadata.namespace = Some("ns".into());
    pcr.metadata.name = Some(name.into());
    pcr.spec.signer_name = signer.into();
    pcr
}

#[tokio::test]
async fn handle_pcr_filters_requests() {
    let fake = FakeApiServer::new();
    let signer = Arc::new(FakeSigner::default());
    let mine = controller(&fake, signer.clone(), true).await;

    mine.handle_pcr(&request("other", "other.example/signer"))
        .await
        .unwrap();
    let answered = with_denied(&request("answered", SIGNER), "r", "m", start_time());
    mine.handle_pcr(&answered).await.unwrap();
    assert!(
        signer.made.lock().is_empty(),
        "foreign or answered request was signed"
    );

    mine.handle_pcr(&request("fresh", SIGNER)).await.unwrap();
    assert_eq!(*signer.made.lock(), ["fresh"]);

    let fake = FakeApiServer::new();
    let theirs = controller(&fake, signer.clone(), false).await;
    let err = theirs
        .handle_pcr(&request("fresh", SIGNER))
        .await
        .unwrap_err();
    assert!(matches!(err, SignerError::NotAssigned));
    assert_eq!(signer.made.lock().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn queued_requests_are_signed_and_retried() {
    let fake = FakeApiServer::new();
    let signer = Arc::new(FakeSigner::default());
    signer.fail.store(true, Ordering::SeqCst);
    let c = controller(&fake, signer.clone(), true).await;
    let cache = c.pcrs.informer().spawn();
    fake.insert(&request("req", SIGNER));
    c.pcrs.informer().wait_synced().await;
    wait_until(|| c.pcrs.get_cached("ns", "req").is_some()).await;

    assert!(c.process_next_work_item().await);
    assert_eq!(signer.made.lock().len(), 1);
    assert!(
        c.queue.pending_at("ns/req").is_some(),
        "failure was not retried"
    );
    assert_eq!(c.limiter.num_requeues("ns/req"), 1);

    signer.fail.store(false, Ordering::SeqCst);
    assert!(c.process_next_work_item().await);
    assert_eq!(signer.made.lock().len(), 2);
    assert_eq!(
        c.limiter.num_requeues("ns/req"),
        0,
        "success did not reset backoff"
    );

    // A deleted request is dropped.
    fake.remove(&hivekeep_k8s::fake::ObjectKey {
        group: "certificates.k8s.io".into(),
        resource: "podcertificaterequests".into(),
        namespace: "ns".into(),
        name: "req".into(),
    });
    assert!(c.process_next_work_item().await);
    assert_eq!(signer.made.lock().len(), 2);
    c.queue.shut_down();
    assert!(!c.process_next_work_item().await);
    cache.abort();
}

#[tokio::test(start_paused = true)]
async fn unassigned_requests_wait_a_lease_duration() {
    let fake = FakeApiServer::new();
    let signer = Arc::new(FakeSigner::default());
    let c = controller(&fake, signer.clone(), false).await;
    let cache = c.pcrs.informer().spawn();
    fake.insert(&request("req", SIGNER));
    wait_until(|| c.pcrs.get_cached("ns", "req").is_some()).await;
    assert!(c.process_next_work_item().await);
    let due = c.queue.pending_at("ns/req").expect("requeued");
    assert_eq!(due - tokio::time::Instant::now(), LEASE_DURATION);
    assert_eq!(
        c.limiter.num_requeues("ns/req"),
        0,
        "not-assigned consumed backoff"
    );
    assert!(signer.made.lock().is_empty());
    cache.abort();
}

#[tokio::test(start_paused = true)]
async fn run_stops_on_shutdown() {
    let fake = FakeApiServer::new();
    let signer = Arc::new(FakeSigner {
        bundles: vec![bundle("root-a", None)],
        ..Default::default()
    });
    let c = controller(&fake, signer.clone(), true).await;
    let cache = c.pcrs.informer().spawn();
    fake.insert(&request("req", SIGNER));
    let stop = CancellationToken::new();
    let run = tokio::spawn(c.clone().run(2, stop.clone()));
    wait_until(|| signer.made.lock().len() == 1).await;
    wait_until(|| {
        fake.get::<ClusterTrustBundle>("", "example.com:signer:bundle")
            .is_some()
    })
    .await;
    stop.cancel();
    run.await.unwrap();
    assert!(c.queue.is_shut_down());
    cache.abort();
}

#[test]
fn keys_round_trip() {
    assert_eq!(key_of(Some("ns"), "a"), "ns/a");
    assert_eq!(key_of(None, "a"), "a");
    assert_eq!(split_key("ns/a"), ("ns", "a"));
    assert_eq!(split_key("a"), ("", "a"));
    let j = jittered(BUNDLE_PERIOD);
    assert!(j >= BUNDLE_PERIOD && j <= 2 * BUNDLE_PERIOD);
}
