use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use hivekeep_identity::extension::{IdentityExtension, PodIdentity};
use hivekeep_proto::keeper::v1::worker_support_server::{WorkerSupport, WorkerSupportServer};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use super::*;
use crate::test_pki::{server_config, LeafOpts, TestCa};
use crate::tunnel::keeper_dial::DEFAULT_KEEPER_SPIFFE_ID;

struct Stub {
    ca: Arc<TestCa>,
    lifetime: Duration,
    expired: bool,
    keys: mpsc::UnboundedSender<Vec<u8>>,
    fail_first: AtomicU32,
    fail_code: tonic::Code,
    calls: Arc<AtomicU32>,
}

#[tonic::async_trait]
impl WorkerSupport for Stub {
    async fn mint_actor_certificate(
        &self,
        req: Request<pb::MintActorCertificateRequest>,
    ) -> Result<Response<pb::MintActorCertificateResponse>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_first.load(Ordering::SeqCst) > 0 {
            self.fail_first.fetch_sub(1, Ordering::SeqCst);
            return Err(Status::new(self.fail_code, "broker busy"));
        }
        let req = req.into_inner();
        if req.actor_uid != "actor-uid" {
            return Err(Status::failed_precondition("unexpected actor UID"));
        }
        let spki = hivekeep_identity::csr::verified_public_key(&req.certificate_signing_request)
            .map_err(|_| Status::invalid_argument("invalid CSR"))?;
        let key = rcgen::SubjectPublicKeyInfo::from_der(spki.spki_der()).unwrap();
        let uri = format!(
            "spiffe://hivekeep-actor.local/worker/actor/{}/{}",
            req.actor_hive, req.actor_name
        );
        let opts = LeafOpts {
            uri: Some(uri),
            client: true,
            lifetime: Some(if self.expired {
                Duration::ZERO
            } else {
                self.lifetime
            }),
            backdate: Some(Duration::from_secs(if self.expired { 120 } else { 60 })),
            ..Default::default()
        };
        let der = self.ca.issue_public(&key, &opts);
        let _ = self.keys.send(spki.spki_der().to_vec());
        Ok(Response::new(pb::MintActorCertificateResponse {
            actor_certificates: vec![der],
        }))
    }
    async fn acquire_worker_epoch(
        &self,
        _: Request<pb::AcquireWorkerEpochRequest>,
    ) -> Result<Response<pb::AcquireWorkerEpochResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn register_worker(
        &self,
        _: Request<pb::RegisterWorkerRequest>,
    ) -> Result<Response<pb::RegisterWorkerResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn set_worker_sandbox_runtimes(
        &self,
        _: Request<pb::SetWorkerSandboxRuntimesRequest>,
    ) -> Result<Response<pb::SetWorkerSandboxRuntimesResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn request_actor_suspend(
        &self,
        _: Request<pb::RequestActorSuspendRequest>,
    ) -> Result<Response<pb::RequestActorSuspendResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn get_actor_egress_policy(
        &self,
        _: Request<pb::GetActorEgressPolicyRequest>,
    ) -> Result<Response<pb::GetActorEgressPolicyResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn report_actor_crashed(
        &self,
        _: Request<pb::ReportActorCrashedRequest>,
    ) -> Result<Response<pb::ReportActorCrashedResponse>, Status> {
        Err(Status::unimplemented(""))
    }
}

pub(crate) fn pod_identity(namespace: &str, sa: &str, node: &str) -> PodIdentity {
    PodIdentity {
        namespace: namespace.into(),
        service_account_name: sa.into(),
        service_account_uid: format!("{sa}-sa-uid"),
        pod_name: sa.into(),
        pod_uid: format!("{sa}-uid"),
        node_name: node.into(),
        node_uid: "node-uid".into(),
    }
}

/// A fake keeper on a unix socket and a worker credential bundle for it.
pub(crate) struct FakeKeeper {
    pub socket: PathBuf,
    pub cred: PathBuf,
    pub trust: PathBuf,
    pub _dir: tempfile::TempDir,
}

pub(crate) async fn serve_keeper<S: WorkerSupport>(
    ca: &TestCa,
    keeper_node: &str,
    svc: S,
) -> FakeKeeper {
    let dir = tempfile::tempdir().unwrap();
    let worker = ca.issue(&LeafOpts {
        uri: Some("spiffe://cluster.local/ns/hivekeep-demo/sa/worker".into()),
        client: true,
        extensions: vec![pod_identity("hivekeep-demo", "worker", "node-a")
            .to_extension()
            .unwrap()],
        ..Default::default()
    });
    let keeper = ca.issue(&LeafOpts {
        uri: Some(DEFAULT_KEEPER_SPIFFE_ID.into()),
        server: true,
        extensions: vec![pod_identity("hivekeep-system", "keeper", keeper_node)
            .to_extension()
            .unwrap()],
        ..Default::default()
    });
    let cred = dir.path().join("worker.pem");
    let trust = dir.path().join("trust.pem");
    std::fs::write(&cred, worker.bundle_pem()).unwrap();
    std::fs::write(&trust, ca.pem()).unwrap();
    let socket = dir.path().join("keeper.sock");
    let lis = tokio::net::UnixListener::bind(&socket).unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(server_config(
        &keeper,
        Some(ca),
        &[b"h2"],
        &[&rustls::version::TLS13],
    ));
    let incoming = async_stream_tls(lis, acceptor);
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(WorkerSupportServer::new(svc))
            .serve_with_incoming(incoming),
    );
    FakeKeeper {
        socket,
        cred,
        trust,
        _dir: dir,
    }
}

fn async_stream_tls(
    lis: tokio::net::UnixListener,
    acceptor: tokio_rustls::TlsAcceptor,
) -> impl futures::Stream<Item = std::io::Result<tokio_rustls::server::TlsStream<tokio::net::UnixStream>>>
{
    futures::stream::unfold((lis, acceptor), |(lis, acceptor)| async move {
        loop {
            let (s, _) = match lis.accept().await {
                Ok(s) => s,
                Err(e) => return Some((Err(e), (lis, acceptor))),
            };
            if let Ok(tls) = acceptor.accept(s).await {
                return Some((Ok(tls), (lis, acceptor)));
            }
        }
    })
}

struct Harness {
    source: BrokerCertificateSource,
    keys: mpsc::UnboundedReceiver<Vec<u8>>,
    calls: Arc<AtomicU32>,
    _keeper: FakeKeeper,
}

async fn harness(
    keeper_node: &str,
    expired: bool,
    fail_first: u32,
    fail_code: tonic::Code,
) -> Harness {
    let ca = Arc::new(TestCa::new());
    let (tx, rx) = mpsc::unbounded_channel();
    let calls = Arc::new(AtomicU32::new(0));
    let stub = Stub {
        ca: ca.clone(),
        lifetime: Duration::from_secs(3600),
        expired,
        keys: tx,
        fail_first: AtomicU32::new(fail_first),
        fail_code,
        calls: calls.clone(),
    };
    let keeper = serve_keeper(&ca, keeper_node, stub).await;
    let source = BrokerCertificateSource::new(BrokerConfig {
        socket_path: keeper.socket.clone(),
        credential_bundle_path: keeper.cred.clone(),
        trust_bundle_path: keeper.trust.clone(),
        actor_hive: "actor-hive".into(),
        actor_name: "actor-name".into(),
        actor_uid: "actor-uid".into(),
        keeper_spiffe_id: DEFAULT_KEEPER_SPIFFE_ID.into(),
    })
    .unwrap();
    Harness {
        source,
        keys: rx,
        calls,
        _keeper: keeper,
    }
}

fn deadline(secs: u64) -> Option<Instant> {
    Some(Instant::now() + Duration::from_secs(secs))
}

#[tokio::test]
async fn mints_and_reuses_key() {
    let mut h = harness("node-a", false, 0, tonic::Code::Ok).await;
    for _ in 0..2 {
        h.source.mint_until(deadline(1)).await.unwrap();
    }
    let first = h.keys.recv().await.unwrap();
    let second = h.keys.recv().await.unwrap();
    assert_eq!(first, second, "renewal replaced the actor private key");
    assert!(h.source.current().is_some());
    assert_eq!(
        h.source.current_uris(),
        vec!["spiffe://hivekeep-actor.local/worker/actor/actor-hive/actor-name".to_string()]
    );
}

#[tokio::test]
async fn rejects_keeper_on_different_node() {
    let h = harness("node-b", false, 0, tonic::Code::Ok).await;
    let err = h.source.mint_until(deadline(1)).await.unwrap_err();
    assert!(err.message.contains("not on worker node"), "{err}");
}

#[tokio::test]
async fn rejects_expired_certificate() {
    let h = harness("node-a", true, 0, tonic::Code::Ok).await;
    let err = h.source.mint_until(deadline(1)).await.unwrap_err();
    assert!(
        err.message.contains("invalid actor certificate lifetime"),
        "{err}"
    );
}

#[tokio::test]
async fn retries_unavailable() {
    let mut h = harness("node-a", false, 2, tonic::Code::Unavailable).await;
    h.source
        .mint_until(deadline(10))
        .await
        .expect("mint after two Unavailable answers");
    h.keys.recv().await.unwrap();
    assert_eq!(h.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn does_not_retry_other_errors() {
    let h = harness("node-a", false, 1, tonic::Code::FailedPrecondition).await;
    let err = h.source.mint_until(deadline(10)).await.unwrap_err();
    assert_eq!(err.code, Some(tonic::Code::FailedPrecondition));
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
}
