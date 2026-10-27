//! The part of the keeper's WorkerSupport API the tunnel calls: minting
//! Actor certificates for egress, and Actor egress policies for the DNS
//! relay. It listens where the keeper does, on the node-local socket, with
//! the keeper's identity, so the tunnel's own client talks to it unchanged.

use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use hivekeep_core::resource::ActorRef;
use hivekeep_identity::actor_cert::ActorCertKind;
use hivekeep_identity::ca::{Ca, LeafSpec};
use hivekeep_identity::csr::verified_public_key;
use hivekeep_identity::extension::{ActorIdentity, IdentityExtension};
use hivekeep_proto::keeper::v1 as pb;
use pb::worker_support_server::{WorkerSupport, WorkerSupportServer};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UnixStream;
use tokio_rustls::server::TlsStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::{pki, Error, Result};

/// How long minted Actor certificates last. The tunnel renews well before
/// the end, which no benchmark run reaches.
pub const ACTOR_CERTIFICATE_LIFETIME: Duration = Duration::from_secs(24 * 3600);

/// The fake keeper.
pub struct FakeKeeper {
    ca: Arc<Ca>,
}

impl FakeKeeper {
    /// Loads the CA from `pki_dir`, written by [`pki::generate`].
    pub fn new(pki_dir: &Path) -> Result<FakeKeeper> {
        Ok(FakeKeeper {
            ca: Arc::new(pki::load_ca(pki_dir)?),
        })
    }

    /// Listens on `socket` with the keeper's certificate, requiring a worker
    /// certificate signed by the CA, until `cancel`.
    pub async fn serve(
        self,
        pki_dir: &Path,
        socket: &Path,
        cancel: CancellationToken,
    ) -> Result<()> {
        let cert = pki::certified_key(&pki_dir.join(pki::KEEPER_BUNDLE))?;
        let roots = Arc::new(pki::roots(&pki_dir.join(pki::CA_FILE))?);
        let verifier = WebPkiClientVerifier::builder_with_provider(roots, pki::provider())
            .build()
            .map_err(|e| Error::msg(format!("client verifier: {e}")))?;
        let mut tls = rustls::ServerConfig::builder_with_provider(pki::provider())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(verifier)
            .with_cert_resolver(Arc::new(OneCert(cert)));
        tls.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));

        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(socket);
        let lis = tokio::net::UnixListener::bind(socket)?;

        let (tx, rx) = tokio::sync::mpsc::channel::<io::Result<Conn>>(64);
        let accept_cancel = cancel.clone();
        tokio::spawn(async move {
            loop {
                let conn = tokio::select! {
                    _ = accept_cancel.cancelled() => return,
                    a = lis.accept() => match a { Ok((c, _)) => c, Err(_) => continue },
                };
                let (acceptor, tx) = (acceptor.clone(), tx.clone());
                tokio::spawn(async move {
                    if let Ok(Ok(tls)) =
                        tokio::time::timeout(Duration::from_secs(10), acceptor.accept(conn)).await
                    {
                        let _ = tx.send(Ok(Conn(tls))).await;
                    }
                });
            }
        });
        tonic::transport::Server::builder()
            .add_service(WorkerSupportServer::new(self))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::ReceiverStream::new(rx),
                cancel.cancelled_owned(),
            )
            .await
            .map_err(|e| Error::msg(format!("fake keeper: {e}")))
    }

    fn mint(&self, req: &pb::MintActorCertificateRequest) -> Result<Vec<u8>> {
        let key = verified_public_key(&req.certificate_signing_request)?;
        let actor = ActorRef::new(req.actor_hive.clone(), req.actor_name.clone());
        let now = SystemTime::now();
        let mut spec = LeafSpec::new(
            now - Duration::from_secs(60),
            now + ACTOR_CERTIFICATE_LIFETIME,
        );
        spec.common_name = Some(req.actor_name.clone());
        spec.client_auth = true;
        spec.uris
            .push(ActorCertKind::WorkerForActor.spiffe_id(&actor));
        let identity = ActorIdentity {
            hive: req.actor_hive.clone(),
            actor_name: req.actor_name.clone(),
            actor_uid: req.actor_uid.clone(),
        };
        spec.extensions.push(identity.to_extension()?);
        Ok(self.ca.sign(&spec, &key)?)
    }
}

#[derive(Debug)]
struct OneCert(Arc<CertifiedKey>);

impl ResolvesServerCert for OneCert {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.clone())
    }
}

/// An accepted TLS connection on the node-local socket.
struct Conn(TlsStream<UnixStream>);

impl tonic::transport::server::Connected for Conn {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for Conn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

#[tonic::async_trait]
impl WorkerSupport for FakeKeeper {
    /// Signs the CSR the tunnel sends as the Actor's identity.
    async fn mint_actor_certificate(
        &self,
        req: Request<pb::MintActorCertificateRequest>,
    ) -> Result<Response<pb::MintActorCertificateResponse>, Status> {
        let der = self
            .mint(req.get_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        Ok(Response::new(pb::MintActorCertificateResponse {
            actor_certificates: vec![der],
        }))
    }

    /// No policy: only the DNS relay asks, and the benchmark's egress goes
    /// to addresses, not names.
    async fn get_actor_egress_policy(
        &self,
        _: Request<pb::GetActorEgressPolicyRequest>,
    ) -> Result<Response<pb::GetActorEgressPolicyResponse>, Status> {
        Ok(Response::new(pb::GetActorEgressPolicyResponse::default()))
    }

    async fn acquire_worker_epoch(
        &self,
        _: Request<pb::AcquireWorkerEpochRequest>,
    ) -> Result<Response<pb::AcquireWorkerEpochResponse>, Status> {
        Err(Status::unimplemented("not served by the fake keeper"))
    }

    async fn register_worker(
        &self,
        _: Request<pb::RegisterWorkerRequest>,
    ) -> Result<Response<pb::RegisterWorkerResponse>, Status> {
        Err(Status::unimplemented("not served by the fake keeper"))
    }

    async fn set_worker_sandbox_runtimes(
        &self,
        _: Request<pb::SetWorkerSandboxRuntimesRequest>,
    ) -> Result<Response<pb::SetWorkerSandboxRuntimesResponse>, Status> {
        Err(Status::unimplemented("not served by the fake keeper"))
    }

    async fn request_actor_suspend(
        &self,
        _: Request<pb::RequestActorSuspendRequest>,
    ) -> Result<Response<pb::RequestActorSuspendResponse>, Status> {
        Err(Status::unimplemented("not served by the fake keeper"))
    }

    async fn report_actor_crashed(
        &self,
        _: Request<pb::ReportActorCrashedRequest>,
    ) -> Result<Response<pb::ReportActorCrashedResponse>, Status> {
        Err(Status::unimplemented("not served by the fake keeper"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_gate::install;
    use hivekeep_gate::tunnel::egress::{
        BrokerCertificateSource, BrokerConfig, ClientCertificateSource,
    };

    /// Mints an Actor certificate the way the tunnel does, with its own
    /// broker client, over the fake keeper's socket.
    #[tokio::test]
    async fn mint_through_the_tunnels_client() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        pki::generate(
            d,
            &pki::Options {
                worker_hosts: vec!["hk-tunnel-worker".into()],
                gateway_hosts: vec!["hk-tunnel-sink".into()],
                ..Default::default()
            },
        )
        .unwrap();
        let keeper = FakeKeeper::new(d).unwrap();
        let socket = d.join("ws.sock");
        let cancel = CancellationToken::new();
        let (pki_dir, sock, c) = (d.to_path_buf(), socket.clone(), cancel.clone());
        tokio::spawn(async move { keeper.serve(&pki_dir, &sock, c).await });

        let source = BrokerCertificateSource::new(BrokerConfig {
            socket_path: socket,
            credential_bundle_path: d.join(pki::WORKER_BUNDLE),
            trust_bundle_path: d.join(pki::CA_FILE),
            actor_hive: "bench".into(),
            actor_name: "actor-0".into(),
            actor_uid: "00000000-0000-4000-8000-000000000000".into(),
            keeper_spiffe_id: install::keeper_spiffe_id(install::SYSTEM_NAMESPACE),
        })
        .unwrap();
        let mut result = Err(hivekeep_gate::tunnel::egress::MintError::other("not tried"));
        for _ in 0..50 {
            result = source.mint_worker_certificate().await;
            if result.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let expiry = result.expect("minting");
        assert!(
            expiry > SystemTime::now() + Duration::from_secs(3600),
            "certificate expires too soon"
        );

        let cert = source.current().expect("a current certificate");
        let roots = Arc::new(pki::roots(&d.join(pki::CA_FILE)).unwrap());
        let clients = WebPkiClientVerifier::builder_with_provider(roots, pki::provider())
            .build()
            .unwrap();
        clients
            .verify_client_cert(
                &cert.cert[0],
                &cert.cert[1..],
                rustls::pki_types::UnixTime::now(),
            )
            .expect("minted certificate verifies as a client");
        let id = ActorIdentity::from_certificate(&cert.cert[0])
            .unwrap()
            .expect("an Actor identity");
        assert_eq!(id.actor_name, "actor-0");
        cancel.cancel();
    }
}
