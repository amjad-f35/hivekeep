//! Fetching the actor an exchange request names from hivekeep-api. The
//! control plugin runs in the hivekeep-api pod, so it dials the loopback and
//! presents the pod identity certificate, which hivekeep-api accepts as an
//! mTLS client. Every file is re-read on each handshake, so rotation needs no
//! restart.

use std::path::PathBuf;

use async_trait::async_trait;
use hivekeep_identity::jwt::claims::ActorClaims;
use hivekeep_proto::api::v1 as pb;
use hivekeep_proto::api::v1::control_client::ControlClient;
use tonic::transport::Channel;

use super::exchange::{Lookup, LookupError};
use crate::tls::{channel, ClientFiles};

/// How hivekeep-api is reached.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// The gRPC target, the loopback in the pod.
    pub address: String,
    /// The CA bundle verifying the serving certificate.
    pub ca_file: PathBuf,
    /// The name expected on the serving certificate, since the address is
    /// not one it carries.
    pub server_name: String,
    /// The PEM bundle with the pod identity certificate and key.
    pub client_cred_bundle: PathBuf,
}

/// Resolves actors over one connection to hivekeep-api.
pub struct ActorLookup {
    client: ControlClient<Channel>,
}

impl ActorLookup {
    /// Dials hivekeep-api lazily: a server that is not up yet fails the
    /// first lookup, not this.
    pub fn new(cfg: Config) -> Result<Self, String> {
        if cfg.address.is_empty() {
            return Err("actorlookup: address is required".into());
        }
        let ch = channel(
            &cfg.address,
            ClientFiles {
                ca_file: cfg.ca_file,
                client_bundle: Some(cfg.client_cred_bundle),
                server_name: cfg.server_name,
            },
        )
        .map_err(|e| format!("actorlookup: while creating the hivekeep-api client: {e}"))?;
        Ok(ActorLookup {
            client: ControlClient::new(ch),
        })
    }
}

#[async_trait]
impl Lookup for ActorLookup {
    /// The actor the claims name. The claims come from a verified JWT, so
    /// hive and name are trusted, but the actor may have been recreated
    /// since, which the UID detects. A status from hivekeep-api is returned
    /// as it is.
    async fn actor(&self, claims: &ActorClaims) -> Result<pb::Actor, LookupError> {
        if claims.hive.is_empty() || claims.actor_name.is_empty() || claims.actor_uid.is_empty() {
            return Err(LookupError::Other(
                "actorlookup: claims lack hive, actor name, or UID".into(),
            ));
        }
        let actor = self
            .client
            .clone()
            .get_actor(pb::GetActorRequest {
                actor: Some(pb::ObjectRef {
                    hive: claims.hive.clone(),
                    name: claims.actor_name.clone(),
                }),
            })
            .await
            .map_err(LookupError::Status)?
            .into_inner();
        if actor.metadata.as_ref().map(|m| m.uid.as_str()) != Some(claims.actor_uid.as_str()) {
            return Err(LookupError::Stale);
        }
        Ok(actor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use parking_lot::Mutex;
    use tonic::server::{NamedService, UnaryService};
    use tonic::{Request, Response, Status};

    const SERVER_NAME: &str = "hivekeep-api.hivekeep-system.svc";

    /// Serves Control.GetActor for one actor and records the client's SPIFFE
    /// IDs.
    #[derive(Clone)]
    struct FakeControl {
        actor: pb::Actor,
        peer_uris: Arc<Mutex<Vec<String>>>,
    }

    impl NamedService for FakeControl {
        const NAME: &'static str = "hivekeep.api.v1.Control";
    }

    struct GetActor(FakeControl);

    impl UnaryService<pb::GetActorRequest> for GetActor {
        type Response = pb::Actor;
        type Future = Pin<Box<dyn Future<Output = Result<Response<pb::Actor>, Status>> + Send>>;
        fn call(&mut self, req: Request<pb::GetActorRequest>) -> Self::Future {
            let f = self.0.clone();
            Box::pin(async move {
                if let Some(certs) = req.peer_certs() {
                    if let Some(leaf) = certs.first() {
                        let (_, cert) = x509_parser::parse_x509_certificate(leaf).unwrap();
                        let mut uris = Vec::new();
                        if let Ok(Some(san)) = cert.subject_alternative_name() {
                            for n in &san.value.general_names {
                                if let x509_parser::extensions::GeneralName::URI(u) = n {
                                    uris.push(u.to_string());
                                }
                            }
                        }
                        *f.peer_uris.lock() = uris;
                    }
                }
                let r = req.into_inner().actor.unwrap_or_default();
                let m = f.actor.metadata.clone().unwrap();
                if r.hive != m.hive || r.name != m.name {
                    return Err(Status::not_found(format!("no actor {}/{}", r.hive, r.name)));
                }
                Ok(Response::new(f.actor.clone()))
            })
        }
    }

    impl tower::Service<http::Request<tonic::body::Body>> for FakeControl {
        type Response = http::Response<tonic::body::Body>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
            let me = self.clone();
            Box::pin(async move {
                if req.uri().path() != "/hivekeep.api.v1.Control/GetActor" {
                    return Ok(Status::unimplemented("only GetActor").into_http());
                }
                let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::<
                    pb::Actor,
                    pb::GetActorRequest,
                >::default());
                Ok(grpc.unary(GetActor(me), req).await)
            })
        }
    }

    struct TestCa {
        cert: rcgen::Certificate,
        issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    }

    fn new_ca() -> TestCa {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        p.distinguished_name
            .push(rcgen::DnType::CommonName, "test-ca");
        let cert = p.self_signed(&key).unwrap();
        TestCa {
            cert,
            issuer: rcgen::Issuer::new(p, key),
        }
    }

    impl TestCa {
        fn client_bundle(&self, spiffe: &str) -> String {
            let key = rcgen::KeyPair::generate().unwrap();
            let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
            p.subject_alt_names = vec![rcgen::SanType::URI(spiffe.try_into().unwrap())];
            p.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
            let cert = p.signed_by(&key, &self.issuer).unwrap();
            format!("{}{}", key.serialize_pem(), cert.pem())
        }

        fn server_identity(&self) -> tonic::transport::Identity {
            let key = rcgen::KeyPair::generate().unwrap();
            let mut p = rcgen::CertificateParams::new(vec![SERVER_NAME.to_string()]).unwrap();
            p.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
            let cert = p.signed_by(&key, &self.issuer).unwrap();
            tonic::transport::Identity::from_pem(cert.pem(), key.serialize_pem())
        }
    }

    struct Started {
        cfg: Config,
        _dir: tempfile::TempDir,
        _h: tokio::task::JoinHandle<()>,
    }

    /// Serves `fake` over mTLS on the loopback and returns a config reaching
    /// it with a client certificate carrying `spiffe`.
    async fn start(fake: FakeControl, spiffe: &str) -> Started {
        let ca = new_ca();
        let dir = tempfile::tempdir().unwrap();
        let ca_file = dir.path().join("ca.pem");
        std::fs::write(&ca_file, ca.cert.pem()).unwrap();
        let bundle = dir.path().join("client.pem");
        std::fs::write(&bundle, ca.client_bundle(spiffe)).unwrap();
        let tls = tonic::transport::ServerTlsConfig::new()
            .identity(ca.server_identity())
            .client_ca_root(tonic::transport::Certificate::from_pem(ca.cert.pem()));
        let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap();
        let router = tonic::transport::Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(fake);
        let h = tokio::spawn(async move {
            let _ = router
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(lis))
                .await;
        });
        Started {
            cfg: Config {
                address: addr.to_string(),
                ca_file,
                server_name: SERVER_NAME.into(),
                client_cred_bundle: bundle,
            },
            _dir: dir,
            _h: h,
        }
    }

    fn test_actor(uid: &str) -> pb::Actor {
        pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: "team-a".into(),
                name: "agent-1".into(),
                uid: uid.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn claims(hive: &str, name: &str, uid: &str) -> ActorClaims {
        ActorClaims {
            hive: hive.into(),
            actor_name: name.into(),
            actor_uid: uid.into(),
            node: String::new(),
        }
    }

    fn fake(uid: &str) -> FakeControl {
        FakeControl {
            actor: test_actor(uid),
            peer_uris: Arc::default(),
        }
    }

    #[tokio::test]
    async fn looks_up_the_actor_over_mtls() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        const SPIFFE: &str = "spiffe://hivekeep.io/ns/hivekeep-system/sa/hivekeep-api";
        let f = fake("uid-1");
        let s = start(f.clone(), SPIFFE).await;
        let l = ActorLookup::new(s.cfg.clone()).unwrap();
        let a = l
            .actor(&claims("team-a", "agent-1", "uid-1"))
            .await
            .unwrap();
        assert_eq!(a.metadata.unwrap().uid, "uid-1");
        assert_eq!(*f.peer_uris.lock(), vec![SPIFFE.to_string()]);
    }

    #[tokio::test]
    async fn lookup_errors() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let s = start(
            fake("uid-1"),
            "spiffe://hivekeep.io/ns/hivekeep-system/sa/hivekeep-api",
        )
        .await;
        let l = ActorLookup::new(s.cfg.clone()).unwrap();
        match l.actor(&claims("team-a", "agent-1", "uid-0")).await {
            Err(LookupError::Stale) => {}
            other => panic!("recreated actor: {other:?}"),
        }
        match l.actor(&claims("team-a", "agent-2", "uid-2")).await {
            Err(LookupError::Status(st)) => assert_eq!(st.code(), tonic::Code::NotFound),
            other => panic!("unknown actor: {other:?}"),
        }
        match l.actor(&claims("team-a", "agent-1", "")).await {
            Err(LookupError::Other(_)) => {}
            other => panic!("incomplete claims: {other:?}"),
        }
        match l.actor(&ActorClaims::default()).await {
            Err(LookupError::Other(_)) => {}
            other => panic!("empty claims: {other:?}"),
        }
    }

    #[tokio::test]
    async fn refuses_an_unverified_server() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let s = start(fake("uid-1"), "spiffe://hivekeep.io/x").await;
        let other = start(fake("uid-1"), "spiffe://hivekeep.io/x").await;
        let mut cfg = s.cfg.clone();
        cfg.address = other.cfg.address.clone();
        let l = ActorLookup::new(cfg).unwrap();
        let r = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            l.actor(&claims("team-a", "agent-1", "uid-1")),
        )
        .await
        .unwrap();
        match r {
            Err(LookupError::Status(st)) => {
                assert_eq!(st.code(), tonic::Code::Unavailable, "{st:?}")
            }
            other => panic!("want Unavailable from a failed handshake, got {other:?}"),
        }
    }

    #[test]
    fn new_requires_an_address() {
        assert!(ActorLookup::new(Config {
            ca_file: "x".into(),
            client_cred_bundle: "y".into(),
            ..Default::default()
        })
        .is_err());
    }
}
