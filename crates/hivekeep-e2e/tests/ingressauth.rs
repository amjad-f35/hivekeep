//! The gate router's mTLS client authentication: who its ingress listeners
//! let through and who they turn away. The other suites cover what happens
//! once a client is let through, since every one of them reaches its Actors
//! as the allowlisted e2e router client.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::env::{resource_name, system_namespace};
use hivekeep_e2e::harness::{run_test, T};
use hivekeep_e2e::ingressauth::{
    router_client_certificate, router_client_spiffe_id, router_rustls_server_name,
    router_tls_config, ROUTER_CLIENT_SERVICE_ACCOUNT,
};
use hivekeep_e2e::kube_util::PodCertificate;
use hivekeep_e2e::suite::e2e_test;
use hivekeep_e2e::{install, portforward};
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Runs in the router client namespace, so the pod-identity signer issues it
/// a certificate from the same CA as the allowlisted router client, under a
/// SPIFFE ID the router does not list.
const UNLISTED_SERVICE_ACCOUNT: &str = "router-client-unlisted";

/// An Actor that does not exist. A client the router lets through reaches
/// Actor routing and gets an HTTP response; authorization must reject an
/// unlisted client before that lookup, even inside CONNECT.
fn target_actor() -> ActorRef {
    ActorRef::new("hivekeep-e2e-ingressauth", "nobody")
}

/// Why a request got no HTTP status.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReqError {
    /// The peer sent a TLS alert.
    TlsAlert(String),
    /// An I/O failure of the given kind.
    Io(io::ErrorKind, String),
    /// Anything else (timeouts, malformed replies).
    Other(String),
}

impl std::fmt::Display for ReqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReqError::TlsAlert(m) => write!(f, "remote error: {m}"),
            ReqError::Io(k, m) => write!(f, "{k:?}: {m}"),
            ReqError::Other(m) => f.write_str(m),
        }
    }
}

fn classify(e: io::Error) -> ReqError {
    if let Some(inner) = e.get_ref() {
        if let Some(rustls::Error::AlertReceived(alert)) = inner.downcast_ref::<rustls::Error>() {
            return ReqError::TlsAlert(format!("{alert:?}"));
        }
    }
    ReqError::Io(e.kind(), e.to_string())
}

type Request = fn(
    String,
    Arc<ClientConfig>,
    ServerName<'static>,
) -> futures::future::BoxFuture<'static, Result<u16, ReqError>>;

/// One of the router's TLS ingress listeners and how to send it one
/// request.
struct Listener {
    name: &'static str,
    service_port: u16,
    request: Request,
}

/// Both TLS ingress listeners; every check runs against both so one losing
/// client authentication cannot hide behind the other.
fn listeners() -> [Listener; 2] {
    [
        Listener {
            name: "https",
            service_port: install::ROUTER_HTTPS_SERVICE_PORT,
            request: |a, c, n| Box::pin(https_request(a, c, n)),
        },
        Listener {
            name: "connect-tls",
            service_port: install::ROUTER_CONNECT_TLS_SERVICE_PORT,
            request: |a, c, n| Box::pin(connect_request(a, c, n)),
        },
    ]
}

async fn forward_router_port(t: &T, service_port: u16) -> String {
    let kube = hivekeep_e2e::suite::clients().kube();
    let fwd = portforward::service_port_forward(
        &kube,
        &system_namespace(),
        &resource_name(install::ROUTER_SERVICE),
        service_port,
    )
    .await
    .unwrap_or_else(|e| {
        t.fatal(format!(
            "port-forwarding to the gate router port {service_port}: {e}"
        ))
    });
    let addr = fwd.addr();
    t.cleanup(move |_| async move { drop(fwd) });
    addr
}

async fn dial(
    address: &str,
    cfg: Arc<ClientConfig>,
    name: ServerName<'static>,
) -> Result<BufReader<tokio_rustls::client::TlsStream<TcpStream>>, ReqError> {
    let tcp = TcpStream::connect(address).await.map_err(classify)?;
    let tls = TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .map_err(classify)?;
    Ok(BufReader::new(tls))
}

/// Reads a response head and returns its status.
async fn read_status<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut BufReader<S>,
) -> Result<u16, ReqError> {
    let mut line = String::new();
    let n = s.read_line(&mut line).await.map_err(classify)?;
    if n == 0 {
        return Err(ReqError::Io(io::ErrorKind::UnexpectedEof, "EOF".into()));
    }
    let code = line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| ReqError::Other(format!("malformed status line {:?}", line.trim_end())))?;
    loop {
        let mut h = String::new();
        let n = s.read_line(&mut h).await.map_err(classify)?;
        if n == 0 || h.trim_end().is_empty() {
            return Ok(code);
        }
    }
}

async fn https_request(
    address: String,
    cfg: Arc<ClientConfig>,
    name: ServerName<'static>,
) -> Result<u16, ReqError> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut s = dial(&address, cfg, name).await?;
        let req = format!(
            "GET / HTTP/1.1\r\nHost: {address}\r\n{}: {}\r\nConnection: close\r\n\r\n",
            install::TARGET_ACTOR_HEADER,
            target_actor()
        );
        s.get_mut()
            .write_all(req.as_bytes())
            .await
            .map_err(classify)?;
        read_status(&mut s).await
    })
    .await
    .map_err(|_| ReqError::Other("context deadline exceeded".into()))?
}

async fn connect_request(
    address: String,
    cfg: Arc<ClientConfig>,
    name: ServerName<'static>,
) -> Result<u16, ReqError> {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut s = dial(&address, cfg, name).await?;
        let actor = target_actor();
        let destination = format!("{}:80", actor.name);
        let req = format!(
            "CONNECT {destination} HTTP/1.1\r\nHost: {destination}\r\n{}: {actor}\r\n\r\n",
            install::TARGET_ACTOR_HEADER
        );
        s.get_mut()
            .write_all(req.as_bytes())
            .await
            .map_err(classify)?;
        let status = read_status(&mut s).await?;
        if status != 200 {
            return Ok(status);
        }
        let inner = format!(
            "GET / HTTP/1.1\r\nHost: {destination}\r\n{}: {actor}\r\n\r\n",
            install::TARGET_ACTOR_HEADER
        );
        s.get_mut()
            .write_all(inner.as_bytes())
            .await
            .map_err(classify)?;
        read_status(&mut s).await
    })
    .await
    .map_err(|_| ReqError::Other("context deadline exceeded".into()))?
}

/// The router TLS config presenting `cert`. rustls presents a configured
/// client certificate whatever CAs the server's request names, so the
/// untrusted-CA check stays one and does not turn into a no-certificate one.
async fn router_config(t: &T, cert: Option<&PodCertificate>) -> Arc<ClientConfig> {
    let clients = hivekeep_e2e::suite::clients();
    let mut cfg = router_tls_config(&clients, cert)
        .await
        .unwrap_or_else(|e| t.fatal(format!("building the router TLS config: {e}")));
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Arc::new(cfg)
}

fn ingress_refused(status: u16, err: Option<&ReqError>, allow_post_tls: bool) -> bool {
    if let Some(ReqError::TlsAlert(_)) = err {
        return true;
    }
    if !allow_post_tls {
        return false;
    }
    match err {
        None => status == 403,
        Some(ReqError::Io(kind, _)) => matches!(
            kind,
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe
        ),
        Some(_) => false,
    }
}

/// Bounds the tries one refused client gets, each on a fresh forward. The
/// router closes right after its alert, and the forward sometimes reports
/// that close first, so the alert never reaches the test.
const REFUSAL_ATTEMPTS: usize = 3;

/// Each kind of client against both TLS listeners. Certificate trust is
/// checked during TLS; the URI SAN allowlist may be enforced during TLS or
/// by network authorization afterwards. Each refused client gets a forward
/// of its own, first shown to carry the allowlisted client through: a
/// forward can tear its tunnel down once one connection fails. Missing or
/// untrusted certificates must receive a TLS alert; a trusted but unlisted
/// identity may instead see a close or HTTP 403 before Actor routing.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn ingress_authentication() {
    e2e_test("TestIngressAuthentication", |t| async move {
        let clients = hivekeep_e2e::suite::clients();
        let allowed = router_client_certificate(&t, &clients, ROUTER_CLIENT_SERVICE_ACCOUNT).await;
        let unlisted = router_client_certificate(&t, &clients, UNLISTED_SERVICE_ACCOUNT).await;
        let foreign = foreign_certificate(&router_client_spiffe_id(ROUTER_CLIENT_SERVICE_ACCOUNT));
        let (allowed, unlisted, foreign) = (&allowed, &unlisted, &foreign);
        for listener in listeners() {
            let listener = &listener;
            t.run(listener.name, |t| async move {
                let cases: [(&str, Option<&PodCertificate>, bool); 3] = [
                    ("no client certificate", None, false),
                    ("podidentity certificate for an unlisted SPIFFE ID", Some(unlisted), true),
                    ("allowlisted SPIFFE ID signed by an untrusted CA", Some(foreign), false),
                ];
                for (name, cert, allow_post_tls) in cases {
                    t.run(name, |t| async move {
                        let server_name = router_rustls_server_name();
                        for attempt in 1.. {
                            let address = forward_router_port(&t, listener.service_port).await;
                            match (listener.request)(address.clone(), router_config(&t, Some(allowed)).await, server_name.clone()).await {
                                Ok(status) => t.log(format!("allowlisted client let through (status {status})")),
                                Err(e) => t.fatal(format!("allowlisted client {} got no response: {e}", router_client_spiffe_id(ROUTER_CLIENT_SERVICE_ACCOUNT))),
                            }
                            let result = (listener.request)(address, router_config(&t, cert).await, server_name.clone()).await;
                            let (status, err) = match &result {
                                Ok(s) => (*s, None),
                                Err(e) => (0, Some(e)),
                            };
                            if ingress_refused(status, err, allow_post_tls) {
                                t.log(format!("refused: {err:?}"));
                                if allow_post_tls {
                                    // A close alone could be a broken forward.
                                    let address = forward_router_port(&t, listener.service_port).await;
                                    if let Err(e) = (listener.request)(address, router_config(&t, Some(allowed)).await, server_name.clone()).await {
                                        t.fatal(format!("allowlisted client failed after unlisted client refusal: {e}"));
                                    }
                                }
                                return;
                            }
                            let Some(err) = err else {
                                t.fatal(format!("router let the client through (status {status}); want client refusal"));
                            };
                            if attempt == REFUSAL_ATTEMPTS {
                                t.fatal(format!("request failed without proving client refusal {attempt} times: {err}"));
                            }
                            t.log(format!("attempt {attempt} failed without proving client refusal; retrying on a fresh port-forward: {err}"));
                        }
                    })
                    .await;
                }
            })
            .await;
        }
    });
}

/// A client certificate carrying `spiffe_id`, signed by a throwaway CA the
/// router does not trust; the chain includes that CA.
fn foreign_certificate(spiffe_id: &str) -> PodCertificate {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose, SanType,
    };
    let ca_key = KeyPair::generate().expect("CA key");
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca_cert = ca_params
        .clone()
        .self_signed(&ca_key)
        .expect("CA certificate");
    let issuer = rcgen::Issuer::new(ca_params, &ca_key);
    let key = KeyPair::generate().expect("leaf key");
    let mut leaf = CertificateParams::default();
    leaf.subject_alt_names = vec![SanType::URI(spiffe_id.try_into().expect("SPIFFE ID"))];
    leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let leaf_cert = leaf.signed_by(&key, &issuer).expect("leaf certificate");
    PodCertificate {
        chain_pem: leaf_cert.pem() + &ca_cert.pem(),
        key_pem: key.serialize_pem(),
    }
}

#[test]
fn ingress_refused_cases() {
    let io = |k| Some(ReqError::Io(k, String::new()));
    let cases: Vec<(&str, u16, Option<ReqError>, bool, bool)> = vec![
        (
            "TLS alert",
            0,
            Some(ReqError::TlsAlert("bad certificate".into())),
            false,
            true,
        ),
        (
            "unlisted identity closed",
            0,
            io(io::ErrorKind::UnexpectedEof),
            true,
            true,
        ),
        (
            "unlisted identity reset",
            0,
            io(io::ErrorKind::ConnectionReset),
            true,
            true,
        ),
        (
            "unlisted identity broken pipe",
            0,
            io(io::ErrorKind::BrokenPipe),
            true,
            true,
        ),
        ("unlisted identity forbidden", 403, None, true, true),
        (
            "missing certificate closed",
            0,
            io(io::ErrorKind::UnexpectedEof),
            false,
            false,
        ),
        (
            "missing certificate broken pipe",
            0,
            io(io::ErrorKind::BrokenPipe),
            false,
            false,
        ),
        ("untrusted certificate forbidden", 403, None, false, false),
        (
            "dial failure",
            0,
            io(io::ErrorKind::ConnectionRefused),
            true,
            false,
        ),
        (
            "timeout",
            0,
            Some(ReqError::Other("context deadline exceeded".into())),
            true,
            false,
        ),
        ("actor route reached", 404, None, true, false),
        ("CONNECT accepted", 200, None, true, false),
    ];
    for (name, status, err, allow, want) in cases {
        assert_eq!(ingress_refused(status, err.as_ref(), allow), want, "{name}");
    }
}

/// connect_request against a local TLS server: the status it reports and
/// whether that counts as a refusal, for each way the server can answer.
#[tokio::test]
async fn connect_request_cases() {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio::net::TcpListener;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cases: [(&str, u16, u16, u16, bool); 5] = [
        ("CONNECT denied", 403, 0, 403, true),
        ("inner denied", 200, 403, 403, true),
        ("inner closed", 200, 0, 0, true),
        ("actor lookup reached", 200, 404, 404, false),
        ("inner accepted", 200, 200, 200, false),
    ];
    for (name, connect_status, inner_status, want_status, want_refused) in cases {
        run_test(name, |t| async move {
            let key = rcgen::KeyPair::generate().unwrap();
            let cert = rcgen::CertificateParams::new(vec!["example.com".to_string()])
                .unwrap()
                .self_signed(&key)
                .unwrap();
            let der = CertificateDer::from(cert.der().to_vec());
            let server_config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![der.clone()],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
                )
                .unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let server = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let tls = tokio_rustls::TlsAcceptor::from(Arc::new(server_config))
                    .accept(tcp)
                    .await
                    .unwrap();
                let mut s = BufReader::new(tls);
                let head = read_head(&mut s).await;
                let mut problems = Vec::new();
                if !head.first().is_some_and(|l| l.starts_with("CONNECT ")) {
                    problems.push(format!("outer request = {:?}, want CONNECT", head.first()));
                }
                if connect_status != 200 {
                    let _ = s
                        .get_mut()
                        .write_all(
                            format!(
                                "HTTP/1.1 {connect_status} Refused\r\nContent-Length: 0\r\n\r\n"
                            )
                            .as_bytes(),
                        )
                        .await;
                    let _ = s.get_mut().shutdown().await;
                    return problems;
                }
                s.get_mut()
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .await
                    .unwrap();
                let inner = read_head(&mut s).await;
                let actor_header = format!("{}: {}", install::TARGET_ACTOR_HEADER, target_actor());
                if !inner.first().is_some_and(|l| l.starts_with("GET "))
                    || !inner.iter().any(|h| h.eq_ignore_ascii_case(&actor_header))
                {
                    problems.push(format!("unexpected inner request: {inner:?}"));
                }
                if inner_status != 0 {
                    let _ = s
                        .get_mut()
                        .write_all(
                            format!("HTTP/1.1 {inner_status} X\r\nContent-Length: 0\r\n\r\n")
                                .as_bytes(),
                        )
                        .await;
                }
                let _ = s.get_mut().shutdown().await;
                problems
            });
            let mut roots = rustls::RootCertStore::empty();
            roots.add(der).unwrap();
            let cfg = Arc::new(
                ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            );
            let result =
                connect_request(addr, cfg, ServerName::try_from("example.com").unwrap()).await;
            let (status, err) = match &result {
                Ok(s) => (*s, None),
                Err(e) => (0, Some(e)),
            };
            if status != want_status {
                t.error(format!(
                    "status = {status}, want {want_status} (error: {err:?})"
                ));
            }
            if ingress_refused(status, err, true) != want_refused {
                t.error(format!(
                    "refused = {}, want {want_refused} (error: {err:?})",
                    !want_refused
                ));
            }
            for p in server.await.unwrap() {
                t.error(p);
            }
        })
        .await;
    }
}

async fn read_head<S: AsyncRead + Unpin>(s: &mut BufReader<S>) -> Vec<String> {
    let mut head = Vec::new();
    loop {
        let mut line = String::new();
        if s.read_line(&mut line).await.unwrap_or(0) == 0 || line == "\r\n" {
            return head;
        }
        head.push(line.trim_end().to_string());
    }
}
