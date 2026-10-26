use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use super::*;
use crate::tls::server::ServerTls;
use crate::tls::testca::TestCa;

const SPIFFE: &str = "spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner";

/// An HTTPS server requiring a client certificate from `ca`, serving a
/// certificate for `dns`, answering 200 with the HTTP version it spoke.
async fn mtls_server(ca: &TestCa, dns: &str) -> std::net::SocketAddr {
    crate::boot::install_crypto_provider();
    let serving = ca.issue_dns(dns);
    let tls = ServerTls::new(
        serving.to_str().unwrap(),
        ca.trust_bundle.to_str().unwrap(),
        None,
    )
    .unwrap();
    let mut cfg = (*tls.server_config().unwrap()).clone();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(cfg));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(tls) = acceptor.accept(stream).await else {
                    return;
                };
                let svc = service_fn(|req: hyper::Request<hyper::body::Incoming>| async move {
                    Ok::<_, Infallible>(hyper::Response::new(Full::new(Bytes::from(format!(
                        "{:?}",
                        req.version()
                    )))))
                });
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(tls), svc)
                    .await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn router_tls_config_mtls() {
    let ca = TestCa::new();
    let bundle = ca.issue(SPIFFE);
    let ca_file = ca.trust_bundle.to_str().unwrap().to_string();
    let name = router_server_name();
    let addr = mtls_server(&ca, &name).await;
    // The URL names the loopback address; the configured server name is
    // what the certificate is checked against.
    let url = format!("https://127.0.0.1:{}/", addr.port());

    // A matching server name succeeds, over HTTP/1.1.
    let cfg = router_tls_config(&ca_file, bundle.to_str().unwrap(), &name).unwrap();
    let client = router_http_client(cfg, 4).unwrap();
    let resp = client
        .get(&url)
        .send()
        .await
        .expect("GET with the matching server name");
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.version(), http::Version::HTTP_11);

    // A mismatched server name is rejected.
    let cfg = router_tls_config(
        &ca_file,
        bundle.to_str().unwrap(),
        "wrong.hivekeep-system.svc",
    )
    .unwrap();
    let client = router_http_client(cfg, 4).unwrap();
    assert!(
        client.get(&url).send().await.is_err(),
        "GET with a mismatched server name succeeded"
    );

    // Missing files are errors.
    let dir = tempfile::tempdir().unwrap();
    let missing_ca = dir.path().join("missing-ca.crt");
    let missing_bundle = dir.path().join("missing-bundle.pem");
    assert!(router_tls_config(
        missing_ca.to_str().unwrap(),
        bundle.to_str().unwrap(),
        &name
    )
    .is_err());
    assert!(router_tls_config(&ca_file, missing_bundle.to_str().unwrap(), &name).is_err());
}

#[test]
fn defaults_name_the_install() {
    assert_eq!(
        default_router_url(),
        "https://gate-router.hivekeep-system.svc"
    );
    assert_eq!(
        default_api_endpoint(),
        "k8s:///hivekeep-api.hivekeep-system.svc:443"
    );
}
