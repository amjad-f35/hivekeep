//! The HTTPS accept loop: TLS, then HTTP/1.1 or HTTP/2 by ALPN, until a
//! shutdown signal; open connections then get a grace period to finish.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_identity::jwt::authority::JwtSigner;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use crate::server::IdpServer;

/// How long a client may take to complete the TLS handshake or send the
/// request head.
pub const READ_HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// Serves `server` over TLS on `listener` until `shutdown` resolves, then
/// waits up to `drain_grace` for open connections.
pub async fn serve<S, F>(
    listener: TcpListener,
    tls: TlsAcceptor,
    server: Arc<IdpServer<S>>,
    shutdown: F,
    drain_grace: Duration,
) where
    S: JwtSigner + 'static,
    F: Future<Output = ()>,
{
    let graceful = GracefulShutdown::new();
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(READ_HEADER_TIMEOUT);
    let builder = Arc::new(builder);
    tokio::pin!(shutdown);
    loop {
        let (tcp, peer) = tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(err = %e, "Accepting a connection failed");
                    continue;
                }
            },
        };
        let tls = tls.clone();
        let server = server.clone();
        let builder = builder.clone();
        let watcher = graceful.watcher();
        tokio::spawn(async move {
            let stream = match tokio::time::timeout(READ_HEADER_TIMEOUT, tls.accept(tcp)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    tracing::debug!(%peer, err = %e, "TLS handshake failed");
                    return;
                }
                Err(_) => {
                    tracing::debug!(%peer, "TLS handshake timed out");
                    return;
                }
            };
            let svc = service_fn(move |req: http::Request<Incoming>| {
                let resp = server.handle(req.method(), req.uri().path());
                async move { Ok::<_, Infallible>(resp) }
            });
            let conn = builder.serve_connection(TokioIo::new(stream), svc);
            if let Err(e) = watcher.watch(conn).await {
                tracing::debug!(%peer, err = %e, "HTTP connection ended with an error");
            }
        });
    }
    if tokio::time::timeout(drain_grace, graceful.shutdown())
        .await
        .is_err()
    {
        tracing::warn!(grace = ?drain_grace, "Graceful shutdown timed out; dropping open connections");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_identity::jwt::authority::{JwtAlgorithm, JwtPool};
    use rustls::pki_types::{CertificateDer, ServerName};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A request over real TLS reaches the routes, and shutdown returns.
    #[tokio::test]
    async fn serves_https_until_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = dir.path().join("bundle.pem");
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["idp.test".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        std::fs::write(&bundle, format!("{}{}", cert.pem(), key.serialize_pem())).unwrap();

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let cfg = crate::tls::server_config(bundle.to_str().unwrap(), provider.clone()).unwrap();
        let server = Arc::new(
            IdpServer::new(
                "https://idp.test",
                JwtPool::generate(JwtAlgorithm::Es256, Some("k1")).unwrap(),
            )
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(serve(
            listener,
            TlsAcceptor::from(Arc::new(cfg)),
            server,
            async {
                let _ = stopped.await;
            },
            Duration::from_secs(1),
        ));

        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(CertificateDer::from(cert.der().to_vec()))
            .unwrap();
        let mut client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client.alpn_protocols = vec![b"http/1.1".to_vec()];
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut s = tokio_rustls::TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("idp.test").unwrap(), tcp)
            .await
            .unwrap();
        s.write_all(b"GET /openid/v1/jwks HTTP/1.1\r\nHost: idp.test\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out).await;
        let out = String::from_utf8_lossy(&out);
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
        assert!(out.contains("\"kid\":\"k1\""), "{out}");

        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("serve did not return after shutdown")
            .unwrap();
    }
}
