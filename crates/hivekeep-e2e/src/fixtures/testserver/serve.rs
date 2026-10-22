//! The HTTP/1.1 serving loop shared by the subcommands that speak HTTP.
//!
//! A subcommand supplies a [`Handler`]; [`serve_listeners`] runs it over a
//! cleartext listener and, optionally, a TLS one, upgrading connections that
//! ask for it so WebSocket handlers can take them over.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// Response body every handler answers with.
pub type Body = Full<Bytes>;

/// A boxed response future.
pub type ResponseFuture = Pin<Box<dyn Future<Output = Response<Body>> + Send>>;

/// An HTTP request handler.
pub type Handler = Arc<dyn Fn(Request<Incoming>) -> ResponseFuture + Send + Sync>;

/// How long a client may take to send its request head.
pub const READ_HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// Wraps an async function as a [`Handler`].
pub fn handler<F, Fut>(f: F) -> Handler
where
    F: Fn(Request<Incoming>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Body>> + Send + 'static,
{
    Arc::new(move |req| Box::pin(f(req)))
}

/// A response with `status` and a plain-text `body`.
pub fn text(status: StatusCode, body: impl Into<Bytes>) -> Response<Body> {
    let mut response = Response::new(Full::new(body.into()));
    *response.status_mut() = status;
    response
}

/// A 200 response carrying `value` as JSON, newline-terminated.
pub fn json<T: serde::Serialize>(value: &T) -> Response<Body> {
    match serde_json::to_vec(value) {
        Ok(mut body) => {
            body.push(b'\n');
            let mut response = text(StatusCode::OK, body);
            response.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/json"),
            );
            response
        }
        Err(e) => text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("encoding response: {e}\n"),
        ),
    }
}

/// Turns a bare `:port` listen address into one a socket can bind.
pub fn bind_address(listen: &str) -> String {
    if listen.starts_with(':') {
        format!("0.0.0.0{listen}")
    } else {
        listen.to_string()
    }
}

/// Serves one connection until the client goes away.
async fn serve_connection<S>(stream: S, handler: Handler)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = service_fn(move |req| {
        let handler = handler.clone();
        async move { Ok::<_, Infallible>(handler(req).await) }
    });
    let result = hyper::server::conn::http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(READ_HEADER_TIMEOUT)
        .serve_connection(TokioIo::new(stream), service)
        .with_upgrades()
        .await;
    if let Err(e) = result {
        if !e.is_incomplete_message() {
            eprintln!("testserver: serving connection: {e}");
        }
    }
}

async fn accept_loop(
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    handler: Handler,
) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let handler = handler.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            match tls {
                None => serve_connection(stream, handler).await,
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(stream) => serve_connection(stream, handler).await,
                    Err(e) => eprintln!("testserver: TLS handshake: {e}"),
                },
            }
        });
    }
}

/// Serves `handler` in cleartext on `cleartext` and, unless `encrypted` is
/// `None`, over TLS on that listener, returning when either stops. Both are
/// bound before this is called, so a readiness probe that reaches one finds
/// the other already listening.
pub async fn serve_listeners(
    handler: Handler,
    cleartext: TcpListener,
    encrypted: Option<(TcpListener, TlsAcceptor)>,
) -> io::Result<()> {
    match encrypted {
        None => accept_loop(cleartext, None, handler).await,
        Some((listener, acceptor)) => {
            tokio::select! {
                r = accept_loop(cleartext, None, handler.clone()) => r,
                r = accept_loop(listener, Some(acceptor), handler) => r,
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    //! Loopback servers for the subcommands' unit tests.

    use std::io::Write as _;
    use std::net::Ipv4Addr;
    use std::path::PathBuf;

    use tokio::task::JoinHandle;

    use super::*;
    use crate::fixtures::testserver::tls::configure_tls_server;
    use crate::testcert;

    /// A certificate for 127.0.0.1 written to disk, and the CA that issued it.
    pub struct OriginCertificate {
        pub ca_pem: Vec<u8>,
        pub cert_file: PathBuf,
        pub key_file: PathBuf,
        _dir: tempfile::TempDir,
    }

    pub fn write_origin_certificate() -> OriginCertificate {
        let material = testcert::new_server_tls(Ipv4Addr::LOCALHOST.into());
        let dir = tempfile::tempdir().unwrap();
        let cert_file = dir.path().join("server.crt");
        let key_file = dir.path().join("server.key");
        std::fs::File::create(&cert_file)
            .unwrap()
            .write_all(&material.certificate)
            .unwrap();
        std::fs::File::create(&key_file)
            .unwrap()
            .write_all(&material.private_key)
            .unwrap();
        OriginCertificate {
            ca_pem: material.root_ca,
            cert_file,
            key_file,
            _dir: dir,
        }
    }

    /// A running origin; its URLs for a scheme it does not serve are empty.
    pub struct OriginServer {
        pub http_url: String,
        pub https_url: String,
        pub ws_url: String,
        pub wss_url: String,
        task: JoinHandle<io::Result<()>>,
    }

    impl Drop for OriginServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    pub async fn listen_loopback() -> TcpListener {
        TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listen on loopback")
    }

    /// Serves `handler` the way the subcommands do: cleartext on one loopback
    /// listener and, given a certificate, TLS on a second.
    pub async fn start_origin_server(
        handler: Handler,
        cert: Option<&OriginCertificate>,
    ) -> OriginServer {
        let acceptor = match cert {
            Some(c) => {
                configure_tls_server(c.cert_file.to_str().unwrap(), c.key_file.to_str().unwrap())
                    .expect("configure TLS")
            }
            None => None,
        };
        let cleartext = listen_loopback().await;
        let clear_addr = cleartext.local_addr().unwrap();
        let mut origin = OriginServer {
            http_url: format!("http://{clear_addr}"),
            https_url: String::new(),
            ws_url: format!("ws://{clear_addr}"),
            wss_url: String::new(),
            task: tokio::spawn(async { Ok(()) }),
        };
        let encrypted = match acceptor {
            Some(acceptor) => {
                let listener = listen_loopback().await;
                let addr = listener.local_addr().unwrap();
                origin.https_url = format!("https://{addr}");
                origin.wss_url = format!("wss://{addr}");
                Some((listener, acceptor))
            }
            None => None,
        };
        origin.task = tokio::spawn(serve_listeners(handler, cleartext, encrypted));
        origin
    }
}
