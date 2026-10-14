//! How proxied requests reach the Actor: HTTP/1.1, except gRPC, which is
//! carried over cleartext HTTP/2 because it cannot be downgraded.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::BoxFuture;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::BodyExt;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::Mutex;

use super::server::Dial;
use crate::tunnel::egress::BoxIo;

/// Boxed error of proxied bodies and requests.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
/// The body type proxied requests and responses carry.
pub type ProxyBody = UnsyncBoxBody<Bytes, BoxError>;

/// Boxes any body as a [`ProxyBody`].
pub fn boxed<B>(b: B) -> ProxyBody
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    b.map_err(Into::into).boxed_unsync()
}

/// An empty body.
pub fn empty() -> ProxyBody {
    boxed(http_body_util::Empty::new())
}

/// A body holding `s`.
pub fn full(s: impl Into<Bytes>) -> ProxyBody {
    boxed(http_body_util::Full::new(s.into()))
}

/// Sends requests to the Actor.
pub trait Transport: Send + Sync + 'static {
    /// Sends one request.
    fn round_trip(
        &self,
        req: http::Request<ProxyBody>,
    ) -> BoxFuture<'static, Result<http::Response<ProxyBody>, BoxError>>;
    /// Drops idle upstream connections.
    fn close_idle(&self) {}
}

/// Whether a request is gRPC: an HTTP/2 POST with an `application/grpc`
/// content type (optionally `+subtype` or `;params`).
pub fn is_grpc(version: http::Version, method: &http::Method, content_type: Option<&str>) -> bool {
    if version != http::Version::HTTP_2 || method != http::Method::POST {
        return false;
    }
    let Some(ct) = content_type.map(|c| c.to_ascii_lowercase()) else {
        return false;
    };
    let base = "application/grpc";
    ct == base || (ct.starts_with(base) && matches!(ct.as_bytes()[base.len()], b'+' | b';'))
}

/// A dialed upstream connection, as the HTTP client sees it.
pub struct DialedIo(TokioIo<BoxIo>);

impl hyper::rt::Read for DialedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for DialedIo {
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

impl Connection for DialedIo {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

/// A connector dialing through `dial` (or plain TCP).
#[derive(Clone)]
pub struct DialConnector(Option<Dial>);

impl tower::Service<http::Uri> for DialConnector {
    type Response = DialedIo;
    type Error = io::Error;
    type Future = BoxFuture<'static, io::Result<DialedIo>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let dial = self.0.clone();
        Box::pin(async move {
            let host = uri
                .host()
                .unwrap_or_default()
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_string();
            let port = uri.port_u16().unwrap_or(80);
            let addr = if host.contains(':') {
                format!("[{host}]:{port}")
            } else {
                format!("{host}:{port}")
            };
            let io: BoxIo = match dial {
                Some(d) => d(addr).await?,
                None => Box::new(tokio::net::TcpStream::connect(addr).await?),
            };
            Ok(DialedIo(TokioIo::new(io)))
        })
    }
}

type HttpClient = Client<DialConnector, ProxyBody>;

/// HTTP/1.1 for everything but gRPC, which goes over prior-knowledge h2c.
pub struct ProtocolMirrorTransport {
    connector: DialConnector,
    clients: Mutex<(HttpClient, HttpClient)>,
}

impl ProtocolMirrorTransport {
    /// A transport dialing with `dial` (plain TCP when `None`).
    pub fn new(dial: Option<Dial>) -> Self {
        let connector = DialConnector(dial);
        let clients = Mutex::new(Self::build(&connector));
        ProtocolMirrorTransport { connector, clients }
    }

    fn build(c: &DialConnector) -> (HttpClient, HttpClient) {
        let h1 = Client::builder(TokioExecutor::new()).build(c.clone());
        let h2c = Client::builder(TokioExecutor::new())
            .http2_only(true)
            .build(c.clone());
        (h1, h2c)
    }
}

impl Transport for ProtocolMirrorTransport {
    fn round_trip(
        &self,
        mut req: http::Request<ProxyBody>,
    ) -> BoxFuture<'static, Result<http::Response<ProxyBody>, BoxError>> {
        let ct = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());
        let grpc = is_grpc(req.version(), req.method(), ct);
        let (h1, h2c) = self.clients.lock().clone();
        Box::pin(async move {
            let client = if grpc {
                *req.version_mut() = http::Version::HTTP_2;
                h2c
            } else {
                *req.version_mut() = http::Version::HTTP_11;
                h1
            };
            let resp = client.request(req).await?;
            Ok(resp.map(boxed))
        })
    }

    fn close_idle(&self) {
        *self.clients.lock() = Self::build(&self.connector);
    }
}

/// A transport from a function, for tests and adapters.
pub struct FnTransport<F>(pub F);

impl<F> Transport for FnTransport<F>
where
    F: Fn(
            http::Request<ProxyBody>,
        ) -> BoxFuture<'static, Result<http::Response<ProxyBody>, BoxError>>
        + Send
        + Sync
        + 'static,
{
    fn round_trip(
        &self,
        req: http::Request<ProxyBody>,
    ) -> BoxFuture<'static, Result<http::Response<ProxyBody>, BoxError>> {
        (self.0)(req)
    }
}

impl<T: Transport + ?Sized> Transport for Arc<T> {
    fn round_trip(
        &self,
        req: http::Request<ProxyBody>,
    ) -> BoxFuture<'static, Result<http::Response<ProxyBody>, BoxError>> {
        (**self).round_trip(req)
    }
    fn close_idle(&self) {
        (**self).close_idle()
    }
}
