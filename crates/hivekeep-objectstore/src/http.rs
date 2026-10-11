//! The HTTP seam the storage clients send through.
//!
//! Every cloud request goes through a [`Transport`]: production uses
//! [`ReqwestTransport`], tests answer requests in-process with
//! [`transport_fn`], the way a recording round-tripper would.

use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::{Stream, StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use tokio::io::AsyncRead;
use url::Url;

/// A streamed response body.
pub type Body = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send>>;

/// An outgoing request. Bodies are in memory: every upload is cut into
/// bounded parts or chunks before it is sent.
#[derive(Clone)]
pub struct Request {
    /// The method.
    pub method: Method,
    /// The full URL.
    pub url: Url,
    /// Headers.
    pub headers: HeaderMap,
    /// The body.
    pub body: Bytes,
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Headers carry credentials and are never printed.
        f.debug_struct("Request")
            .field("method", &self.method)
            .field("path", &self.url.path())
            .field("body_len", &self.body.len())
            .finish()
    }
}

impl Request {
    /// A request with no headers and an empty body.
    pub fn new(method: Method, url: Url) -> Self {
        Request {
            method,
            url,
            headers: HeaderMap::new(),
            body: Bytes::new(),
        }
    }

    /// Sets a header; values that are not valid header text are dropped.
    pub fn header(mut self, name: &'static str, value: impl AsRef<str>) -> Self {
        if let Ok(v) = HeaderValue::from_str(value.as_ref()) {
            self.headers.insert(name, v);
        }
        self
    }

    /// Sets the body.
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    /// The value of a query parameter.
    pub fn query(&self, name: &str) -> Option<String> {
        self.url
            .query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned())
    }

    /// A header's value as text.
    pub fn header_str(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

/// A response whose body has not been read.
pub struct Response {
    /// The status.
    pub status: StatusCode,
    /// Headers.
    pub headers: HeaderMap,
    /// The body.
    pub body: Body,
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .finish()
    }
}

impl Response {
    /// A response with an in-memory body.
    pub fn new(status: u16, body: impl Into<Bytes>) -> Self {
        let body: Bytes = body.into();
        Response {
            status: StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers: HeaderMap::new(),
            body: Box::pin(futures::stream::once(async move { Ok(body) })),
        }
    }

    /// A response streaming `body`.
    pub fn streaming(status: u16, body: Body) -> Self {
        Response {
            status: StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            headers: HeaderMap::new(),
            body,
        }
    }

    /// Sets a header.
    pub fn with_header(mut self, name: &'static str, value: impl AsRef<str>) -> Self {
        if let Ok(v) = HeaderValue::from_str(value.as_ref()) {
            self.headers.insert(name, v);
        }
        self
    }

    /// A header's value as text.
    pub fn header_str(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    /// Reads the whole body.
    pub async fn bytes(self) -> io::Result<Bytes> {
        let mut out = BytesMut::new();
        let mut body = self.body;
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(out.freeze())
    }

    /// Reads at most `limit` bytes of the body, for error descriptions.
    pub async fn bytes_limited(self, limit: usize) -> Bytes {
        let mut out = BytesMut::new();
        let mut body = self.body;
        while let Some(Ok(chunk)) = body.next().await {
            let room = limit.saturating_sub(out.len());
            out.extend_from_slice(&chunk[..chunk.len().min(room)]);
            if out.len() >= limit {
                break;
            }
        }
        out.freeze()
    }

    /// The body as an [`AsyncRead`].
    pub fn into_reader(self) -> Pin<Box<dyn AsyncRead + Send>> {
        Box::pin(tokio_util::io::StreamReader::new(self.body))
    }
}

/// A request that never got a response.
#[derive(Debug, Clone, thiserror::Error)]
#[error("transport error: {0}")]
pub struct TransportError(pub String);

/// Sends requests.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Sends one request.
    async fn send(&self, req: Request) -> Result<Response, TransportError>;
}

/// A [`Transport`] over a function, for tests and adapters.
pub struct FnTransport<F>(pub F);

#[async_trait]
impl<F> Transport for FnTransport<F>
where
    F: Fn(Request) -> BoxFuture<'static, Result<Response, TransportError>> + Send + Sync + 'static,
{
    async fn send(&self, req: Request) -> Result<Response, TransportError> {
        (self.0)(req).await
    }
}

/// Wraps a function as a shared [`Transport`].
pub fn transport_fn<F>(f: F) -> Arc<dyn Transport>
where
    F: Fn(Request) -> BoxFuture<'static, Result<Response, TransportError>> + Send + Sync + 'static,
{
    Arc::new(FnTransport(f))
}

/// The production transport, one connection pool per instance.
#[derive(Debug, Clone)]
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    /// A transport with its own connection pool. `timeout`, if set, bounds
    /// each whole request.
    pub fn new(timeout: Option<Duration>) -> Result<Self, TransportError> {
        let mut b = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(t) = timeout {
            b = b.timeout(t);
        }
        let client = b.build().map_err(|e| TransportError(e.to_string()))?;
        Ok(ReqwestTransport { client })
    }
}

#[async_trait]
impl Transport for ReqwestTransport {
    async fn send(&self, req: Request) -> Result<Response, TransportError> {
        let resp = self
            .client
            .request(req.method, req.url)
            .headers(req.headers)
            .body(req.body)
            .send()
            .await
            // The URL may carry a signature, so only the error kind is kept.
            .map_err(|e| TransportError(describe_reqwest(&e)))?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp
            .bytes_stream()
            .map_err(|e| io::Error::other(describe_reqwest(&e)));
        Ok(Response {
            status,
            headers,
            body: Box::pin(body),
        })
    }
}

fn describe_reqwest(e: &reqwest::Error) -> String {
    let what = if e.is_timeout() {
        "timed out"
    } else if e.is_connect() {
        "connect failed"
    } else if e.is_body() || e.is_decode() {
        "body failed"
    } else {
        "request failed"
    };
    let mut src: Option<&dyn std::error::Error> = std::error::Error::source(e);
    let mut detail = String::new();
    while let Some(s) = src {
        detail = s.to_string();
        src = s.source();
    }
    if detail.is_empty() {
        what.to_string()
    } else {
        format!("{what}: {detail}")
    }
}

/// Percent-encodes a path segment or query value (RFC 3986 unreserved set
/// kept).
pub fn encode_component(s: &str) -> String {
    const SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    percent_encoding::utf8_percent_encode(s, SET).to_string()
}

/// Percent-encodes an object key as a URL path, keeping `/`.
pub fn encode_path(s: &str) -> String {
    s.split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fn_transport_answers() {
        let t = transport_fn(|req: Request| {
            Box::pin(async move {
                Ok(Response::new(200, req.url.path().to_string()).with_header("x-a", "b"))
            })
        });
        let resp = t
            .send(Request::new(
                Method::GET,
                Url::parse("http://h/p/q").unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.header_str("x-a"), Some("b"));
        assert_eq!(&resp.bytes().await.unwrap()[..], b"/p/q");
    }

    #[test]
    fn encodes() {
        assert_eq!(encode_component("a/b c"), "a%2Fb%20c");
        assert_eq!(encode_path("a/b c/d~"), "a/b%20c/d~");
    }
}
