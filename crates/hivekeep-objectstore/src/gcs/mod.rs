//! Google Cloud Storage over its JSON and XML APIs.
//!
//! The client is a thin reqwest-based one rather than a general SDK: the
//! snapshot path needs exactly ranged reads, single and resumable uploads,
//! compose and rewrite, with a retry policy and a connection pool of its own,
//! and every one of those is exercised in tests against an in-process fake.

pub mod read;
pub mod sparse;
pub mod write;

#[cfg(test)]
pub(crate) mod fake;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use serde::Deserialize;
use url::Url;

use crate::error::{Error, Result, ServiceDetail, ServiceResponse};
use crate::gcp::{Anonymous, StaticToken, TokenSource};
use crate::http::{encode_component, encode_path, Request, ReqwestTransport, Response, Transport};
use crate::retry::{Backoff, RetryPolicy};
use crate::storage::{BackendKind, ObjectReader, ObjectStorage, SparseFilePutter, UploadBody};

/// The production endpoint.
pub const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com/";
/// How many connections transfers are spread over. One HTTP/2 connection
/// multiplexing every part measured 334 MB/s at eight streams against
/// 518 MB/s with a connection each; the many small objects a node moves at
/// once draw on the pool too.
pub const UPLOAD_POOL_SIZE: usize = 8;
/// How long the first retry waits at most, and the largest wait. Suspend is
/// latency-sensitive, so the first retry comes within 250 ms.
pub const RETRY_BACKOFF: Backoff = Backoff::new(Duration::from_millis(250), Duration::from_secs(5));
/// How long removing an upload's scratch objects may take. Cleanup runs
/// detached from the upload, which may be why it is unwinding, and retries
/// until this ends.
pub const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
/// How many scratch objects are deleted at once.
pub const CLEANUP_CONCURRENCY: usize = 8;

/// One connection to GCS: a transport, the credential its requests carry,
/// and the endpoint.
#[derive(Clone)]
pub struct GcsConn {
    /// Sends requests.
    pub transport: Arc<dyn Transport>,
    /// Authorizes them.
    pub auth: Arc<dyn TokenSource>,
    /// The endpoint, ending in `/`.
    pub base: Url,
}

#[derive(Deserialize, Default)]
struct JsonErrorBody {
    #[serde(default)]
    error: JsonError,
}

#[derive(Deserialize, Default)]
struct JsonError {
    #[serde(default)]
    errors: Vec<JsonErrorItem>,
}

#[derive(Deserialize, Default)]
struct JsonErrorItem {
    #[serde(default)]
    reason: String,
}

#[derive(Deserialize, Default)]
struct XmlErrorBody {
    #[serde(rename = "Code", default)]
    code: String,
    #[serde(rename = "Message", default)]
    message: String,
}

/// Classifies a failed response: a JSON API error by its first reason (its
/// message names the caller and resource), an XML API error by its generic
/// code and message (never its `Details`), anything else by status alone.
pub fn service_response(status: u16, body: &[u8]) -> ServiceResponse {
    if let Ok(b) = serde_json::from_slice::<JsonErrorBody>(body) {
        if let Some(item) = b.error.errors.first() {
            return ServiceResponse {
                status,
                detail: ServiceDetail::GcsJson {
                    reason: item.reason.clone(),
                },
            };
        }
    }
    let text = String::from_utf8_lossy(body);
    if text.contains("<Error>") {
        if let Ok(x) =
            quick_xml::de::from_str::<XmlErrorBody>(text.trim_start_matches(|c| c != '<').trim())
        {
            if !x.code.is_empty() || !x.message.is_empty() {
                return ServiceResponse {
                    status,
                    detail: ServiceDetail::GcsXml {
                        code: x.code,
                        message: x.message,
                    },
                };
            }
        }
    }
    ServiceResponse {
        status,
        detail: ServiceDetail::Other,
    }
}

/// The error for a failed GCS response. Its message carries the status
/// only.
pub fn gcs_error(status: u16, body: &[u8]) -> Error {
    Error::service(
        service_response(status, body),
        format!("gcs: HTTP {status}"),
    )
}

impl GcsConn {
    /// A connection over `transport` to `base`.
    pub fn new(transport: Arc<dyn Transport>, auth: Arc<dyn TokenSource>, base: Url) -> Self {
        GcsConn {
            transport,
            auth,
            base,
        }
    }

    fn url(&self, path_and_query: &str) -> Result<Url> {
        Url::parse(&format!("{}{path_and_query}", self.base))
            .map_err(|e| Error::other(format!("building a GCS URL: {e}")))
    }

    /// The JSON API URL of an object.
    pub fn object_url(&self, bucket: &str, object: &str, suffix: &str) -> Result<Url> {
        self.url(&format!(
            "storage/v1/b/{}/o/{}{suffix}",
            encode_component(bucket),
            encode_component(object)
        ))
    }

    /// The JSON API URL listing a bucket's objects.
    pub fn list_url(&self, bucket: &str) -> Result<Url> {
        self.url(&format!("storage/v1/b/{}/o", encode_component(bucket)))
    }

    /// The upload URL of a bucket.
    pub fn upload_url(&self, bucket: &str, upload_type: &str, object: &str) -> Result<Url> {
        let mut u = self.url(&format!(
            "upload/storage/v1/b/{}/o",
            encode_component(bucket)
        ))?;
        u.query_pairs_mut()
            .append_pair("uploadType", upload_type)
            .append_pair("name", object);
        Ok(u)
    }

    /// The XML API URL reading an object.
    pub fn media_url(&self, bucket: &str, object: &str) -> Result<Url> {
        self.url(&format!(
            "{}/{}",
            encode_component(bucket),
            encode_path(object)
        ))
    }

    /// Sends `req` with this connection's credential. A response of 400 or
    /// above becomes an error.
    pub async fn send(&self, mut req: Request) -> Result<Response> {
        if let Some(t) = self.auth.token().await? {
            req = req.header("authorization", format!("Bearer {}", t.token));
        }
        let resp = self.transport.send(req).await?;
        let status = resp.status.as_u16();
        if status >= 400 {
            let body = resp.bytes_limited(64 << 10).await;
            return Err(gcs_error(status, &body));
        }
        Ok(resp)
    }

    /// Sends and reads the whole response body.
    pub async fn send_read(&self, req: Request) -> Result<Bytes> {
        let resp = self.send(req).await?;
        Ok(resp.bytes().await?)
    }
}

/// How a [`GcsClient`] is built: its endpoint, credential, and a factory
/// for each pooled connection's transport.
#[derive(Clone)]
pub struct GcsOptions {
    /// The endpoint.
    pub endpoint: Url,
    /// The credential, shared by every pooled connection: a pooled
    /// connection that authenticated differently would fail partway
    /// through an object.
    pub auth: Arc<dyn TokenSource>,
    /// Makes one connection's transport.
    pub transport: Arc<dyn Fn() -> Result<Arc<dyn Transport>> + Send + Sync>,
    /// Pooled connections.
    pub pool_size: usize,
}

impl GcsOptions {
    /// Production options with `auth` against the real endpoint, or the
    /// emulator `STORAGE_EMULATOR_HOST` names (anonymously).
    pub fn from_env(auth: impl FnOnce() -> Result<Arc<dyn TokenSource>>) -> Result<Self> {
        let (endpoint, auth): (Url, Arc<dyn TokenSource>) =
            match std::env::var("STORAGE_EMULATOR_HOST") {
                Ok(h) if !h.is_empty() => {
                    let h = if h.contains("://") {
                        h
                    } else {
                        format!("http://{h}")
                    };
                    let h = if h.ends_with('/') { h } else { format!("{h}/") };
                    (
                        Url::parse(&h)
                            .map_err(|e| Error::other(format!("STORAGE_EMULATOR_HOST: {e}")))?,
                        Arc::new(Anonymous),
                    )
                }
                _ => (
                    Url::parse(DEFAULT_ENDPOINT).expect("valid endpoint"),
                    auth()?,
                ),
            };
        Ok(GcsOptions {
            endpoint,
            auth,
            transport: Arc::new(
                || Ok(Arc::new(ReqwestTransport::new(None)?) as Arc<dyn Transport>),
            ),
            pool_size: UPLOAD_POOL_SIZE,
        })
    }

    /// Options sending every connection through `transport`.
    pub fn with_transport(
        endpoint: Url,
        auth: Arc<dyn TokenSource>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        GcsOptions {
            endpoint,
            auth,
            transport: Arc::new(move || Ok(transport.clone())),
            pool_size: UPLOAD_POOL_SIZE,
        }
    }

    /// The same options authenticating with a fixed token.
    pub fn with_token(mut self, token: crate::gcp::AccessToken) -> Self {
        self.auth = Arc::new(StaticToken(token));
        self
    }
}

pub(crate) struct Inner {
    pub client: GcsConn,
    pub pool: Vec<GcsConn>,
    pub next: AtomicU64,
    pub retry: RetryPolicy,
    pub cleanup_timeout: Duration,
}

/// A GCS-backed [`ObjectStorage`]. It installs an always-retry policy, safe
/// only for this client's own operations (every write targets a unique name
/// or is idempotent), so it builds its connections rather than accept one.
///
/// Nothing caps the attempts: an operation retries transient errors (408,
/// 429, 5xx) until it succeeds or the caller drops it, so every caller
/// bounds its work. A single 429, routine while GCS scales a cold bucket's
/// key ranges up to a suspend burst, must not fail a snapshot.
#[derive(Clone)]
pub struct GcsClient {
    pub(crate) inner: Arc<Inner>,
}

impl GcsClient {
    /// A client per `opts`, with its connection pool. A pool that could not
    /// be completed is used as far as it got: that costs throughput, not
    /// correctness.
    pub fn new(opts: &GcsOptions) -> Result<Self> {
        let client = GcsConn::new(
            (opts.transport)()?,
            opts.auth.clone(),
            opts.endpoint.clone(),
        );
        let mut pool = Vec::with_capacity(opts.pool_size);
        for _ in 0..opts.pool_size {
            match (opts.transport)() {
                Ok(t) => pool.push(GcsConn::new(t, opts.auth.clone(), opts.endpoint.clone())),
                Err(e) => {
                    tracing::warn!(built = pool.len(), want = opts.pool_size, err = %e,
                        "GCS connection pool is short; transfers share fewer connections");
                    break;
                }
            }
        }
        Ok(Self::from_parts(client, pool))
    }

    /// A client over explicit connections.
    pub fn from_parts(client: GcsConn, pool: Vec<GcsConn>) -> Self {
        GcsClient {
            inner: Arc::new(Inner {
                client,
                pool,
                next: AtomicU64::new(0),
                retry: RetryPolicy {
                    backoff: RETRY_BACKOFF,
                    max_attempts: None,
                },
                cleanup_timeout: CLEANUP_TIMEOUT,
            }),
        }
    }

    /// The same connections with another retry backoff and cleanup bound,
    /// for tests that drive many retries quickly.
    pub fn with_timings(self, backoff: Backoff, cleanup_timeout: Duration) -> Self {
        let inner = Arc::try_unwrap(self.inner).unwrap_or_else(|a| Inner {
            client: a.client.clone(),
            pool: a.pool.clone(),
            next: AtomicU64::new(0),
            retry: a.retry,
            cleanup_timeout: a.cleanup_timeout,
        });
        GcsClient {
            inner: Arc::new(Inner {
                retry: RetryPolicy {
                    backoff,
                    max_attempts: None,
                },
                cleanup_timeout,
                ..inner
            }),
        }
    }

    /// The connection part or range `i` uses, or the main one without a
    /// pool.
    pub fn upload_client(&self, i: usize) -> &GcsConn {
        if self.inner.pool.is_empty() {
            return &self.inner.client;
        }
        &self.inner.pool[i % self.inner.pool.len()]
    }

    /// Pooled connections round-robin, for a transfer that is one request.
    pub fn next_client(&self) -> &GcsConn {
        let i = self.inner.next.fetch_add(1, Ordering::Relaxed) as usize;
        self.upload_client(i)
    }

    /// The main connection.
    pub fn client(&self) -> &GcsConn {
        &self.inner.client
    }

    /// The pooled connections.
    pub fn pool(&self) -> &[GcsConn] {
        &self.inner.pool
    }

    pub(crate) fn retry(&self) -> RetryPolicy {
        self.inner.retry
    }
}

/// Builds `Request`s for an object operation.
pub(crate) fn request(method: Method, url: Url) -> Request {
    Request::new(method, url)
}

#[async_trait]
impl ObjectStorage for GcsClient {
    async fn get_object(&self, bucket: &str, object: &str) -> Result<ObjectReader> {
        self.get_object_impl(bucket, object).await
    }

    async fn put_object(&self, bucket: &str, object: &str, body: UploadBody) -> Result<()> {
        self.put_object_impl(bucket, object, body).await
    }

    fn backend(&self) -> BackendKind {
        BackendKind::Gcs
    }

    fn streams_puts(&self) -> bool {
        true
    }

    fn sparse_putter(&self) -> Option<&dyn SparseFilePutter> {
        Some(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ServiceError;

    #[test]
    fn describes_gcs_errors() {
        const SECRET: &str = "caller@project.iam.gserviceaccount.com does not have storage.objects.get access to gvisor/releases/runsc";
        let cases = [
            (
                "XML API error",
                412,
                format!("<?xml version='1.0' encoding='UTF-8'?><Error><Code>PreconditionFailed</Code><Message>The operation requires that Uniform Bucket Level Access be enabled.</Message><Details>{SECRET}</Details></Error>"),
                ServiceError {
                    http_status: 412,
                    code: "PreconditionFailed".into(),
                    message: "The operation requires that Uniform Bucket Level Access be enabled.".into(),
                },
            ),
            (
                "XML API error, wrapped",
                403,
                format!("<Error><Code>AccessDenied</Code><Message>Access denied.</Message><Details>{SECRET}</Details></Error>"),
                ServiceError {
                    http_status: 403,
                    code: "AccessDenied".into(),
                    message: "Access denied.".into(),
                },
            ),
            (
                "JSON API error",
                403,
                format!(r#"{{"error":{{"code":403,"message":"{SECRET}","errors":[{{"reason":"forbidden","message":"{SECRET}"}}]}}}}"#),
                ServiceError {
                    http_status: 403,
                    code: "forbidden".into(),
                    message: String::new(),
                },
            ),
            (
                "body that is not an XML error",
                502,
                format!("<html><body>Bad Gateway {SECRET}</body></html>"),
                ServiceError {
                    http_status: 502,
                    ..Default::default()
                },
            ),
            (
                "empty body",
                500,
                String::new(),
                ServiceError {
                    http_status: 500,
                    ..Default::default()
                },
            ),
        ];
        for (name, status, body, want) in cases {
            let mut e = gcs_error(status, body.as_bytes());
            if name.ends_with("wrapped") {
                e = e.context("while opening gs://gvisor/releases/runsc");
            }
            let got = e
                .describe()
                .unwrap_or_else(|| panic!("{name}: no service error"));
            assert_eq!(got, want, "{name}");
            assert!(
                !(got.code.clone() + &got.message).contains("caller@"),
                "{name}: leaks the caller"
            );
            assert!(
                !e.to_string().contains("caller@"),
                "{name}: message leaks the caller"
            );
        }
        assert!(Error::other("boom").describe().is_none(), "not a GCS error");
    }

    #[test]
    fn pooled_clients_are_built_like_the_client() {
        let made = Arc::new(AtomicU64::new(0));
        let m = made.clone();
        let opts = GcsOptions {
            endpoint: Url::parse("http://gcs.test/").unwrap(),
            auth: Arc::new(Anonymous),
            transport: Arc::new(move || {
                m.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(ReqwestTransport::new(None)?) as Arc<dyn Transport>)
            }),
            pool_size: UPLOAD_POOL_SIZE,
        };
        let g = GcsClient::new(&opts).unwrap();
        assert_eq!(g.pool().len(), UPLOAD_POOL_SIZE);
        assert_eq!(made.load(Ordering::SeqCst), UPLOAD_POOL_SIZE as u64 + 1);
        for i in 0..UPLOAD_POOL_SIZE {
            let c = g.upload_client(i);
            assert!(
                !Arc::ptr_eq(&c.transport, &g.client().transport),
                "upload_client({i}) fell back to the main connection"
            );
            assert!(
                Arc::ptr_eq(&c.auth, &g.client().auth),
                "upload_client({i}) authenticates differently"
            );
        }
    }
}
