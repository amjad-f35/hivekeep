//! S3 and S3-compatible stores (MinIO, R2, Ceph, rustfs) over their REST
//! API with SigV4 signing.

pub mod credentials;
pub mod objects;
pub mod sigv4;

#[cfg(test)]
pub(crate) mod fake;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use serde::Deserialize;
use url::Url;

use crate::error::{Error, Result, ServiceDetail, ServiceResponse};
use crate::http::{encode_component, encode_path, Request, ReqwestTransport, Response, Transport};
use crate::storage::{BackendKind, ObjectReader, ObjectStorage, UploadBody};
use credentials::CredentialsProvider;

/// Where and how an [`S3Client`] connects.
#[derive(Clone)]
pub struct S3Config {
    /// The endpoint, e.g. `https://s3.us-east-1.amazonaws.com`.
    pub endpoint: Url,
    /// The signing region.
    pub region: String,
    /// Address buckets as `endpoint/bucket/key` rather than
    /// `bucket.endpoint/key`.
    pub path_style: bool,
    /// Signs requests.
    pub credentials: Arc<dyn CredentialsProvider>,
    /// Sends requests.
    pub transport: Arc<dyn Transport>,
    /// Attempts per request for transient errors.
    pub max_attempts: u32,
}

impl S3Config {
    /// The configuration the standard AWS environment variables describe:
    /// `AWS_REGION`/`AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL_S3` or
    /// `AWS_ENDPOINT_URL`, and `AWS_S3_USE_PATH_STYLE=true` for path-style
    /// addressing. Credentials come from [`credentials::from_env`].
    pub fn from_env() -> Result<Self> {
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string());
        let endpoint = std::env::var("AWS_ENDPOINT_URL_S3")
            .or_else(|_| std::env::var("AWS_ENDPOINT_URL"))
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let endpoint = Url::parse(&endpoint)
            .map_err(|e| Error::other(format!("S3 endpoint {endpoint:?}: {e}")))?;
        let path_style = std::env::var("AWS_S3_USE_PATH_STYLE").as_deref() == Ok("true");
        Ok(S3Config {
            endpoint,
            region,
            path_style,
            credentials: credentials::from_env()?,
            transport: Arc::new(ReqwestTransport::new(None)?),
            max_attempts: 3,
        })
    }

    /// A configuration for tests and fixed endpoints.
    pub fn new(
        endpoint: Url,
        region: &str,
        credentials: Arc<dyn CredentialsProvider>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        S3Config {
            endpoint,
            region: region.to_string(),
            path_style: true,
            credentials,
            transport,
            max_attempts: 1,
        }
    }
}

#[derive(Deserialize, Default)]
struct XmlError {
    #[serde(rename = "Code", default)]
    code: String,
}

/// The error for a failed S3 response: its code, or the status alone.
pub fn s3_error(status: u16, body: &[u8]) -> Error {
    let code = std::str::from_utf8(body)
        .ok()
        .filter(|t| t.contains("<Error"))
        .and_then(|t| quick_xml::de::from_str::<XmlError>(t).ok())
        .map(|e| e.code)
        .unwrap_or_default();
    let msg = if code.is_empty() {
        format!("S3 answered HTTP {status}")
    } else {
        format!("S3 answered HTTP {status}: {code}")
    };
    Error::service(
        ServiceResponse {
            status,
            detail: ServiceDetail::S3 { code },
        },
        msg,
    )
}

/// Whether `err` says the object or its bucket is not there. AccessDenied
/// is never read as absence.
pub fn object_absent(err: &Error) -> bool {
    err.http_status() == Some(404) || matches!(err.s3_code(), Some("NoSuchKey"))
}

/// An S3 client.
#[derive(Clone)]
pub struct S3Client {
    pub(crate) cfg: Arc<S3Config>,
}

impl S3Client {
    /// A client per `cfg`.
    pub fn new(cfg: S3Config) -> Self {
        S3Client { cfg: Arc::new(cfg) }
    }

    /// A client from the standard AWS environment.
    pub fn from_env() -> Result<Self> {
        tracing::info!("Using S3 storage backend");
        Ok(Self::new(S3Config::from_env()?))
    }

    /// Whether buckets are addressed path-style.
    pub fn path_style(&self) -> bool {
        self.cfg.path_style
    }

    /// The URL of `key` in `bucket`, with `query` pairs.
    pub fn url(&self, bucket: &str, key: &str, query: &[(&str, &str)]) -> Result<Url> {
        let ep = &self.cfg.endpoint;
        let scheme = ep.scheme();
        let host = match ep.port() {
            Some(p) => format!("{}:{p}", ep.host_str().unwrap_or_default()),
            None => ep.host_str().unwrap_or_default().to_string(),
        };
        let base_path = ep.path().trim_end_matches('/');
        let key = encode_path(key);
        let mut s = if self.cfg.path_style {
            format!(
                "{scheme}://{host}{base_path}/{}/{key}",
                encode_component(bucket)
            )
        } else {
            format!("{scheme}://{bucket}.{host}{base_path}/{key}")
        };
        if !query.is_empty() {
            s.push('?');
            s.push_str(
                &query
                    .iter()
                    .map(|(k, v)| {
                        if v.is_empty() {
                            encode_component(k)
                        } else {
                            format!("{}={}", encode_component(k), encode_component(v))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("&"),
            );
        }
        Url::parse(&s).map_err(|e| Error::other(format!("building an S3 URL: {e}")))
    }

    /// Signs and sends `req`, retrying transient failures. A response of 300
    /// or above becomes an error.
    pub async fn send(&self, req: Request) -> Result<Response> {
        let policy = crate::retry::RetryPolicy {
            backoff: crate::retry::Backoff::new(
                std::time::Duration::from_millis(100),
                std::time::Duration::from_secs(5),
            ),
            max_attempts: Some(self.cfg.max_attempts.max(1)),
        };
        crate::retry::retry(policy, || {
            let mut req = req.clone();
            async move {
                let creds = self.cfg.credentials.credentials().await?;
                sigv4::sign(&mut req, &creds, &self.cfg.region, chrono::Utc::now());
                let resp = self.cfg.transport.send(req).await?;
                let status = resp.status.as_u16();
                if status >= 300 {
                    let body = resp.bytes_limited(64 << 10).await;
                    return Err(s3_error(status, &body));
                }
                Ok(resp)
            }
        })
        .await
    }

    /// Sends and reads the body.
    pub async fn send_read(&self, req: Request) -> Result<Bytes> {
        Ok(self.send(req).await?.bytes().await?)
    }

    /// A request.
    pub fn request(
        &self,
        method: Method,
        bucket: &str,
        key: &str,
        query: &[(&str, &str)],
    ) -> Result<Request> {
        Ok(Request::new(method, self.url(bucket, key, query)?))
    }
}

#[async_trait]
impl ObjectStorage for S3Client {
    async fn get_object(&self, bucket: &str, object: &str) -> Result<ObjectReader> {
        self.get_object_impl(bucket, object).await
    }

    async fn put_object(&self, bucket: &str, object: &str, body: UploadBody) -> Result<()> {
        self.put_object_impl(bucket, object, body).await
    }

    fn backend(&self) -> BackendKind {
        BackendKind::S3
    }

    /// The multipart uploader buffers each part itself, so it takes a
    /// non-seekable body and compression is piped straight into it.
    fn streams_puts(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::fake::FakeS3;

    #[tokio::test]
    async fn get_object_classifies_absence() {
        for (name, status, body, absent) in [
            (
                "NoSuchKey is absence",
                404,
                r#"<?xml version="1.0"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>"#,
                true,
            ),
            ("bare 404 is absence", 404, "", true),
            (
                "NoSuchBucket is absence",
                404,
                r#"<?xml version="1.0"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist.</Message></Error>"#,
                true,
            ),
            (
                "AccessDenied is not absence",
                403,
                r#"<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"#,
                false,
            ),
            (
                "server error is not absence",
                500,
                r#"<?xml version="1.0"?><Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>"#,
                false,
            ),
        ] {
            let fake = FakeS3::new();
            fake.respond_all(status, body);
            let e = fake
                .client()
                .get_object("bkt", "obj")
                .await
                .err()
                .expect("an error");
            assert_eq!(e.is_not_found(), absent, "{name}: {e}");
        }
    }

    #[test]
    fn urls_by_style() {
        let fake = FakeS3::new();
        let c = fake.client();
        assert_eq!(
            c.url("b", "root/a b", &[("x-id", "CopyObject")])
                .unwrap()
                .as_str(),
            "http://s3.test/b/root/a%20b?x-id=CopyObject"
        );
        let mut cfg = (*c.cfg).clone();
        cfg.path_style = false;
        let v = S3Client::new(cfg);
        assert_eq!(v.url("b", "k", &[]).unwrap().as_str(), "http://b.s3.test/k");
    }
}
