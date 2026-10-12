//! An in-process S3 for tests: canned responses for the control-plane
//! calls, a recording of every request, and an override hook.

use std::sync::Arc;

use http::Method;
use parking_lot::Mutex;

use super::credentials::StaticCredentials;
use super::{S3Client, S3Config};
use crate::http::{transport_fn, Request, Response};

/// One request, reduced to what tests assert on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Request {
    pub method: Method,
    pub path: String,
    pub query: String,
    pub copy_source: String,
    pub copy_range: String,
}

type Handle = Arc<dyn Fn(&Request) -> Option<Response> + Send + Sync>;

#[derive(Clone, Default)]
pub struct FakeS3 {
    requests: Arc<Mutex<Vec<S3Request>>>,
    handle: Arc<Mutex<Option<Handle>>>,
}

pub fn xml(body: &str) -> String {
    format!(r#"<?xml version="1.0" encoding="UTF-8"?>{body}"#)
}

impl FakeS3 {
    pub fn new() -> Self {
        Self::default()
    }

    /// Answers before the default handling when `h` returns a response.
    pub fn set_handle(&self, h: impl Fn(&Request) -> Option<Response> + Send + Sync + 'static) {
        *self.handle.lock() = Some(Arc::new(h));
    }

    /// Answers every request with `status` and `body`.
    pub fn respond_all(&self, status: u16, body: &'static str) {
        self.set_handle(move |_| {
            Some(Response::new(status, body).with_header("content-type", "application/xml"))
        });
    }

    pub fn requests(&self) -> Vec<S3Request> {
        self.requests.lock().clone()
    }

    pub fn client(&self) -> S3Client {
        let me = self.clone();
        let t = transport_fn(move |req: Request| {
            let me = me.clone();
            Box::pin(async move { Ok(me.serve(&req)) })
        });
        S3Client::new(S3Config::new(
            url::Url::parse("http://s3.test").unwrap(),
            "us-east-1",
            Arc::new(StaticCredentials::new("ak", "sk")),
            t,
        ))
    }

    fn serve(&self, req: &Request) -> Response {
        let path = percent_encoding::percent_decode_str(req.url.path())
            .decode_utf8_lossy()
            .into_owned();
        self.requests.lock().push(S3Request {
            method: req.method.clone(),
            path,
            query: req.url.query().unwrap_or_default().to_string(),
            copy_source: req
                .header_str("x-amz-copy-source")
                .unwrap_or_default()
                .to_string(),
            copy_range: req
                .header_str("x-amz-copy-source-range")
                .unwrap_or_default()
                .to_string(),
        });
        let h = self.handle.lock().clone();
        if let Some(r) = h.and_then(|h| h(req)) {
            return r;
        }
        let has = |k: &str| req.url.query_pairs().any(|(q, _)| q == k);
        match req.method {
            Method::HEAD => Response::new(200, "").with_header("content-length", "1024"),
            Method::POST if has("uploads") => Response::new(
                200,
                xml("<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"),
            ),
            Method::POST if has("uploadId") => Response::new(
                200,
                xml(r#"<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>"#),
            ),
            Method::PUT if has("partNumber") => {
                let n = req.query("partNumber").unwrap_or_default();
                Response::new(200, xml(&format!(r#"<CopyPartResult><ETag>"part-{n}"</ETag></CopyPartResult>"#)))
            }
            Method::PUT => Response::new(200, xml(r#"<CopyObjectResult><ETag>"copied"</ETag></CopyObjectResult>"#)),
            Method::DELETE => Response::new(204, ""),
            _ => Response::new(501, ""),
        }
    }
}
