//! Reading objects: the first range doubles as the size probe, and an
//! object of more than one chunk is fetched as parallel ranges.

use std::sync::Arc;

use bytes::Bytes;
use http::Method;
use tokio_util::sync::CancellationToken;

use super::{request, GcsClient, GcsConn};
use crate::error::{Error, Result};
use crate::retry::retry;
use crate::storage::ranged::{new_ranged_reader, FetchRange, DOWNLOAD_CHUNK_SIZE};
use crate::storage::{ObjectReader, StreamObject};

/// The inclusive HTTP range header for `n` bytes at `off`. An off-by-one
/// here drops or repeats a byte at every chunk boundary.
pub fn byte_range(off: i64, n: i64) -> String {
    format!("bytes={off}-{}", off + n - 1)
}

/// The object's full size from a `Content-Range` header such as
/// `bytes 0-16777215/450000000`.
pub fn total_from_content_range(cr: Option<&str>) -> Option<i64> {
    let (_, total) = cr?.split_once('/')?;
    total.trim().parse().ok()
}

impl GcsClient {
    pub(crate) async fn get_object_impl(&self, bucket: &str, object: &str) -> Result<ObjectReader> {
        // The first chunk is the size probe: a range read reports the whole
        // size, so nothing pays an extra round trip. It goes out on a pooled
        // connection since for most snapshots it is the whole download.
        let conn = self.next_client().clone();
        let url = conn.media_url(bucket, object)?;
        let head = retry(self.retry(), || {
            let conn = conn.clone();
            let url = url.clone();
            async move {
                match conn
                    .send(
                        request(Method::GET, url)
                            .header("range", byte_range(0, DOWNLOAD_CHUNK_SIZE)),
                    )
                    .await
                {
                    Err(e) if e.http_status() == Some(416) => Ok(None),
                    Err(e) => Err(e),
                    Ok(r) => Ok(Some(r)),
                }
            }
        })
        .await;
        let head = match head {
            Ok(Some(r)) => r,
            // Range past the end of an empty object.
            Ok(None) => return Ok(Box::new(crate::storage::BytesReader::sized(Bytes::new()))),
            Err(e) if e.http_status() == Some(404) => {
                return Err(Error::not_found(format!(
                    "object not found in storage backend: Bucket:{bucket:?}, Object:{object:?}"
                )))
            }
            Err(e) => return Err(e),
        };
        let size = match total_from_content_range(head.header_str("content-range")) {
            Some(s) => s,
            None => head
                .header_str("x-goog-stored-content-length")
                .or_else(|| head.header_str("content-length"))
                .and_then(|v| v.parse().ok())
                .unwrap_or(-1),
        };
        let reader = StreamObject::new(head.into_reader(), Some(size));
        if size <= DOWNLOAD_CHUNK_SIZE {
            return Ok(Box::new(reader));
        }
        let me = self.clone();
        let (bucket, object) = (bucket.to_string(), object.to_string());
        let fetch: FetchRange = Arc::new(move |i, off, n| {
            let conn = me.upload_client(i).clone();
            let policy = me.retry();
            let (bucket, object) = (bucket.clone(), object.clone());
            Box::pin(async move { fetch_range(&conn, policy, &bucket, &object, off, n).await })
        });
        Ok(Box::new(new_ranged_reader(
            &CancellationToken::new(),
            size,
            Box::new(reader),
            fetch,
        )))
    }
}

async fn fetch_range(
    conn: &GcsConn,
    policy: crate::retry::RetryPolicy,
    bucket: &str,
    object: &str,
    off: i64,
    n: i64,
) -> Result<Bytes> {
    let url = conn.media_url(bucket, object)?;
    let data = retry(policy, || {
        let url = url.clone();
        async move {
            conn.send_read(request(Method::GET, url).header("range", byte_range(off, n)))
                .await
        }
    })
    .await
    .map_err(|e| e.context(format!("while opening range {off}+{n} of {object:?}")))?;
    if data.len() as i64 != n {
        return Err(Error::other(format!(
            "while reading range {off}+{n} of {object:?}: got {} bytes",
            data.len()
        )));
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcp::Anonymous;
    use crate::http::{transport_fn, Request, Response, Transport};
    use crate::storage::{object_size, ObjectStorage};
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use tokio::io::AsyncReadExt;
    use url::Url;

    fn conn(t: Arc<dyn Transport>) -> GcsConn {
        GcsConn::new(
            t,
            Arc::new(Anonymous),
            Url::parse("http://gcs.test/").unwrap(),
        )
    }

    #[test]
    fn byte_ranges_are_inclusive() {
        assert_eq!(byte_range(0, DOWNLOAD_CHUNK_SIZE), "bytes=0-16777215");
        assert_eq!(
            byte_range(DOWNLOAD_CHUNK_SIZE, 10),
            "bytes=16777216-16777225"
        );
    }

    #[test]
    fn total_from_content_range_cases() {
        for (name, input, want) in [
            ("absent", None, None),
            ("unparsable", Some("bytes 0-15/*"), None),
            ("no total", Some("bytes 0-15"), None),
            ("full", Some("bytes 0-16777215/450000000"), Some(450000000)),
            ("short object", Some("bytes 0-9/10"), Some(10)),
        ] {
            assert_eq!(total_from_content_range(input), want, "{name}");
        }
    }

    #[tokio::test]
    async fn get_object_reports_size() {
        let content = b"micro-vm kernel bytes";
        let t = transport_fn(move |_req: Request| {
            Box::pin(async move {
                Ok(Response::new(206, &content[..]).with_header(
                    "content-range",
                    format!("bytes 0-{}/{}", content.len() - 1, content.len()),
                ))
            })
        });
        let g = GcsClient::from_parts(conn(t), vec![]);
        let rc = g.get_object("bucket", "object").await.unwrap();
        assert_eq!(object_size(&rc), Some(content.len() as i64));
    }

    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn close_cancels_initial_request() {
        let dropped = Arc::new(AtomicBool::new(false));
        let d = dropped.clone();
        let t = transport_fn(move |_req: Request| {
            let flag = DropFlag(d.clone());
            Box::pin(async move {
                let body = futures::stream::once(async move {
                    let _hold = flag;
                    std::future::pending::<std::io::Result<Bytes>>().await
                });
                Ok(Response::streaming(206, Box::pin(body)).with_header(
                    "content-range",
                    format!(
                        "bytes 0-{}/{}",
                        DOWNLOAD_CHUNK_SIZE - 1,
                        DOWNLOAD_CHUNK_SIZE + 1
                    ),
                ))
            })
        });
        let g = GcsClient::from_parts(conn(t), vec![]);
        let mut r = g.get_object("snapshots", "object").await.unwrap();
        // Let the first range start reading.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        r.close().unwrap();
        for _ in 0..100 {
            if dropped.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("close did not cancel the initial GCS request");
    }

    fn recording(hits: Arc<AtomicU32>) -> Arc<dyn Transport> {
        transport_fn(move |req: Request| {
            hits.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if req.method == Method::GET {
                    // A whole small object in one range.
                    return Ok(Response::new(206, &b"payload"[..])
                        .with_header("content-range", "bytes 0-6/7"));
                }
                Ok(Response::new(
                    200,
                    r#"{"name":"object","bucket":"snapshots"}"#,
                ))
            })
        })
    }

    #[tokio::test]
    async fn single_request_transfers_spread_over_the_pool() {
        let wrapped = Arc::new(AtomicU32::new(0));
        let hits: Vec<Arc<AtomicU32>> = (0..3).map(|_| Arc::new(AtomicU32::new(0))).collect();
        let g = GcsClient::from_parts(
            conn(recording(wrapped.clone())),
            hits.iter().map(|h| conn(recording(h.clone()))).collect(),
        );
        for i in 0..2 * hits.len() {
            if i % 2 == 0 {
                let mut rc = g.get_object("snapshots", "object").await.unwrap();
                let mut sink = Vec::new();
                rc.read_to_end(&mut sink).await.unwrap();
            } else {
                g.put_object(
                    "snapshots",
                    "object",
                    crate::storage::UploadBody::from_bytes(&b"payload"[..]),
                )
                .await
                .unwrap();
            }
        }
        assert_eq!(
            wrapped.load(Ordering::SeqCst),
            0,
            "small transfers bypassed the pool"
        );
        for (i, h) in hits.iter().enumerate() {
            assert_eq!(h.load(Ordering::SeqCst), 2, "pooled client {i}");
        }
    }

    #[tokio::test]
    async fn missing_object_is_not_found() {
        let t = transport_fn(|_req: Request| {
            Box::pin(async { Ok(Response::new(404, "<Error><Code>NoSuchKey</Code></Error>")) })
        });
        let g = GcsClient::from_parts(conn(t), vec![]);
        let e = g.get_object("b", "o").await.err().expect("error");
        assert!(e.is_not_found(), "{e}");
    }
}
