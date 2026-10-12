//! Reading and writing S3 objects: ranged parallel reads, and multipart
//! uploads of [`UPLOAD_PART_SIZE`] parts, [`UPLOAD_CONCURRENCY`] at a time.
//!
//! Unlike GCS no connection pool is needed: the transport keeps a pool of
//! HTTP/1.1 connections, so concurrent ranges and parts already get
//! connections of their own.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::Method;
use serde::Deserialize;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{object_absent, S3Client};
use crate::error::{Error, Result};
use crate::gcs::read::{byte_range, total_from_content_range};
use crate::gcs::write::read_full;
use crate::storage::ranged::{new_ranged_reader, FetchRange, DOWNLOAD_CHUNK_SIZE};
use crate::storage::uploader::{UPLOAD_CONCURRENCY, UPLOAD_PART_SIZE};
use crate::storage::{ObjectReader, StreamObject, UploadBody};

/// How long aborting a failed multipart upload may take; it runs detached
/// from the operation that failed, and an abandoned upload keeps billing.
pub const ABORT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    upload_id: String,
}

impl S3Client {
    pub(crate) async fn get_object_impl(&self, bucket: &str, object: &str) -> Result<ObjectReader> {
        let req = self
            .request(Method::GET, bucket, object, &[])?
            // The first chunk is the size probe: Content-Range reports the
            // whole size, so nothing pays an extra round trip.
            .header("range", byte_range(0, DOWNLOAD_CHUNK_SIZE));
        let head = match self.send(req).await {
            Ok(r) => r,
            Err(e) if object_absent(&e) => {
                return Err(Error::not_found(format!(
                    "object not found in storage backend: Failed to get S3 Bucket:{bucket:?}, Object:{object:?}"
                )))
            }
            Err(e) if e.http_status() == Some(416) => {
                return Ok(Box::new(crate::storage::BytesReader::sized(Bytes::new())));
            }
            Err(e) => return Err(e),
        };
        let content_length = head
            .header_str("content-length")
            .and_then(|v| v.parse::<i64>().ok());
        let Some(size) = total_from_content_range(head.header_str("content-range")) else {
            // No Content-Range: the server ignored the range, so this is the
            // whole object and Content-Length, if set, is its size.
            return Ok(Box::new(StreamObject::new(
                head.into_reader(),
                content_length,
            )));
        };
        let reader = StreamObject::new(head.into_reader(), Some(size));
        if size <= DOWNLOAD_CHUNK_SIZE {
            return Ok(Box::new(reader));
        }
        let me = self.clone();
        let (bucket, object) = (bucket.to_string(), object.to_string());
        let fetch: FetchRange = Arc::new(move |_, off, n| {
            let me = me.clone();
            let (bucket, object) = (bucket.clone(), object.clone());
            Box::pin(async move {
                let req = me
                    .request(Method::GET, &bucket, &object, &[])?
                    .header("range", byte_range(off, n));
                let data = me.send_read(req).await.map_err(|e| {
                    e.context(format!("while opening range {off}+{n} of {object:?}"))
                })?;
                if data.len() as i64 != n {
                    return Err(Error::other(format!(
                        "while reading range {off}+{n} of {object:?}: got {} bytes",
                        data.len()
                    )));
                }
                Ok(data)
            })
        });
        Ok(Box::new(new_ranged_reader(
            &CancellationToken::new(),
            size,
            Box::new(reader),
            fetch,
        )))
    }

    /// Uploads `data` with one PutObject.
    pub async fn put_single(&self, bucket: &str, object: &str, data: Bytes) -> Result<()> {
        let req = self.request(Method::PUT, bucket, object, &[])?.body(data);
        self.send_read(req).await.map(|_| ())
    }

    pub(crate) async fn put_object_impl(
        &self,
        bucket: &str,
        object: &str,
        mut body: UploadBody,
    ) -> Result<()> {
        let part = UPLOAD_PART_SIZE as usize;
        let mut first = BytesMut::with_capacity(part);
        read_full(&mut body.reader, &mut first, part).await?;
        if first.len() < part {
            // Under one part: a single PutObject.
            return self.put_single(bucket, object, first.freeze()).await;
        }
        let created = self
            .send_read(self.request(Method::POST, bucket, object, &[("uploads", "")])?)
            .await?;
        let upload_id = quick_xml::de::from_str::<InitiateMultipartUploadResult>(
            &String::from_utf8_lossy(&created),
        )
        .map_err(|e| Error::other(format!("multipart upload response: {e}")))?
        .upload_id;
        match self
            .upload_parts(bucket, object, &upload_id, first.freeze(), &mut body)
            .await
        {
            Ok(parts) => {
                let res = self
                    .complete_multipart(bucket, object, &upload_id, &parts)
                    .await;
                if let Err(e) = res {
                    return Err(self.abort_multipart(bucket, object, &upload_id, e).await);
                }
                Ok(())
            }
            Err(e) => Err(self.abort_multipart(bucket, object, &upload_id, e).await),
        }
    }

    async fn upload_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        first: Bytes,
        body: &mut UploadBody,
    ) -> Result<Vec<(u32, String)>> {
        let part = UPLOAD_PART_SIZE as usize;
        let slots = Arc::new(Semaphore::new(UPLOAD_CONCURRENCY as usize));
        let mut tasks: JoinSet<Result<(u32, String)>> = JoinSet::new();
        let mut etags = Vec::new();
        let mut next = Some(first);
        let mut number = 1u32;
        let res: Result<()> = async {
            loop {
                let Some(data) = next.take() else { break };
                let permit = loop {
                    tokio::select! {
                        biased;
                        Some(done) = tasks.join_next(), if !tasks.is_empty() => {
                            etags.push(done.map_err(|e| Error::other(format!("part task failed: {e}")))??);
                        }
                        p = slots.clone().acquire_owned() => break p.expect("semaphore is never closed"),
                    }
                };
                let me = self.clone();
                let (b, o, id) = (bucket.to_string(), object.to_string(), upload_id.to_string());
                let n = number;
                tasks.spawn(async move {
                    let _permit = permit;
                    let req = me
                        .request(Method::PUT, &b, &o, &[("partNumber", &n.to_string()), ("uploadId", &id)])?
                        .body(data);
                    let resp = me.send(req).await?;
                    let etag = resp.header_str("etag").unwrap_or_default().to_string();
                    Ok((n, etag))
                });
                number += 1;
                // The producer buffer: the next part is read while the
                // others upload.
                let mut buf = BytesMut::with_capacity(part);
                read_full(&mut body.reader, &mut buf, part).await?;
                if !buf.is_empty() {
                    next = Some(buf.freeze());
                }
            }
            while let Some(done) = tasks.join_next().await {
                etags.push(done.map_err(|e| Error::other(format!("part task failed: {e}")))??);
            }
            Ok(())
        }
        .await;
        tasks.abort_all();
        res?;
        etags.sort();
        Ok(etags)
    }

    pub(crate) async fn complete_multipart(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> Result<()> {
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (n, etag) in parts {
            xml.push_str(&format!(
                "<Part><ETag>{}</ETag><PartNumber>{n}</PartNumber></Part>",
                etag.replace('&', "&amp;").replace('<', "&lt;")
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let req = self
            .request(Method::POST, bucket, object, &[("uploadId", upload_id)])?
            .header("content-type", "application/xml")
            .body(xml);
        let body = self.send_read(req).await?;
        // A completion can fail with a 200 carrying an error document.
        if String::from_utf8_lossy(&body).contains("<Error>") {
            return Err(super::s3_error(500, &body));
        }
        Ok(())
    }

    /// Discards a failed multipart upload's parts and returns `cause`, with
    /// what went wrong discarding them.
    pub(crate) async fn abort_multipart(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        cause: Error,
    ) -> Error {
        let me = self.clone();
        let (b, o, id) = (
            bucket.to_string(),
            object.to_string(),
            upload_id.to_string(),
        );
        // Detached: the failing operation may be why this is unwinding.
        let abort = tokio::spawn(async move {
            let req = me.request(Method::DELETE, &b, &o, &[("uploadId", &id)])?;
            tokio::time::timeout(ABORT_TIMEOUT, me.send_read(req))
                .await
                .map_err(|_| Error::deadline_exceeded())?
                .map(|_| ())
        });
        match abort.await {
            Ok(Ok(())) => cause,
            Ok(Err(e)) => cause.context(format!(
                "(while aborting multipart upload to s3://{bucket}/{object}: {e})"
            )),
            Err(e) => cause.context(format!("(abort task failed: {e})")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{transport_fn, Request, Response};
    use crate::s3::credentials::StaticCredentials;
    use crate::s3::S3Config;
    use crate::storage::uploader::{upload_buffer_memory, Uploader, PARALLEL_WORKER_MEMORY};
    use crate::storage::{object_size, ObjectStorage};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncReadExt;
    use tokio::sync::{mpsc, watch};

    fn client(t: Arc<dyn crate::http::Transport>) -> S3Client {
        S3Client::new(S3Config::new(
            url::Url::parse("http://s3.test").unwrap(),
            "us-east-1",
            Arc::new(StaticCredentials::new("ak", "sk")),
            t,
        ))
    }

    /// Serves `content` as every object, honoring Range unless told not to.
    fn object_client(content: Bytes, ignore_range: bool) -> S3Client {
        client(transport_fn(move |req: Request| {
            let content = content.clone();
            Box::pin(async move {
                let rng = req
                    .header_str("range")
                    .unwrap_or_default()
                    .trim_start_matches("bytes=")
                    .to_string();
                if ignore_range || rng.is_empty() {
                    return Ok(Response::new(200, content.clone())
                        .with_header("content-length", content.len().to_string()));
                }
                let (from, to) = rng.split_once('-').unwrap();
                let off: usize = from.parse().unwrap();
                let end = to.parse::<usize>().unwrap().min(content.len() - 1);
                Ok(Response::new(206, content.slice(off..=end))
                    .with_header(
                        "content-range",
                        format!("bytes {off}-{end}/{}", content.len()),
                    )
                    .with_header("content-length", (end - off + 1).to_string()))
            })
        }))
    }

    #[tokio::test]
    async fn get_object_reports_size() {
        for (name, size, ignore) in [
            ("one range", 10usize, false),
            ("empty object", 0, true),
            ("range ignored", 10, true),
            ("several ranges", DOWNLOAD_CHUNK_SIZE as usize + 1, false),
        ] {
            let content = Bytes::from(vec![b'a'; size]);
            let mut rc = object_client(content.clone(), ignore)
                .get_object("bucket", "object")
                .await
                .unwrap();
            assert_eq!(object_size(&rc), Some(size as i64), "{name}");
            let mut got = Vec::new();
            rc.read_to_end(&mut got).await.unwrap();
            assert!(got == content, "{name}: read {} bytes", got.len());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn uploader_slow_s3_multipart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse-memory");
        {
            // Incompressible data forces several parts; the rest is a hole.
            std::fs::write(&path, crate::storage::testdata::noise(2553, 40 << 20)).unwrap();
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(2 << 30).unwrap();
        }
        for failure in [false, true] {
            let (created_tx, mut created) = mpsc::unbounded_channel::<String>();
            let parts = Arc::new(AtomicUsize::new(0));
            let aborted = Arc::new(AtomicUsize::new(0));
            let (gate, release) = watch::channel(false);
            let (p, a) = (parts.clone(), aborted.clone());
            let t = transport_fn(move |req: Request| {
                let (created_tx, p, a, mut release) =
                    (created_tx.clone(), p.clone(), a.clone(), release.clone());
                Box::pin(async move {
                    let key = req
                        .url
                        .path()
                        .rsplit('/')
                        .next()
                        .unwrap_or_default()
                        .to_string();
                    let q = req.url.query().unwrap_or_default().to_string();
                    if req.method == Method::POST && q.contains("uploads") {
                        let _ = created_tx.send(key.clone());
                        return Ok(Response::new(200, format!("<InitiateMultipartUploadResult><UploadId>{key}</UploadId></InitiateMultipartUploadResult>")));
                    }
                    if req.method == Method::PUT && q.contains("partNumber") {
                        p.fetch_add(1, Ordering::SeqCst);
                        while !*release.borrow() {
                            if release.changed().await.is_err() {
                                break;
                            }
                        }
                        if failure && key == "first" {
                            return Ok(Response::new(
                                500,
                                "<Error><Code>InternalError</Code></Error>",
                            ));
                        }
                        return Ok(Response::new(200, "").with_header("etag", "\"etag\""));
                    }
                    if req.method == Method::POST {
                        return Ok(Response::new(200, "<CompleteMultipartUploadResult/>"));
                    }
                    if req.method == Method::DELETE {
                        a.fetch_add(1, Ordering::SeqCst);
                        return Ok(Response::new(204, ""));
                    }
                    Ok(Response::new(400, "expected a multipart upload"))
                })
            });
            let c: Arc<dyn ObjectStorage> = Arc::new(client(t));
            let u = Uploader::new(
                c.clone(),
                upload_buffer_memory(c.backend()) + PARALLEL_WORKER_MEMORY,
            )
            .unwrap();
            let send = |name: &'static str| {
                let u = u.clone();
                let path = path.clone();
                tokio::spawn(async move {
                    let f = std::fs::File::open(&path).unwrap();
                    u.send_file(&format!("gs://bucket/{name}"), &f).await
                })
            };
            let first = send("first");
            assert_eq!(created.recv().await.unwrap(), "first");
            for _ in 0..200 {
                if parts.load(Ordering::SeqCst) >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(parts.load(Ordering::SeqCst), 2, "active parts");
            let next = send("next");
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert!(
                created.try_recv().is_err(),
                "next started while first held the buffers"
            );
            gate.send(true).unwrap();
            assert_eq!(first.await.unwrap().is_err(), failure);
            next.await.unwrap().unwrap();
            assert_eq!(created.recv().await.unwrap(), "next");
            if failure {
                assert_eq!(
                    aborted.load(Ordering::SeqCst),
                    1,
                    "failed multipart upload was not aborted"
                );
            }
        }
    }

    #[test]
    #[ignore = "needs S3_BENCH_ENDPOINT, S3_BENCH_BUCKET and S3_BENCH_FILE pointing at a real S3 server"]
    fn s3_upload_against_real_server() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let endpoint = std::env::var("S3_BENCH_ENDPOINT").expect("S3_BENCH_ENDPOINT");
            let bucket = std::env::var("S3_BENCH_BUCKET").expect("S3_BENCH_BUCKET");
            let src =
                std::path::PathBuf::from(std::env::var("S3_BENCH_FILE").expect("S3_BENCH_FILE"));
            let creds = StaticCredentials::new(
                &std::env::var("S3_BENCH_KEY").unwrap_or_default(),
                &std::env::var("S3_BENCH_SECRET").unwrap_or_default(),
            );
            let c: Arc<dyn ObjectStorage> = Arc::new(S3Client::new(S3Config {
                max_attempts: 3,
                ..S3Config::new(
                    url::Url::parse(&endpoint).unwrap(),
                    "us-east-1",
                    Arc::new(creds),
                    Arc::new(crate::http::ReqwestTransport::new(None).unwrap()),
                )
            }));
            let url = format!(
                "gs://{bucket}/bench/memory-ranges-{}.zstd",
                rand::random::<u64>()
            );
            let start = std::time::Instant::now();
            crate::storage::send_path_with_zstd(c.clone(), &url, &src)
                .await
                .unwrap();
            eprintln!("uploaded in {:?}", start.elapsed());
            let dir = tempfile::tempdir().unwrap();
            let dst = dir.path().join("restored");
            crate::storage::fetch_to_path_with_zstd(c.as_ref(), &url, &dst)
                .await
                .unwrap();
            assert!(
                std::fs::read(&dst).unwrap() == std::fs::read(&src).unwrap(),
                "round trip mismatch"
            );
        });
    }
}
