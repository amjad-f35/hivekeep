//! Writing objects: one request below the composite threshold, otherwise
//! concurrent parts composed server-side.
//!
//! One stream to GCS tops out near 100 MiB/s however it is chunked; several
//! do not (300 MiB measured 82-107 MiB/s on one, 233-257 on four). So a large
//! object is cut into parts, uploaded concurrently and composed. The parts
//! are byte ranges of the same stream, so the result is byte-identical to a
//! single upload and the download path is unaffected.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream::{self, StreamExt};
use http::Method;
use serde_json::json;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::{request, GcsClient, GcsConn};
use crate::error::{Error, Result};
use crate::retry::{retry, RetryPolicy};
use crate::storage::uploader::{
    single_upload_chunk_size, single_upload_lookahead, UPLOAD_CONCURRENCY, UPLOAD_PART_SIZE,
};
use crate::storage::UploadBody;

/// GCS's limit on sources per compose call; more parts are folded in rounds.
pub const MAX_COMPOSE_SOURCES: usize = 32;

/// An id that keeps concurrent or retried uploads of one object from
/// colliding on part names, and marks a crash's leftovers.
pub fn new_run_id() -> String {
    hex::encode(rand::random::<[u8; 8]>())
}

/// Reads until `buf` holds `want` bytes or the reader ends.
pub(crate) async fn read_full(
    r: &mut (dyn AsyncRead + Send + Unpin),
    buf: &mut BytesMut,
    want: usize,
) -> std::io::Result<()> {
    while buf.len() < want {
        let before = buf.len();
        let n = r.take((want - before) as u64).read_buf(buf).await?;
        if n == 0 {
            break;
        }
    }
    Ok(())
}

impl GcsConn {
    /// Uploads `data` in one request.
    pub async fn upload_single(
        &self,
        policy: RetryPolicy,
        bucket: &str,
        object: &str,
        data: Bytes,
    ) -> Result<()> {
        let url = self.upload_url(bucket, "media", object)?;
        retry(policy, || {
            let req = request(Method::POST, url.clone())
                .header("content-type", "application/octet-stream")
                .body(data.clone());
            async move { self.send_read(req).await.map(|_| ()) }
        })
        .await
    }

    async fn start_resumable(
        &self,
        policy: RetryPolicy,
        bucket: &str,
        object: &str,
    ) -> Result<url::Url> {
        let url = self.upload_url(bucket, "resumable", object)?;
        let body = Bytes::from(json!({ "name": object }).to_string());
        let resp = retry(policy, || {
            let req = request(Method::POST, url.clone())
                .header("content-type", "application/json")
                .body(body.clone());
            async move { self.send(req).await }
        })
        .await?;
        let loc = resp
            .header_str("location")
            .ok_or_else(|| Error::other("resumable upload answered without a session"))?
            .to_string();
        url::Url::parse(&loc).map_err(|e| Error::other(format!("resumable session URL: {e}")))
    }

    async fn put_chunk(
        &self,
        policy: RetryPolicy,
        session: &url::Url,
        data: Bytes,
        offset: i64,
        total: Option<i64>,
    ) -> Result<()> {
        let range = match (data.is_empty(), total) {
            (true, Some(t)) => format!("bytes */{t}"),
            (false, Some(t)) => format!("bytes {offset}-{}/{t}", offset + data.len() as i64 - 1),
            (_, None) => format!("bytes {offset}-{}/*", offset + data.len() as i64 - 1),
        };
        retry(policy, || {
            let req = request(Method::PUT, session.clone())
                .header("content-range", &range)
                .body(data.clone());
            async move { self.send_read(req).await.map(|_| ()) }
        })
        .await
    }

    /// Uploads a stream, buffering `chunk` bytes at a time: a stream shorter
    /// than one chunk goes in one request, a longer one as a resumable
    /// session of whole chunks.
    pub async fn upload_stream(
        &self,
        policy: RetryPolicy,
        bucket: &str,
        object: &str,
        r: &mut (dyn AsyncRead + Send + Unpin),
        chunk: usize,
    ) -> Result<()> {
        let mut buf = BytesMut::with_capacity(chunk);
        read_full(r, &mut buf, chunk).await?;
        if buf.len() < chunk {
            return self
                .upload_single(policy, bucket, object, buf.freeze())
                .await;
        }
        let session = self.start_resumable(policy, bucket, object).await?;
        let mut offset = 0i64;
        loop {
            let full = buf.split().freeze();
            let n = full.len() as i64;
            buf.reserve(chunk);
            if n < chunk as i64 {
                return self
                    .put_chunk(policy, &session, full, offset, Some(offset + n))
                    .await;
            }
            self.put_chunk(policy, &session, full, offset, None).await?;
            offset += n;
            read_full(r, &mut buf, chunk).await?;
            if buf.is_empty() {
                return self
                    .put_chunk(policy, &session, Bytes::new(), offset, Some(offset))
                    .await;
            }
        }
    }

    /// Composes `sources` into `dest`.
    pub async fn compose(
        &self,
        policy: RetryPolicy,
        bucket: &str,
        dest: &str,
        sources: &[String],
    ) -> Result<()> {
        let url = self.object_url(bucket, dest, "/compose")?;
        let body = Bytes::from(
            json!({
                "sourceObjects": sources.iter().map(|s| json!({ "name": s })).collect::<Vec<_>>(),
                "destination": { "name": dest },
            })
            .to_string(),
        );
        retry(policy, || {
            let req = request(Method::POST, url.clone())
                .header("content-type", "application/json")
                .body(body.clone());
            async move { self.send_read(req).await.map(|_| ()) }
        })
        .await
    }

    /// Copies an object server-side through the rewrite API, following the
    /// rewrite token until the copy completes. The bytes move inside GCS
    /// whatever the size.
    pub async fn rewrite(
        &self,
        policy: RetryPolicy,
        src_bucket: &str,
        src: &str,
        dst_bucket: &str,
        dst: &str,
    ) -> Result<()> {
        #[derive(serde::Deserialize)]
        struct RewriteResponse {
            #[serde(default)]
            done: bool,
            #[serde(default, rename = "rewriteToken")]
            rewrite_token: String,
        }
        let base = self.object_url(
            src_bucket,
            src,
            &format!(
                "/rewriteTo/b/{}/o/{}",
                crate::http::encode_component(dst_bucket),
                crate::http::encode_component(dst)
            ),
        )?;
        let mut token = String::new();
        loop {
            let mut url = base.clone();
            if !token.is_empty() {
                url.query_pairs_mut().append_pair("rewriteToken", &token);
            }
            let body = retry(policy, || {
                let req = request(Method::POST, url.clone())
                    .header("content-type", "application/json")
                    .body("{}");
                async move { self.send_read(req).await }
            })
            .await?;
            let r: RewriteResponse = serde_json::from_slice(&body)
                .map_err(|e| Error::other(format!("rewrite response: {e}")))?;
            if r.done {
                return Ok(());
            }
            if r.rewrite_token.is_empty() {
                return Err(Error::other("rewrite neither finished nor continued"));
            }
            token = r.rewrite_token;
        }
    }

    /// Deletes an object. A 404 comes back as a not-found error.
    pub async fn delete(&self, policy: RetryPolicy, bucket: &str, object: &str) -> Result<()> {
        let url = self.object_url(bucket, object, "")?;
        retry(policy, || {
            let req = request(Method::DELETE, url.clone());
            async move { self.send_read(req).await.map(|_| ()) }
        })
        .await
        .map_err(|e| {
            if e.http_status() == Some(404) {
                Error::not_found(format!("storage: object doesn't exist: {object}"))
            } else {
                e
            }
        })
    }
}

/// Scratch objects removed when an upload ends, however it ends.
pub(crate) struct Scratch {
    client: GcsClient,
    bucket: String,
    names: Vec<String>,
}

impl Scratch {
    pub(crate) fn new(client: &GcsClient, bucket: &str) -> Self {
        Scratch {
            client: client.clone(),
            bucket: bucket.to_string(),
            names: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, name: String) {
        self.names.push(name);
    }

    /// Removes the scratch objects now.
    pub(crate) async fn finish(mut self) {
        let names = std::mem::take(&mut self.names);
        self.client.remove_scratch(&self.bucket, &names).await;
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if self.names.is_empty() {
            return;
        }
        let names = std::mem::take(&mut self.names);
        let (client, bucket) = (self.client.clone(), self.bucket.clone());
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            h.spawn(async move { client.remove_scratch(&bucket, &names).await });
        }
    }
}

impl GcsClient {
    pub(crate) async fn put_object_impl(
        &self,
        bucket: &str,
        object: &str,
        mut body: UploadBody,
    ) -> Result<()> {
        let lookahead = single_upload_lookahead(body.size_bound) as usize;
        let mut head = BytesMut::with_capacity(lookahead);
        read_full(&mut body.reader, &mut head, lookahead)
            .await
            .map_err(|e| Error::from(e).context("while reading object body"))?;
        if head.len() < lookahead {
            // The whole object is in hand and fits in one request.
            return self.put_single(bucket, object, head.freeze()).await;
        }
        self.put_composite(bucket, object, head.freeze(), &mut body.reader)
            .await
    }

    /// Writes `body` in one request on a pooled connection, so the small
    /// objects a node uploads at once do not queue on one connection.
    pub(crate) async fn put_single(&self, bucket: &str, object: &str, body: Bytes) -> Result<()> {
        let chunk = single_upload_chunk_size(body.len() as i64) as usize;
        let conn = self.next_client();
        let mut r: &[u8] = &body;
        conn.upload_stream(self.retry(), bucket, object, &mut r, chunk)
            .await
            .map_err(|e| e.context("while putting GCS object"))
    }

    /// Uploads `head` then `rest` as concurrent parts of
    /// [`UPLOAD_PART_SIZE`], at most [`UPLOAD_CONCURRENCY`] buffered at once,
    /// then composes them.
    pub(crate) async fn put_composite(
        &self,
        bucket: &str,
        object: &str,
        head: Bytes,
        rest: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<()> {
        let run_id = new_run_id();
        let mut scratch = Scratch::new(self, bucket);
        let mut parts = Vec::new();
        let buffers = Arc::new(Semaphore::new(UPLOAD_CONCURRENCY as usize));
        let mut tasks: JoinSet<Result<()>> = JoinSet::new();
        let mut src = std::io::Cursor::new(head).chain(rest);
        let mut i = 0usize;
        let result: Result<()> = async {
            loop {
                let permit = loop {
                    tokio::select! {
                        biased;
                        Some(done) = tasks.join_next(), if !tasks.is_empty() => {
                            done.map_err(|e| Error::other(format!("part upload task failed: {e}")))??;
                        }
                        p = buffers.clone().acquire_owned() => break p.expect("semaphore is never closed"),
                    }
                };
                let mut buf = BytesMut::with_capacity(UPLOAD_PART_SIZE as usize);
                read_full(&mut src, &mut buf, UPLOAD_PART_SIZE as usize)
                    .await
                    .map_err(|e| Error::from(e).context(format!("while reading upload part {i}")))?;
                let n = buf.len();
                if n > 0 {
                    let name = format!("{object}.part-{run_id}-{i:04}");
                    scratch.push(name.clone());
                    parts.push(name.clone());
                    let conn = self.upload_client(i).clone();
                    let policy = self.retry();
                    let bucket = bucket.to_string();
                    let data = buf.freeze();
                    tasks.spawn(async move {
                        let _permit = permit;
                        conn.upload_single(policy, &bucket, &name, data).await
                    });
                }
                if n < UPLOAD_PART_SIZE as usize {
                    break;
                }
                i += 1;
            }
            while let Some(done) = tasks.join_next().await {
                done.map_err(|e| Error::other(format!("part upload task failed: {e}")))?
                    .map_err(|e| e.context(format!("while uploading parts of {object:?}")))?;
            }
            self.compose_all(bucket, object, &parts, &run_id).await
        }
        .await;
        tasks.abort_all();
        scratch.finish().await;
        result
    }

    /// Composes `parts` into `object`, folding in rounds when there are more
    /// than GCS accepts at once. Intermediates are removed on return; the
    /// caller removes the parts.
    pub(crate) async fn compose_all(
        &self,
        bucket: &str,
        object: &str,
        parts: &[String],
        run_id: &str,
    ) -> Result<()> {
        if parts.is_empty() {
            return Err(Error::other(format!("no parts to compose into {object:?}")));
        }
        let mut mids = Scratch::new(self, bucket);
        let conn = self.client();
        let policy = self.retry();
        let res: Result<()> = async {
            let mut parts = parts.to_vec();
            let mut round = 0;
            while parts.len() > MAX_COMPOSE_SOURCES {
                let mut next = Vec::new();
                for (gi, group) in parts.chunks(MAX_COMPOSE_SOURCES).enumerate() {
                    if group.len() == 1 {
                        next.push(group[0].clone());
                        continue;
                    }
                    let mid = format!(
                        "{object}.compose-{run_id}-{round}-{:04}",
                        gi * MAX_COMPOSE_SOURCES
                    );
                    // Recorded first: the call can fail after GCS committed it.
                    mids.push(mid.clone());
                    conn.compose(policy, bucket, &mid, group)
                        .await
                        .map_err(|e| e.context(format!("while composing parts of {object:?}")))?;
                    next.push(mid);
                }
                parts = next;
                round += 1;
            }
            if parts.len() == 1 {
                // Compose needs two sources; a single part is copied.
                return conn
                    .rewrite(policy, bucket, &parts[0], bucket, object)
                    .await
                    .map_err(|e| e.context(format!("while copying single part into {object:?}")));
            }
            conn.compose(policy, bucket, object, &parts)
                .await
                .map_err(|e| e.context(format!("while composing {object:?}")))
        }
        .await;
        mids.finish().await;
        res
    }

    /// Deletes scratch objects, [`super::CLEANUP_CONCURRENCY`] at a time and
    /// all within the cleanup timeout. Best effort: what cannot be deleted is
    /// left and logged, and never fails the upload.
    pub(crate) async fn remove_scratch(&self, bucket: &str, names: &[String]) {
        if names.is_empty() {
            return;
        }
        let policy = self.retry();
        let conn = self.client().clone();
        let work = stream::iter(names.iter().cloned())
            .map(|name| {
                let conn = conn.clone();
                async move {
                    match conn.delete(policy, bucket, &name).await {
                        Ok(()) => None,
                        Err(e) if e.is_not_found() => None,
                        Err(e) => {
                            Some(e.context(format!("while removing scratch object {name:?}")))
                        }
                    }
                }
            })
            .buffer_unordered(super::CLEANUP_CONCURRENCY)
            .filter_map(|r| async move { r })
            .collect::<Vec<Error>>();
        let (left, first) = match tokio::time::timeout(self.inner.cleanup_timeout, work).await {
            Ok(errs) => (errs.len(), errs.into_iter().next().map(|e| e.to_string())),
            Err(_) => (names.len(), Some("cleanup timed out".to_string())),
        };
        if left > 0 {
            tracing::warn!(count = left, err = ?first, "Leaving scratch objects behind");
        }
    }

    /// The cleanup bound.
    pub fn cleanup_timeout(&self) -> Duration {
        self.inner.cleanup_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcs::fake::FakeGcs;
    use crate::retry::Backoff;
    use crate::storage::ObjectStorage;
    use sha2::{Digest, Sha256};

    fn fast(g: GcsClient, cleanup: Duration) -> GcsClient {
        g.with_timings(
            Backoff::new(Duration::from_millis(1), Duration::from_millis(1)),
            cleanup,
        )
    }

    /// Deterministic incompressible data, position-sensitive so a dropped or
    /// reordered part shows up.
    fn body(n: usize) -> Vec<u8> {
        crate::storage::testdata::noise(42, n)
    }

    #[tokio::test]
    async fn put_object_retries_transient_429() {
        for fail in [1, 6] {
            let fake = FakeGcs::new();
            fake.fail("upload", fail);
            let g = fast(fake.client(), Duration::from_secs(1));
            tokio::time::timeout(
                Duration::from_secs(20),
                g.put_object(
                    "snapshots",
                    "snap/pages.img.zstd",
                    UploadBody::from_bytes(&b"payload"[..]),
                ),
            )
            .await
            .expect("bounded")
            .unwrap_or_else(|e| panic!("after {fail} 429s: {e}"));
            assert_eq!(fake.attempts("upload"), fail + 1);
        }
    }

    #[tokio::test]
    async fn put_object_retries_until_deadline() {
        let fake = FakeGcs::new();
        fake.fail("upload", usize::MAX);
        let g = fake.client();
        let r = tokio::time::timeout(
            Duration::from_secs(1),
            g.put_object(
                "snapshots",
                "snap/pages.img.zstd",
                UploadBody::from_bytes(&b"payload"[..]),
            ),
        )
        .await;
        assert!(
            r.is_err(),
            "put_object = {r:?} before its deadline, want it to retry until then"
        );
    }

    #[tokio::test]
    async fn compose_retries_past_5_attempts() {
        let fake = FakeGcs::new();
        fake.fail("compose", 6);
        fake.insert("snapshots", "snap/part-0", b"a".to_vec());
        fake.insert("snapshots", "snap/part-1", b"b".to_vec());
        let g = fast(fake.client(), Duration::from_secs(1));
        let parts = vec!["snap/part-0".to_string(), "snap/part-1".to_string()];
        tokio::time::timeout(
            Duration::from_secs(20),
            g.compose_all("snapshots", "snap/pages.img.zstd", &parts, "run"),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(fake.attempts("compose"), 7);
    }

    #[tokio::test]
    async fn remove_scratch_stops_at_cleanup_timeout() {
        let fake = FakeGcs::new();
        fake.fail("delete", usize::MAX);
        let g = fast(fake.client(), Duration::from_millis(200));
        let start = std::time::Instant::now();
        tokio::time::timeout(
            Duration::from_secs(10),
            g.remove_scratch("snapshots", &["snap/part-0".into(), "snap/part-1".into()]),
        )
        .await
        .expect("remove_scratch still running after 10s");
        assert!(
            start.elapsed() >= Duration::from_millis(200),
            "returned before the cleanup timeout"
        );
    }

    #[tokio::test]
    async fn put_composite_succeeds_when_cleanup_fails() {
        let fake = FakeGcs::new();
        fake.fail("delete", usize::MAX);
        let g = fast(fake.client(), Duration::from_millis(200));
        let mut rest: &[u8] = b"";
        tokio::time::timeout(
            Duration::from_secs(20),
            g.put_composite(
                "snapshots",
                "snap/pages.img.zstd",
                Bytes::from_static(b"payload"),
                &mut rest,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(fake.attempts("copy") > 0 && fake.attempts("delete") > 0);
    }

    #[tokio::test]
    async fn remove_scratch_runs_in_parallel() {
        let fake = FakeGcs::new();
        fake.set_delete_delay(Duration::from_millis(20));
        let g = fake.client();
        let names: Vec<String> = (0..4 * super::super::CLEANUP_CONCURRENCY)
            .map(|i| format!("snap/part-{i:02}"))
            .collect();
        g.remove_scratch("snapshots", &names).await;
        assert_eq!(fake.attempts("delete"), names.len());
        let peak = fake.peak_deletes();
        assert!(
            (2..=super::super::CLEANUP_CONCURRENCY).contains(&peak),
            "ran {peak} deletes at once"
        );
    }

    #[tokio::test]
    async fn put_object_composite_round_trip() {
        let fake = FakeGcs::new();
        let g = fake.client();
        let composite_min = crate::storage::uploader::UPLOAD_COMPOSITE_MIN as usize;
        let part = UPLOAD_PART_SIZE as usize;
        for (name, size) in [
            ("single request, under the threshold", 3 << 20),
            ("exactly the threshold", composite_min),
            ("one part past the threshold", composite_min + 1),
            (
                "several parts, last one partial",
                composite_min + 2 * part + 12345,
            ),
        ] {
            let want = body(size);
            let object = format!("snap/{}", name.replace(' ', "-"));
            g.put_object(
                "bucket",
                &object,
                UploadBody::new(std::io::Cursor::new(want.clone())),
            )
            .await
            .unwrap();
            let got = fake.get("bucket", &object).expect("object written");
            assert_eq!(got.len(), want.len(), "{name}");
            assert_eq!(
                Sha256::digest(&got),
                Sha256::digest(&want),
                "{name}: content differs"
            );
        }
        // Parts are scratch and must not outlive the upload.
        for k in fake.keys() {
            assert!(
                !k.contains(".part-") && !k.contains(".compose-"),
                "left scratch object {k}"
            );
        }
    }

    #[tokio::test]
    async fn compose_all_removes_intermediates() {
        const PART: usize = 1 << 10;
        let want = body((MAX_COMPOSE_SOURCES + 8) * PART);
        let fake = FakeGcs::new();
        let mut parts = Vec::new();
        for (i, c) in want.chunks(PART).enumerate() {
            let name = format!("fold/obj.part-run-{i:04}");
            fake.insert("test", &name, c.to_vec());
            parts.push(name);
        }
        fake.client()
            .compose_all("test", "fold/obj", &parts, "run")
            .await
            .unwrap();
        assert!(
            fake.get("test", "fold/obj").unwrap() == want,
            "composed bytes differ"
        );
        for k in fake.keys() {
            assert!(!k.contains(".compose-"), "left intermediate {k}");
        }
    }

    // An object whose
    // size is a multiple of the unit still takes one request.
    #[tokio::test]
    async fn exact_multiple_of_the_unit_is_one_request() {
        let fake = FakeGcs::new();
        let g = fake.client();
        let unit = crate::storage::uploader::MIN_UPLOAD_CHUNK_SIZE as usize;
        g.put_object(
            "bucket",
            "exact-256k",
            UploadBody::from_bytes(vec![0xab; unit]),
        )
        .await
        .unwrap();
        assert_eq!(fake.attempts("upload"), 1);
    }
}
