//! The range-parallel sparse upload: each part reads its own extents,
//! compresses them and streams into its own object over its own connection,
//! so every stream starts at once, and the parts are composed back into one
//! object.
//!
//! Piping one compressed stream into a part splitter serializes the fan-out
//! behind a single compressor: a 541 MB actor took 1.78 s that way on a
//! cloud worker node, against 87 MB/s per stream times eight available. The
//! parts are consecutive ranges of the same sparse-extent stream (the first
//! carries the header, the last the sentinel), so the composed object is
//! what the single-stream writer would have produced.

use std::fs::File;
use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::try_join_all;

use super::write::{new_run_id, Scratch};
use super::GcsClient;
use crate::error::{Error, Result};
use crate::storage::pipe::pipe;
use crate::storage::sparse::{plan_sparse_parts, sparse_extents, sparse_part_count};
use crate::storage::sparsezstd::write_sparse_part;
use crate::storage::uploader::PART_WRITER_CHUNK;
use crate::storage::{SparseFilePutter, SparsePut};

#[async_trait]
impl SparseFilePutter for GcsClient {
    async fn put_sparse_file(
        &self,
        bucket: &str,
        object: &str,
        file: Arc<File>,
        max_parts: usize,
    ) -> Result<Option<SparsePut>> {
        let f = file.clone();
        let (size, exts, populated) = tokio::task::spawn_blocking(move || -> io::Result<_> {
            let size = f.metadata()?.len() as i64;
            let (exts, populated) = sparse_extents(&f, size)?;
            Ok((size, exts, populated))
        })
        .await
        .map_err(|e| Error::other(format!("extent scan failed: {e}")))??;
        let res = SparsePut {
            logical_bytes: size,
            populated_bytes: populated,
        };
        let n = sparse_part_count(populated).min(max_parts);
        if n < 2 {
            // Too small to be worth splitting; the streaming path takes it.
            return Ok(None);
        }
        let groups = plan_sparse_parts(&exts, populated, n);
        let run_id = new_run_id();
        let mut scratch = Scratch::new(self, bucket);
        let names: Vec<String> = (0..groups.len())
            .map(|i| format!("{object}.part-{run_id}-{i:04}"))
            .collect();
        for name in &names {
            scratch.push(name.clone());
        }
        let last = groups.len() - 1;
        let uploads = groups.into_iter().enumerate().map(|(i, ranges)| {
            // Each part uploads over its own connection.
            let conn = self.upload_client(i).clone();
            let policy = self.retry();
            let (bucket, name, file) = (bucket.to_string(), names[i].clone(), file.clone());
            async move {
                let (mut pw, mut pr) = pipe();
                let writer = tokio::task::spawn_blocking(move || {
                    let res = write_sparse_part(&mut pw, &file, size, &ranges, i == 0, i == last);
                    pw.close(
                        res.as_ref()
                            .err()
                            .map(|e| io::Error::new(e.kind(), e.to_string())),
                    );
                    res
                });
                let up = conn
                    .upload_stream(policy, &bucket, &name, &mut pr, PART_WRITER_CHUNK as usize)
                    .await;
                drop(pr);
                let wrote = writer
                    .await
                    .map_err(|e| Error::other(format!("part writer failed: {e}")))?;
                up.map_err(|e| e.context(format!("while writing part {i} of {name:?}")))?;
                wrote.map_err(|e| {
                    Error::from(e).context(format!("while writing part {i} of {name:?}"))
                })
            }
        });
        let result = async {
            try_join_all(uploads).await?;
            self.compose_all(bucket, object, &names, &run_id).await
        }
        .await;
        scratch.finish().await;
        result.map(|()| Some(res))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcs::fake::FakeGcs;
    use crate::storage::sparse::SPARSE_PART_TARGET;
    use crate::storage::sparsezstd::tests::decode_sparse;
    use crate::storage::testdata::{write_sparse_source, Region};
    use crate::storage::uploader::{
        upload_buffer_memory, Uploader, DEFAULT_UPLOAD_MEMORY, PARALLEL_WORKER_MEMORY,
    };
    use crate::storage::{ObjectStorage, UploadBody};
    use std::os::unix::fs::FileExt;
    use std::time::Duration;

    const SETTLE: Duration = Duration::from_millis(500);

    async fn wait_for(cond: impl Fn() -> bool) {
        for _ in 0..600 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sparse_upload_honors_reserved_parts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory");
        {
            let f = File::create(&path).unwrap();
            let block = vec![0x5au8; 1 << 20];
            for i in 0..(3 * SPARSE_PART_TARGET / (1 << 20)) {
                f.write_all_at(&block, (i << 20) as u64).unwrap();
            }
            f.set_len(2 << 30).unwrap();
            f.sync_all().unwrap();
        }
        let fake = FakeGcs::new();
        fake.hold_uploads();
        let g = fake.client();
        let file = Arc::new(File::open(&path).unwrap());
        let g2 = g.clone();
        let done =
            tokio::spawn(async move { g2.put_sparse_file("bucket", "memory", file, 2).await });
        wait_for(|| fake.held() >= 2).await;
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            fake.held(),
            2,
            "started parts, want the reserved limit of 2"
        );
        fake.release_uploads();
        let res = done.await.unwrap().unwrap().expect("split");
        assert_eq!(res.logical_bytes, 2 << 30);
        let stored = fake.get("bucket", "memory").unwrap();
        let decoded = decode_sparse(&stored);
        assert_eq!(decoded.len(), 2 << 30);
        assert!(decoded[..(3 * SPARSE_PART_TARGET) as usize]
            .iter()
            .all(|&b| b == 0x5a));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn uploader_slow_gcs_composite() {
        let fake = FakeGcs::new();
        fake.hold_uploads();
        let g: Arc<dyn ObjectStorage> = Arc::new(fake.client());
        let u = Uploader::new(
            g.clone(),
            upload_buffer_memory(g.backend()) + PARALLEL_WORKER_MEMORY,
        )
        .unwrap();
        // Just over the lookahead: three parts.
        let data = bytes::Bytes::from(crate::storage::testdata::noise(
            2553,
            crate::storage::uploader::UPLOAD_COMPOSITE_MIN as usize + 1,
        ));
        let send = |name: &'static str| {
            let u = u.clone();
            let body = data.clone();
            tokio::spawn(async move {
                u.send(
                    &format!("gs://bucket/{name}"),
                    crate::storage::uploader::SendContent::Body(UploadBody::new(
                        std::io::Cursor::new(body),
                    )),
                )
                .await
            })
        };
        let first = send("first");
        wait_for(|| fake.held() >= 3).await;
        tokio::time::sleep(SETTLE).await;
        assert_eq!(fake.held(), 3, "started parts");
        let next = send("next");
        tokio::time::sleep(SETTLE).await;
        assert_eq!(
            fake.held(),
            3,
            "next upload started before the first released its buffers"
        );
        fake.release_uploads();
        first.await.unwrap().unwrap();
        next.await.unwrap().unwrap();
        assert_eq!(fake.uploads_done(), 6);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_burst_fits_default_budget() {
        let dir = tempfile::tempdir().unwrap();
        let mut small = Vec::new();
        for i in 0..6 {
            let p = dir.path().join(format!("small-{i}.img"));
            write_sparse_source(
                &p,
                2 << 30,
                &[Region {
                    off: 0,
                    len: 64 << 10,
                    fill: i as u8 + 1,
                }],
            );
            small.push(File::open(&p).unwrap());
        }
        let pages_path = dir.path().join("pages.img");
        write_sparse_source(
            &pages_path,
            2 << 30,
            &[Region {
                off: 0,
                len: 2 * SPARSE_PART_TARGET,
                fill: 0x7f,
            }],
        );
        let pages = File::open(&pages_path).unwrap();
        let fake = FakeGcs::new();
        fake.hold_uploads();
        let u = Uploader::new(Arc::new(fake.client()), DEFAULT_UPLOAD_MEMORY).unwrap();
        let mut tasks = Vec::new();
        for (i, f) in small.into_iter().enumerate() {
            let u = u.clone();
            tasks.push(tokio::spawn(async move {
                u.send_file(&format!("gs://bucket/small-{i}.img"), &f).await
            }));
        }
        let u2 = u.clone();
        tasks.push(tokio::spawn(async move {
            u2.send_file("gs://bucket/pages.img", &pages).await
        }));
        wait_for(|| fake.held() >= 8).await;
        tokio::time::sleep(SETTLE).await;
        // All six small files and both ranged parts fit the default budget at
        // once.
        assert_eq!(fake.held(), 8, "concurrent GCS uploads");
        fake.release_uploads();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
    }
}
