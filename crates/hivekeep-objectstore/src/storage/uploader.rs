//! Snapshot uploads within a shared memory budget.
//!
//! Every upload reserves, before it starts, the memory its compression
//! workers and the backend's buffers will hold, and keeps the reservation
//! until both are done. One [`Uploader`] serves every snapshot a node moves,
//! so a burst of suspends queues instead of exhausting the node. The budget
//! covers compression and backend buffering only, not downloads or runtime
//! overhead.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::Instant;

use tokio_util::sync::CancellationToken;

use super::codec::{write_content, Content, WriteContentResult};
use super::parzstd::{
    max_procs, par_zstd_chunk_size, par_zstd_worker_memory, PAR_ZSTD_CHUNK, PAR_ZSTD_QUEUE,
};
use super::pipe::pipe;
use super::sparse::{sparse_extents, sparse_part_count};
use super::{parse_object_url, BackendKind, ObjectStorage, UploadBody};
use crate::error::{Error, Result};
use crate::semaphore::WeightedSemaphore;

/// The default reservation budget for snapshot compression and upload
/// buffers. It is not a limit on the process's memory.
pub const DEFAULT_UPLOAD_MEMORY: i64 = 512 << 20;

/// How much of a streamed object S3's uploader and GCS's composite upload
/// buffer per part. An object under this goes up in one request on S3; 32 MiB
/// covers a typical golden snapshot in one.
pub const UPLOAD_PART_SIZE: i64 = 32 << 20;
/// Parts of a larger object in flight at once: one stream tops out well
/// below what a node can push (82-107 MiB/s on one against 233-257 on four),
/// and the snapshot upload is the largest item in a suspend.
pub const UPLOAD_CONCURRENCY: i64 = 4;
/// The size below which a GCS object goes up as one request. Small objects
/// lose to parts (24 MiB took 258-314 ms as one request, 310-410 ms as two).
pub const UPLOAD_COMPOSITE_MIN: i64 = 64 << 20;
/// GCS rounds a writer's chunk size up to a multiple of this.
pub const MIN_UPLOAD_CHUNK_SIZE: i64 = 256 << 10;
/// How much of a sparse part the GCS writer buffers before it sends.
pub const PART_WRITER_CHUNK: i64 = 16 << 20;

/// What a parallel encoder holds: its input buffers, queued frames, zstd
/// state, and the writer's and producer's pending frames.
pub const PARALLEL_WORKER_MEMORY: i64 = (PAR_ZSTD_QUEUE * 3 + 2) * PAR_ZSTD_CHUNK;
/// What one part's inline encoder holds: an input chunk, its frame, the
/// encoder's history and a copy buffer (about 26 MiB with 8 MiB chunks).
pub const SERIAL_WORKER_MEMORY: i64 = 32 << 20;
/// What one range-parallel sparse part reserves.
pub const SPARSE_UPLOAD_MEMORY: i64 = SERIAL_WORKER_MEMORY + PART_WRITER_CHUNK;

/// The writer chunk size that sends an object of at most `size` bytes in one
/// request: rounded up past `size` to a multiple of the 256 KiB unit, since
/// media strictly smaller than the chunk goes in one request.
pub fn single_upload_chunk_size(size: i64) -> i64 {
    let unit = MIN_UPLOAD_CHUNK_SIZE;
    (size.max(0) + unit) / unit * unit
}

/// How much a GCS put reads ahead for a body bounded by `bound`.
pub fn single_upload_lookahead(bound: Option<i64>) -> i64 {
    match bound {
        Some(b) if (0..UPLOAD_COMPOSITE_MIN).contains(&b) => single_upload_chunk_size(b),
        _ => UPLOAD_COMPOSITE_MIN,
    }
}

/// The buffers a backend retains while the compressor runs.
pub fn upload_buffer_memory(backend: BackendKind) -> i64 {
    match backend {
        // The SDK-style uploader holds one producer buffer besides the active
        // parts.
        BackendKind::S3 => (UPLOAD_CONCURRENCY + 1) * UPLOAD_PART_SIZE,
        // Composite uploads retain the lookahead, the part pool and the
        // writer's copy of each part; single uploads fit within this too.
        BackendKind::Gcs => UPLOAD_COMPOSITE_MIN + 2 * UPLOAD_CONCURRENCY * UPLOAD_PART_SIZE,
        BackendKind::Other => 0,
    }
}

/// The backend buffering for a streamed object of at most `size` bytes. GCS
/// sends one under [`UPLOAD_COMPOSITE_MIN`] in one request: its lookahead
/// and writer chunk are both [`single_upload_chunk_size`].
pub fn stream_buffer_memory(backend: BackendKind, size: i64) -> i64 {
    if backend == BackendKind::Gcs && size < UPLOAD_COMPOSITE_MIN {
        return 2 * single_upload_chunk_size(size);
    }
    upload_buffer_memory(backend)
}

/// Bounds the sparse-extent stream of a file whose `populated` bytes lie in
/// `extents` extents: zstd's worst-case expansion, the header, per-extent
/// frames and per-chunk frame headers.
pub fn sparse_encoded_bound(populated: i64, extents: usize) -> i64 {
    populated + populated / 128 + extents as i64 * 16 + (populated / PAR_ZSTD_CHUNK + 1) * 64 + 64
}

#[derive(Debug)]
struct Budget {
    capacity: i64,
    reserved: Arc<WeightedSemaphore>,
}

/// How an upload will run and what it reserves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadPlan {
    /// Compression workers.
    pub workers: i64,
    /// Range-parallel parts; one means a single stream.
    pub parts: usize,
    /// Bytes reserved from the budget.
    pub memory: i64,
}

/// Shares a memory reservation budget across concurrent uploads. Reuse one
/// for every snapshot a node serves.
#[derive(Clone)]
pub struct Uploader {
    client: Arc<dyn ObjectStorage>,
    budget: Arc<Budget>,
    procs: usize,
}

/// What is being uploaded.
pub enum Source {
    /// A snapshot file; the caller keeps it open.
    File(File),
    /// Any other stream.
    Reader(Box<dyn Read + Send>),
}

impl Uploader {
    /// An uploader with `capacity` bytes of reservations, which must fit one
    /// compression worker plus the backend's buffers; larger files then use
    /// fewer workers rather than wait for an impossible reservation.
    pub fn new(client: Arc<dyn ObjectStorage>, capacity: i64) -> Result<Self> {
        let minimum = upload_buffer_memory(client.backend()) + PARALLEL_WORKER_MEMORY;
        if capacity < minimum {
            return Err(Error::other(format!(
                "upload memory budget must be at least {minimum} bytes for this backend"
            )));
        }
        Ok(Uploader {
            client,
            budget: Arc::new(Budget {
                capacity,
                reserved: WeightedSemaphore::new(capacity as u64),
            }),
            procs: max_procs(),
        })
    }

    /// An uploader sending through `client` on this one's budget, for a
    /// transfer with its own credentials. `client` must be the same kind of
    /// backend, since the budget was sized for it.
    pub fn with_client(&self, client: Arc<dyn ObjectStorage>) -> Self {
        Uploader {
            client,
            budget: self.budget.clone(),
            procs: self.procs,
        }
    }

    /// Plans as if this many cores were available.
    pub fn with_procs(mut self, procs: usize) -> Self {
        self.procs = procs.max(1);
        self
    }

    /// The client uploads go through.
    pub fn client(&self) -> &Arc<dyn ObjectStorage> {
        &self.client
    }

    /// Whether two uploaders share a budget.
    pub fn shares_budget(&self, other: &Uploader) -> bool {
        Arc::ptr_eq(&self.budget, &other.budget)
    }

    /// The budget's capacity.
    pub fn capacity(&self) -> i64 {
        self.budget.capacity
    }

    /// Takes the whole budget if nothing holds any of it, for tests that
    /// check a failed upload released its reservation.
    pub fn try_reserve_all(&self) -> bool {
        self.budget
            .reserved
            .try_acquire(self.budget.capacity as u64)
            .is_some()
    }

    /// Plans an upload of `file` without sending it. The file position is
    /// kept.
    pub fn plan_file(&self, file: &File) -> Result<UploadPlan> {
        Ok(self.plan_stream(Source::File(file.try_clone()?))?.0)
    }

    fn plan_stream(&self, source: Source) -> Result<(UploadPlan, Option<i64>, Content)> {
        let backend = self.client.backend();
        let mut buffers = upload_buffer_memory(backend);
        let mut workers =
            (self.procs as i64).min((self.budget.capacity - buffers) / PARALLEL_WORKER_MEMORY);
        let mut worker_memory = PARALLEL_WORKER_MEMORY;
        let mut parts = 1usize;
        let mut stream_max = None;
        let content = match source {
            Source::Reader(r) => Content::Stream(r),
            Source::File(mut f) => {
                let size = f.metadata()?.len() as i64;
                let position = f.stream_position()?;
                let scanned = sparse_extents(&f, size);
                let restored = f.seek(SeekFrom::Start(position));
                let (exts, populated) = scanned?;
                restored?;
                // Holes never reach the compressor or the range uploads.
                workers = workers.min(populated / PAR_ZSTD_CHUNK + 1);
                parts = sparse_part_count(populated)
                    .min((self.budget.capacity / SPARSE_UPLOAD_MEMORY) as usize);
                if self.client.sparse_putter().is_some() && parts >= 2 {
                    // A file with two or more parts is always split, so the
                    // streaming fallback never runs: reserve only the parts.
                    let plan = UploadPlan {
                        workers,
                        parts,
                        memory: parts as i64 * SPARSE_UPLOAD_MEMORY,
                    };
                    return Ok((plan, None, Content::File(f)));
                }
                let bound = sparse_encoded_bound(populated, exts.len());
                stream_max = Some(bound);
                buffers = stream_buffer_memory(backend, bound);
                worker_memory = par_zstd_worker_memory(par_zstd_chunk_size(populated));
                Content::Sparse {
                    file: f,
                    size,
                    exts,
                    populated,
                }
            }
        };
        let memory = buffers + workers * worker_memory;
        Ok((
            UploadPlan {
                workers,
                parts,
                memory,
            },
            stream_max,
            content,
        ))
    }

    /// Compresses and uploads an open snapshot file to `url`. The caller
    /// keeps the file. Waiting for memory ends when the future is dropped.
    pub async fn send_file(&self, url: &str, file: &File) -> Result<()> {
        self.send_zstd(url, Source::File(file.try_clone()?)).await
    }

    /// Compresses and uploads `source` to `url`.
    pub async fn send_zstd(&self, url: &str, source: Source) -> Result<()> {
        let (bucket, object) = parse_object_url(url).map_err(|e| e.context("while parsing URL"))?;
        let file = match &source {
            Source::File(f) => Some(f.try_clone()?),
            Source::Reader(_) => None,
        };
        let (plan, stream_max, content) = self.plan_stream(source)?;
        let _permit = self.budget.reserved.acquire(plan.memory as u64).await;
        let started = Instant::now();
        if let (Some(sp), Some(f)) = (self.client.sparse_putter(), &file) {
            if plan.parts >= 2 {
                match sp
                    .put_sparse_file(&bucket, &object, Arc::new(f.try_clone()?), plan.parts)
                    .await
                {
                    Ok(Some(res)) => {
                        tracing::info!(
                            object = %object,
                            sparse = true,
                            ranged = true,
                            logical_bytes = res.logical_bytes,
                            populated_bytes = res.populated_bytes,
                            total_ms = started.elapsed().as_millis() as u64,
                            "Compressed zstd upload"
                        );
                        return Ok(());
                    }
                    Ok(None) => {
                        // Too small to split: stream it instead.
                        f.try_clone()?.seek(SeekFrom::Start(0))?;
                    }
                    Err(e) => return Err(e.context(format!("while putting object {object:?}"))),
                }
            }
        }
        if self.client.streams_puts() {
            self.send_streaming(&bucket, &object, content, started, plan.workers, stream_max)
                .await
        } else {
            self.send_buffered(&bucket, &object, content, started, plan.workers)
                .await
        }
    }

    /// Compresses into a temporary file, then uploads it, for backends that
    /// need a seekable body.
    async fn send_buffered(
        &self,
        bucket: &str,
        object: &str,
        mut content: Content,
        started: Instant,
        workers: i64,
    ) -> Result<()> {
        let cancel = CancellationToken::new();
        let _stop = cancel.clone().drop_guard();
        let compressed =
            tokio::task::spawn_blocking(move || -> io::Result<(File, WriteContentResult)> {
                let mut tmp = tempfile::tempfile()?;
                let res = write_content(&cancel, &mut tmp, &mut content, workers)?;
                tmp.seek(SeekFrom::Start(0))?;
                Ok((tmp, res))
            })
            .await
            .map_err(|e| Error::other(format!("compression task failed: {e}")))?;
        let (tmp, res) = compressed
            .map_err(|e| Error::from(e).context(format!("while compressing {object:?}")))?;
        let compress_ms = started.elapsed().as_millis() as u64;
        let len = tmp.metadata()?.len() as i64;
        let body = UploadBody::bounded(tokio::fs::File::from_std(tmp), len);
        self.client
            .put_object(bucket, object, body)
            .await
            .map_err(|e| e.context(format!("while putting object {object:?}")))?;
        tracing::info!(
            object = %object,
            sparse = res.sparse,
            logical_bytes = res.logical_bytes,
            populated_bytes = res.populated_bytes,
            compress_ms,
            total_ms = started.elapsed().as_millis() as u64,
            "Compressed zstd upload"
        );
        Ok(())
    }

    /// Compresses and uploads in one overlapped pass: the compressor writes
    /// into a pipe the upload reads, so nothing is staged on disk.
    async fn send_streaming(
        &self,
        bucket: &str,
        object: &str,
        mut content: Content,
        started: Instant,
        workers: i64,
        stream_max: Option<i64>,
    ) -> Result<()> {
        let cancel = CancellationToken::new();
        let _stop = cancel.clone().drop_guard();
        let (mut pw, pr) = pipe();
        let c = cancel.clone();
        let compress = tokio::task::spawn_blocking(move || {
            let res = write_content(&c, &mut pw, &mut content, workers);
            // Closing delivers EOF, or the failure, to the upload.
            pw.close(
                res.as_ref()
                    .err()
                    .map(|e| io::Error::new(e.kind(), e.to_string())),
            );
            res
        });
        let body = UploadBody {
            reader: Box::pin(pr),
            size_bound: stream_max,
        };
        let put = self.client.put_object(bucket, object, body).await;
        if put.is_err() {
            // The upload gave up; stop compressing.
            cancel.cancel();
        }
        let compressed = compress
            .await
            .map_err(|e| Error::other(format!("compression task failed: {e}")))?;
        put.map_err(|e| e.context(format!("while putting object {object:?}")))?;
        let res = compressed
            .map_err(|e| Error::from(e).context(format!("while compressing {object:?}")))?;
        tracing::info!(
            object = %object,
            sparse = res.sparse,
            streaming = true,
            logical_bytes = res.logical_bytes,
            populated_bytes = res.populated_bytes,
            total_ms = started.elapsed().as_millis() as u64,
            "Compressed zstd upload"
        );
        Ok(())
    }

    /// Uploads uncompressed content on the same budget, as snapshot manifests
    /// are. A regular file reserves only what its size needs.
    pub async fn send(&self, url: &str, content: SendContent) -> Result<()> {
        let (bucket, object) = parse_object_url(url)?;
        let backend = self.client.backend();
        let (body, memory) = match content {
            SendContent::File(f) => {
                let meta = f.metadata()?;
                if meta.is_file() {
                    let size = meta.len() as i64;
                    (
                        UploadBody::bounded(tokio::fs::File::from_std(f), size),
                        stream_buffer_memory(backend, size),
                    )
                } else {
                    (
                        UploadBody::new(tokio::fs::File::from_std(f)),
                        upload_buffer_memory(backend),
                    )
                }
            }
            SendContent::Body(b) => (b, upload_buffer_memory(backend)),
        };
        let _permit = self.budget.reserved.acquire(memory as u64).await;
        self.client.put_object(&bucket, &object, body).await
    }
}

/// What [`Uploader::send`] uploads.
pub enum SendContent {
    /// A file, read from its current position.
    File(File),
    /// Any body.
    Body(UploadBody),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sparsezstd::SPARSE_MAGIC;
    use crate::storage::testdata::{sparse_file_with, write_sparse_source, Region};
    use crate::storage::{ObjectReader, SparseFilePutter, SparsePut};
    use crate::testing::MemObjects;
    use async_trait::async_trait;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::sync::{mpsc, watch};

    /// A store kind that claims a backend's budget without moving bytes.
    struct KindOnly {
        kind: BackendKind,
        sparse: bool,
    }

    struct NoSparse;

    #[async_trait]
    impl SparseFilePutter for NoSparse {
        async fn put_sparse_file(
            &self,
            _: &str,
            _: &str,
            _: Arc<File>,
            _: usize,
        ) -> Result<Option<SparsePut>> {
            Ok(None)
        }
    }

    #[async_trait]
    impl ObjectStorage for KindOnly {
        async fn get_object(&self, _: &str, _: &str) -> Result<ObjectReader> {
            Err(Error::other("unused"))
        }
        async fn put_object(&self, _: &str, _: &str, _: UploadBody) -> Result<()> {
            Err(Error::other("unused"))
        }
        fn backend(&self) -> BackendKind {
            self.kind
        }
        fn streams_puts(&self) -> bool {
            true
        }
        fn sparse_putter(&self) -> Option<&dyn SparseFilePutter> {
            if self.sparse {
                Some(&NoSparse)
            } else {
                None
            }
        }
    }

    fn gcs() -> Arc<dyn ObjectStorage> {
        Arc::new(KindOnly {
            kind: BackendKind::Gcs,
            sparse: true,
        })
    }
    fn s3() -> Arc<dyn ObjectStorage> {
        Arc::new(KindOnly {
            kind: BackendKind::S3,
            sparse: false,
        })
    }

    #[test]
    fn budget_fits_both_paths() {
        let dir = tempfile::tempdir().unwrap();
        let f = File::create(dir.path().join("sparse")).unwrap();
        f.set_len(8 << 30).unwrap();
        let mem: Arc<dyn ObjectStorage> = Arc::new(MemObjects::new());
        for client in [s3(), gcs(), mem] {
            let minimum = upload_buffer_memory(client.backend()) + PARALLEL_WORKER_MEMORY;
            assert!(Uploader::new(client.clone(), minimum - 1).is_err());
            for capacity in [minimum, DEFAULT_UPLOAD_MEMORY] {
                let u = Uploader::new(client.clone(), capacity).unwrap();
                let plan = u.plan_file(&f).unwrap();
                assert!(
                    plan.workers >= 1
                        && plan.workers as usize <= max_procs()
                        && plan.memory <= capacity,
                    "invalid plan {plan:?} for capacity {capacity}"
                );
                if client.sparse_putter().is_some() && plan.parts >= 2 {
                    assert!(plan.parts as i64 * SPARSE_UPLOAD_MEMORY <= plan.memory);
                    continue;
                }
                let floor = stream_buffer_memory(client.backend(), sparse_encoded_bound(0, 0))
                    + plan.workers
                        * par_zstd_worker_memory(crate::storage::parzstd::PAR_ZSTD_MIN_CHUNK);
                assert!(
                    plan.memory >= floor,
                    "streaming pipeline under-reserved: {plan:?}"
                );
            }
        }
    }

    #[test]
    fn plans_from_populated_bytes() {
        let cases = [
            (
                "all_holes",
                0,
                (1, 1, (8 << 20) + (512 << 10)),
                (1, 1, 168 << 20),
            ),
            (
                "small",
                1,
                (1, 1, (8 << 20) + (2560 << 10)),
                (1, 1, 168 << 20),
            ),
            (
                "two_chunks",
                9,
                (2, 1, (146 << 20) + (512 << 10)),
                (2, 1, 288 << 20),
            ),
            ("multiple_ranges", 192, (3, 3, 144 << 20), (5, 3, 480 << 20)),
        ];
        let dir = tempfile::tempdir().unwrap();
        for (name, mib, want_gcs, want_s3) in cases {
            let offsets: Vec<i64> = (0..mib).map(|i| i << 20).collect();
            let mut f = sparse_file_with(dir.path(), 2 << 30, &offsets);
            for (backend, client, want) in [("gcs", gcs(), want_gcs), ("s3", s3(), want_s3)] {
                let u = Uploader::new(client, DEFAULT_UPLOAD_MEMORY)
                    .unwrap()
                    .with_procs(8);
                f.seek(SeekFrom::Start(7)).unwrap();
                let got = u.plan_file(&f).unwrap();
                let want = UploadPlan {
                    workers: want.0,
                    parts: want.1,
                    memory: want.2,
                };
                assert_eq!(got, want, "{name}/{backend}");
                assert_eq!(
                    f.stream_position().unwrap(),
                    7,
                    "{name}/{backend}: planning moved the file"
                );
            }
        }
    }

    #[test]
    fn reserves_only_the_chosen_path() {
        let dir = tempfile::tempdir().unwrap();
        let offsets: Vec<i64> = (0..292).map(|i| i << 20).collect();
        let ranged = sparse_file_with(dir.path(), 2 << 30, &offsets);
        let small = sparse_file_with(dir.path(), 2 << 30, &[0]);
        for capacity in [DEFAULT_UPLOAD_MEMORY, 16 << 30] {
            let u = Uploader::new(gcs(), capacity).unwrap().with_procs(192);
            let got = u.plan_file(&ranged).unwrap();
            assert!(
                got.parts == 4 && got.memory == 4 * SPARSE_UPLOAD_MEMORY,
                "capacity {capacity}: ranged plan {got:?}"
            );
            let got = u.plan_file(&small).unwrap();
            let want = stream_buffer_memory(BackendKind::Gcs, sparse_encoded_bound(1 << 20, 1))
                + par_zstd_worker_memory(crate::storage::parzstd::PAR_ZSTD_MIN_CHUNK);
            assert_eq!(got.memory, want, "capacity {capacity}: small plan {got:?}");
        }
    }

    /// A streaming store that drains each body, reports it started, and holds
    /// the upload until released.
    struct GatedStore {
        started: mpsc::UnboundedSender<String>,
        release: watch::Receiver<bool>,
        fail: Option<&'static str>,
    }

    #[async_trait]
    impl ObjectStorage for GatedStore {
        async fn get_object(&self, _: &str, _: &str) -> Result<ObjectReader> {
            Err(Error::other("unused"))
        }
        async fn put_object(&self, _: &str, object: &str, mut body: UploadBody) -> Result<()> {
            // Drain compression first: the reservation must stay held while
            // the destination is slow, after the producer is done.
            let mut sink = Vec::new();
            body.reader.read_to_end(&mut sink).await?;
            let _ = self.started.send(object.to_string());
            let mut r = self.release.clone();
            while !*r.borrow() {
                if r.changed().await.is_err() {
                    break;
                }
            }
            match self.fail {
                Some(m) => Err(Error::other(m)),
                None => Ok(()),
            }
        }
        fn streams_puts(&self) -> bool {
            true
        }
    }

    fn gated(
        fail: Option<&'static str>,
    ) -> (
        Arc<GatedStore>,
        mpsc::UnboundedReceiver<String>,
        watch::Sender<bool>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (gate, release) = watch::channel(false);
        (
            Arc::new(GatedStore {
                started: tx,
                release,
                fail,
            }),
            rx,
            gate,
        )
    }

    const SETTLE: Duration = Duration::from_millis(300);

    fn payload() -> Source {
        Source::Reader(Box::new(io::Cursor::new(b"payload".to_vec())))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn small_sparse_files_share_budget() {
        let dir = tempfile::tempdir().unwrap();
        let files = [
            sparse_file_with(dir.path(), 2 << 30, &[1 << 30]),
            sparse_file_with(dir.path(), 2 << 30, &[1 << 30]),
        ];
        let (store, mut started, gate) = gated(None);
        let u = Uploader::new(store, PARALLEL_WORKER_MEMORY)
            .unwrap()
            .with_procs(2);
        let mut tasks = Vec::new();
        for (i, f) in files.iter().enumerate() {
            let u = u.clone();
            let f = f.try_clone().unwrap();
            tasks.push(tokio::spawn(async move {
                u.send_file(&format!("gs://bucket/f{i}"), &f).await
            }));
        }
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(10), started.recv())
                .await
                .expect("both small resident sets must fit the budget");
        }
        gate.send(true).unwrap();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wait_cancel_and_release() {
        for failure in [None, Some("upload failed")] {
            let (store, mut started, gate) = gated(failure);
            let u = Uploader::new(store, PARALLEL_WORKER_MEMORY).unwrap();
            let u1 = u.clone();
            let first =
                tokio::spawn(async move { u1.send_zstd("gs://bucket/first", payload()).await });
            assert_eq!(started.recv().await.unwrap(), "first");
            // A second upload waits for the budget, and is cancelled.
            let waiting =
                tokio::time::timeout(SETTLE, u.send_zstd("gs://bucket/waiting", payload())).await;
            assert!(waiting.is_err(), "waiting upload bypassed the budget");
            assert!(started.try_recv().is_err());
            // Cancelling a waiter does not release the first's reservation.
            let u2 = u.clone();
            let next =
                tokio::spawn(async move { u2.send_zstd("gs://bucket/next", payload()).await });
            tokio::time::sleep(SETTLE).await;
            assert!(started.try_recv().is_err(), "next bypassed the held budget");
            gate.send(true).unwrap();
            let r1 = first.await.unwrap();
            let r2 = next.await.unwrap();
            assert_eq!(r1.is_err(), failure.is_some(), "first: {r1:?}");
            assert_eq!(r2.is_err(), failure.is_some(), "next: {r2:?}");
            assert_eq!(started.recv().await.unwrap(), "next");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancellation_releases_active_reservation() {
        let (store, mut started, gate) = gated(None);
        let u = Uploader::new(store, PARALLEL_WORKER_MEMORY).unwrap();
        let u1 = u.clone();
        let canceled =
            tokio::spawn(async move { u1.send_zstd("gs://bucket/canceled", payload()).await });
        started.recv().await.unwrap();
        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());
        gate.send(true).unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            u.send_zstd("gs://bucket/next", payload()),
        )
        .await
        .expect("the cancelled upload kept its reservation")
        .unwrap();
    }

    struct FailingReader;
    impl Read for FailingReader {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("source failed"))
        }
    }

    #[tokio::test]
    async fn read_failure_releases_reservation() {
        let u = Uploader::new(Arc::new(MemObjects::new()), PARALLEL_WORKER_MEMORY).unwrap();
        let e = u
            .send_zstd("gs://bucket/bad", Source::Reader(Box::new(FailingReader)))
            .await
            .unwrap_err();
        assert!(e.to_string().contains("source failed"), "{e}");
        u.send_zstd("gs://bucket/next", payload()).await.unwrap();
    }

    /// A streaming store that reads the header and fails.
    struct EarlyFailure;

    #[async_trait]
    impl ObjectStorage for EarlyFailure {
        async fn get_object(&self, _: &str, _: &str) -> Result<ObjectReader> {
            Err(Error::other("unused"))
        }
        async fn put_object(&self, _: &str, _: &str, mut body: UploadBody) -> Result<()> {
            let mut header = vec![0u8; SPARSE_MAGIC.len() + 4];
            body.reader.read_exact(&mut header).await?;
            Err(Error::other("destination failed before payload"))
        }
        fn streams_puts(&self) -> bool {
            true
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn destination_failure_stops_compression() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source");
        write_sparse_source(
            &path,
            2 << 30,
            &[Region {
                off: 0,
                len: 8 << 20,
                fill: 0x5a,
            }],
        );
        let mut f = File::open(&path).unwrap();
        let u = Uploader::new(Arc::new(EarlyFailure), PARALLEL_WORKER_MEMORY).unwrap();
        let e = u.send_file("gs://bucket/object", &f).await.unwrap_err();
        assert!(
            e.to_string().contains("destination failed before payload"),
            "{e}"
        );
        let at = f.stream_position().unwrap();
        assert!(
            at < 8 << 20,
            "read the entire extent after destination failure: {at}"
        );
        assert!(
            u.try_reserve_all(),
            "failed upload retained its reservation"
        );
    }

    #[test]
    fn with_client_shares_the_budget() {
        let u = Uploader::new(gcs(), DEFAULT_UPLOAD_MEMORY).unwrap();
        let other = gcs();
        let w = u.with_client(other.clone());
        assert!(
            Arc::ptr_eq(w.client(), &other),
            "with_client did not switch the client"
        );
        assert!(w.shares_budget(&u), "with_client did not share the budget");
    }

    /// A streaming store recording the lookahead each body would get.
    #[derive(Default)]
    struct LookaheadCapture {
        lookaheads: parking_lot::Mutex<Vec<i64>>,
    }

    #[async_trait]
    impl ObjectStorage for LookaheadCapture {
        async fn get_object(&self, _: &str, _: &str) -> Result<ObjectReader> {
            Err(Error::other("unused"))
        }
        async fn put_object(&self, _: &str, _: &str, body: UploadBody) -> Result<()> {
            self.lookaheads
                .lock()
                .push(single_upload_lookahead(body.size_bound));
            body.read_all().await?;
            Ok(())
        }
        fn streams_puts(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn single_upload_lookahead_through_public_uploaders() {
        let dir = tempfile::tempdir().unwrap();
        let sparse_path = dir.path().join("small.img");
        write_sparse_source(
            &sparse_path,
            2 << 30,
            &[Region {
                off: 0,
                len: 64 << 10,
                fill: 0x33,
            }],
        );
        let manifest_path = dir.path().join("manifest.json");
        std::fs::write(&manifest_path, br#"{"version":1}"#).unwrap();
        let store = Arc::new(LookaheadCapture::default());
        let u = Uploader::new(store.clone(), DEFAULT_UPLOAD_MEMORY).unwrap();
        u.send_file("gs://bucket/small.img", &File::open(&sparse_path).unwrap())
            .await
            .unwrap();
        u.send(
            "gs://bucket/manifest.json",
            SendContent::File(File::open(&manifest_path).unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(
            *store.lookaheads.lock(),
            vec![MIN_UPLOAD_CHUNK_SIZE, MIN_UPLOAD_CHUNK_SIZE]
        );
    }

    #[test]
    fn single_upload_chunk_size_rule() {
        let unit = MIN_UPLOAD_CHUNK_SIZE;
        for (size, want) in [
            (-1, unit),
            (0, unit),
            (1, unit),
            (unit - 1, unit),
            (unit, 2 * unit),
            (unit + 1, 2 * unit),
            (UPLOAD_COMPOSITE_MIN - 1, UPLOAD_COMPOSITE_MIN),
        ] {
            let got = single_upload_chunk_size(size);
            assert_eq!(got, want, "size {size}");
            assert!(got > size.max(0));
            assert_eq!(got % unit, 0);
        }
    }
}
