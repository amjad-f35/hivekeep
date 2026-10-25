//! The glutton service: RAM, disk, file-descriptor, ping, CPU-burn and
//! ingest operations. Gossip, the CPU load pool and scripts live in their
//! own modules as further `impl Service` blocks.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::RngCore;
use sha2::{Digest, Sha256};
use tonic::Status;

use super::cpu_load::CpuLoad;
use super::ctx::Ctx;
use super::gossip::PeerGossip;
use super::metrics::Metrics;
use super::sizes::parse_bytes;
use crate::pb::glutton::v1 as pb;

/// The pattern disk keys must match, as shown in error messages. Only
/// alphanumerics, underscore and dash are allowed, so a key cannot escape
/// the data dir or name a hidden file.
pub const DISK_KEY_PATTERN: &str = "^[a-zA-Z0-9_-]+$";

/// Whether `key` is a valid disk key.
pub fn valid_disk_key(key: &str) -> bool {
    !key.is_empty()
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn check_disk_key(key: &str) -> Result<(), Status> {
    if valid_disk_key(key) {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "key {key:?} must match {DISK_KEY_PATTERN}"
        )))
    }
}

/// Stride of the ReadRAM walk: one byte per 4 KiB page is enough to force
/// every page resident without reading them all.
pub const PAGE_SIZE: usize = 4096;

/// Per-chunk size of streamed random disk writes, so a multi-gigabyte
/// WriteDisk never has to sit in RAM.
const STREAM_CHUNK: usize = 1 << 20;

/// Size of the buffer each burner thread re-hashes: large enough that
/// hashing dominates the loop, small enough to stay in cache.
const BURN_BLOCK_BYTES: usize = 64 << 10;

#[derive(Default)]
pub(crate) struct RamState {
    pub(crate) arrays: HashMap<String, Vec<u8>>,
    /// Each array's next rotate offset. Absent means 0; dropped whenever
    /// the array is reallocated.
    pub(crate) cursor: HashMap<String, usize>,
}

/// Construction options.
#[derive(Debug, Clone, Copy, Default)]
pub struct Options {
    /// Makes WriteDisk fsync each file before it returns, so the measured
    /// write includes the flush to the backing volume.
    pub sync_writes: bool,
    /// Makes `/readyz` fail until the data dir is writable, so a resume
    /// waits for an external volume mount before the Actor takes load.
    pub writable_readyz: bool,
}

/// The workload. Share it behind an `Arc`; every operation takes `&self`.
pub struct Service {
    pub(crate) data_dir: PathBuf,
    pub(crate) sync_writes: bool,
    /// When set, the directory `/readyz` must be able to write to.
    pub(crate) readyz_dir: Option<PathBuf>,
    pub(crate) ram: Mutex<RamState>,
    pub(crate) fds: Mutex<Vec<File>>,
    pub(crate) peers: Mutex<HashMap<String, PeerGossip>>,
    pub(crate) cpu: CpuLoad,
    pub(crate) metrics: Metrics,
}

impl Service {
    /// A service storing disk files under `dir`. The caller creates `dir`.
    pub fn new(dir: impl Into<PathBuf>, opts: Options) -> prometheus::Result<Service> {
        let data_dir = dir.into();
        Ok(Service {
            readyz_dir: opts.writable_readyz.then(|| data_dir.clone()),
            data_dir,
            sync_writes: opts.sync_writes,
            ram: Mutex::new(RamState::default()),
            fds: Mutex::new(Vec::new()),
            peers: Mutex::new(HashMap::new()),
            cpu: CpuLoad::new(),
            metrics: Metrics::new()?,
        })
    }

    /// The registry the service's instruments are on.
    pub fn registry(&self) -> &prometheus::Registry {
        &self.metrics.registry
    }

    /// The data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Stops every gossip loop and waits for them, then stops the CPU load.
    pub async fn close(&self) {
        let peers: Vec<PeerGossip> = self.peers.lock().drain().map(|(_, p)| p).collect();
        for p in peers {
            p.stop().await;
        }
        self.cpu.stop();
    }

    /// Writes random bytes to the RAM array `key`: replacing it, on top of
    /// it from offset 0 (growing it if needed), or at its rotating cursor.
    pub fn write_ram(&self, req: &pb::WriteRamRequest) -> Result<pb::WriteRamResponse, Status> {
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key is required"));
        }
        let size =
            parse_bytes(&req.size).map_err(|e| Status::invalid_argument(format!("size: {e}")))?;
        if size < 0 {
            return Err(Status::invalid_argument("size must be non-negative"));
        }
        let size = size as usize;
        match pb::WriteMode::try_from(req.write_mode) {
            Ok(pb::WriteMode::Truncate) => {
                let mut buf = vec![0u8; size];
                rand::rng().fill_bytes(&mut buf);
                let mut ram = self.ram.lock();
                ram.arrays.insert(req.key.clone(), buf);
                ram.cursor.remove(&req.key);
            }
            Ok(pb::WriteMode::Overwrite) => {
                let mut ram = self.ram.lock();
                let RamState { arrays, cursor } = &mut *ram;
                let existing = arrays.entry(req.key.clone()).or_default();
                if size > existing.len() {
                    *existing = vec![0u8; size];
                    cursor.remove(&req.key);
                }
                rand::rng().fill_bytes(&mut existing[..size]);
            }
            Ok(pb::WriteMode::OverwriteRotate) => self.rotate_ram(&req.key, size)?,
            Err(_) => {
                return Err(Status::invalid_argument(format!(
                    "unknown write_mode {}",
                    req.write_mode
                )))
            }
        }
        self.metrics.ram_write_bytes.inc_by(size as u64);
        Ok(pb::WriteRamResponse {})
    }

    /// Re-randomizes `size` bytes from the key's cursor, wrapping at the end
    /// of the array, then moves the cursor past the write. Repeated rotates
    /// walk the whole array instead of re-dirtying one prefix; the cursor
    /// lives in process memory, so it survives suspend and resume.
    fn rotate_ram(&self, key: &str, size: usize) -> Result<(), Status> {
        let mut ram = self.ram.lock();
        let RamState { arrays, cursor } = &mut *ram;
        let existing = match arrays.get_mut(key) {
            Some(a) if !a.is_empty() => a,
            _ => {
                return Err(Status::not_found(format!(
                    "rotate needs an existing array {key:?}; fill with TRUNCATE first"
                )))
            }
        };
        let len = existing.len();
        let size = size.min(len);
        let start = cursor.get(key).copied().unwrap_or(0);
        let head_end = (start + size).min(len);
        let mut rng = rand::rng();
        rng.fill_bytes(&mut existing[start..head_end]);
        let wrapped = size - (head_end - start);
        if wrapped > 0 {
            rng.fill_bytes(&mut existing[..wrapped]);
        }
        cursor.insert(key.to_string(), (start + size) % len);
        Ok(())
    }

    /// Walks the RAM array `key`, reading one byte per page, so every
    /// touched page is resident before the answer returns. After a
    /// demand-paged restore this turns restore-time laziness into read
    /// latency.
    pub fn read_ram(&self, req: &pb::ReadRamRequest) -> Result<pb::ReadRamResponse, Status> {
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key is required"));
        }
        let ram = self.ram.lock();
        let arr = ram
            .arrays
            .get(&req.key)
            .ok_or_else(|| Status::not_found(format!("no RAM array {:?}", req.key)))?;
        let mut walk = arr.len() as i64;
        if !req.size.is_empty() {
            let n = parse_bytes(&req.size)
                .map_err(|e| Status::invalid_argument(format!("size: {e}")))?;
            if n < 0 {
                return Err(Status::invalid_argument("size must be non-negative"));
            }
            walk = walk.min(n);
        }
        let checksum = arr[..walk as usize]
            .iter()
            .step_by(PAGE_SIZE)
            .fold(0u32, |sum, b| sum ^ u32::from(*b));
        self.metrics.ram_read_bytes.inc_by(walk as u64);
        Ok(pb::ReadRamResponse {
            size: walk,
            checksum,
        })
    }

    /// Writes `size` random bytes to the file `key`, truncating it first or
    /// writing over its head. The digest always covers the whole file.
    pub async fn write_disk(
        &self,
        req: &pb::WriteDiskRequest,
    ) -> Result<pb::WriteDiskResponse, Status> {
        check_disk_key(&req.key)?;
        if req.size < 0 {
            return Err(Status::invalid_argument("size must be non-negative"));
        }
        let mode = match pb::WriteMode::try_from(req.write_mode) {
            Ok(m @ (pb::WriteMode::Truncate | pb::WriteMode::Overwrite)) => m,
            _ => {
                return Err(Status::invalid_argument(format!(
                    "unknown write_mode {}",
                    req.write_mode
                )))
            }
        };
        let path = self.data_dir.join(&req.key);
        let size = req.size as i64;
        let sync = self.sync_writes;
        let resp = blocking(move || write_disk_file(&path, mode, size, sync)).await?;
        self.metrics.disk_write_bytes.inc_by(req.size as u64);
        Ok(resp)
    }

    /// Reads the file `key`, returning its size and digest and, unless
    /// digest-only was asked for, its bytes.
    pub async fn read_disk(
        &self,
        req: &pb::ReadDiskRequest,
    ) -> Result<pb::ReadDiskResponse, Status> {
        check_disk_key(&req.key)?;
        let path = self.data_dir.join(&req.key);
        let key = req.key.clone();
        let digest_only = req.read_mode == pb::ReadMode::DigestOnly as i32;
        let resp = blocking(move || {
            let mut f = File::open(&path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Status::not_found(format!("file {key:?} not found"))
                } else {
                    Status::internal(format!("open {}: {e}", path.display()))
                }
            })?;
            let read_err =
                |e: std::io::Error| Status::internal(format!("read {}: {e}", path.display()));
            if digest_only {
                let mut h = Sha256::new();
                let n = hash_reader(&mut f, &mut h).map_err(read_err)?;
                return Ok(pb::ReadDiskResponse {
                    size: n as i64,
                    sha256: h.finalize().to_vec(),
                    data: Vec::new(),
                });
            }
            let mut data = Vec::new();
            f.read_to_end(&mut data).map_err(read_err)?;
            Ok(pb::ReadDiskResponse {
                size: data.len() as i64,
                sha256: Sha256::digest(&data).to_vec(),
                data,
            })
        })
        .await?;
        self.metrics.disk_read_bytes.inc_by(resp.size as u64);
        Ok(resp)
    }

    /// Opens or closes file descriptors until exactly `count` are held
    /// (beyond the ones the process needs anyway).
    pub fn open_fd(&self, req: &pb::OpenFdRequest) -> Result<pb::OpenFdResponse, Status> {
        if req.count < 0 {
            return Err(Status::invalid_argument("count must be non-negative"));
        }
        let target = req.count as usize;
        let mut fds = self.fds.lock();
        fds.truncate(target);
        while fds.len() < target {
            let f = File::open(DEV_NULL)
                .map_err(|e| Status::internal(format!("open {DEV_NULL}: {e}")))?;
            fds.push(f);
        }
        self.metrics.fds_open.set(fds.len() as i64);
        Ok(pb::OpenFdResponse {})
    }

    /// Echoes the message back.
    pub fn ping(&self, req: &pb::PingRequest) -> pb::PingResponse {
        self.metrics.pings_received.inc();
        pb::PingResponse {
            message: req.message.clone(),
        }
    }

    /// Spins `parallelism` threads in a sha256 loop for the wall-clock
    /// duration (or until `ctx` ends). The iteration count keeps the work
    /// observable. It takes no service lock, so a burn never blocks the
    /// other operations.
    pub async fn burn_cpu(
        &self,
        ctx: &Ctx,
        req: &pb::BurnCpuRequest,
    ) -> Result<pb::BurnCpuResponse, Status> {
        if req.duration_ms < 0 {
            return Err(Status::invalid_argument("duration_ms must be non-negative"));
        }
        // Past a few threads per CPU the burn gains nothing, and an uncapped
        // value would let one request start threads without bound.
        let max_par = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            * 4;
        let parallelism = (req.parallelism.max(1) as usize).min(max_par);
        let deadline = Instant::now() + Duration::from_millis(req.duration_ms as u64);
        let ctx = ctx.clone();
        let total = Arc::new(AtomicI64::new(0));
        let sum = total.clone();
        blocking(move || {
            std::thread::scope(|scope| {
                for _ in 0..parallelism {
                    let ctx = &ctx;
                    let sum = &sum;
                    scope.spawn(move || {
                        let mut block = vec![0u8; BURN_BLOCK_BYTES];
                        let mut n = 0i64;
                        while Instant::now() < deadline && !ctx.is_done() {
                            let digest = Sha256::digest(&block);
                            block[..digest.len()].copy_from_slice(&digest);
                            n += 1;
                        }
                        sum.fetch_add(n, Ordering::Relaxed);
                    });
                }
            });
            Ok(())
        })
        .await?;
        Ok(pb::BurnCpuResponse {
            iterations: total.load(Ordering::Relaxed),
        })
    }

    /// Writes the caller's payload to the file `key`. Unlike WriteDisk the
    /// bytes crossed the network, modelling a download arriving through the
    /// Actor's ingress path before it lands on disk.
    pub async fn ingest(&self, req: &pb::IngestRequest) -> Result<pb::IngestResponse, Status> {
        check_disk_key(&req.key)?;
        let path = self.data_dir.join(&req.key);
        let append = req.append;
        let payload = req.payload.clone();
        let size = blocking(move || {
            let mut opts = OpenOptions::new();
            opts.write(true).create(true);
            if append {
                opts.append(true);
            } else {
                opts.truncate(true);
            }
            let mut f = open_private(&mut opts, &path)?;
            f.write_all(&payload)
                .map_err(|e| Status::internal(format!("write {}: {e}", path.display())))?;
            let meta = f
                .metadata()
                .map_err(|e| Status::internal(format!("stat {}: {e}", path.display())))?;
            Ok(meta.len() as i64)
        })
        .await?;
        self.metrics
            .disk_write_bytes
            .inc_by(req.payload.len() as u64);
        Ok(pb::IngestResponse {
            size,
            sha256: Sha256::digest(&req.payload).to_vec(),
        })
    }

    /// A copy of the RAM array `key`, for tests.
    #[cfg(test)]
    pub(crate) fn ram_copy(&self, key: &str) -> Vec<u8> {
        self.ram.lock().arrays.get(key).cloned().unwrap_or_default()
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        for (_, p) in self.peers.lock().drain() {
            p.cancel();
        }
        self.cpu.stop();
    }
}

#[cfg(unix)]
const DEV_NULL: &str = "/dev/null";
#[cfg(not(unix))]
const DEV_NULL: &str = "NUL";

fn open_private(opts: &mut OpenOptions, path: &Path) -> Result<File, Status> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
        .map_err(|e| Status::internal(format!("open {}: {e}", path.display())))
}

/// Runs blocking file work off the async executor.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Status::internal(format!("worker task: {e}")))?
}

fn write_disk_file(
    path: &Path,
    mode: pb::WriteMode,
    size: i64,
    sync: bool,
) -> Result<pb::WriteDiskResponse, Status> {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create(true);
    if mode == pb::WriteMode::Truncate {
        opts.truncate(true);
    }
    let mut f = open_private(&mut opts, path)?;
    let write_err = |e: std::io::Error| Status::internal(format!("write {}: {e}", path.display()));
    let mut h = Sha256::new();
    stream_random_bytes(&mut f, &mut h, size).map_err(write_err)?;
    if sync {
        f.sync_all()
            .map_err(|e| Status::internal(format!("sync {}: {e}", path.display())))?;
    }
    let mut total = size;
    // Without truncation, bytes beyond `size` from an earlier, larger write
    // remain. The cursor already sits at `size`, so hashing the rest
    // completes the whole-file digest without re-reading the head.
    if mode == pb::WriteMode::Overwrite {
        f.seek(std::io::SeekFrom::Start(size as u64))
            .map_err(write_err)?;
        let tail = hash_reader(&mut f, &mut h)
            .map_err(|e| Status::internal(format!("hash tail {}: {e}", path.display())))?;
        total += tail as i64;
    }
    Ok(pb::WriteDiskResponse {
        size: total,
        sha256: h.finalize().to_vec(),
    })
}

/// Feeds everything left in `r` into `h`, returning the byte count.
fn hash_reader(r: &mut impl Read, h: &mut Sha256) -> std::io::Result<u64> {
    let mut buf = vec![0u8; 64 << 10];
    let mut n = 0u64;
    loop {
        let got = r.read(&mut buf)?;
        if got == 0 {
            return Ok(n);
        }
        h.update(&buf[..got]);
        n += got as u64;
    }
}

/// Writes `total` random bytes to `w` (and the digest) from its current
/// position, one chunk at a time.
fn stream_random_bytes(w: &mut impl Write, h: &mut Sha256, total: i64) -> std::io::Result<()> {
    if total <= 0 {
        return Ok(());
    }
    let mut buf = vec![0u8; STREAM_CHUNK.min(total as usize)];
    let mut rng = rand::rng();
    let mut written = 0i64;
    while written < total {
        let n = ((total - written) as usize).min(buf.len());
        let chunk = &mut buf[..n];
        rng.fill_bytes(chunk);
        w.write_all(chunk)?;
        h.update(&*chunk);
        written += n as i64;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tonic::Code;

    pub(crate) fn new_service() -> (Service, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (Service::new(dir.path(), Options::default()).unwrap(), dir)
    }

    fn write_ram_req(key: &str, size: &str, mode: pb::WriteMode) -> pb::WriteRamRequest {
        pb::WriteRamRequest {
            key: key.into(),
            size: size.into(),
            write_mode: mode as i32,
        }
    }

    fn fill(svc: &Service, key: &str, size: &str) {
        svc.write_ram(&write_ram_req(key, size, pb::WriteMode::Truncate))
            .unwrap();
    }

    fn rotate(svc: &Service, key: &str, size: &str) {
        svc.write_ram(&write_ram_req(key, size, pb::WriteMode::OverwriteRotate))
            .unwrap();
    }

    // --- sizes_test ---

    #[test]
    fn write_ram_suffixed_size() {
        let (svc, _d) = new_service();
        fill(&svc, "a", "1Mi");
        assert_eq!(svc.ram_copy("a").len(), 1 << 20);
        assert!(svc
            .write_ram(&write_ram_req("b", "zap", pb::WriteMode::Truncate))
            .is_err());
        assert!(svc
            .write_ram(&write_ram_req("c", "-1Gi", pb::WriteMode::Truncate))
            .is_err());
        assert!(svc
            .write_ram(&write_ram_req("d", "", pb::WriteMode::Truncate))
            .is_err());
    }

    // --- ram_test ---

    #[test]
    fn read_ram_walks_array() {
        let (svc, _d) = new_service();
        fill(&svc, "m", "64Ki");
        let read = |size: &str| {
            svc.read_ram(&pb::ReadRamRequest {
                key: "m".into(),
                size: size.into(),
            })
            .unwrap()
        };
        let whole = read("");
        assert_eq!(whole.size, 64 << 10);
        assert_eq!(
            read("").checksum,
            whole.checksum,
            "walk must be deterministic"
        );
        assert_eq!(read("4Ki").size, 4 << 10);
        assert_eq!(read("1Gi").size, 64 << 10, "clamped to the array");
    }

    #[test]
    fn read_ram_errors() {
        let (svc, _d) = new_service();
        fill(&svc, "m", "4Ki");
        for (name, key, size, code) in [
            ("empty key", "", "", Code::InvalidArgument),
            ("missing key", "nope", "", Code::NotFound),
            ("bad size", "m", "lots", Code::InvalidArgument),
            ("negative size", "m", "-1", Code::InvalidArgument),
        ] {
            let err = svc
                .read_ram(&pb::ReadRamRequest {
                    key: key.into(),
                    size: size.into(),
                })
                .unwrap_err();
            assert_eq!(err.code(), code, "{name}");
        }
    }

    #[test]
    fn write_ram_rotate_advances() {
        let (svc, _d) = new_service();
        fill(&svc, "m", "8Ki");
        let before = svc.ram_copy("m");
        rotate(&svc, "m", "4Ki");
        let first = svc.ram_copy("m");
        assert_ne!(
            before[..4096],
            first[..4096],
            "first rotate left [0,4Ki) unchanged"
        );
        assert_eq!(
            before[4096..],
            first[4096..],
            "first rotate touched [4Ki,8Ki)"
        );
        rotate(&svc, "m", "4Ki");
        let second = svc.ram_copy("m");
        assert_eq!(
            first[..4096],
            second[..4096],
            "second rotate touched [0,4Ki)"
        );
        assert_ne!(
            first[4096..],
            second[4096..],
            "second rotate left [4Ki,8Ki) unchanged"
        );
    }

    #[test]
    fn write_ram_rotate_wraps() {
        let (svc, _d) = new_service();
        fill(&svc, "m", "8Ki");
        rotate(&svc, "m", "6Ki");
        let first = svc.ram_copy("m");
        rotate(&svc, "m", "6Ki");
        let second = svc.ram_copy("m");
        assert_ne!(first[6 << 10..], second[6 << 10..]);
        assert_ne!(first[..4 << 10], second[..4 << 10]);
        assert_eq!(first[4 << 10..6 << 10], second[4 << 10..6 << 10]);
    }

    #[test]
    fn write_ram_rotate_clamps_and_keeps_rotating() {
        let (svc, _d) = new_service();
        fill(&svc, "m", "4Ki");
        let before = svc.ram_copy("m");
        rotate(&svc, "m", "1Mi");
        assert_ne!(before, svc.ram_copy("m"));
        rotate(&svc, "m", "1Ki");
    }

    #[test]
    fn write_ram_rotate_needs_existing_array() {
        let (svc, _d) = new_service();
        let err = svc
            .write_ram(&write_ram_req(
                "nope",
                "4Ki",
                pb::WriteMode::OverwriteRotate,
            ))
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[test]
    fn write_ram_truncate_resets_rotate_cursor() {
        let (svc, _d) = new_service();
        fill(&svc, "m", "8Ki");
        rotate(&svc, "m", "4Ki");
        fill(&svc, "m", "8Ki");
        let before = svc.ram_copy("m");
        rotate(&svc, "m", "4Ki");
        let after = svc.ram_copy("m");
        assert_ne!(before[..4096], after[..4096], "cursor was not reset");
        assert_eq!(before[4096..], after[4096..]);
    }

    // --- service_test ---

    fn write_disk_req(key: &str, size: i32, mode: pb::WriteMode) -> pb::WriteDiskRequest {
        pb::WriteDiskRequest {
            key: key.into(),
            size,
            write_mode: mode as i32,
        }
    }

    fn read_disk_req(key: &str, mode: pb::ReadMode) -> pb::ReadDiskRequest {
        pb::ReadDiskRequest {
            key: key.into(),
            read_mode: mode as i32,
        }
    }

    #[tokio::test]
    async fn write_disk_read_disk_round_trip() {
        let (svc, _d) = new_service();
        for (name, key, size) in [
            ("zero size", "zero", 0),
            ("small size", "small", 1024),
            ("chunk unaligned size", "unaligned", (1 << 20) + 1),
        ] {
            let w = svc
                .write_disk(&write_disk_req(key, size, pb::WriteMode::Truncate))
                .await
                .unwrap();
            assert_eq!(w.size, size as i64, "{name}");
            let r = svc
                .read_disk(&read_disk_req(key, pb::ReadMode::Data))
                .await
                .unwrap();
            assert_eq!(r.size, size as i64, "{name}");
            assert_eq!(r.sha256, w.sha256, "{name}");
            assert_eq!(r.data.len(), size as usize, "{name}");
            assert_eq!(r.sha256, Sha256::digest(&r.data).to_vec(), "{name}");
            let d = svc
                .read_disk(&read_disk_req(key, pb::ReadMode::DigestOnly))
                .await
                .unwrap();
            assert_eq!(d.size, size as i64, "{name}");
            assert_eq!(d.sha256, w.sha256, "{name}");
            assert!(d.data.is_empty(), "{name}");
        }
    }

    #[tokio::test]
    async fn write_disk_truncate_produces_exact_size() {
        let (svc, d) = new_service();
        svc.write_disk(&write_disk_req("testfile", 2048, pb::WriteMode::Truncate))
            .await
            .unwrap();
        assert_eq!(
            std::fs::metadata(d.path().join("testfile")).unwrap().len(),
            2048
        );
    }

    #[tokio::test]
    async fn write_disk_overwrite_digest_matches_read_disk() {
        let (svc, _d) = new_service();
        svc.write_disk(&write_disk_req("f", 4096, pb::WriteMode::Truncate))
            .await
            .unwrap();
        let o = svc
            .write_disk(&write_disk_req("f", 1024, pb::WriteMode::Overwrite))
            .await
            .unwrap();
        assert_eq!(o.size, 4096);
        let r = svc
            .read_disk(&read_disk_req("f", pb::ReadMode::Data))
            .await
            .unwrap();
        assert_eq!(r.size, 4096);
        assert_eq!(r.sha256, o.sha256);
    }

    #[tokio::test]
    async fn read_disk_rejects_invalid_key() {
        let (svc, _d) = new_service();
        let err = svc
            .read_disk(&read_disk_req("../escape", pb::ReadMode::Data))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn read_disk_not_found() {
        let (svc, _d) = new_service();
        let err = svc
            .read_disk(&read_disk_req("nonexistent", pb::ReadMode::Data))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn write_disk_sync_writes() {
        let dir = tempfile::tempdir().unwrap();
        let svc = Service::new(
            dir.path(),
            Options {
                sync_writes: true,
                ..Default::default()
            },
        )
        .unwrap();
        let w = svc
            .write_disk(&write_disk_req("synced", 4096, pb::WriteMode::Truncate))
            .await
            .unwrap();
        assert_eq!(w.size, 4096);
    }

    #[test]
    fn open_fd_tracks_count() {
        let (svc, _d) = new_service();
        svc.open_fd(&pb::OpenFdRequest { count: 3 }).unwrap();
        assert_eq!(svc.fds.lock().len(), 3);
        svc.open_fd(&pb::OpenFdRequest { count: 1 }).unwrap();
        assert_eq!(svc.fds.lock().len(), 1);
        assert_eq!(svc.metrics.fds_open.get(), 1);
        assert_eq!(
            svc.open_fd(&pb::OpenFdRequest { count: -1 })
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    // --- session_ops_test ---

    #[tokio::test(flavor = "multi_thread")]
    async fn burn_cpu() {
        let (svc, _d) = new_service();
        let start = Instant::now();
        let resp = svc
            .burn_cpu(
                &Ctx::background(),
                &pb::BurnCpuRequest {
                    duration_ms: 50,
                    parallelism: 2,
                },
            )
            .await
            .unwrap();
        assert!(resp.iterations > 0);
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn burn_cpu_zero_duration_returns_immediately() {
        let (svc, _d) = new_service();
        let resp = svc
            .burn_cpu(&Ctx::background(), &pb::BurnCpuRequest::default())
            .await
            .unwrap();
        assert_eq!(resp.iterations, 0);
    }

    #[tokio::test]
    async fn burn_cpu_negative_duration_rejected() {
        let (svc, _d) = new_service();
        let err = svc
            .burn_cpu(
                &Ctx::background(),
                &pb::BurnCpuRequest {
                    duration_ms: -1,
                    parallelism: 0,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn ingest() {
        let (svc, d) = new_service();
        let payload = b"pretend this is a repository tarball".to_vec();
        let resp = svc
            .ingest(&pb::IngestRequest {
                key: "clone".into(),
                payload: payload.clone(),
                append: false,
            })
            .await
            .unwrap();
        assert_eq!(resp.size, payload.len() as i64);
        assert_eq!(resp.sha256, Sha256::digest(&payload).to_vec());
        assert_eq!(std::fs::read(d.path().join("clone")).unwrap(), payload);
    }

    #[tokio::test]
    async fn ingest_append_grows_file() {
        let (svc, _d) = new_service();
        let req = |p: &[u8], append| pb::IngestRequest {
            key: "deps".into(),
            payload: p.to_vec(),
            append,
        };
        svc.ingest(&req(b"aaaa", false)).await.unwrap();
        assert_eq!(svc.ingest(&req(b"bb", true)).await.unwrap().size, 6);
    }

    #[tokio::test]
    async fn ingest_rejects_path_escape() {
        let (svc, _d) = new_service();
        let err = svc
            .ingest(&pb::IngestRequest {
                key: "../escape".into(),
                payload: b"x".to_vec(),
                append: false,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}
