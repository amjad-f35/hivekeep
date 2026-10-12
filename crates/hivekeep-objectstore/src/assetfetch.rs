//! The checks every object-store plugin applies when it serves
//! `AssetProvider.FetchAsset`, whatever its storage backend:
//!
//! - [`check_object_name`] refuses an object stored under a snapshot or tag
//!   location, before anything is read.
//! - [`Stager::fetch`] downloads into a directory private to the plugin,
//!   within its capacity, enforces `max_bytes` while copying, verifies the
//!   sha256, and only then replaces the caller's file.
//!
//! A plugin parses its own URI scheme into an object name and an opener and
//! leaves the rest here. Errors are gRPC statuses with the codes the API
//! documents for FetchAsset.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use cap_std::fs::PermissionsExt as _;
use futures::future::BoxFuture;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::error::Error;
use crate::proto::v1::FetchAssetRequest;
use crate::semaphore::{WeightedPermit, WeightedSemaphore};
use crate::storage::ObjectReader;

/// The longest asset URI, as the SandboxConfig bounds its asset URLs, so
/// these checks stay cheap whatever a SandboxConfig names.
pub const MAX_URI_BYTES: usize = 2048;
/// Every download in the staging directory starts with this.
pub const STAGING_PREFIX: &str = "asset-";
/// Files written next to the caller's start with this.
pub const TEMP_PREFIX: &str = ".assetfetch-";

/// An asset URI split into its parts. The path is percent-decoded and keeps
/// its leading `/`; it is the raw path, never dot-segment-normalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetUri {
    /// The scheme.
    pub scheme: String,
    /// The bucket or host.
    pub host: String,
    /// The path.
    pub path: String,
}

/// Parses an asset URI of at most [`MAX_URI_BYTES`] with no user info, query
/// or fragment, that is not opaque. Errors never quote the URI, since user
/// info or a query may carry credentials; once it parses, the URI may be
/// named in messages.
pub fn parse_uri(uri: &str) -> Result<AssetUri, Status> {
    if uri.len() > MAX_URI_BYTES {
        return Err(Status::invalid_argument(format!(
            "asset URI is {} bytes long, more than the limit of {MAX_URI_BYTES}",
            uri.len()
        )));
    }
    let u = url::Url::parse(uri)
        .map_err(|_| Status::invalid_argument("asset URI is not a valid URI"))?;
    if !u.username().is_empty()
        || u.password().is_some()
        || u.query().is_some()
        || u.fragment().is_some()
        || u.cannot_be_a_base()
        || uri.contains('?')
        || uri.contains('#')
    {
        return Err(Status::invalid_argument(
            "asset URI must not carry user info, a query or a fragment",
        ));
    }
    let Some((_, rest)) = uri.split_once("://") else {
        return Err(Status::invalid_argument("asset URI is not a valid URI"));
    };
    let raw_path = rest.find('/').map(|i| &rest[i..]).unwrap_or("");
    let path = percent_encoding::percent_decode_str(raw_path)
        .decode_utf8()
        .map_err(|_| Status::invalid_argument("asset URI is not a valid URI"))?
        .into_owned();
    Ok(AssetUri {
        scheme: u.scheme().to_string(),
        host: u.host_str().unwrap_or_default().to_string(),
        path,
    })
}

/// Requires `object`, the name the URI points at within its bucket, to be
/// canonical and not stored under a snapshot or tag location. `base` is the
/// URI up to the object name; the error names the location as
/// `base/<prefix>`.
///
/// Snapshot files, manifests and tags are never assets, however their
/// sha256 came to be known, so they are refused with `PERMISSION_DENIED`.
/// Every root a snapshot location may have is matched, so the check holds
/// whatever prefix the plugin stores snapshots under.
pub fn check_object_name(uri: &str, base: &str, object: &str) -> Result<(), Status> {
    if object.is_empty() {
        return Err(Status::invalid_argument(format!(
            "asset URI {uri} must name an object"
        )));
    }
    // Snapshot names are always canonical; a non-canonical name could still
    // resolve to one on a backend that cleans or trims names.
    if object.contains('\\') {
        return Err(Status::invalid_argument(format!(
            "asset URI {uri} must not contain '\\'"
        )));
    }
    let segments: Vec<&str> = object.split('/').collect();
    for seg in &segments {
        if seg.is_empty() || seg.trim() != *seg || seg.ends_with('.') {
            return Err(Status::invalid_argument(format!(
                "asset URI {uri} has an empty, '.'-terminated or space-padded path segment"
            )));
        }
    }
    let n = snapshot_prefix_len(&segments);
    if n > 0 {
        return Err(Status::permission_denied(format!(
            "asset URI {uri} is stored under snapshot {}/{}; assets must not live under a snapshot location",
            base.trim_end_matches('/'),
            segments[..n].join("/")
        )));
    }
    Ok(())
}

/// The length of a prefix of `segments` naming a snapshot or a tag, the way
/// snapshot URIs are parsed, or 0:
///
/// ```text
/// <root>/hives/<hive>/actors/<uid>/snapshots/<name>
/// <root>/hives/<hive>/tags/<uid>
/// ```
///
/// Each segment is read a bounded number of times, so this is linear.
pub fn snapshot_prefix_len(segments: &[&str]) -> usize {
    let name = hivekeep_core::names::is_valid_resource_name;
    for (i, seg) in segments.iter().enumerate() {
        if *seg != "hives" {
            continue;
        }
        let owner = &segments[i + 1..];
        if owner.len() >= 3 && owner[1] == "tags" && name(owner[0]) && name(owner[2]) {
            return i + 4;
        }
        if owner.len() >= 5
            && owner[1] == "actors"
            && owner[3] == "snapshots"
            && name(owner[0])
            && name(owner[2])
            && name(owner[4])
        {
            return i + 6;
        }
    }
    0
}

/// Opens the asset for reading and returns its size as the storage
/// reported it, or a negative size if it reported none. [`Stager::fetch`]
/// may call it twice, when it has to wait for staging space.
pub type OpenFn<'a> =
    dyn Fn() -> BoxFuture<'a, Result<(ObjectReader, i64), Error>> + Send + Sync + 'a;

/// Maps an error from opening, downloading or a local file to a status; it
/// must keep a cancellation's code.
pub type ToStatus<'a> = dyn Fn(Error) -> Status + Send + Sync + 'a;

/// Copies a verified asset into the file that replaces the caller's.
pub type CopyFn = dyn Fn(&mut dyn Write, &mut dyn Read) -> io::Result<u64> + Send + Sync;

/// Downloads assets into a staging directory private to the plugin and
/// copies each to the caller's file only once its size and sha256 check out,
/// so the caller never reads bytes it did not pin, such as a snapshot's.
///
/// Downloads in flight never hold more than the capacity. Each opens its
/// object first, so a missing object, or one larger than `max_bytes` or the
/// capacity, fails without waiting for space; it then reserves the object's
/// size, or as much as `max_bytes` allows if no size was reported, and waits
/// for that much with the object closed meanwhile.
pub struct Stager {
    root: PathBuf,
    staging_dir: PathBuf,
    capacity: i64,
    reserved: Arc<WeightedSemaphore>,
    copy: Option<Arc<CopyFn>>,
}

/// Lexically normalizes an absolute path.
fn clean(p: &Path) -> PathBuf {
    let mut out = PathBuf::from("/");
    for c in p.components() {
        match c {
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(s) => out.push(s),
            _ => {}
        }
    }
    out
}

/// `path` relative to `root`, if it is strictly below it.
fn rel_below(root: &Path, path: &Path) -> Option<PathBuf> {
    let rel = clean(path).strip_prefix(root).ok()?.to_path_buf();
    (!rel.as_os_str().is_empty()).then_some(rel)
}

fn io_status(to_status: &ToStatus<'_>, ctx: String, e: io::Error) -> Status {
    to_status(Error::from(e).context(ctx))
}

impl Stager {
    /// A stager writing caller files only below `root` and staging in
    /// `staging_dir`, which must not be below `root`, holding at most
    /// `capacity` bytes there at once. The staging directory is private to
    /// the plugin, so downloads an earlier run left there are removed.
    pub fn new(root: &Path, staging_dir: &Path, capacity: i64) -> Result<Self, String> {
        if !root.is_absolute() {
            return Err(format!(
                "root {:?} is not an absolute path",
                root.display().to_string()
            ));
        }
        if !staging_dir.is_absolute() {
            return Err(format!(
                "asset staging directory {:?} is not an absolute path",
                staging_dir.display().to_string()
            ));
        }
        if capacity <= 0 {
            return Err(format!("asset staging capacity {capacity} is not positive"));
        }
        let (root, staging) = (clean(root), clean(staging_dir));
        if staging.starts_with(&root) {
            return Err(format!(
                "asset staging directory {:?} is inside root {:?}, where the caller could read unverified downloads",
                staging.display().to_string(),
                root.display().to_string()
            ));
        }
        let entries = std::fs::read_dir(&staging)
            .map_err(|e| format!("while reading asset staging directory: {e}"))?;
        for e in entries.flatten() {
            if e.file_name().to_string_lossy().starts_with(STAGING_PREFIX) {
                std::fs::remove_file(e.path()).map_err(|e| {
                    format!(
                        "while removing a previous download from the asset staging directory: {e}"
                    )
                })?;
            }
        }
        Ok(Stager {
            root,
            staging_dir: staging,
            capacity,
            reserved: WeightedSemaphore::new(capacity as u64),
            copy: None,
        })
    }

    /// Replaces the final copy, for tests that fail it.
    pub fn with_copy(mut self, copy: Arc<CopyFn>) -> Self {
        self.copy = Some(copy);
        self
    }

    /// The root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Takes `n` bytes of capacity if free, for tests.
    pub fn try_reserve(&self, n: i64) -> Option<WeightedPermit> {
        self.reserved.try_acquire(n as u64)
    }

    fn write_rel(&self, path: &str) -> Result<PathBuf, Status> {
        let p = Path::new(path);
        if !p.is_absolute() {
            return Err(Status::invalid_argument(format!(
                "write path {path:?} is not absolute"
            )));
        }
        rel_below(&self.root, p).ok_or_else(|| {
            Status::invalid_argument(format!(
                "write path {path:?} is not below {:?}",
                self.root.display().to_string()
            ))
        })
    }

    /// Checks `req`'s sha256, `max_bytes` and `write_path`, downloads the
    /// asset `open` returns into staging and, if it is at most `max_bytes`
    /// long and hashes to `sha256`, swaps it into the existing file at
    /// `write_path`. `uri` is the request's URI once safe to quote.
    /// Cancelling `cancel` ends a wait for staging space with CANCELLED.
    ///
    /// The file at `write_path` keeps its old content on any error; callers
    /// open it after this returns, since a descriptor opened before reads
    /// the old file. The caller checks the URI first, so a refused request
    /// reads nothing.
    pub async fn fetch(
        &self,
        cancel: &CancellationToken,
        req: &FetchAssetRequest,
        uri: &str,
        open: &OpenFn<'_>,
        to_status: &ToStatus<'_>,
    ) -> Result<(), Status> {
        let want = &req.sha256;
        if want.len() != 64
            || !want
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Status::invalid_argument(format!(
                "sha256 {want:?} is not 64 lower-case hex characters"
            )));
        }
        let want_sum = hex::decode(want).expect("checked hex");
        if req.max_bytes <= 0 {
            return Err(Status::invalid_argument(format!(
                "max_bytes {} is not positive",
                req.max_bytes
            )));
        }
        let rel = self.write_rel(&req.write_path)?;
        let root = Dir::open_ambient_dir(&self.root, ambient_authority()).map_err(|e| {
            io_status(
                to_status,
                format!("while opening {}", self.root.display()),
                e,
            )
        })?;
        // Checked before downloading, so a bad request costs no transfer. A
        // symlink is never the caller's file, a directory cannot be replaced
        // by a file, and a FIFO is no place for an asset.
        let perm = match root.symlink_metadata(&rel) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(Status::failed_precondition(format!(
                    "write path {:?} does not exist",
                    req.write_path
                )))
            }
            Err(e) => {
                return Err(io_status(
                    to_status,
                    format!("while inspecting write path {:?}", req.write_path),
                    e,
                ))
            }
            Ok(m) if !m.is_file() => {
                return Err(Status::failed_precondition(format!(
                    "write path {:?} is not a regular file",
                    req.write_path
                )))
            }
            Ok(m) => m.permissions().mode() & 0o7777,
        };
        let max_bytes = req.max_bytes;
        let (mut rc, size) = self.open(open, uri, max_bytes, to_status).await?;
        let reserve = if size >= 0 {
            size
        } else {
            max_bytes.min(self.capacity)
        };
        let _permit = match self.reserved.try_acquire(reserve as u64) {
            Some(p) => p,
            None => {
                // Waiting can take as long as other downloads do; closing the
                // object meanwhile frees its connection and read-ahead.
                let _ = rc.close();
                drop(rc);
                let permit = tokio::select! {
                    p = self.reserved.acquire(reserve as u64) => p,
                    _ = cancel.cancelled() => {
                        return Err(to_status(Error::canceled().context("while waiting for asset staging space")));
                    }
                };
                let (again, reopened) = self.open(open, uri, max_bytes, to_status).await?;
                if reopened != size {
                    let mut again = again;
                    let _ = again.close();
                    return Err(Status::unavailable(format!(
                        "asset {uri} changed while waiting for staging space"
                    )));
                }
                rc = again;
                permit
            }
        };
        let staged = self
            .stage(rc, uri, max_bytes, size, reserve, &want_sum, to_status)
            .await?;
        let (staged_file, staged_path) = staged;
        let copy = self.copy.clone();
        let shown = req.write_path.clone();
        let uri_owned = uri.to_string();
        let res = tokio::task::spawn_blocking(move || {
            replace(&root, &rel, perm, staged_file, &uri_owned, &shown, copy)
        })
        .await
        .map_err(|e| Status::internal(format!("replace task failed: {e}")))?;
        let _ = std::fs::remove_file(&staged_path);
        res.map_err(|e| match e {
            ReplaceError::Status(s) => s,
            ReplaceError::Io(ctx, e) => io_status(to_status, ctx, e),
        })
    }

    async fn open(
        &self,
        open: &OpenFn<'_>,
        uri: &str,
        max_bytes: i64,
        to_status: &ToStatus<'_>,
    ) -> Result<(ObjectReader, i64), Status> {
        let (mut rc, size) = open()
            .await
            .map_err(|e| to_status(e.context(format!("while opening asset {uri}"))))?;
        if size > max_bytes {
            let _ = rc.close();
            return Err(Status::failed_precondition(format!(
                "asset {uri} is {size} bytes, larger than max_bytes {max_bytes}"
            )));
        }
        if size > self.capacity {
            let _ = rc.close();
            return Err(Status::failed_precondition(format!(
                "asset {uri} is {size} bytes, larger than the plugin's asset staging capacity of {} bytes",
                self.capacity
            )));
        }
        Ok((rc, size.max(-1)))
    }

    /// Downloads the asset into a new staging file and returns it rewound,
    /// once its size and sha256 check out. `reserve` bounds what is written.
    #[allow(clippy::too_many_arguments)]
    async fn stage(
        &self,
        mut rc: ObjectReader,
        uri: &str,
        max_bytes: i64,
        size: i64,
        reserve: i64,
        want: &[u8],
        to_status: &ToStatus<'_>,
    ) -> Result<(File, PathBuf), Status> {
        let tmp = tempfile::Builder::new()
            .prefix(STAGING_PREFIX)
            .tempfile_in(&self.staging_dir)
            .map_err(|e| io_status(to_status, "while creating staging file".into(), e))?;
        let (file, path) = tmp
            .keep()
            .map_err(|e| io_status(to_status, "while creating staging file".into(), e.error))?;
        let cleanup = scopeguard(path.clone());
        let res = self
            .stage_into(
                &mut rc, file, uri, max_bytes, size, reserve, want, to_status,
            )
            .await;
        let _ = rc.close();
        match res {
            Ok(f) => {
                cleanup.disarm();
                Ok((f, path))
            }
            Err(e) => Err(e),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_into(
        &self,
        rc: &mut ObjectReader,
        file: File,
        uri: &str,
        max_bytes: i64,
        size: i64,
        reserve: i64,
        want: &[u8],
        to_status: &ToStatus<'_>,
    ) -> Result<File, Status> {
        let mut out = tokio::fs::File::from_std(file);
        let mut hasher = Sha256::new();
        let mut n: i64 = 0;
        let mut buf = vec![0u8; 256 << 10];
        let dl = |e: io::Error| io_status(to_status, format!("while downloading asset {uri}"), e);
        while n < reserve {
            let want_n = ((reserve - n) as usize).min(buf.len());
            let k = rc.read(&mut buf[..want_n]).await.map_err(dl)?;
            if k == 0 {
                break;
            }
            tokio::io::AsyncWriteExt::write_all(&mut out, &buf[..k])
                .await
                .map_err(dl)?;
            hasher.update(&buf[..k]);
            n += k as i64;
        }
        if n == reserve {
            // One more byte, read but not staged, tells a full-length object
            // from an over-long one.
            let mut probe = [0u8; 1];
            match rc.read(&mut probe).await {
                Ok(1) if size >= 0 => {
                    return Err(Status::unavailable(format!(
                        "asset {uri} is longer than the {size} bytes its storage reported"
                    )))
                }
                Ok(1) if reserve < max_bytes => {
                    return Err(Status::failed_precondition(format!(
                        "asset {uri} is larger than the plugin's asset staging capacity of {reserve} bytes"
                    )))
                }
                Ok(1) => {
                    return Err(Status::failed_precondition(format!(
                        "asset {uri} is larger than max_bytes {max_bytes}"
                    )))
                }
                Ok(_) => {}
                Err(e) => return Err(dl(e)),
            }
        }
        let got = hasher.finalize();
        if got.as_slice() != want {
            return Err(Status::failed_precondition(format!(
                "asset {uri} has sha256 {}, want {}",
                hex::encode(got),
                hex::encode(want)
            )));
        }
        tokio::io::AsyncWriteExt::flush(&mut out)
            .await
            .map_err(dl)?;
        let mut f = out.into_std().await;
        f.seek(SeekFrom::Start(0))
            .map_err(|e| io_status(to_status, "while rewinding staging file".into(), e))?;
        Ok(f)
    }
}

/// Removes a path on drop unless disarmed.
struct PathGuard(Option<PathBuf>);

fn scopeguard(p: PathBuf) -> PathGuard {
    PathGuard(Some(p))
}

impl PathGuard {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for PathGuard {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            let _ = std::fs::remove_file(p);
        }
    }
}

enum ReplaceError {
    Status(Status),
    Io(String, io::Error),
}

/// Writes `staged` to a new file of mode `perm` next to `rel` below `root`
/// and swaps it into `rel`'s place, so the file holds either its old content
/// or all of `staged`. It never creates `rel`: if the caller removed it
/// meanwhile, this fails and leaves nothing behind.
fn replace(
    root: &Dir,
    rel: &Path,
    perm: u32,
    mut staged: File,
    uri: &str,
    shown: &str,
    copy: Option<Arc<CopyFn>>,
) -> Result<(), ReplaceError> {
    let dir_rel = rel
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let base = rel.file_name().expect("a path below root has a name");
    let dir = root.open_dir(dir_rel).map_err(|e| {
        ReplaceError::Io(
            format!("while opening the directory of write path {shown:?}"),
            e,
        )
    })?;
    // A fixed-length random name, so the caller's name may be as long as
    // the filesystem allows.
    let name = format!("{TEMP_PREFIX}{}", hex::encode(rand::random::<[u8; 12]>()));
    let mut opts = cap_std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    let tmp = dir.open_with(&name, &opts).map_err(|e| {
        ReplaceError::Io(
            format!("while creating a file next to write path {shown:?}"),
            e,
        )
    })?;
    // After a swap the temporary name holds the caller's old file; either
    // way it goes.
    struct Remove<'a>(&'a Dir, &'a str);
    impl Drop for Remove<'_> {
        fn drop(&mut self) {
            let _ = self.0.remove_file(self.1);
        }
    }
    let _remove = Remove(&dir, &name);
    let mut tmp = tmp.into_std();
    tmp.set_permissions(std::fs::Permissions::from_mode(perm))
        .map_err(|e| {
            ReplaceError::Io(
                format!("while setting the mode of a file next to write path {shown:?}"),
                e,
            )
        })?;
    let copied = match &copy {
        Some(c) => c(&mut tmp, &mut staged),
        None => io::copy(&mut staged, &mut tmp),
    };
    copied.map_err(|e| {
        ReplaceError::Io(
            format!("while writing asset {uri} next to write path {shown:?}"),
            e,
        )
    })?;
    tmp.sync_all().map_err(|e| {
        ReplaceError::Io(
            format!("while closing a file next to write path {shown:?}"),
            e,
        )
    })?;
    drop(tmp);
    match exchange(&dir, &name, &base.to_string_lossy()) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(ReplaceError::Status(
            Status::failed_precondition(format!("write path {shown:?} does not exist")),
        )),
        Err(e) => Err(ReplaceError::Io(
            format!("while replacing write path {shown:?}"),
            e,
        )),
    }
}

/// Atomically swaps entries `a` and `b` of `dir`; fails with NotFound if
/// either is missing, so it never creates `b`.
#[cfg(target_os = "linux")]
fn exchange(dir: &Dir, a: &str, b: &str) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let ca = std::ffi::CString::new(a)?;
    let cb = std::ffi::CString::new(b)?;
    let fd = dir.as_raw_fd();
    // SAFETY: valid descriptor and NUL-terminated names.
    let r = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            fd,
            ca.as_ptr(),
            fd,
            cb.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    if r != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Atomically swaps entries `a` and `b` of `dir`.
#[cfg(target_os = "macos")]
fn exchange(dir: &Dir, a: &str, b: &str) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let ca = std::ffi::CString::new(a)?;
    let cb = std::ffi::CString::new(b)?;
    let fd = dir.as_raw_fd();
    // SAFETY: valid descriptor and NUL-terminated names.
    let r = unsafe { libc::renameatx_np(fd, ca.as_ptr(), fd, cb.as_ptr(), libc::RENAME_SWAP) };
    if r != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Moves `a` over `b` if `b` exists. Unlike the swapping versions, a `b`
/// removed between the check and the rename is recreated.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn exchange(dir: &Dir, a: &str, b: &str) -> io::Result<()> {
    dir.symlink_metadata(b)?;
    dir.rename(a, dir, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::storage::{BytesReader, ObjectRead};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    const BUCKET: &str = "bucket";
    const UNTOUCHED: &str = "the caller's original bytes";
    const KERNEL: &str = "kernel bytes";

    #[test]
    fn parse_uri_cases() {
        for (uri, ok) in [
            ("gs://bucket/kata-assets/vmlinux".to_string(), true),
            (
                "https://acct.blob.core.windows.net/assets/vmlinux".into(),
                true,
            ),
            (
                "https://acct.blob.core.windows.net/assets/vmlinux?sig=secret".into(),
                false,
            ),
            (
                "https://acct.blob.core.windows.net/assets/vmlinux?".into(),
                false,
            ),
            (
                "https://user:secret@acct.blob.core.windows.net/assets/vmlinux".into(),
                false,
            ),
            (
                "https://acct.blob.core.windows.net/assets/vmlinux#frag".into(),
                false,
            ),
            ("mailto:secret@example.com".into(), false),
            ("https://acct\x7f/x".into(), false),
            (format!("gs://bucket/{}", "x".repeat(MAX_URI_BYTES)), false),
        ] {
            let r = parse_uri(&uri);
            assert_eq!(r.is_ok(), ok, "{:.60}: {r:?}", uri);
            if let Err(e) = r {
                assert_eq!(e.code(), tonic::Code::InvalidArgument);
                assert!(!e.message().contains("secret"), "{e:?} quotes credentials");
            }
        }
    }

    #[test]
    fn check_object_name_cases() {
        const BASE: &str = "https://acct.blob.core.windows.net/assets";
        use tonic::Code::*;
        for (object, want) in [
            ("kata-assets/vmlinux", Ok),
            ("vmlinux", Ok),
            ("hives/readme", Ok),
            ("root/hives/team-a/actors/uid1", Ok),
            ("root/hives/Not_A_Name/tags/tag1/manifest.json", Ok),
            ("", InvalidArgument),
            ("a//b", InvalidArgument),
            ("a/b/", InvalidArgument),
            ("/a", InvalidArgument),
            ("a/../b", InvalidArgument),
            ("a/./b", InvalidArgument),
            ("a/b.", InvalidArgument),
            ("a/ b", InvalidArgument),
            ("a\\b", InvalidArgument),
            ("hivekeep/hives/team-a/actors/uid1/snapshots/snap1/pages.img", PermissionDenied),
            ("hivekeep/hives/team-a/actors/uid1/snapshots/snap1", PermissionDenied),
            ("hives/team-a/actors/uid1/snapshots/snap1/manifest.json", PermissionDenied),
            ("x/y/hives/hivekeep-golden/tags/d33a8f2f-c6ce-45b7-9659-69d1fee12fa5/checkpoint.img", PermissionDenied),
            ("x/hives/team-a/tags/tag1", PermissionDenied),
        ] {
            let got = check_object_name(&format!("{BASE}/{object}"), BASE, object)
                .map(|_| Ok)
                .unwrap_or_else(|s| s.code());
            assert_eq!(got, want, "{object:?}");
        }
        let e = check_object_name(
            &format!("{BASE}/s/hives/a/tags/t/f"),
            BASE,
            "s/hives/a/tags/t/f",
        )
        .unwrap_err();
        assert!(
            e.message().contains(&format!("{BASE}/s/hives/a/tags/t;")),
            "{e:?}"
        );
    }

    #[test]
    fn snapshot_prefix_len_matches_snapshot_uri_parsing() {
        let alphabet = ["hives", "actors", "tags", "snapshots", "Not_A_Name"];
        let parses_at = |segs: &[&str]| -> usize {
            (1..=segs.len())
                .find(|&i| {
                    hivekeep_core::snapshot::SnapshotUri::parse(&format!(
                        "gs://{BUCKET}/{}",
                        segs[..i].join("/")
                    ))
                    .is_ok()
                })
                .unwrap_or(0)
        };
        fn walk(segs: &mut Vec<&'static str>, alphabet: &[&'static str], check: &dyn Fn(&[&str])) {
            if !segs.is_empty() {
                check(segs);
            }
            if segs.len() == 7 {
                return;
            }
            for &a in alphabet {
                segs.push(a);
                walk(segs, alphabet, check);
                segs.pop();
            }
        }
        let check = |segs: &[&str]| {
            let (got, want) = (snapshot_prefix_len(segs), parses_at(segs));
            assert_eq!(got > 0, want > 0, "{segs:?}: {got} vs parse at {want}");
        };
        walk(&mut Vec::new(), &alphabet, &check);
    }

    #[test]
    fn snapshot_prefix_len_is_linear() {
        let unit = [
            "hives",
            "team-a",
            "actors",
            "uid1",
            "snapshots",
            "Not_A_Name",
            "hives",
            "team-a",
            "hives",
        ];
        let mut segs: Vec<&str> = Vec::with_capacity((1 << 20) + 6);
        while segs.len() < 1 << 20 {
            segs.extend_from_slice(&unit);
        }
        assert_eq!(snapshot_prefix_len(&segs), 0);
        segs.extend_from_slice(&["hives", "team-a", "tags", "tag1"]);
        assert_eq!(snapshot_prefix_len(&segs), segs.len());
    }

    pub(crate) fn sum(s: &str) -> String {
        hex::encode(Sha256::digest(s.as_bytes()))
    }

    fn req(sha: &str, write_path: &str, max_bytes: i64) -> FetchAssetRequest {
        FetchAssetRequest {
            asset_uri: String::new(),
            sha256: sha.into(),
            write_path: write_path.into(),
            max_bytes,
        }
    }

    struct Fixture {
        _root: tempfile::TempDir,
        staging_dir: tempfile::TempDir,
        stager: Stager,
        dst: PathBuf,
    }

    fn fixture_with(capacity: i64) -> Fixture {
        let root = tempfile::tempdir().unwrap();
        let staging_dir = tempfile::tempdir().unwrap();
        let stager = Stager::new(root.path(), staging_dir.path(), capacity).unwrap();
        let dst = stager.root().join("static-files").join("asset-download-1");
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        std::fs::write(&dst, UNTOUCHED).unwrap();
        Fixture {
            _root: root,
            staging_dir,
            stager,
            dst,
        }
    }

    fn fixture() -> Fixture {
        fixture_with(1 << 30)
    }

    fn with_copy(f: Fixture, copy: Arc<CopyFn>) -> Fixture {
        Fixture {
            stager: f.stager.with_copy(copy),
            ..f
        }
    }

    impl Fixture {
        fn assert_dst(&self, want: &str) {
            assert_eq!(std::fs::read_to_string(&self.dst).unwrap(), want);
            self.assert_no_temp_files();
        }
        fn assert_no_temp_files(&self) {
            for e in std::fs::read_dir(self.dst.parent().unwrap())
                .unwrap()
                .flatten()
            {
                assert!(
                    !e.file_name().to_string_lossy().starts_with(TEMP_PREFIX),
                    "left {:?}",
                    e.file_name()
                );
            }
        }
        fn assert_staging_empty(&self) {
            assert_eq!(
                std::fs::read_dir(self.staging_dir.path()).unwrap().count(),
                0,
                "staging not empty"
            );
        }
        fn new_caller_file(&self, name: &str) -> PathBuf {
            let p = self.dst.parent().unwrap().join(name);
            std::fs::write(&p, "").unwrap();
            p
        }
    }

    /// Serves `content`, counting opens and closes.
    #[derive(Default)]
    struct Opener {
        content: String,
        size: Option<i64>,
        err: Option<ErrorKind>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
    }

    struct Counted(BytesReader, Arc<AtomicUsize>, bool);

    impl tokio::io::AsyncRead for Counted {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl ObjectRead for Counted {
        fn close(&mut self) -> io::Result<()> {
            if !self.2 {
                self.2 = true;
                self.1.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }
    }

    impl Drop for Counted {
        fn drop(&mut self) {
            let _ = self.close();
        }
    }

    impl Opener {
        fn new(content: &str) -> Self {
            Opener {
                content: content.into(),
                ..Default::default()
            }
        }
        fn sized(mut self, n: i64) -> Self {
            self.size = Some(n);
            self
        }
        fn failing(kind: ErrorKind) -> Self {
            Opener {
                err: Some(kind),
                ..Default::default()
            }
        }
        fn open(&self) -> BoxFuture<'_, Result<(ObjectReader, i64), Error>> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            let r = match &self.err {
                Some(k) => Err(Error::new(k.clone(), "backend error")),
                None => Ok((
                    Box::new(Counted(
                        BytesReader::new(self.content.clone().into_bytes()),
                        self.closes.clone(),
                        false,
                    )) as ObjectReader,
                    self.size.unwrap_or(-1),
                )),
            };
            Box::pin(async move { r })
        }
    }

    /// Maps a transport error to Unavailable, absence to NotFound,
    /// cancellation to Cancelled and the rest to Internal.
    pub(crate) fn test_status(e: Error) -> Status {
        match e.kind() {
            ErrorKind::Transport => Status::unavailable(e.to_string()),
            ErrorKind::NotFound => Status::not_found(e.to_string()),
            ErrorKind::Canceled => Status::cancelled(e.to_string()),
            _ => Status::internal(e.to_string()),
        }
    }

    async fn run(f: &Fixture, r: &FetchAssetRequest, o: &Opener) -> Result<(), Status> {
        let open = || o.open();
        f.stager
            .fetch(
                &CancellationToken::new(),
                r,
                "gs://bucket/vmlinux",
                &open,
                &test_status,
            )
            .await
    }

    fn dst(f: &Fixture) -> String {
        f.dst.display().to_string()
    }

    #[tokio::test]
    async fn stager_fetch() {
        {
            let f = fixture();
            run(
                &f,
                &req(&sum(KERNEL), &dst(&f), KERNEL.len() as i64),
                &Opener::new(KERNEL),
            )
            .await
            .unwrap();
            f.assert_dst(KERNEL);
            f.assert_staging_empty();
        }
        use tonic::Code::*;
        type MkReq = Box<dyn Fn(&Fixture) -> FetchAssetRequest>;
        let cases: Vec<(&str, MkReq, Opener, tonic::Code, usize)> = vec![
            (
                "sha256 mismatch",
                Box::new(|f| req(&sum("other"), &dst(f), 1 << 20)),
                Opener::new(KERNEL),
                FailedPrecondition,
                1,
            ),
            (
                "one byte over max_bytes",
                Box::new(|f| req(&sum(KERNEL), &dst(f), KERNEL.len() as i64 - 1)),
                Opener::new(KERNEL),
                FailedPrecondition,
                1,
            ),
            (
                "reported size over max_bytes fails before reading",
                Box::new(|f| req(&sum(KERNEL), &dst(f), KERNEL.len() as i64 - 1)),
                Opener::new(KERNEL).sized(KERNEL.len() as i64),
                FailedPrecondition,
                1,
            ),
            (
                "reported size over the staging capacity fails before reading",
                Box::new(|f| req(&sum(KERNEL), &dst(f), 1 << 40)),
                Opener::new(KERNEL).sized(1 << 31),
                FailedPrecondition,
                1,
            ),
            (
                "object longer than its reported size",
                Box::new(|f| req(&sum(KERNEL), &dst(f), 1 << 20)),
                Opener::new(KERNEL).sized(KERNEL.len() as i64 - 1),
                Unavailable,
                1,
            ),
            (
                "open error goes through to_status",
                Box::new(|f| req(&sum(KERNEL), &dst(f), 1 << 20)),
                Opener::failing(ErrorKind::Transport),
                Unavailable,
                1,
            ),
            (
                "upper-case sha256",
                Box::new(|f| req(&sum(KERNEL).to_uppercase(), &dst(f), 1 << 20)),
                Opener::new(KERNEL),
                InvalidArgument,
                0,
            ),
            (
                "zero max_bytes",
                Box::new(|f| req(&sum(KERNEL), &dst(f), 0)),
                Opener::new(KERNEL),
                InvalidArgument,
                0,
            ),
            (
                "relative write path",
                Box::new(|_| req(&sum(KERNEL), "static-files/x", 1 << 20)),
                Opener::new(KERNEL),
                InvalidArgument,
                0,
            ),
            (
                "write path outside root",
                Box::new(|f| {
                    req(
                        &sum(KERNEL),
                        &f.staging_dir.path().join("x").display().to_string(),
                        1 << 20,
                    )
                }),
                Opener::new(KERNEL),
                InvalidArgument,
                0,
            ),
            (
                "missing write path",
                Box::new(|f| req(&sum(KERNEL), &format!("{}-missing", dst(f)), 1 << 20)),
                Opener::new(KERNEL),
                FailedPrecondition,
                0,
            ),
            (
                "write path is a directory",
                Box::new(|f| {
                    req(
                        &sum(KERNEL),
                        &f.dst.parent().unwrap().display().to_string(),
                        1 << 20,
                    )
                }),
                Opener::new(KERNEL),
                FailedPrecondition,
                0,
            ),
            (
                "write path is a symlink",
                Box::new(|f| {
                    let link = format!("{}-link", dst(f));
                    std::os::unix::fs::symlink(&f.dst, &link).unwrap();
                    req(&sum(KERNEL), &link, 1 << 20)
                }),
                Opener::new(KERNEL),
                FailedPrecondition,
                0,
            ),
            (
                "write path is a FIFO",
                Box::new(|f| {
                    let fifo = format!("{}-fifo", dst(f));
                    let c = std::ffi::CString::new(fifo.clone()).unwrap();
                    // SAFETY: a valid path.
                    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
                    req(&sum(KERNEL), &fifo, 1 << 20)
                }),
                Opener::new(KERNEL),
                FailedPrecondition,
                0,
            ),
        ];
        for (name, mk, opener, want, want_opens) in cases {
            let f = fixture();
            let r = mk(&f);
            let got = run(&f, &r, &opener).await.unwrap_err();
            assert_eq!(got.code(), want, "{name}: {got:?}");
            let opens = opener.opens.load(Ordering::SeqCst);
            assert_eq!(opens, want_opens, "{name}: opens");
            if opener.err.is_none() {
                assert_eq!(
                    opener.closes.load(Ordering::SeqCst),
                    opens,
                    "{name}: closes"
                );
            }
            f.assert_dst(UNTOUCHED);
            f.assert_staging_empty();
        }
        {
            let f = fixture();
            run(
                &f,
                &req(&sum(KERNEL), &dst(&f), 1 << 20),
                &Opener::new(KERNEL).sized(KERNEL.len() as i64),
            )
            .await
            .unwrap();
            f.assert_dst(KERNEL);
            f.assert_staging_empty();
        }
        {
            let f = fixture();
            let o = Opener::failing(ErrorKind::Canceled);
            let e = run(&f, &req(&sum(KERNEL), &dst(&f), 1 << 20), &o)
                .await
                .unwrap_err();
            assert_eq!(e.code(), Cancelled);
            f.assert_dst(UNTOUCHED);
            f.assert_staging_empty();
        }
    }

    #[test]
    fn new_stager_rejects_staging_below_root() {
        let root = tempfile::tempdir().unwrap();
        let r = root.path();
        for staging in [
            r.to_path_buf(),
            r.join("staging"),
            PathBuf::from(format!("{}/./staging/..", r.display())),
        ] {
            let _ = std::fs::create_dir_all(&staging);
            assert!(
                Stager::new(r, &staging, 1 << 30).is_err(),
                "{}",
                staging.display()
            );
        }
        let other = tempfile::tempdir().unwrap();
        assert!(Stager::new(Path::new("relative"), other.path(), 1 << 30).is_err());
        assert!(Stager::new(r, Path::new("relative"), 1 << 30).is_err());
        assert!(Stager::new(r, other.path(), 1 << 30).is_ok());
    }

    #[test]
    fn new_stager_rejects_non_positive_capacity() {
        for capacity in [0, -1] {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            assert!(Stager::new(a.path(), b.path(), capacity).is_err());
        }
    }

    #[test]
    fn new_stager_removes_previous_downloads() {
        let staging = tempfile::tempdir().unwrap();
        for name in ["asset-123", "asset-456", "unrelated"] {
            std::fs::write(staging.path().join(name), "x").unwrap();
        }
        let root = tempfile::tempdir().unwrap();
        Stager::new(root.path(), staging.path(), 1 << 30).unwrap();
        let left: Vec<String> = std::fs::read_dir(staging.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(left, vec!["unrelated"]);
    }

    #[tokio::test]
    async fn stager_fetch_replaces_write_path_atomically() {
        // A failed copy leaves the write path untouched.
        {
            let f = with_copy(
                fixture(),
                Arc::new(|dst: &mut dyn Write, src: &mut dyn Read| {
                    let mut b = Vec::new();
                    src.read_to_end(&mut b)?;
                    dst.write_all(&b[..b.len() / 2])?;
                    Err(io::Error::other("no space left on device"))
                }),
            );
            let e = run(
                &f,
                &req(&sum(KERNEL), &dst(&f), 1 << 20),
                &Opener::new(KERNEL),
            )
            .await
            .unwrap_err();
            assert_eq!(e.code(), tonic::Code::Internal, "{e:?}");
            f.assert_dst(UNTOUCHED);
            f.assert_staging_empty();
        }
        // A write path removed during the copy is not recreated.
        {
            let f = fixture();
            let target = f.dst.clone();
            let f = with_copy(
                f,
                Arc::new(move |dst: &mut dyn Write, src: &mut dyn Read| {
                    std::fs::remove_file(&target)?;
                    io::copy(src, dst)
                }),
            );
            let e = run(
                &f,
                &req(&sum(KERNEL), &dst(&f), 1 << 20),
                &Opener::new(KERNEL),
            )
            .await
            .unwrap_err();
            assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e:?}");
            assert!(!f.dst.exists(), "write path exists after the call");
            f.assert_no_temp_files();
            f.assert_staging_empty();
        }
        // Keeps the write path's mode.
        {
            let f = fixture();
            std::fs::set_permissions(&f.dst, std::fs::Permissions::from_mode(0o640)).unwrap();
            run(
                &f,
                &req(&sum(KERNEL), &dst(&f), 1 << 20),
                &Opener::new(KERNEL),
            )
            .await
            .unwrap();
            f.assert_dst(KERNEL);
            assert_eq!(
                std::fs::metadata(&f.dst).unwrap().permissions().mode() & 0o777,
                0o640
            );
        }
        // The longest file name.
        {
            let mut f = fixture();
            f.dst = f.dst.parent().unwrap().join("a".repeat(255));
            std::fs::write(&f.dst, UNTOUCHED).unwrap();
            run(
                &f,
                &req(&sum(KERNEL), &dst(&f), 1 << 20),
                &Opener::new(KERNEL),
            )
            .await
            .unwrap();
            f.assert_dst(KERNEL);
        }
    }

    /// A reader that announces its first read and then blocks until released.
    struct Gated {
        inner: BytesReader,
        started: bool,
        reading: tokio::sync::mpsc::UnboundedSender<()>,
        release: tokio::sync::watch::Receiver<bool>,
        closed: tokio::sync::mpsc::UnboundedSender<()>,
        closes: Arc<AtomicUsize>,
        done: bool,
        wait: Option<BoxFuture<'static, ()>>,
    }

    impl tokio::io::AsyncRead for Gated {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            if !self.started {
                self.started = true;
                let _ = self.reading.send(());
                let mut rx = self.release.clone();
                self.wait = Some(Box::pin(async move {
                    while !*rx.borrow() {
                        if rx.changed().await.is_err() {
                            break;
                        }
                    }
                }));
            }
            if let Some(w) = self.wait.as_mut() {
                match w.as_mut().poll(cx) {
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                    std::task::Poll::Ready(()) => self.wait = None,
                }
            }
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl ObjectRead for Gated {
        fn close(&mut self) -> io::Result<()> {
            if !self.done {
                self.done = true;
                self.closes.fetch_add(1, Ordering::SeqCst);
                let _ = self.closed.send(());
            }
            Ok(())
        }
    }

    impl Drop for Gated {
        fn drop(&mut self) {
            let _ = self.close();
        }
    }

    struct GatedOpener {
        content: String,
        sized: bool,
        reading: tokio::sync::mpsc::UnboundedSender<()>,
        closed: tokio::sync::mpsc::UnboundedSender<()>,
        release: tokio::sync::watch::Sender<bool>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
    }

    struct GatedRx {
        reading: tokio::sync::mpsc::UnboundedReceiver<()>,
        closed: tokio::sync::mpsc::UnboundedReceiver<()>,
    }

    fn gated_opener(content: &str, sized: bool) -> (Arc<GatedOpener>, GatedRx) {
        let (rt, rr) = tokio::sync::mpsc::unbounded_channel();
        let (ct, cr) = tokio::sync::mpsc::unbounded_channel();
        let (release, _) = tokio::sync::watch::channel(false);
        (
            Arc::new(GatedOpener {
                content: content.into(),
                sized,
                reading: rt,
                closed: ct,
                release,
                opens: Arc::default(),
                closes: Arc::default(),
            }),
            GatedRx {
                reading: rr,
                closed: cr,
            },
        )
    }

    impl GatedOpener {
        fn open(&self) -> BoxFuture<'static, Result<(ObjectReader, i64), Error>> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            let size = if self.sized {
                self.content.len() as i64
            } else {
                -1
            };
            let g = Gated {
                inner: BytesReader::new(self.content.clone().into_bytes()),
                started: false,
                reading: self.reading.clone(),
                release: self.release.subscribe(),
                closed: self.closed.clone(),
                closes: self.closes.clone(),
                done: false,
                wait: None,
            };
            Box::pin(async move { Ok((Box::new(g) as ObjectReader, size)) })
        }
    }

    const MAX: i64 = 60;

    /// Starts a download that reserves MAX and blocks in its first read.
    async fn hold_capacity(
        f: &Arc<Fixture>,
    ) -> (
        Arc<GatedOpener>,
        tokio::task::JoinHandle<Result<(), Status>>,
    ) {
        let (first, mut rx) = gated_opener(KERNEL, false);
        let held = f.new_caller_file("held");
        let (f2, o) = (f.clone(), first.clone());
        let h = tokio::spawn(async move {
            let open = || o.open();
            f2.stager
                .fetch(
                    &CancellationToken::new(),
                    &req(&sum(KERNEL), &held.display().to_string(), MAX),
                    "gs://bucket/held",
                    &open,
                    &test_status,
                )
                .await
        });
        rx.reading.recv().await.unwrap();
        (first, h)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stager_fetch_staging_capacity() {
        // A download waits while the capacity is reserved.
        {
            let f = Arc::new(fixture_with(100));
            let (first, first_done) = hold_capacity(&f).await;
            let (second, mut rx) = gated_opener(KERNEL, false);
            second.release.send_replace(true);
            let b = f.new_caller_file("b");
            let (f2, o) = (f.clone(), second.clone());
            let second_done = tokio::spawn(async move {
                let open = || o.open();
                f2.stager
                    .fetch(
                        &CancellationToken::new(),
                        &req(&sum(KERNEL), &b.display().to_string(), MAX),
                        "gs://bucket/b",
                        &open,
                        &test_status,
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(10), rx.closed.recv())
                .await
                .expect("second download did not close its object to wait for space");
            assert!(
                rx.reading.try_recv().is_err(),
                "second started while the first held the capacity"
            );
            assert_eq!(
                (
                    second.opens.load(Ordering::SeqCst),
                    second.closes.load(Ordering::SeqCst)
                ),
                (1, 1)
            );
            first.release.send_replace(true);
            first_done.await.unwrap().unwrap();
            second_done.await.unwrap().unwrap();
            assert_eq!(
                (
                    second.opens.load(Ordering::SeqCst),
                    second.closes.load(Ordering::SeqCst)
                ),
                (2, 2)
            );
            f.assert_staging_empty();
        }
        // Downloads that fit run together.
        for (name, capacity, sized) in [
            ("max_bytes each", 2 * MAX, false),
            ("reported sizes", MAX + KERNEL.len() as i64, true),
        ] {
            let f = Arc::new(fixture_with(capacity));
            let (o, mut rx) = gated_opener(KERNEL, sized);
            let mut tasks = Vec::new();
            for n in ["a", "b"] {
                let p = f.new_caller_file(n);
                let (f2, o) = (f.clone(), o.clone());
                tasks.push(tokio::spawn(async move {
                    let open = || o.open();
                    f2.stager
                        .fetch(
                            &CancellationToken::new(),
                            &req(&sum(KERNEL), &p.display().to_string(), MAX),
                            "gs://bucket/x",
                            &open,
                            &test_status,
                        )
                        .await
                }));
            }
            for _ in 0..2 {
                tokio::time::timeout(Duration::from_secs(10), rx.reading.recv())
                    .await
                    .unwrap_or_else(|_| {
                        panic!("{name}: two downloads within the capacity did not run together")
                    });
            }
            o.release.send_replace(true);
            for t in tasks {
                t.await.unwrap().unwrap();
            }
        }
        // A missing object does not wait for space.
        {
            let f = Arc::new(fixture_with(MAX));
            let (first, first_done) = hold_capacity(&f).await;
            let o = Opener::failing(ErrorKind::NotFound);
            let r = tokio::time::timeout(
                Duration::from_secs(10),
                run(&f, &req(&sum(KERNEL), &dst(&f), MAX), &o),
            )
            .await
            .expect("fetch of a missing object waited for staging space");
            assert_eq!(r.unwrap_err().code(), tonic::Code::NotFound);
            first.release.send_replace(true);
            first_done.await.unwrap().unwrap();
        }
        // The object changed while waiting.
        {
            let f = Arc::new(fixture_with(MAX));
            let (first, first_done) = hold_capacity(&f).await;
            let opens = Arc::new(AtomicUsize::new(0));
            let (closed_tx, mut closed_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            let contents = [KERNEL.to_string(), format!("{KERNEL} v2")];
            let (f2, ot) = (f.clone(), opens.clone());
            let d = dst(&f);
            let waiting = tokio::spawn(async move {
                let open = || {
                    let i = ot.fetch_add(1, Ordering::SeqCst).min(1);
                    let c = contents[i].clone();
                    let tx = closed_tx.clone();
                    let n = c.len() as i64;
                    Box::pin(async move {
                        let r = crate::storage::StreamObject::new(
                            BytesReader::new(c.into_bytes()),
                            Some(n),
                        )
                        .on_close(move || {
                            let _ = tx.send(());
                        });
                        Ok((Box::new(r) as ObjectReader, n))
                    }) as BoxFuture<'static, _>
                };
                f2.stager
                    .fetch(
                        &CancellationToken::new(),
                        &req(&sum(KERNEL), &d, MAX),
                        "gs://bucket/vmlinux",
                        &open,
                        &test_status,
                    )
                    .await
            });
            closed_rx.recv().await.unwrap();
            first.release.send_replace(true);
            first_done.await.unwrap().unwrap();
            assert_eq!(
                waiting.await.unwrap().unwrap_err().code(),
                tonic::Code::Unavailable
            );
            f.assert_dst(UNTOUCHED);
            f.assert_staging_empty();
            assert!(
                f.stager.try_reserve(MAX).is_some(),
                "space still reserved after the failed fetch"
            );
        }
        // Cancelled while waiting.
        {
            let f = Arc::new(fixture_with(MAX));
            let (first, first_done) = hold_capacity(&f).await;
            let cancel = CancellationToken::new();
            cancel.cancel();
            let o = Opener::new(KERNEL);
            let open = || o.open();
            let e = f
                .stager
                .fetch(
                    &cancel,
                    &req(&sum(KERNEL), &dst(&f), MAX),
                    "gs://bucket/vmlinux",
                    &open,
                    &test_status,
                )
                .await
                .unwrap_err();
            assert_eq!(e.code(), tonic::Code::Cancelled);
            assert_eq!(
                (
                    o.opens.load(Ordering::SeqCst),
                    o.closes.load(Ordering::SeqCst)
                ),
                (1, 1)
            );
            f.assert_dst(UNTOUCHED);
            first.release.send_replace(true);
            first_done.await.unwrap().unwrap();
        }
        // max_bytes above the capacity.
        const CAP: i64 = 16;
        for (name, content, want) in [
            ("asset fits", "k".repeat(CAP as usize), None),
            (
                "asset larger than the capacity",
                "k".repeat(CAP as usize + 10),
                Some(tonic::Code::FailedPrecondition),
            ),
        ] {
            let f = fixture_with(CAP);
            let o = Opener::new(&content);
            let r = run(&f, &req(&sum(&content), &dst(&f), 1 << 20), &o).await;
            match want {
                None => {
                    r.unwrap();
                    f.assert_dst(&content);
                }
                Some(code) => {
                    let e = r.unwrap_err();
                    assert_eq!(e.code(), code, "{name}");
                    assert!(e.message().contains("staging capacity"), "{name}: {e:?}");
                    f.assert_dst(UNTOUCHED);
                }
            }
            f.assert_staging_empty();
        }
    }
}
