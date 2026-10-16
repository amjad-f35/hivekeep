//! Completing the memory image of a guest restored on demand.
//!
//! After an on-demand restore the VMM's next snapshot holds only the pages the
//! guest faulted in since; every other page is unchanged from the snapshot it
//! restored from. The complete image is therefore the base with the delta's
//! populated regions overlaid at the same offsets. Both are flat images of one
//! guest, so they have the same size and layout.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};

use tokio_util::sync::CancellationToken;

/// The block size at which zero runs are left as holes.
pub const SPARSE_BLOCK: usize = 4096;
const BUF: usize = 1 << 20;

fn cancelled() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "context canceled")
}

/// The next populated region `[start, end)` at or after `off`, if any.
fn next_data(f: &File, off: u64) -> io::Result<Option<(u64, u64)>> {
    let fd = f.as_raw_fd();
    // SAFETY: lseek on an open descriptor.
    let ds = unsafe { libc::lseek(fd, off as libc::off_t, libc::SEEK_DATA) };
    if ds < 0 {
        let e = io::Error::last_os_error();
        return if e.raw_os_error() == Some(libc::ENXIO) {
            Ok(None)
        } else {
            Err(io::Error::new(e.kind(), format!("SEEK_DATA: {e}")))
        };
    }
    // SAFETY: as above.
    let de = unsafe { libc::lseek(fd, ds, libc::SEEK_HOLE) };
    if de < 0 {
        let e = io::Error::last_os_error();
        return Err(io::Error::new(e.kind(), format!("SEEK_HOLE: {e}")));
    }
    Ok(Some((ds as u64, de as u64)))
}

/// Overwrites `dst` with every populated region of `src` at the same offsets,
/// leaving `dst`'s other bytes alone. Returns the bytes copied.
pub fn copy_sparse_regions(src: &File, dst: &File, cancel: &CancellationToken) -> io::Result<u64> {
    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    let size = src.metadata()?.len();
    let mut buf = vec![0u8; BUF];
    let mut copied = 0;
    let mut off = 0;
    while off < size {
        let Some((ds, de)) = next_data(src, off)? else {
            break;
        };
        let mut pos = ds;
        while pos < de {
            if cancel.is_cancelled() {
                return Err(cancelled());
            }
            let n = ((de - pos) as usize).min(BUF);
            src.read_exact_at(&mut buf[..n], pos)
                .map_err(|e| io::Error::new(e.kind(), format!("reading data region: {e}")))?;
            dst.write_all_at(&buf[..n], pos)?;
            copied += n as u64;
            pos += n as u64;
        }
        off = de;
    }
    Ok(copied)
}

fn all_zero(b: &[u8]) -> bool {
    b.iter().all(|x| *x == 0)
}

/// Writes `p` at `off`, skipping all-zero blocks and coalescing the rest.
fn write_non_zero(dst: &File, p: &[u8], off: u64) -> io::Result<()> {
    let mut start: Option<usize> = None;
    let mut i = 0;
    while i < p.len() {
        let end = (i + SPARSE_BLOCK).min(p.len());
        if !all_zero(&p[i..end]) {
            start.get_or_insert(i);
        } else if let Some(s) = start.take() {
            dst.write_all_at(&p[s..i], off + s as u64)?;
        }
        i = end;
    }
    if let Some(s) = start {
        dst.write_all_at(&p[s..], off + s as u64)?;
    }
    Ok(())
}

/// Creates `dst` as a sparse copy of `src`: holes stay holes, and so does any
/// all-zero block inside a data region.
pub fn copy_sparse_file(src: &Path, dst: &Path, cancel: &CancellationToken) -> io::Result<()> {
    let s = File::open(src)?;
    let size = s.metadata()?.len();
    let d = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(dst)?;
    // A fresh file of the full size reads as zeros, so skipped blocks need
    // no write.
    d.set_len(size)?;
    let mut buf = vec![0u8; BUF];
    let mut off = 0;
    while off < size {
        let Some((ds, de)) = next_data(&s, off)? else {
            break;
        };
        let mut pos = ds;
        while pos < de {
            if cancel.is_cancelled() {
                return Err(cancelled());
            }
            let n = ((de - pos) as usize).min(BUF);
            s.read_exact_at(&mut buf[..n], pos)
                .map_err(|e| io::Error::new(e.kind(), format!("reading data region: {e}")))?;
            write_non_zero(&d, &buf[..n], pos)?;
            pos += n as u64;
        }
        off = de;
    }
    Ok(())
}

fn tmp_path(out: &Path) -> PathBuf {
    let mut s = out.as_os_str().to_os_string();
    s.push(".merge.tmp");
    PathBuf::from(s)
}

/// Writes `out`: a sparse copy of `base` with `delta`'s populated regions
/// overlaid. `out` may be `delta` itself or a fresh path.
///
/// No fsync: the keeper ships the image (the durability point), and a partial
/// local file after a crash is discarded and the suspend retried. The old
/// `out` is unlinked before the rename, since renaming over an existing file
/// makes ext4 write back the whole image synchronously.
pub fn merge_sparse_overlay(
    base: &Path,
    delta: &Path,
    out: &Path,
    cancel: &CancellationToken,
) -> io::Result<()> {
    let bsize = std::fs::metadata(base)
        .map_err(|e| io::Error::new(e.kind(), format!("stat base {}: {e}", base.display())))?
        .len();
    let tmp = tmp_path(out);
    let _ = std::fs::remove_file(&tmp);
    copy_sparse_file(base, &tmp, cancel)
        .map_err(|e| io::Error::new(e.kind(), format!("sparse copy base->tmp: {e}")))?;
    let d = File::open(delta)
        .map_err(|e| io::Error::new(e.kind(), format!("open delta {}: {e}", delta.display())))?;
    let dsize = d.metadata()?.len();
    if dsize != bsize {
        let _ = std::fs::remove_file(&tmp);
        return Err(io::Error::other(format!(
            "merge: size mismatch base={bsize} delta={dsize}"
        )));
    }
    let o = OpenOptions::new().read(true).write(true).open(&tmp)?;
    copy_sparse_regions(&d, &o, cancel)?;
    drop(o);
    match std::fs::remove_file(out) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(io::Error::new(e.kind(), format!("remove old out: {e}"))),
    }
    std::fs::rename(&tmp, out)
}

/// The same result as [`merge_sparse_overlay`] with `out = delta`, without
/// copying the base: the delta is overlaid onto the base in place and the base
/// hard-linked at the delta's name. The base is the per-Actor restore staging
/// file, discarded afterwards, and it stays linked where it was so a retry
/// after a failed sibling step can merge into it again. A base with a second
/// link (a cached snapshot behind it) or on another filesystem falls back to
/// the copying merge.
pub fn merge_delta_into_base(
    base: &Path,
    delta: &Path,
    cancel: &CancellationToken,
) -> io::Result<()> {
    let bi = std::fs::metadata(base)
        .map_err(|e| io::Error::new(e.kind(), format!("stat base {}: {e}", base.display())))?;
    let di = std::fs::metadata(delta)
        .map_err(|e| io::Error::new(e.kind(), format!("stat delta {}: {e}", delta.display())))?;
    if bi.len() != di.len() {
        return Err(io::Error::other(format!(
            "merge: size mismatch base={} delta={}",
            bi.len(),
            di.len()
        )));
    }
    if bi.nlink() > 1 || bi.dev() != di.dev() {
        return merge_sparse_overlay(base, delta, delta, cancel);
    }
    {
        let d = File::open(delta).map_err(|e| {
            io::Error::new(e.kind(), format!("open delta {}: {e}", delta.display()))
        })?;
        let b = OpenOptions::new().read(true).write(true).open(base)?;
        copy_sparse_regions(&d, &b, cancel)?;
    }
    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    std::fs::remove_file(delta)
        .map_err(|e| io::Error::new(e.kind(), format!("remove old delta: {e}")))?;
    std::fs::hard_link(base, delta)
}

// The merge relies on the filesystem reporting holes at block granularity,
// as Linux filesystems do; other platforms report coarser data regions.
#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    fn fill(seed: usize, n: usize) -> Vec<u8> {
        (0..n).map(|i| ((i + seed) % 251 + 1) as u8).collect()
    }

    fn write_sparse(path: &Path, size: u64, regions: &[(u64, Vec<u8>)]) {
        let f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .unwrap();
        f.set_len(size).unwrap();
        for (off, data) in regions {
            f.write_all_at(data, *off).unwrap();
        }
        f.sync_all().unwrap();
    }

    fn expected(size: u64, layers: &[&[(u64, Vec<u8>)]]) -> Vec<u8> {
        let mut want = vec![0u8; size as usize];
        for l in layers {
            for (off, d) in l.iter() {
                want[*off as usize..*off as usize + d.len()].copy_from_slice(d);
            }
        }
        want
    }

    #[test]
    fn merges_in_place_and_agrees_with_the_copy() {
        let size = 8u64 << 20;
        let base_r = vec![
            (0, fill(1, 4096)),
            (1 << 20, fill(2, 70000)),
            (size - 8192, fill(3, 4096)),
        ];
        let delta_r = vec![(1 << 20, fill(99, 70000)), (4 << 20, fill(42, 12345))];
        let want = expected(size, &[&base_r, &delta_r]);
        let d = tempfile::tempdir().unwrap();
        for sub in ["restore-state", "checkpoint-state"] {
            std::fs::create_dir(d.path().join(sub)).unwrap();
        }
        let base = d.path().join("restore-state/memory-ranges");
        let delta = d.path().join("checkpoint-state/memory-ranges");
        write_sparse(&base, size, &base_r);
        write_sparse(&delta, size, &delta_r);
        let c = CancellationToken::new();
        merge_delta_into_base(&base, &delta, &c).unwrap();
        let got = std::fs::read(&delta).unwrap();
        assert!(got == want, "merged image differs");
        assert_eq!(
            std::fs::metadata(&base).unwrap().ino(),
            std::fs::metadata(&delta).unwrap().ino()
        );

        let r = tempfile::tempdir().unwrap();
        let (rb, rd) = (r.path().join("base"), r.path().join("delta"));
        write_sparse(&rb, size, &base_r);
        write_sparse(&rd, size, &delta_r);
        merge_sparse_overlay(&rb, &rd, &rd, &c).unwrap();
        assert!(
            std::fs::read(&rd).unwrap() == got,
            "the two merges disagree"
        );
    }

    #[test]
    fn overlay_writes_a_new_out_file() {
        let size = 8u64 << 20;
        let base_r = vec![(0, fill(1, 4096))];
        let delta_r = vec![(4 << 20, fill(42, 12345))];
        let d = tempfile::tempdir().unwrap();
        let (b, dl, out) = (
            d.path().join("base"),
            d.path().join("delta"),
            d.path().join("out"),
        );
        write_sparse(&b, size, &base_r);
        write_sparse(&dl, size, &delta_r);
        merge_sparse_overlay(&b, &dl, &out, &CancellationToken::new()).unwrap();
        assert!(std::fs::read(&out).unwrap() == expected(size, &[&base_r, &delta_r]));
    }

    #[test]
    fn a_hardlinked_base_is_copied_not_mutated() {
        let size = 8u64 << 20;
        let base_r = vec![(0, fill(1, 4096))];
        let delta_r = vec![(4 << 20, fill(42, 12345))];
        let d = tempfile::tempdir().unwrap();
        let (b, dl, cached) = (
            d.path().join("base"),
            d.path().join("delta"),
            d.path().join("cached-snapshot"),
        );
        write_sparse(&b, size, &base_r);
        write_sparse(&dl, size, &delta_r);
        std::fs::hard_link(&b, &cached).unwrap();
        merge_delta_into_base(&b, &dl, &CancellationToken::new()).unwrap();
        assert!(std::fs::read(&dl).unwrap() == expected(size, &[&base_r, &delta_r]));
        assert!(
            std::fs::read(&cached).unwrap() == expected(size, &[&base_r]),
            "the cached snapshot was corrupted"
        );
        assert!(b.exists());
    }

    #[test]
    fn refuses_mismatched_sizes() {
        let d = tempfile::tempdir().unwrap();
        let (b, dl) = (d.path().join("base"), d.path().join("delta"));
        write_sparse(&b, 1 << 20, &[(0, fill(1, 4096))]);
        write_sparse(&dl, 2 << 20, &[(0, fill(2, 4096))]);
        assert!(merge_delta_into_base(&b, &dl, &CancellationToken::new()).is_err());
        assert!(b.exists());
    }

    #[test]
    fn zero_blocks_stay_holes() {
        let d = tempfile::tempdir().unwrap();
        let mut data = vec![0u8; 1 << 20];
        data[512 << 10..(512 << 10) + 4096].fill(0xab);
        let (src, dst) = (d.path().join("src"), d.path().join("dst"));
        std::fs::write(&src, &data).unwrap();
        copy_sparse_file(&src, &dst, &CancellationToken::new()).unwrap();
        assert!(std::fs::read(&dst).unwrap() == data);
        let allocated = std::fs::metadata(&dst).unwrap().blocks() * 512;
        assert!(allocated <= 64 << 10, "dst allocates {allocated} bytes");
    }

    #[test]
    fn survives_a_cancelled_attempt_and_a_wiped_checkpoint_dir() {
        let size = 4u64 << 20;
        let base_r = vec![(0, fill(1, 4096)), (1 << 20, fill(2, 65536))];
        let delta_r = vec![(1 << 20, fill(99, 65536)), (2 << 20, fill(42, 8192))];
        let want = expected(size, &[&base_r, &delta_r]);
        let d = tempfile::tempdir().unwrap();
        let (rs, cs) = (
            d.path().join("restore-state"),
            d.path().join("checkpoint-state"),
        );
        std::fs::create_dir(&rs).unwrap();
        std::fs::create_dir(&cs).unwrap();
        let (base, delta) = (rs.join("memory-ranges"), cs.join("memory-ranges"));
        write_sparse(&base, size, &base_r);
        write_sparse(&delta, size, &delta_r);
        write_sparse(&delta, size, &[]);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(merge_delta_into_base(&base, &delta, &cancelled).is_err());
        write_sparse(&delta, size, &delta_r);
        assert!(merge_delta_into_base(&base, &delta, &cancelled).is_err());
        write_sparse(&delta, size, &delta_r);
        let live = CancellationToken::new();
        merge_delta_into_base(&base, &delta, &live).unwrap();
        // A sibling step fails and the retry wipes the checkpoint dir.
        std::fs::remove_dir_all(&cs).unwrap();
        std::fs::create_dir(&cs).unwrap();
        write_sparse(&delta, size, &delta_r);
        merge_delta_into_base(&base, &delta, &live).unwrap();
        assert!(std::fs::read(&delta).unwrap() == want);
        assert_eq!(
            std::fs::metadata(&base).unwrap().ino(),
            std::fs::metadata(&delta).unwrap().ino()
        );
    }
}
