//! Hole-preserving file copies, and the disk space a file's data takes.
//!
//! The largest files the keeper copies are guest memory images, which are
//! mostly unallocated. A plain copy reads holes as zeroes and writes them as
//! data, inflating a snapshot to its full logical size on every copy and
//! destroying the sparseness later stages use to tell which parts of guest
//! memory hold anything.

use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// The disk space allocated to a file (blocks times 512), rather than its
/// apparent size: for a sparse memory image the apparent size is the guest
/// memory ceiling, often far more than the data it holds.
pub fn allocated_bytes(meta: &fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (meta.blocks() as i64).saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        meta.len() as i64
    }
}

/// The number of hard links to a file.
pub fn link_count(meta: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        meta.nlink()
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        1
    }
}

/// Copies `src` to a new or truncated `dst`, preserving holes where the
/// filesystem reports them, and returns the logical bytes copied.
pub fn copy_file(src: &Path, dst: &Path) -> io::Result<u64> {
    let meta = fs::metadata(src)?;
    if !meta.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} is not a regular file", src.display()),
        ));
    }
    let mut source = File::open(src)?;
    let mut dest = File::create(dst)?;
    copy_to(&mut source, &mut dest, meta.len())
}

/// Copies the open `src` into the open, empty `dst`, preserving holes where
/// it can. Neither handle is closed and both offsets are clobbered. A
/// non-empty destination is refused: holes are never written, so stale bytes
/// would show through them.
pub fn copy(src: &mut File, dst: &mut File) -> io::Result<u64> {
    let meta = src.metadata()?;
    if !meta.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source is not a regular file",
        ));
    }
    let dlen = dst.metadata()?.len();
    if dlen != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("destination is not empty ({dlen} bytes); stale bytes would show through the copy's holes"),
        ));
    }
    copy_to(src, dst, meta.len())
}

fn copy_to(src: &mut File, dst: &mut File, size: u64) -> io::Result<u64> {
    match copy_sparse(src, dst, size) {
        Ok(()) => return Ok(size),
        Err(SparseError::Unsupported) => {}
        Err(SparseError::Io(e)) => return Err(e),
    }
    // Probing moved the read offset; nothing was written yet.
    src.seek(SeekFrom::Start(0))?;
    dst.seek(SeekFrom::Start(0))?;
    io::copy(src, dst)
}

enum SparseError {
    /// The filesystem cannot report holes; copy densely instead.
    Unsupported,
    Io(io::Error),
}

impl From<io::Error> for SparseError {
    fn from(e: io::Error) -> Self {
        SparseError::Io(e)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod seek {
    use std::fs::File;
    use std::io;
    use std::os::unix::io::AsRawFd;

    /// The next offset at or after `off` holding data (`hole` false) or a
    /// hole (`hole` true). `Ok(None)` means ENXIO: no data past `off`.
    pub fn next(f: &File, off: u64, hole: bool) -> io::Result<Option<u64>> {
        let whence = if hole {
            libc::SEEK_HOLE
        } else {
            libc::SEEK_DATA
        };
        // SAFETY: lseek on a valid descriptor has no memory-safety concerns.
        let r = unsafe { libc::lseek(f.as_raw_fd(), off as libc::off_t, whence) };
        if r < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENXIO) {
                return Ok(None);
            }
            return Err(err);
        }
        Ok(Some(r as u64))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod seek {
    use std::fs::File;
    use std::io;

    pub fn next(_: &File, _: u64, _: bool) -> io::Result<Option<u64>> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "no SEEK_DATA"))
    }
}

/// Copies `len` bytes at `off` in the kernel. `Ok(None)` means the kernel
/// or filesystem declined, so the caller should copy through userspace.
#[cfg(target_os = "linux")]
fn kernel_copy_range(src: &File, dst: &File, off: u64, len: u64) -> io::Result<Option<u64>> {
    use std::os::unix::io::AsRawFd;
    const MAX_KERNEL_COPY: u64 = 1 << 30;
    let len = len.min(MAX_KERNEL_COPY);
    loop {
        let mut roff = off as libc::loff_t;
        let mut woff = off as libc::loff_t;
        // SAFETY: valid descriptors and pointers to live locals.
        let n = unsafe {
            libc::copy_file_range(
                src.as_raw_fd(),
                &mut roff,
                dst.as_raw_fd(),
                &mut woff,
                len as usize,
                0,
            )
        };
        if n > 0 {
            return Ok(Some(n as u64));
        }
        if n == 0 {
            return Ok(None);
        }
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(
                libc::ENOSYS
                | libc::EXDEV
                | libc::EOPNOTSUPP
                | libc::EPERM
                | libc::EINVAL
                | libc::EBADF,
            ) => return Ok(None),
            _ => return Err(err),
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn kernel_copy_range(_: &File, _: &File, _: u64, _: u64) -> io::Result<Option<u64>> {
    Ok(None)
}

fn read_at(f: &mut File, buf: &mut [u8], off: u64) -> io::Result<()> {
    f.seek(SeekFrom::Start(off))?;
    f.read_exact(buf)
}

fn write_at(f: &mut File, buf: &[u8], off: u64) -> io::Result<()> {
    f.seek(SeekFrom::Start(off))?;
    f.write_all(buf)
}

/// Writes only `src`'s populated extents to `dst`, leaving the rest of
/// `dst` unallocated. Reports `Unsupported` before writing anything if the
/// filesystem cannot report holes.
fn copy_sparse(src: &mut File, dst: &mut File, size: u64) -> Result<(), SparseError> {
    match seek::next(src, 0, false) {
        Ok(None) => {
            // One big hole.
            dst.set_len(size)?;
            return Ok(());
        }
        Ok(Some(_)) => {}
        Err(_) => return Err(SparseError::Unsupported),
    }
    dst.set_len(size)?;
    let mut use_kernel = true;
    let mut buf: Vec<u8> = Vec::new();
    let mut off = 0u64;
    while off < size {
        let data = match seek::next(src, off, false)? {
            None => break,
            Some(d) => d,
        };
        if data >= size {
            break;
        }
        let mut hole = seek::next(src, data, true)?.unwrap_or(size);
        if hole <= off {
            return Err(SparseError::Io(io::Error::other(format!(
                "seeking to hole at {data} returned non-advancing offset {hole}"
            ))));
        }
        hole = hole.min(size);
        let mut pos = data;
        while pos < hole {
            if use_kernel {
                if let Some(n) = kernel_copy_range(src, dst, pos, hole - pos)? {
                    pos += n;
                    continue;
                }
                use_kernel = false;
            }
            if buf.is_empty() {
                buf = vec![0u8; 4 << 20];
            }
            let n = (buf.len() as u64).min(hole - pos) as usize;
            read_at(src, &mut buf[..n], pos)?;
            write_at(dst, &buf[..n], pos)?;
            pos += n as u64;
        }
        off = hole;
    }
    Ok(())
}

#[cfg(test)]
mod behavior_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_file_copies_contents() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        fs::write(&src, b"hello world").unwrap();
        assert_eq!(copy_file(&src, &dst).unwrap(), 11);
        assert_eq!(fs::read(&dst).unwrap(), b"hello world");
    }

    #[test]
    fn copy_file_preserves_content_around_holes() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        let mut f = File::create(&src).unwrap();
        f.write_all(b"head").unwrap();
        f.seek(SeekFrom::Start(8 << 20)).unwrap();
        f.write_all(b"tail").unwrap();
        drop(f);
        let n = copy_file(&src, &dst).unwrap();
        assert_eq!(n, (8 << 20) + 4);
        let got = fs::read(&dst).unwrap();
        assert_eq!(&got[..4], b"head");
        assert_eq!(&got[got.len() - 4..], b"tail");
        assert!(got[4..got.len() - 4].iter().all(|b| *b == 0));
        // Only a filesystem that kept the source sparse can keep the copy so.
        if allocated_bytes(&fs::metadata(&src).unwrap()) < 4 << 20 {
            let alloc = allocated_bytes(&fs::metadata(&dst).unwrap());
            assert!(alloc < 4 << 20, "copy allocated {alloc} bytes");
        }
    }

    #[test]
    fn copy_file_all_holes() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        File::create(&src).unwrap().set_len(1 << 20).unwrap();
        assert_eq!(copy_file(&src, &dst).unwrap(), 1 << 20);
        assert_eq!(fs::metadata(&dst).unwrap().len(), 1 << 20);
    }

    #[test]
    fn copy_rejects_non_empty_destination() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        fs::write(&src, b"abc").unwrap();
        fs::write(&dst, b"stale").unwrap();
        let mut s = File::open(&src).unwrap();
        let mut d = fs::OpenOptions::new().write(true).open(&dst).unwrap();
        assert!(copy(&mut s, &mut d).is_err());
    }

    #[test]
    fn copy_handles() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        let dst = dir.path().join("dst");
        fs::write(&src, b"payload").unwrap();
        let mut s = File::open(&src).unwrap();
        let mut d = fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(&dst)
            .unwrap();
        assert_eq!(copy(&mut s, &mut d).unwrap(), 7);
        assert_eq!(fs::read(&dst).unwrap(), b"payload");
    }

    #[test]
    fn allocated_bytes_dense() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("dense");
        fs::write(&p, vec![1u8; 1 << 20]).unwrap();
        let alloc = allocated_bytes(&fs::metadata(&p).unwrap());
        assert!(alloc >= 1 << 20, "dense file allocated only {alloc}");
    }
}
