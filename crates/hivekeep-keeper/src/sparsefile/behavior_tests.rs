//! Copies of sparse memory-image shapes, refusal of stale destinations, and
//! allocated-size measurement.

use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

use super::*;

fn alloc(p: &Path) -> i64 {
    allocated_bytes(&fs::metadata(p).unwrap())
}

/// A sparse file of `size` bytes with `extents` written at their offsets.
fn sparse_file(p: &Path, size: u64, extents: &[(u64, &[u8])]) {
    let f = File::create(p).unwrap();
    f.set_len(size).unwrap();
    for (off, data) in extents {
        f.write_all_at(data, *off).unwrap();
    }
    f.sync_all().unwrap();
}

/// Incompressible, never-zero bytes.
fn pattern(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251 + 1) as u8).collect()
}

#[test]
fn copy_file_copies_and_rejects_directories() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::write(&src, "checkpoint pages").unwrap();
    let dst = dir.path().join("dst");
    assert_eq!(copy_file(&src, &dst).unwrap(), 16);
    assert_eq!(fs::read(&dst).unwrap(), b"checkpoint pages");
    assert!(copy_file(dir.path(), &dir.path().join("dst2")).is_err());
}

#[test]
fn copy_file_preserves_holes() {
    const SIZE: u64 = 32 << 20;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("memory-ranges");
    sparse_file(
        &src,
        SIZE,
        &[(0, &[0xAB; 4 << 10]), (16 << 20, &[0xCD; 4 << 10])],
    );
    let dst = dir.path().join("copied");
    assert_eq!(copy_file(&src, &dst).unwrap(), SIZE);
    assert!(
        fs::read(&src).unwrap() == fs::read(&dst).unwrap(),
        "copy differs"
    );
    let (sa, da) = (alloc(&src), alloc(&dst));
    if sa >= (SIZE / 2) as i64 {
        eprintln!("skipping: source is not sparse on this filesystem");
        return;
    }
    assert!(
        da <= sa * 4,
        "copy allocated {da} for a {sa}-byte source: holes filled"
    );
}

#[test]
fn copy_file_all_holes() {
    const SIZE: u64 = 8 << 20;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("empty");
    sparse_file(&src, SIZE, &[]);
    let dst = dir.path().join("copied");
    copy_file(&src, &dst).unwrap();
    assert_eq!(fs::metadata(&dst).unwrap().len(), SIZE);
}

/// The userspace fallback: across filesystems the kernel copy declines and
/// extents go through userspace. Needs a second filesystem (/dev/shm).
#[test]
fn copy_file_preserves_holes_across_filesystems() {
    let other = match tempfile::Builder::new()
        .prefix("sparsefile-test-")
        .tempdir_in("/dev/shm")
    {
        Ok(d) => d,
        Err(e) => {
            eprintln!("skipping: no second filesystem available: {e}");
            return;
        }
    };
    const SIZE: u64 = 32 << 20;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("memory-ranges");
    sparse_file(&src, SIZE, &[(8 << 20, &[0xEF; 4 << 10])]);
    if fs::hard_link(&src, other.path().join("probe")).is_ok() {
        eprintln!("skipping: test dirs share a filesystem");
        return;
    }
    let dst = other.path().join("copied");
    copy_file(&src, &dst).unwrap();
    assert!(
        fs::read(&src).unwrap() == fs::read(&dst).unwrap(),
        "copy differs"
    );
    let (sa, da) = (alloc(&src), alloc(&dst));
    if sa >= (SIZE / 2) as i64 {
        eprintln!("skipping: source is not sparse on this filesystem");
        return;
    }
    assert!(
        da <= sa * 4,
        "cross-filesystem copy allocated {da} for {sa}"
    );
}

#[test]
fn copy_rejects_non_empty_destination_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src");
    sparse_file(&src_path, 1 << 20, &[]);
    let dst_path = dir.path().join("dst");
    fs::write(&dst_path, "stale bytes").unwrap();
    let mut src = File::open(&src_path).unwrap();
    let mut dst = OpenOptions::new().write(true).open(&dst_path).unwrap();
    assert!(copy(&mut src, &mut dst).is_err());
    assert_eq!(fs::read(&dst_path).unwrap(), b"stale bytes");
}

#[test]
fn copy_handles_with_source_unlinked() {
    const SIZE: u64 = 16 << 20;
    let marker = [0x42u8; 4 << 10];
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src");
    sparse_file(&src_path, SIZE, &[(4 << 20, &marker)]);
    let mut src = File::open(&src_path).unwrap();
    // Only the handle may matter: the name can be evicted mid-copy.
    fs::remove_file(&src_path).unwrap();
    let dst_path = dir.path().join("dst");
    let mut dst = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&dst_path)
        .unwrap();
    assert_eq!(copy(&mut src, &mut dst).unwrap(), SIZE);
    drop(dst);
    let got = fs::read(&dst_path).unwrap();
    assert_eq!(got.len() as u64, SIZE);
    assert_eq!(&got[4 << 20..(4 << 20) + marker.len()], &marker[..]);
    assert!(
        got[..4 << 20].iter().all(|b| *b == 0),
        "hole region holds data"
    );
}

#[test]
fn allocated_bytes_sparse_image() {
    const APPARENT: u64 = 2 << 30;
    const WRITTEN: usize = 1 << 20;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("memory-ranges");
    let f = File::create(&p).unwrap();
    f.write_all_at(&pattern(WRITTEN), 0).unwrap();
    f.set_len(APPARENT).unwrap();
    drop(f);
    let meta = fs::symlink_metadata(&p).unwrap();
    assert_eq!(meta.len(), APPARENT);
    let got = allocated_bytes(&meta);
    if got >= (APPARENT / 2) as i64 {
        eprintln!("skipping: file is not sparse on this filesystem");
        return;
    }
    assert!(got > 0 && got <= (WRITTEN * 16) as i64, "allocated {got}");
}

#[test]
fn allocated_bytes_dense_image() {
    const SIZE: usize = 64 << 10;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("state.json");
    fs::write(&p, pattern(SIZE)).unwrap();
    let got = allocated_bytes(&fs::symlink_metadata(&p).unwrap());
    assert!(
        got >= SIZE as i64 && got <= 2 * SIZE as i64,
        "allocated {got}"
    );
}

/// The kernel copy path on its own: the userspace fallback would hide a
/// broken syscall from every other test.
#[cfg(target_os = "linux")]
#[test]
fn kernel_copy_range_copies_exactly_the_range() {
    const SIZE: u64 = 1 << 20;
    const OFF: u64 = 64 << 10;
    let payload = vec![0x5Au8; 128 << 10];
    let dir = tempfile::tempdir().unwrap();
    let src = File::create(dir.path().join("src")).unwrap();
    src.set_len(SIZE).unwrap();
    src.write_all_at(&payload, OFF).unwrap();
    src.sync_all().unwrap();
    let src = File::open(dir.path().join("src")).unwrap();
    let dst = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.path().join("dst"))
        .unwrap();
    dst.set_len(SIZE).unwrap();

    let mut pos = OFF;
    let mut remaining = payload.len() as u64;
    while remaining > 0 {
        match kernel_copy_range(&src, &dst, pos, remaining).unwrap() {
            None => {
                eprintln!("skipping: copy_file_range unavailable on this filesystem");
                return;
            }
            Some(n) => {
                assert!(n > 0);
                pos += n;
                remaining -= n;
            }
        }
    }
    let mut got = vec![0u8; payload.len()];
    dst.read_exact_at(&mut got, OFF).unwrap();
    assert_eq!(got, payload);
    let mut head = vec![1u8; OFF as usize];
    dst.read_exact_at(&mut head, 0).unwrap();
    assert!(head.iter().all(|b| *b == 0), "copy wrote outside the range");
}
